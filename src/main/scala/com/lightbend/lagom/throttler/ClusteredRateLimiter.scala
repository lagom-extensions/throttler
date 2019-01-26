package com.lightbend.lagom.throttler

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicReference

import akka.actor.{ActorRef, ActorSystem, PoisonPill}
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings}
import akka.pattern.ask
import akka.util.Timeout
import com.lightbend.lagom.throttler.ClusterInMemoryRateLimiterSemaphore.{ReservedPermitsReply, SyncPermitsCommand}
import play.api.libs.json.{Format, Json}

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}

class ClusteredRateLimiter(
    rateLimiterName: String,
    duration: FiniteDuration,
    maxInvocation: Int,
    syncsPerDuration: Int
)(implicit ec: ExecutionContext, actorSystem: ActorSystem) {
  private val queue = new ConcurrentLinkedQueue[() => Any]()
  private val syncDuration = duration / syncsPerDuration.toLong
  implicit private val timeout: Timeout = Timeout(syncDuration / 2)
  private lazy val semaphoreCoordinator: ActorRef = ClusterInMemoryRateLimiterSemaphore.clusterRateLimiterActorProxy(rateLimiterName)

  private val reservedPermits: AtomicReference[ReservedPermits] = new AtomicReference[ReservedPermits]()
  private val usedPermits: ListBuffer[UsedPermit] = ListBuffer.empty

  actorSystem.scheduler.schedule(syncDuration, syncDuration) {
    this synchronized {
      @tailrec
      def mayBePoll(): Unit =
        if (!queue.isEmpty) {
          val permitUsed = getPermit match {
            case Some(usedPermit) =>
              Option(queue.poll()).exists { fun =>
                usedPermits += usedPermit
                fun.apply()
                true
              }
            case None =>
              false
          }
          if (permitUsed) mayBePoll() else ()
        }
      mayBePoll()
    }
  }

  private def getPermit: Option[UsedPermit] = {
    this synchronized {
      val currentTime = System.currentTimeMillis()
      Option(reservedPermits.get) match {
        case Some(permits) if (permits.permitsCount > 0) && (permits.validTillMills > currentTime) =>
          reservedPermits.set(permits.copy(permitsCount = permits.permitsCount - 1))
          Some(UsedPermit(currentTime))
        case _ => None
      }
    }
  }

  actorSystem.scheduler.schedule(1.nano, syncDuration) {
    this synchronized {
      reservedPermits.set(null)
      val used = usedPermits.toList
      usedPermits.clear()
      semaphoreCoordinator.ask(SyncPermitsCommand(used)).map {
        case ReservedPermitsReply(permits) => reservedPermits.set(permits)
      }
    }
  }

  actorSystem.actorOf(
    ClusterSingletonManager.props(
      singletonProps = ClusterInMemoryRateLimiterSemaphore.props(duration, maxInvocation, syncsPerDuration),
      terminationMessage = PoisonPill,
      settings = ClusterSingletonManagerSettings(actorSystem)
    ),
    name = rateLimiterName
  )

  def apply[T](f: => Future[T]): Future[T] =
    this synchronized {
      getPermit match {
        case Some(usedPermit) =>
          usedPermits += usedPermit
          f
        case None =>
          val res = Promise[T]()
          queue.add(() => { res.completeWith(f) })
          res.future
      }
    }
}

private[lagom] case class ReservedPermits(validTillMills: Long, permitsCount: Int)
object ReservedPermits {
  implicit val format: Format[ReservedPermits] = Json.format
}
private[lagom] case class UsedPermit(usedTimeMills: Long)
object UsedPermit {
  implicit val format: Format[UsedPermit] = Json.format
}
