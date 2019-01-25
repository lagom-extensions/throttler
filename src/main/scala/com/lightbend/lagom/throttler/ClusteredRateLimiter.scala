package com.lightbend.lagom.throttler

import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}

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
    syncsPerDuration: Int = 100
)(implicit ec: ExecutionContext, actorSystem: ActorSystem) {
  private val queue = new ConcurrentLinkedQueue[() => Any]()
  private val syncDuration = duration / syncsPerDuration.toLong
  implicit private val timeout: Timeout = Timeout(syncDuration / 2)
  private lazy val semaphoreCoordinator: ActorRef = ClusterInMemoryRateLimiterSemaphore.clusterRateLimiterActorProxy(rateLimiterName)

  private val reservedPermits: ListBuffer[ReservedPermit] = ListBuffer.empty
  private val usedPermits: ListBuffer[UsedPermit] = ListBuffer.empty

  actorSystem.scheduler.schedule(syncDuration, syncDuration) {
    this synchronized {
      @tailrec
      def mayBePoll(): Unit =
        if (!queue.isEmpty) {
          val permitUsed = getPermit match {
            case Some((currentTime, _)) =>
              Option(queue.poll()).exists { fun =>
                usedPermits += UsedPermit(currentTime)
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

  private def getPermit: Option[(Long, ReservedPermit)] = {
    this synchronized {
      val currentTime = System.currentTimeMillis()
      val res = reservedPermits
        .find(_.validTillMills > currentTime)
        .map((currentTime, _))
      res.foreach(reservedPermits -= _._2)
      res
    }
  }

  actorSystem.scheduler.schedule(FiniteDuration(1, TimeUnit.NANOSECONDS), syncDuration) {
    this synchronized {
      reservedPermits.clear()
      val used = usedPermits.toList
      usedPermits.clear()
      semaphoreCoordinator.ask(SyncPermitsCommand(used)).map {
        case ReservedPermitsReply(permits) => reservedPermits ++= permits
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
        case Some((currentTime, _)) =>
          usedPermits += UsedPermit(currentTime)
          f
        case None =>
          val res = Promise[T]()
          queue.add(() => { res.completeWith(f) })
          res.future
      }
    }
}

private[lagom] case class ReservedPermit(validTillMills: Long)
object ReservedPermit {
  implicit val format: Format[ReservedPermit] = Json.format
}
private[lagom] case class UsedPermit(usedTimeMills: Long)
object UsedPermit {
  implicit val format: Format[UsedPermit] = Json.format
}
