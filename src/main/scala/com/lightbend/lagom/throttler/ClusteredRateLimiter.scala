package com.lightbend.lagom.throttler

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicReference

import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props}
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings}
import akka.util.Timeout
import com.lightbend.lagom.scaladsl.playjson.{JsonSerializer, JsonSerializerRegistry}
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
  private val delegateSyncActor = actorSystem.actorOf(Props(new DelegateSyncActor()))

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
      delegateSyncActor ! SyncPermitsCommand(used)
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

  private class DelegateSyncActor extends Actor {
    private lazy val semaphoreCoordinator: ActorRef = ClusterInMemoryRateLimiterSemaphore.clusterRateLimiterActorProxy(rateLimiterName)
    override def receive: Receive = {
      case cmd: SyncPermitsCommand       => semaphoreCoordinator ! cmd
      case ReservedPermitsReply(permits) => reservedPermits.set(permits)
    }
  }
}

private[lagom] case class ReservedPermits(validTillMills: Long, permitsCount: Int)
private[lagom] case class UsedPermit(usedTimeMills: Long)

object ClusteredRateLimiter extends JsonSerializerRegistry {
  implicit val f1: Format[ReservedPermits] = Json.format
  implicit val f2: Format[UsedPermit] = Json.format
  implicit val f3: Format[SyncPermitsCommand] = Json.format
  implicit val f4: Format[ReservedPermitsReply] = Json.format

  override val serializers = Vector(
    JsonSerializer[ReservedPermits],
    JsonSerializer[UsedPermit],
    JsonSerializer[SyncPermitsCommand],
    JsonSerializer[ReservedPermitsReply],
  )
}
