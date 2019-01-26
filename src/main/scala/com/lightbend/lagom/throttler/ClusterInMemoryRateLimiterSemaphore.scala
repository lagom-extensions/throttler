package com.lightbend.lagom.throttler

import java.lang.System.currentTimeMillis

import akka.actor.{Actor, ActorLogging, ActorPath, ActorRef, ActorSystem, Props}
import akka.cluster.singleton.{ClusterSingletonProxy, ClusterSingletonProxySettings}
import com.lightbend.lagom.throttler.ClusterInMemoryRateLimiterSemaphore.{ReservedPermitsReply, SyncPermitsCommand}
import org.apache.commons.collections4.map.PassiveExpiringMap
import org.apache.commons.collections4.map.PassiveExpiringMap.ConstantTimeToLiveExpirationPolicy

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

private[throttler] class ClusterInMemoryRateLimiterSemaphore(duration: FiniteDuration, maxInvocation: Int, syncsPerDuration: Int) extends Actor with ActorLogging {

  private val oneSyncInterval = duration.div(syncsPerDuration.toLong).toMillis
  private val windowSlideDurationMills = duration.toMillis
  private val consumersInChunk = new PassiveExpiringMap(new ConstantTimeToLiveExpirationPolicy[ActorPath, Null](oneSyncInterval))
  private val confirmedUsedPermits = new PassiveExpiringMap(new ConstantTimeToLiveExpirationPolicy[Long, Seq[UsedPermit]](windowSlideDurationMills))
  private val reservedPermits = new PassiveExpiringMap(new ConstantTimeToLiveExpirationPolicy[ActorPath, ReservedPermits](windowSlideDurationMills))

  override def receive: Receive = {
    case SyncPermitsCommand(usedPermits) =>
      confirmedUsedPermits.put(currentTimeMillis, usedPermits)
      consumersInChunk.put(sender().path, null)
      reservedPermits.remove(sender().path)
      val consumerReservations = makeReservationPermits
      reservedPermits.put(sender().path, consumerReservations)
      sender() ! ReservedPermitsReply(consumerReservations)
  }

  private def makeReservationPermits: ReservedPermits = {
    val usedPermits = calcWindowCountingUsedPermits
    val reservedPermitsCount = calcReservedPermits
    val totalAllowedPermits = maxInvocation - usedPermits - reservedPermitsCount
    val approxConsumers = {
      val size = consumersInChunk.size()
      if (size == 0) 1 else size
    }
    val consumersThatReservePermitsInChunk = reservedPermits.asScala.values.count(_.permitsCount > 0)
    val permitsDivider = {
      val res = approxConsumers - consumersThatReservePermitsInChunk
      if (res <= 0) 1 else res
    }
    val consumerAllowedPermits = totalAllowedPermits / permitsDivider
    val validTill = oneSyncInterval + currentTimeMillis
    ReservedPermits(validTill, consumerAllowedPermits.intValue())
  }

  private def calcWindowCountingUsedPermits: Long = {
    val windowFrom = currentTimeMillis - windowSlideDurationMills
    var used = 0L
    confirmedUsedPermits.asScala.foreach {
      case (_, reservations) =>
        used += reservations.count(_.usedTimeMills > windowFrom)
    }
    used
  }

  private def calcReservedPermits: Long = reservedPermits.asScala.mapValues(_.permitsCount.toLong).values.sum

}

private[lagom] object ClusterInMemoryRateLimiterSemaphore {
  def props(duration: FiniteDuration, maxInvocation: Int, syncsPerDuration: Int): Props =
    Props(new ClusterInMemoryRateLimiterSemaphore(duration, maxInvocation, syncsPerDuration))
  // lagom don't comes with akka typed now
  case class SyncPermitsCommand(usedPermits: Seq[UsedPermit])
  case class ReservedPermitsReply(permits: ReservedPermits)

  private[lagom] def clusterRateLimiterActorProxy(clusterSingletonManagerName: String)(implicit actorSystem: ActorSystem): ActorRef =
    actorSystem.actorOf(
      ClusterSingletonProxy.props(
        singletonManagerPath = s"/user/$clusterSingletonManagerName",
        settings = ClusterSingletonProxySettings(actorSystem)
      )
    )
}
