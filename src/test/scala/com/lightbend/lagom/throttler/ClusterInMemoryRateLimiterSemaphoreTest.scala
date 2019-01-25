package com.lightbend.lagom.throttler
import java.util.concurrent.TimeUnit.MINUTES

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.lightbend.lagom.throttler.ClusterInMemoryRateLimiterSemaphore.{ReservedPermitsReply, SyncPermitsCommand}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration.FiniteDuration

class ClusterInMemoryRateLimiterSemaphoreTest extends TestKit(ActorSystem("test")) with WordSpecLike with ImplicitSender with Matchers with BeforeAndAfterAll {
  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "An rate limit semaphore actor" must {

    "reply no permits when 0 max allowed" in {
      val semaphore = system.actorOf(
        ClusterInMemoryRateLimiterSemaphore.props(duration = FiniteDuration(1, MINUTES), maxInvocation = 0, syncsPerDuration = 60)
      )
      semaphore ! SyncPermitsCommand(usedPermits = Seq.empty)
      expectMsg(ReservedPermitsReply(Seq.empty))
    }

    "reply with all permits for first requested" in {
      val semaphore = system.actorOf(
        ClusterInMemoryRateLimiterSemaphore.props(duration = FiniteDuration(1, MINUTES), maxInvocation = 10, syncsPerDuration = 60)
      )
      semaphore ! SyncPermitsCommand(usedPermits = Seq.empty)
      expectMsgPF() { case ReservedPermitsReply(permits) if permits.size == 10 => () }
    }

    "reply with left permits after sync with used" in {
      val semaphore = system.actorOf(
        ClusterInMemoryRateLimiterSemaphore.props(duration = FiniteDuration(1, MINUTES), maxInvocation = 10, syncsPerDuration = 60)
      )
      semaphore ! SyncPermitsCommand(usedPermits = Seq.empty)
      semaphore ! SyncPermitsCommand(
        usedPermits = Seq(
          UsedPermit(System.currentTimeMillis()),
          UsedPermit(System.currentTimeMillis()),
        )
      )
      expectMsgPF() { case ReservedPermitsReply(permits) if permits.size == 10 => () }
      expectMsgPF() { case ReservedPermitsReply(permits) if permits.size == 8  => () }
    }

    "correctly do window sliding and not track permits that out of slide" in {
      val semaphore = system.actorOf(
        ClusterInMemoryRateLimiterSemaphore.props(duration = FiniteDuration(1, MINUTES), maxInvocation = 10, syncsPerDuration = 60)
      )
      semaphore ! SyncPermitsCommand(usedPermits = Seq.empty)
      semaphore ! SyncPermitsCommand(
        usedPermits = Seq(
          UsedPermit(System.currentTimeMillis()),
          UsedPermit(System.currentTimeMillis() - FiniteDuration(1, MINUTES).toMillis - 1),
          UsedPermit(System.currentTimeMillis() - FiniteDuration(1, MINUTES).toMillis - 2),
          UsedPermit(System.currentTimeMillis() - FiniteDuration(1, MINUTES).toMillis - 3)
        )
      )
      expectMsgPF() { case ReservedPermitsReply(permits) if permits.size == 10 => () }
      expectMsgPF() { case ReservedPermitsReply(permits) if permits.size == 9  => () }
    }

    "split permits to multiple consumers" in {
      val semaphore = system.actorOf(
        ClusterInMemoryRateLimiterSemaphore.props(duration = FiniteDuration(1, MINUTES), maxInvocation = 9, syncsPerDuration = 60)
      )

      val consumer1 = TestProbe()
      val consumer2 = TestProbe()
      val consumer3 = TestProbe()

      // first will get all
      semaphore.tell(SyncPermitsCommand(usedPermits = Seq.empty), consumer1.ref)
      // get none
      semaphore.tell(SyncPermitsCommand(usedPermits = Seq.empty), consumer2.ref)
      // get none
      semaphore.tell(SyncPermitsCommand(usedPermits = Seq.empty), consumer3.ref)

      // sync on first, release all previous and should get as fair divide permits
      semaphore.tell(SyncPermitsCommand(usedPermits = Seq.empty), consumer1.ref)
      consumer1.expectMsgPF() { case ReservedPermitsReply(permits) if permits.size == 9 => () }
      consumer2.expectMsg(ReservedPermitsReply(Seq.empty))
      consumer3.expectMsg(ReservedPermitsReply(Seq.empty))
      consumer1.expectMsgPF() { case ReservedPermitsReply(permits) if permits.size == 3 => () }
    }

  }

}
