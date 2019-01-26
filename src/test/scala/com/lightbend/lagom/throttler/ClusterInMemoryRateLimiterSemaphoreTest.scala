package com.lightbend.lagom.throttler
import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.lightbend.lagom.throttler.ClusterInMemoryRateLimiterSemaphore.{ReservedPermitsReply, SyncPermitsCommand}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._

class ClusterInMemoryRateLimiterSemaphoreTest extends TestKit(ActorSystem("test-actor-system")) with WordSpecLike with ImplicitSender with Matchers with BeforeAndAfterAll {
  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "An rate limit semaphore actor" must {

    "reply no permits when 0 max allowed" in {
      val semaphore = system.actorOf(
        ClusterInMemoryRateLimiterSemaphore.props(duration = 1.minute, maxInvocation = 0, syncsPerDuration = 60)
      )
      semaphore ! SyncPermitsCommand(usedPermits = Seq.empty)
      expectMsgPF() { case ReservedPermitsReply(permits) if permits.permitsCount == 0 => () }
    }

    "reply with all permits for first requested" in {
      val semaphore = system.actorOf(
        ClusterInMemoryRateLimiterSemaphore.props(duration = 1.minute, maxInvocation = 10, syncsPerDuration = 60)
      )
      semaphore ! SyncPermitsCommand(usedPermits = Seq.empty)
      expectMsgPF() { case ReservedPermitsReply(permits) if permits.permitsCount == 10 => () }
    }

    "reply with left permits after sync with used" in {
      val semaphore = system.actorOf(
        ClusterInMemoryRateLimiterSemaphore.props(duration = 1.minute, maxInvocation = 10, syncsPerDuration = 60)
      )
      semaphore ! SyncPermitsCommand(usedPermits = Seq.empty)
      semaphore ! SyncPermitsCommand(
        usedPermits = Seq(
          UsedPermit(System.currentTimeMillis()),
          UsedPermit(System.currentTimeMillis()),
        )
      )
      expectMsgPF() { case ReservedPermitsReply(permits) if permits.permitsCount == 10 => () }
      expectMsgPF() { case ReservedPermitsReply(permits) if permits.permitsCount == 8 => () }
    }

    "correctly do window sliding and not track permits that out of slide" in {
      val semaphore = system.actorOf(
        ClusterInMemoryRateLimiterSemaphore.props(duration = 1.minute, maxInvocation = 10, syncsPerDuration = 60)
      )
      semaphore ! SyncPermitsCommand(usedPermits = Seq.empty)
      semaphore ! SyncPermitsCommand(
        usedPermits = Seq(
          UsedPermit(System.currentTimeMillis()),
          UsedPermit(System.currentTimeMillis() - 1.minute.toMillis - 1),
          UsedPermit(System.currentTimeMillis() - 1.minute.toMillis - 2),
          UsedPermit(System.currentTimeMillis() - 1.minute.toMillis - 3)
        )
      )
      expectMsgPF() { case ReservedPermitsReply(permits) if permits.permitsCount == 10 => () }
      expectMsgPF() { case ReservedPermitsReply(permits) if permits.permitsCount == 9 => () }
    }

    "split permits to multiple consumers" in {
      val semaphore = system.actorOf(
        ClusterInMemoryRateLimiterSemaphore.props(duration = 1.minute, maxInvocation = 9, syncsPerDuration = 60)
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
      semaphore.tell(SyncPermitsCommand(usedPermits = Seq.empty), consumer2.ref)
      semaphore.tell(SyncPermitsCommand(usedPermits = Seq.empty), consumer3.ref)

      consumer1.expectMsgPF() { case ReservedPermitsReply(permits) if permits.permitsCount == 9 => () }
      consumer2.expectMsgPF() { case ReservedPermitsReply(permits) if permits.permitsCount == 0 => () }
      consumer3.expectMsgPF() { case ReservedPermitsReply(permits) if permits.permitsCount == 0 => () }
      consumer1.expectMsgPF() { case ReservedPermitsReply(permits) if permits.permitsCount == 3 => () }
      consumer2.expectMsgPF() { case ReservedPermitsReply(permits) if permits.permitsCount == 3 => () }
      consumer3.expectMsgPF() { case ReservedPermitsReply(permits) if permits.permitsCount == 3 => () }
    }

  }

}
