package com.lightbend.lagom.throttler
import java.lang.System.currentTimeMillis

import akka.Done
import akka.actor.ActorSystem
import org.scalatest._

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{Future, Promise}

class ClusteredRateLimiterTest extends AsyncWordSpec with Matchers with BeforeAndAfterAll {
  implicit private val system = ActorSystem("test-actor-system")

  override def afterAll: Unit = {
    system.terminate()
  }

  "An cluster rate limiter" should {

    "not exceed configured limits" in {
      val rateLimitSeconds = 1.second
      val maxPerSecond = 3
      val stressSeconds = 2
      val totalSend = maxPerSecond * stressSeconds
      val rateLimiter = new ClusteredRateLimiter("limiter", rateLimitSeconds, maxPerSecond, syncsPerDuration = 2)
      val testService = new TestService()
      val allSend = Future.sequence(for { _ <- 1 to totalSend } yield rateLimiter { testService.query })
      awaitSuccess((stressSeconds + 1).second) {
        for {
          _ <- allSend
        } yield {
          testService.queryHistory.size shouldBe totalSend
          testService.slideInPermitLimits(rateLimitSeconds, maxPerSecond) shouldBe true
        }
      }
    }

    "work correctly for multiple different names limiters" in {
      val rateLimitSeconds = 1.second
      val maxPerSecond = 3
      val stressSeconds = 2
      val totalSend = maxPerSecond * stressSeconds
      val rateLimiter1 = new ClusteredRateLimiter("limiter-1", rateLimitSeconds, maxPerSecond, syncsPerDuration = 2)
      val rateLimiter2 = new ClusteredRateLimiter("limiter-2", rateLimitSeconds, maxPerSecond, syncsPerDuration = 2)
      val testService1 = new TestService()
      val testService2 = new TestService()
      val allSend1 = Future.sequence(for { _ <- 1 to totalSend } yield rateLimiter1 { testService1.query })
      val allSend2 = Future.sequence(for { _ <- 1 to totalSend } yield rateLimiter2 { testService2.query })
      awaitSuccess((stressSeconds + 1).second) {
        for {
          _ <- allSend1
          _ <- allSend2
        } yield {
          testService1.queryHistory.size shouldBe totalSend
          testService2.queryHistory.size shouldBe totalSend
          testService1.slideInPermitLimits(rateLimitSeconds, maxPerSecond) shouldBe true
          testService2.slideInPermitLimits(rateLimitSeconds, maxPerSecond) shouldBe true
        }
      }
    }

  }

  private class TestService() {
    def slideInPermitLimits(duration: FiniteDuration, maxInDuration: Int): Boolean = {
      val durationMills = duration.toMillis
      def isDurationSlideValid(eventTimes: Seq[Long], chunkEndMills: Long): Boolean = {
        eventTimes match {
          case Nil => true
          case Seq(x, xs @ _*) =>
            val slideValid = maxInDuration >= eventTimes.count(_ < chunkEndMills)
            slideValid && isDurationSlideValid(xs, x + durationMills)
        }
      }
      isDurationSlideValid(queryHistory, queryHistory.headOption.map(_ + durationMills).getOrElse(currentTimeMillis()))
    }

    val queryHistory = ListBuffer.empty[Long]
    def query: Future[Done] = {
      queryHistory += currentTimeMillis()
      Future.successful(Done)
    }
  }

  def awaitSuccess[T](maxDuration: FiniteDuration, checkEvery: FiniteDuration = 1.milli)(block: => Future[T])(implicit actorSystem: ActorSystem): Future[T] = {
    val checkUntil = currentTimeMillis() + maxDuration.toMillis

    def doCheck(): Future[T] = {
      block.recoverWith {
        case _ if checkUntil > currentTimeMillis() =>
          val timeout = Promise[T]()
          actorSystem.scheduler.scheduleOnce(checkEvery) {
            timeout.completeWith(doCheck())
          }(actorSystem.dispatcher)
          timeout.future
        case _ => throw new AssertionError("failed await success")
      }
    }
    doCheck()
  }
}
