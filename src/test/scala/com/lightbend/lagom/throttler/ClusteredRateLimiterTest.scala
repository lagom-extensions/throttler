package com.lightbend.lagom.throttler
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
    // TODO check that data in test is filled with not more than expected in window chunk
    // todo one more test that one limiter don't intercept with another one
    "not exceed configured limits" in {
      val maxPerSecond = 3
      val stressSeconds = 2
      val totalSend = maxPerSecond * stressSeconds
      val rateLimiter = new ClusteredRateLimiter("limiter-1", duration = 1.second, maxPerSecond, syncsPerDuration = 2)
      val testService = new TestService()
      val allSend = Future.sequence(for { _ <- 1 to totalSend } yield rateLimiter { testService.query })
      awaitSuccess((stressSeconds + 1).second) {
        for {
          _       <- allSend
          history <- Future(testService.queryHistory)
        } yield {
          history.size shouldBe totalSend
        }
      }
    }
  }

  private class TestService() {
    val queryHistory = ListBuffer.empty[Long]
    def query: Future[Done] = {
      queryHistory += System.currentTimeMillis()
      Future.successful(Done)
    }
  }

  def awaitSuccess[T](maxDuration: FiniteDuration, checkEvery: FiniteDuration = 1.milli)(block: => Future[T])(implicit actorSystem: ActorSystem): Future[T] = {
    val checkUntil = System.currentTimeMillis() + maxDuration.toMillis

    def doCheck(): Future[T] = {
      block.recoverWith {
        case _ if checkUntil > System.currentTimeMillis() =>
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
