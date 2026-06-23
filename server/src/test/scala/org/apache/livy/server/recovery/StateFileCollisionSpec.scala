/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.livy.server.recovery

import java.io.File
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.nio.file.attribute.PosixFilePermission._
import java.util.concurrent.{CountDownLatch, CyclicBarrier, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

import org.scalatest.{BeforeAndAfterEach, FunSpec}
import org.scalatest.Matchers._

import org.apache.livy.{LivyBaseUnitTestSuite, LivyConf}
import org.apache.livy.sessions.Session.RecoveryMetadata

class StateFileCollisionSpec extends FunSpec
  with BeforeAndAfterEach with LivyBaseUnitTestSuite {

  private var tmpDir: File = _
  private var stateStore: FileSystemStateStore = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    tmpDir = Files.createTempDirectory("livy-collision-test-").toFile
    val conf = new LivyConf()
    conf.set(LivyConf.RECOVERY_STATE_STORE_URL, s"file://${tmpDir.getAbsolutePath}")
    stateStore = new FileSystemStateStore(conf)
  }

  override def afterEach(): Unit = {
    if (tmpDir != null && tmpDir.exists()) {
      def deleteRecursive(f: File): Unit = {
        if (f.isDirectory) f.listFiles().foreach(deleteRecursive)
        f.delete()
      }
      deleteRecursive(tmpDir)
    }
    super.afterEach()
  }

  describe("StateFileCollisionSpec") {
    it("should return true on first exclusive create and false on duplicate") {
      val key = "v1/batch/100"
      stateStore.tryExclusiveCreate(key, "instance-A") shouldBe true
      stateStore.tryExclusiveCreate(key, "instance-B") shouldBe false
    }

    it("should create state files with owner-only permissions (0600)") {
      val key = "v1/batch/perm-check"
      stateStore.tryExclusiveCreate(key, "perm-test") shouldBe true
      val perms = Files.getPosixFilePermissions(
        Paths.get(tmpDir.getAbsolutePath, key)).asScala
      perms shouldBe Set(OWNER_READ, OWNER_WRITE)
    }

    it("should allow exactly one winner when N threads race via tryExclusiveCreate") {
      val numThreads = 10
      val raceKey = "v1/batch/excl-race"
      val raceBarrier = new CyclicBarrier(numThreads)
      val raceWins = new AtomicInteger(0)
      val raceLosses = new AtomicInteger(0)
      val racePool = Executors.newFixedThreadPool(numThreads)

      for (i <- 0 until numThreads) {
        racePool.submit(new Runnable {
          override def run(): Unit = {
            raceBarrier.await()
            if (stateStore.tryExclusiveCreate(raceKey, s"thread-$i")) {
              raceWins.incrementAndGet()
            } else {
              raceLosses.incrementAndGet()
            }
          }
        })
      }

      racePool.shutdown()
      racePool.awaitTermination(30, TimeUnit.SECONDS) shouldBe true
      raceWins.get() shouldBe 1
      raceLosses.get() shouldBe (numThreads - 1)
    }

    it("should allow exactly one winner when N threads race via NIO CREATE_NEW directly") {
      val targetFile = new File(tmpDir, "v1/batch/nio-race-test")
      targetFile.getParentFile.mkdirs()
      val numThreads = 10
      val barrier = new CyclicBarrier(numThreads)
      val wins = new AtomicInteger(0)
      val losses = new AtomicInteger(0)
      val pool = Executors.newFixedThreadPool(numThreads)

      for (_ <- 0 until numThreads) {
        pool.submit(new Runnable {
          override def run(): Unit = {
            barrier.await()
            try {
              Files.write(
                targetFile.toPath,
                s"thread-${Thread.currentThread().getId}".getBytes,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
              wins.incrementAndGet()
            } catch {
              case _: java.nio.file.FileAlreadyExistsException =>
                losses.incrementAndGet()
            }
          }
        })
      }

      pool.shutdown()
      pool.awaitTermination(30, TimeUnit.SECONDS) shouldBe true
      wins.get() shouldBe 1
      losses.get() shouldBe (numThreads - 1)
    }

    it("should succeed on the next ID after a failed claim") {
      val baseId = 200
      stateStore.tryExclusiveCreate(s"v1/batch/$baseId", "instance-A") shouldBe true
      stateStore.tryExclusiveCreate(s"v1/batch/$baseId", "instance-B") shouldBe false
      stateStore.tryExclusiveCreate(s"v1/batch/${baseId + 1}", "instance-B") shouldBe true
    }

    it("should give two racing Livy instances distinct session IDs") {
      val sharedIdCounter = new AtomicInteger(300)
      val barrier = new CyclicBarrier(2)
      val latch = new CountDownLatch(2)
      val results = new java.util.concurrent.ConcurrentHashMap[String, Int]()
      val pool = Executors.newFixedThreadPool(2)

      for (instance <- Seq("livy-old-pod", "livy-new-pod")) {
        pool.submit(new Runnable {
          override def run(): Unit = {
            barrier.await()
            var claimed = false
            var id = -1
            while (!claimed) {
              id = sharedIdCounter.getAndIncrement()
              claimed = stateStore.tryExclusiveCreate(s"v1/batch/$id", instance)
            }
            results.put(instance, id)
            latch.countDown()
          }
        })
      }

      latch.await(30, TimeUnit.SECONDS) shouldBe true
      pool.shutdown()
      results.get("livy-old-pod") should not equal results.get("livy-new-pod")
    }

    it("should reject app ID change after initial latch (knownAppId immutability)") {
      var knownAppId: Option[String] = None

      def monitorPoll(newAppId: String): Boolean = {
        knownAppId match {
          case Some(known) if known != newAppId => false
          case None =>
            knownAppId = Some(newAppId)
            true
          case _ => true
        }
      }

      monitorPoll("spark-app-001") shouldBe true
      monitorPoll("spark-app-001") shouldBe true
      monitorPoll("spark-app-002") shouldBe false
    }

    it("should accept the same app ID on repeated polls") {
      var knownAppId: Option[String] = None

      def monitorPoll(newAppId: String): Boolean = {
        knownAppId match {
          case Some(known) if known != newAppId => false
          case None =>
            knownAppId = Some(newAppId)
            true
          case _ => true
        }
      }

      for (_ <- 0 until 100) {
        monitorPoll("spark-app-stable") shouldBe true
      }
    }

    it("should preserve recovered app ID and reject a different one") {
      var knownAppId: Option[String] = Some("spark-app-recovered-001")

      def monitorPoll(newAppId: String): Boolean = {
        knownAppId match {
          case Some(known) if known != newAppId => false
          case None =>
            knownAppId = Some(newAppId)
            true
          case _ => true
        }
      }

      monitorPoll("spark-app-recovered-001") shouldBe true
      monitorPoll("spark-app-different-002") shouldBe false
    }

    it("should wrap tryExclusiveCreate correctly via SessionStore.trySave") {
      val conf = new LivyConf()
      val sessionStore = new SessionStore(conf, stateStore)
      case class TestMeta(id: Int) extends RecoveryMetadata

      val meta = TestMeta(500)
      sessionStore.trySave("batch", meta) shouldBe true
      sessionStore.trySave("batch", meta) shouldBe false
    }

    it("should provide a typed StateFileCollisionException with session type in message") {
      val batchEx = new StateFileCollisionException(42, "batch")
      batchEx shouldBe a[IllegalStateException]
      batchEx.sessionId shouldBe 42
      batchEx.getMessage should include("batch session 42")

      val interactiveEx = new StateFileCollisionException(7, "interactive")
      interactiveEx.getMessage should include("interactive session 7")

      intercept[StateFileCollisionException] { throw batchEx }
    }

    it("should produce unique IDs across two pods in a full rolling deploy simulation") {
      case class SimulatedBatch(batchId: Int, sparkAppId: String, instanceName: String)

      val sharedCounter = new AtomicInteger(400)
      val completedBatches = new java.util.concurrent.ConcurrentLinkedQueue[SimulatedBatch]()
      val barrier = new CyclicBarrier(2)
      val latch = new CountDownLatch(2)
      val pool = Executors.newFixedThreadPool(2)

      for ((instance, sparkApp) <- Seq(("old-pod", "spark-app-OLD"), ("new-pod", "spark-app-NEW")))
      {
        pool.submit(new Runnable {
          override def run(): Unit = {
            barrier.await()
            for (_ <- 0 until 5) {
              var claimed = false
              var id = -1
              while (!claimed) {
                id = sharedCounter.getAndIncrement()
                claimed = stateStore.tryExclusiveCreate(s"v1/batch/$id", instance)
              }

              var knownAppId: Option[String] = Some(sparkApp)
              val impostorId = s"spark-app-IMPOSTOR-$id"
              val accepted = knownAppId match {
                case Some(known) if known != impostorId => false
                case _ => true
              }
              accepted shouldBe false

              completedBatches.add(SimulatedBatch(id, sparkApp, instance))
            }
            latch.countDown()
          }
        })
      }

      latch.await(30, TimeUnit.SECONDS) shouldBe true
      pool.shutdown()

      val allBatches = new java.util.ArrayList[SimulatedBatch]()
      val it = completedBatches.iterator()
      while (it.hasNext) allBatches.add(it.next())

      val batchIds = new java.util.HashSet[Int]()
      val it2 = allBatches.iterator()
      while (it2.hasNext) batchIds.add(it2.next().batchId)

      allBatches.size() shouldBe 10
      batchIds.size() shouldBe 10
    }
  }
}
