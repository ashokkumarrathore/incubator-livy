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

package org.apache.livy.server

import javax.servlet.http.HttpServletRequest

import org.scalatest.FunSpec
import org.scalatest.Matchers._

import org.apache.livy.{LivyBaseUnitTestSuite, LivyConf}
import org.apache.livy.server.recovery.StateFileCollisionException
import org.apache.livy.sessions.{Session, SessionManager, SessionState}
import org.apache.livy.sessions.Session.RecoveryMetadata

class SessionServletRetrySpec extends FunSpec with LivyBaseUnitTestSuite {

  private case class StubMeta(id: Int) extends RecoveryMetadata
  private class StubSession(id: Int, conf: LivyConf)
    extends Session(id, None, "owner", conf) {
    override val proxyUser: Option[String] = None
    override def recoveryMetadata: RecoveryMetadata = StubMeta(id)
    override def state: SessionState = SessionState.Idle
    override def start(): Unit = ()
    override protected def stopSession(): Unit = ()
    override def logLines(): IndexedSeq[String] = IndexedSeq.empty
  }

  // Anonymous subclass exposing the protected withCollisionRetry helper.
  private class TestableServlet(conf: LivyConf, mgr: SessionManager[Session, RecoveryMetadata])
    extends SessionServlet[Session, RecoveryMetadata](mgr, conf, new AccessManager(conf)) {
    override protected def createSession(req: HttpServletRequest): Session =
      throw new UnsupportedOperationException("not used in this spec")
    def runRetry(body: => Session): Session = withCollisionRetry(body)
  }

  private def newServlet(conf: LivyConf = new LivyConf()): (TestableServlet, LivyConf) = {
    val mgr = new SessionManager[Session, RecoveryMetadata](
      conf,
      { _ => assert(false).asInstanceOf[Session] },
      null.asInstanceOf[org.apache.livy.server.recovery.SessionStore],
      "test",
      Some(Seq.empty))
    (new TestableServlet(conf, mgr), conf)
  }

  describe("SessionServlet.withCollisionRetry") {
    it("retries past a single StateFileCollisionException and returns the session") {
      val (servlet, conf) = newServlet()
      var attempts = 0
      val produced = new StubSession(0, conf)
      val result = servlet.runRetry {
        attempts += 1
        if (attempts == 1) throw new StateFileCollisionException(1, "batch")
        produced
      }
      attempts shouldBe 2
      result shouldBe produced
    }

    it("propagates the exception after exhausting MAX_ID_COLLISION_RETRIES") {
      val (servlet, _) = newServlet()
      var attempts = 0
      val ex = intercept[StateFileCollisionException] {
        servlet.runRetry {
          attempts += 1
          throw new StateFileCollisionException(attempts, "batch")
        }
      }
      // Initial attempt + 10 retries = 11 invocations before the final throw escapes.
      attempts shouldBe 11
      ex shouldBe a[StateFileCollisionException]
    }

    it("honors a configured max-retries value") {
      val conf = new LivyConf().set(LivyConf.SESSION_ID_COLLISION_MAX_RETRIES, 2)
      val (servlet, _) = newServlet(conf)
      var attempts = 0
      intercept[StateFileCollisionException] {
        servlet.runRetry {
          attempts += 1
          throw new StateFileCollisionException(attempts, "batch")
        }
      }
      // Initial attempt + 2 retries = 3 invocations.
      attempts shouldBe 3
    }

    it("does not retry on a non-collision exception") {
      val (servlet, _) = newServlet()
      var attempts = 0
      intercept[IllegalArgumentException] {
        servlet.runRetry {
          attempts += 1
          throw new IllegalArgumentException("not a collision")
        }
      }
      attempts shouldBe 1
    }

    it("returns immediately on first-attempt success without invoking body twice") {
      val (servlet, conf) = newServlet()
      var attempts = 0
      val produced = new StubSession(0, conf)
      val result = servlet.runRetry {
        attempts += 1
        produced
      }
      attempts shouldBe 1
      result shouldBe produced
    }
  }
}
