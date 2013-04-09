/**
 *  Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.remote

import language.postfixOps
import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory
import akka.actor.Actor
import akka.actor.ActorIdentity
import akka.actor.ActorRef
import akka.actor.Identify
import akka.actor.Props
import akka.actor.Terminated
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.remote.testkit.STMultiNodeSpec
import akka.testkit._

object RemoteDeathWatchMultiJvmSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")

  commonConfig(debugConfig(on = false).withFallback(
    ConfigFactory.parseString("""
      akka.loglevel = INFO
      akka.remote.log-remote-lifecycle-events = off
      """)))

  case class WatchIt(watchee: ActorRef)
  case class UnwatchIt(watchee: ActorRef)
  case object Ack
  // FIXME Terminated can't be forwarded, but that should be solved when "Terminated as sys msg" is in
  case class WrappedTerminated(terminated: Terminated)

  class ProbeActor(testActor: ActorRef) extends Actor {
    def receive = {
      case WatchIt(watchee) ⇒
        context watch watchee
        sender ! Ack
      case UnwatchIt(watchee) ⇒
        context unwatch watchee
        sender ! Ack
      case t @ Terminated(subject) ⇒
        testActor forward WrappedTerminated(t)
      case msg ⇒ testActor forward msg
    }
  }

}

class RemoteDeathWatchMultiJvmNode1 extends RemoteDeathWatchSpec
class RemoteDeathWatchMultiJvmNode2 extends RemoteDeathWatchSpec

abstract class RemoteDeathWatchSpec
  extends MultiNodeSpec(RemoteDeathWatchMultiJvmSpec)
  with STMultiNodeSpec with ImplicitSender {

  import RemoteDeathWatchMultiJvmSpec._

  def initialParticipants = roles.size

  "An actor watching a remote actor" must {

    "receive Terminated when watched node is shutdown" taggedAs LongRunningTest in within(20 seconds) {
      runOn(first) {
        val watcher = system.actorOf(Props(new ProbeActor(testActor)), "watcher")
        enterBarrier("actors-started")

        system.actorSelection(node(second) / "user" / "subject") ! Identify("subject")
        val subject = expectMsgType[ActorIdentity].ref.get
        watcher ! WatchIt(subject)
        expectMsg(1 second, Ack)
        subject ! "hello"
        enterBarrier("watch-established")

        // FIXME add another test without this sleep, it will test the triggerFirstHeartbeat (it works!)
        // let them heartbeat for a while
        Thread.sleep(5000)

        testConductor.shutdown(second, 0).await
        expectMsgType[WrappedTerminated].terminated.actor must be(subject)
      }

      runOn(second) {
        system.actorOf(Props(new ProbeActor(testActor)), "subject")
        enterBarrier("actors-started")

        expectMsg(3 seconds, "hello")
        enterBarrier("watch-established")
      }

      enterBarrier("after-1")
    }

  }
}
