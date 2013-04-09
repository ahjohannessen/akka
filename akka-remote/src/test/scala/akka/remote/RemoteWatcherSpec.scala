/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.remote

import language.postfixOps
import scala.concurrent.duration._
import akka.testkit._
import akka.actor.ActorSystem
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.ExtendedActorSystem
import akka.actor.RootActorPath
import akka.actor.Identify
import akka.actor.ActorIdentity
import akka.actor.PoisonPill

object RemoteWatcherSpec {

  class TestActorProxy(testActor: ActorRef) extends Actor {
    def receive = {
      case msg ⇒ testActor forward msg
    }
  }

  class MyActor extends Actor {
    def receive = Actor.emptyBehavior
  }

}

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class RemoteWatcherSpec extends AkkaSpec(
  """akka {
       loglevel = INFO
       actor.provider = "akka.remote.RemoteActorRefProvider"
       remote.netty.tcp {
         hostname = localhost
         port = 0
       }
     }""") with ImplicitSender {

  import RemoteWatcherSpec._
  import RemoteWatcher._

  override def expectedTestDuration = 2.minutes

  val remoteSystem = ActorSystem("RemoteSystem", system.settings.config)
  val remoteAddress = remoteSystem.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress

  override def afterTermination() {
    remoteSystem.shutdown()
  }

  def createRemoteActor(props: Props, name: String): ActorRef = {
    remoteSystem.actorOf(props, name)
    system.actorSelection(RootActorPath(remoteAddress) / "user" / name) ! Identify(name)
    expectMsgType[ActorIdentity].ref.get
  }

  "A RemoteWatcher" must {

    "have correct interaction when watching" in {
      // turn off all periodic activity
      val TurnOff = 5.minutes
      val endWatchingDuration = 2.seconds

      val monitorA = system.actorOf(Props(new RemoteWatcher(
        heartbeatInterval = TurnOff,
        heartbeatExpectedResponseAfter = TurnOff,
        unreachableReaperInterval = TurnOff,
        endWatchingDuration = endWatchingDuration)), "monitor1")
      val monitorB = createRemoteActor(Props(new TestActorProxy(testActor)), "monitor1")

      val b1 = createRemoteActor(Props[MyActor], "b1")
      val b2 = createRemoteActor(Props[MyActor], "b2")

      monitorA ! WatchRemote(b1)
      monitorA ! WatchRemote(b2)
      expectNoMsg(100 millis)
      monitorA ! HeartbeatTick
      expectMsg(HeartbeatRequest)
      expectNoMsg(100 millis)
      monitorA ! HeartbeatTick
      expectMsg(HeartbeatRequest)
      expectNoMsg(100 millis)
      monitorA.tell(Heartbeat, monitorB)
      monitorA ! HeartbeatTick
      expectNoMsg(100 millis)

      monitorA ! UnwatchRemote(b1)
      expectNoMsg(100 millis)
      // still one left, no EndHeartbeatRequest
      monitorA ! HeartbeatTick
      expectNoMsg(100 millis)

      monitorA ! UnwatchRemote(b2)
      expectNoMsg(100 millis)
      monitorA ! HeartbeatTick
      expectMsg(EndHeartbeatRequest)
      expectNoMsg(100 millis)
      monitorA ! HeartbeatTick
      expectMsg(EndHeartbeatRequest)
      expectNoMsg(endWatchingDuration)
      monitorA ! HeartbeatTick
      expectNoMsg(100 millis)

      // make sure nothing floods over to next test
      expectNoMsg(2 seconds)
    }

    "have correct interaction when beeing watched" in {
      // turn off all periodic activity
      val TurnOff = 5.minutes
      val endWatchingDuration = 2.seconds

      val monitorA = system.actorOf(Props(new TestActorProxy(testActor)), "monitor2")
      val monitorB = createRemoteActor(Props(new RemoteWatcher(
        heartbeatInterval = TurnOff,
        heartbeatExpectedResponseAfter = TurnOff,
        unreachableReaperInterval = TurnOff,
        endWatchingDuration = TurnOff)), "monitor2")

      val b3 = createRemoteActor(Props[MyActor], "b3")

      // watch
      monitorB.tell(HeartbeatRequest, monitorA)
      expectNoMsg(100 millis)
      monitorB ! HeartbeatTick
      expectMsg(Heartbeat)
      expectNoMsg(100 millis)
      monitorB ! HeartbeatTick
      expectMsg(Heartbeat)
      expectNoMsg(100 millis)

      // unwatch
      monitorB.tell(EndHeartbeatRequest, monitorA)
      expectNoMsg(100 millis)
      monitorB ! HeartbeatTick
      expectNoMsg(100 millis)

      // start heartbeating again
      monitorB.tell(HeartbeatRequest, monitorA)
      expectNoMsg(100 millis)
      monitorB ! HeartbeatTick
      expectMsg(Heartbeat)
      expectNoMsg(100 millis)

      // then kill other side, which should stop the heartbeating
      monitorA ! PoisonPill
      monitorB ! HeartbeatTick
      expectNoMsg(500 millis)

      // make sure nothing floods over to next test
      expectNoMsg(2 seconds)
    }

    /* FIXME Remove these, add multi-node tests
    "detect failure" in {
      println("########### detect failure")
      val monitorA = system.actorOf(Props(new RemoteWatcher), "monitor3")
      val monitorB = createRemoteActor(Props(new RemoteWatcher), "monitor3")

      val b4 = createRemoteActor(Props[MyActor], "b4")

      monitorA ! WatchRemote(b4)
      Thread.sleep(10000)

      println("# stop monitorB")
      monitorB ! PoisonPill
      Thread.sleep(15000)
    }

    "detect failure in opposite direction" in {
      println("########### detect failure in opposite direction")
      val monitorA = system.actorOf(Props(new RemoteWatcher), "monitor4")
      val monitorB = createRemoteActor(Props(new RemoteWatcher), "monitor4")

      val b5 = createRemoteActor(Props[MyActor], "b5")

      monitorA ! WatchRemote(b5)
      Thread.sleep(10000)

      println("# stop monitorA")
      monitorA ! PoisonPill
      Thread.sleep(15000)
    }

    "stop heartbeating after unwatch" in {
      println("########### stop heartbeating after unwatch")
      val monitorA = system.actorOf(Props(new RemoteWatcher), "monitor5")
      createRemoteActor(Props(new RemoteWatcher), "monitor5")

      val b6 = createRemoteActor(Props[MyActor], "b6")
      val b7 = createRemoteActor(Props[MyActor], "b7")

      monitorA ! WatchRemote(b6)
      monitorA ! WatchRemote(b7)
      Thread.sleep(10000)

      monitorA ! UnwatchRemote(b6)
      Thread.sleep(10000)

      monitorA ! UnwatchRemote(b7)
      Thread.sleep(25000)
    }

    */

  }

}
