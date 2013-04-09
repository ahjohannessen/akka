/**
 *  Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.remote

import scala.concurrent.duration._
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Address
import akka.actor.AddressTerminated
import akka.actor.RootActorPath
import akka.actor.Terminated

/**
 * INTERNAL API
 */
private[akka] object RemoteWatcher {
  case class WatchRemote(watchee: ActorRef)
  case class UnwatchRemote(watchee: ActorRef)

  @SerialVersionUID(1L)
  case object Heartbeat
  @SerialVersionUID(1L)
  case object HeartbeatRequest
  @SerialVersionUID(1L)
  case object EndHeartbeatRequest

  // sent to self only
  case object HeartbeatTick
  case object ReapUnreachableTick
  case class ExpectedFirstHeartbeat(from: Address)
}

/**
 * INTERNAL API
 */
private[akka] class RemoteWatcher(
  // FIXME from config
  heartbeatInterval: FiniteDuration = 1.second,
  heartbeatExpectedResponseAfter: FiniteDuration = 3.seconds,
  unreachableReaperInterval: FiniteDuration = 1.seconds,
  endWatchingDuration: FiniteDuration = 10.seconds)
  extends Actor with ActorLogging {
  import RemoteWatcher._
  import context.dispatcher
  def scheduler = context.system.scheduler

  val failureDetector: FailureDetectorRegistry[Address] = {
    // FIXME create from config
    def createFailureDetector(): FailureDetector =
      new PhiAccrualFailureDetector(
        threshold = 8.0,
        maxSampleSize = 1000,
        minStdDeviation = 100.millis,
        acceptableHeartbeatPause = 3.seconds,
        firstHeartbeatEstimate = 1.second)

    new DefaultFailureDetectorRegistry(() ⇒ createFailureDetector())
  }

  // actors that this node is watching
  var watching: Set[ActorRef] = Set.empty
  // nodes that this node is watching, i.e. expecting hearteats from these nodes
  var watchingNodes: Set[Address] = Set.empty
  // heartbeats will be sent to watchedByNodes, ref is RemoteWatcher at other side
  var watchedByNodes: Set[ActorRef] = Set.empty
  var unreachable: Set[Address] = Set.empty
  var endWatchingNodes: Map[Address, Deadline] = Map.empty

  val heartbeatTask = scheduler.schedule(heartbeatInterval, heartbeatInterval, self, HeartbeatTick)
  val failureDetectorReaperTask = scheduler.schedule(unreachableReaperInterval, unreachableReaperInterval,
    self, ReapUnreachableTick)

  override def postStop(): Unit = {
    super.postStop()
    heartbeatTask.cancel()
    failureDetectorReaperTask.cancel()
  }

  // FIXME change log level

  def receive = {
    case Heartbeat ⇒
      val from = sender.path.address

      if (failureDetector.isMonitoring(from))
        log.info("Received heartbeat from [{}]", from)
      else
        log.info("Received first heartbeat from [{}]", from)

      if (watchingNodes(from) && !unreachable(from))
        failureDetector.heartbeat(from)

    case HeartbeatTick ⇒
      sendHeartbeat()
      sendHeartbeatRequest()
      sendEndHeartbeatRequest()

    case HeartbeatRequest ⇒
      // request to start sending heartbeats to the node
      watchedByNodes += sender
      // watch back to stop heartbeating if other side dies
      context watch sender

    case EndHeartbeatRequest ⇒
      // request to stop sending heartbeats to the node
      watchedByNodes -= sender
      context unwatch sender

    case ExpectedFirstHeartbeat(from) ⇒
      triggerFirstHeartbeat(from)

    case ReapUnreachableTick ⇒
      watchingNodes foreach { a ⇒
        if (!unreachable(a) && !failureDetector.isAvailable(a)) {
          log.warning("Detected unreachable: [{}]", a)
          context.system.eventStream.publish(AddressTerminated(a))
          unreachable += a
        }
      }

    case Terminated(actor) ⇒
      log.info("Other side terminated: [{}]", actor.path.address)
      // stop heartbeating to that node immediately
      watchedByNodes -= actor

    case WatchRemote(watchee) ⇒
      log.info("Watching: [{}]", watchee.path)
      watching += watchee
      watchingNodes += watchee.path.address

    case UnwatchRemote(watchee) ⇒
      log.info("Unwatching: [{}]", watchee.path)
      watching -= watchee
      val watcheeAddress = watchee.path.address
      if (watching.forall(_.path.address != watcheeAddress)) {
        // unwatched last watchee on that node
        watchingNodes -= watcheeAddress
        // continue by sending EndHeartbeatRequest for a while
        endWatchingNodes += (watcheeAddress -> (Deadline.now + endWatchingDuration))
        failureDetector.remove(watcheeAddress)
      }
  }

  def sendHeartbeat(): Unit =
    watchedByNodes foreach { ref ⇒
      val a = ref.path.address
      if (!unreachable(a)) {
        log.info("Sending Heartbeat to [{}]", ref.path.address)
        ref ! Heartbeat
      }
    }

  def sendHeartbeatRequest(): Unit =
    watchingNodes.foreach { a ⇒
      if (!unreachable(a) && !failureDetector.isMonitoring(a)) {
        log.info("Sending HeartbeatRequest to [{}]", a)
        context.actorSelection(RootActorPath(a) / self.path.elements) ! HeartbeatRequest
        // schedule the expected heartbeat for later, which will give the
        // other side a chance to start heartbeating, and also trigger some resends of
        // the heartbeat request
        scheduler.scheduleOnce(heartbeatExpectedResponseAfter, self, ExpectedFirstHeartbeat(a))
        endWatchingNodes -= a
      }
    }

  def sendEndHeartbeatRequest(): Unit =
    endWatchingNodes.foreach {
      case (a, deadline) ⇒
        if (deadline.isOverdue) {
          failureDetector.remove(a)
          endWatchingNodes -= a
          // FIXME think about when unreachable should be cleared
        } else if (!unreachable(a)) {
          log.info("Sending EndHeartbeatRequest to [{}]", a)
          context.actorSelection(RootActorPath(a) / self.path.elements) ! EndHeartbeatRequest
        }
    }

  def triggerFirstHeartbeat(address: Address): Unit =
    if (!failureDetector.isMonitoring(address)) {
      log.info("Trigger extra expected heartbeat from [{}]", address)
      failureDetector.heartbeat(address)
    }

}