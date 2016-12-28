/**
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.persistence.chronicle

import java.io.File
import java.io.PrintWriter
import akka.actor.ActorCell
import akka.actor.ActorRef
import akka.actor.ActorRefWithCell
import akka.actor.ActorSystem
import akka.persistence.PersistentActor
import akka.persistence.SaveSnapshotSuccess
import akka.persistence.SaveSnapshotFailure
import akka.persistence.SnapshotOffer
import akka.persistence.RecoveryCompleted
//import akka.persistence.RecoveryFailure
import akka.actor.Props

/** Plugin test tools. */
object PluginTest {

  /** Extract actor instance from actor reference. */
  def extractActor[T](actor: ActorRef): T = actor
    .asInstanceOf[ActorRefWithCell]
    .underlying
    .asInstanceOf[ActorCell]
    .actor
    .asInstanceOf[T]

  def saveApplicationConf(system: ActorSystem, name: String): Unit = {
    import java.io.File
    import java.io.PrintWriter
    val conf = system.settings.config
    val text = conf.root.render
    val file = new File(s"target/${name}/application.conf")
    file.getParentFile.mkdirs()
    Some(new PrintWriter(file)) map { p ⇒ p.write(text); p.close }
  }

}

/** Persistent actor model. */
class StoicActor(id: String) extends PersistentActor {
  import StoicActor._

  def display(text: String) = {
    //     println(text)
  }

  override def persistenceId: String = id

  override def receiveCommand: Receive = {
    case Command(data) => persist(Event(data)) { event =>
      display(s"=== ${id} command data ${data}")
      updateState(event)
      context.system.eventStream.publish(event)
    }
    case "clear" =>
      state = State()
      display(s"=== ${id} command clear")
    case "restart" =>
      display(s"=== ${id} command restart")
      throw new RuntimeException("restart")
    case "snapshot" =>
      display(s"=== ${id} command snapshot")
      saveSnapshot(state)
    case SaveSnapshotSuccess(metadata) =>
      display(s"=== ${id} command snap ok ${metadata}")
    case SaveSnapshotFailure(metadata, error) =>
      display(s"=== ${id} command snap ng ${metadata} ${error}")
    case message =>
      display(s"=== ${id} command unexpected ${message}")
  }

  override def receiveRecover: Receive = {
    case event: Event =>
      display(s"+++ ${id} recover message ${event}")
      updateState(event)
      context.system.eventStream.publish(event)
    case SnapshotOffer(metadata, snapshot: State) =>
      display(s"+++ ${id} recover snapshot ${metadata} ${snapshot}")
      state = snapshot
    case RecoveryCompleted =>
      display(s"+++ ${id} recover success")
      context.system.eventStream.publish("recover-complete")
    //    case RecoveryFailure(error) =>
    //      display(s"+++ ${id} recover failure ${error}")
    case message =>
      display(s"+++ ${id} recover unexpected ${message}")
  }

  @volatile var state = State()

  def eventCount = state.events.size

  def updateState(event: Event): Unit = state = state.updated(event)

}

/** Persistent actor provider. */
object StoicActor {

  def apply(id: String) = Props(classOf[StoicActor], id)

  @SerialVersionUID(1L)
  final case class Event(data: String) extends Serializable

  @SerialVersionUID(1L)
  final case class Command(data: String) extends Serializable

  @SerialVersionUID(1L)
  final case class State(events: List[String] = Nil) {
    def updated(event: Event): State = copy(event.data :: events)
    def size: Int = events.length
    override def toString: String = events.reverse.toString
  }

}
