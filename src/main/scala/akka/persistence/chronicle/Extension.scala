/**
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.persistence.chronicle

import java.net.URLDecoder
import java.net.URLEncoder

import scala.util.Try

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.actor.actorRef2Scala
import akka.cluster.Cluster
import akka.event.Logging
import akka.persistence.Persistence

/** Chronicle persistence extension. */
class ChronicleExtension(val system: ExtendedActorSystem) extends Extension with Settings {
  import ExtensionProtocol._
  import ReplicationProtocol._

  override def chronicleConfig = system.settings.config.getConfig(chroniclePath)
  val settings = chronicleSettings
  import settings._

  private val log = Logging(system, getClass.getName)

  /** Verify if running in clustered environment. */
  private[chronicle] val clusterEnable = extension.clusterEnable && Try(Cluster(system)).isSuccess

  /** Replication coordinator actor. */
  private[chronicle] val replicator: ActorRef = if (clusterEnable)
    system.actorOf(ReplicationCoordinator(), extension.replicator.name)
  else
    system.provider.resolveActorRef("resolve replicator to system dead leaters")

  /** Akka persistence extension. */
  private[chronicle] val persistence = Persistence(system)

  /** [[ChronicleSyncJournal]] actor */
  private[chronicle] val journalActor = persistence.journalFor(null)

  /** [[ChronicleSnapshotStore]] actor */
  private[chronicle] val snapshotActor = persistence.snapshotStoreFor(null)

  /** Publish local content to remote replicas. */
  private[chronicle] def publish(message: PublishCommand) = if (clusterEnable) {
    replicator ! message
    if (extension.exposeReplicationStream) system.eventStream.publish(message)
  }

  /** Consume remote content by local participants. */
  private[chronicle] def consume(message: ConsumeCommand) = {
    message.payload match {
      case payload: JournalPayload  => journalActor ! message
      case payload: SnapshotPayload => snapshotActor ! message
    }
    if (extension.exposeReplicationStream) system.eventStream.publish(message)
  }

  /** Execute extension command on local and remote system. */
  def command(message: PluginCommand): Unit = {
    message match {
      case command: PluginJournalCommand => command match {
        case PluginJournalClearCommand                 => communicate(JournalClearPayload)
        case PluginJournalClearCommand(persistenceId)  => communicate(JournalClearPayload(persistenceId))
        case PluginJournalRotateCommand(persistenceId) => communicate(JournalRotatePayload(persistenceId))
      }
      case command: PluginSnapshotCommand => command match {
        case PluginSnapshotClearCommand                => communicate(SnapshotClearPayload)
        case PluginSnapshotClearCommand(persistenceId) => communicate(SnapshotClearPayload(persistenceId))
      }
    }
  }

  /** Communicate user command payload to both local and remote system. */
  private[chronicle] def communicate(payload: Payload): Unit = {
    consume(ConsumeCommand(payload))
    publish(PublishCommand(payload))
  }

  private def initialize(): Unit = {
    log.info(s"Akka cluster is present and replication is enabled.")
  }

  private def complain(): Unit = {
    log.warning(s"Akka cluster is not present or replication is not enabled.")
  }

  if (clusterEnable) initialize() else complain()

}

/** Chronicle persistence extension provider. */
private[chronicle] object ChronicleExtension extends ExtensionId[ChronicleExtension] with ExtensionIdProvider {
  override def get(system: ActorSystem): ChronicleExtension = super.get(system)
  override def lookup() = ChronicleExtension
  override def createExtension(system: ExtendedActorSystem): ChronicleExtension = new ChronicleExtension(system)
}

/** End-user plugin communication protocol. */
object ExtensionProtocol {

  /** Marker trait for user commands. */
  sealed trait PluginCommand
  /** Marker trait for journal commands. */
  sealed trait PluginJournalCommand extends PluginCommand
  /** Marker trait for snapshot store commands. */
  sealed trait PluginSnapshotCommand extends PluginCommand

  /** Remove persistent state for all journal queues. */
  final case object PluginJournalClearCommand extends PluginJournalCommand
  /** Remove persistent state for selected journal queue. */
  final case class PluginJournalClearCommand(persistenceId: String) extends PluginJournalCommand
  /** Rotate persistent journal queue segments. */
  final case class PluginJournalRotateCommand(persistenceId: String) extends PluginJournalCommand

  /** Remove persistent state for all snapshot store. */
  final case object PluginSnapshotClearCommand extends PluginSnapshotCommand
  /** Remove persistent state for selected snapshot store. */
  final case class PluginSnapshotClearCommand(persistenceId: String) extends PluginSnapshotCommand

  sealed trait PluginNotification
  sealed trait PluginJournalNotification extends PluginNotification
  sealed trait PluginSnapshotNotification extends PluginNotification

  final case object PluginJournalOpenNotification extends PluginJournalNotification
  final case object PluginJournalCloseNotification extends PluginJournalNotification
  final case object PluginJournalClearNotification extends PluginJournalNotification
  final case class PluginJournalClearNotification(persistenceId: String) extends PluginJournalNotification
  final case class PluginJournalRotateNotification(persistenceId: String) extends PluginJournalNotification

  final case object PluginSnapshotOpenNotification extends PluginSnapshotNotification
  final case object PluginSnapshotCloseNotification extends PluginSnapshotNotification
  final case object PluginSnapshotClearNotification extends PluginSnapshotNotification
  final case class PluginSnapshotClearNotification(persistenceId: String) extends PluginSnapshotNotification

}

/** Provide safe file system path name codec. */
trait NamingMapper {
  /** Decode persistence id form file system path. */
  def decode(path: String): String
  /** Encode persistence id into file system path. */
  def encode(name: String): String
}

/** Provide safe file system path name. */
final case object NamingMapper {
  def apply(system: ActorSystem, fqcn: String): NamingMapper = {
    system.asInstanceOf[ExtendedActorSystem].dynamicAccess.createInstanceFor[NamingMapper](fqcn, Nil)
      .getOrElse(throw new RuntimeException(s"Failed to create ${NamingMapper.getClass.getName} from ${fqcn}"))
  }
}

/** Provide file system name assuming original name is already safe. */
final case class DirectNamingMapper() extends NamingMapper {
  override def decode(path: String): String = path
  override def encode(name: String): String = name
}

/** Provide file system name mapping with URLDecoder / URLEncoder codec. */
final case class URLNamingMapper() extends NamingMapper {
  val UTF8 = "UTF-8"
  override def decode(path: String): String = URLDecoder.decode(path, UTF8)
  override def encode(name: String): String = URLEncoder.encode(name, UTF8)
}
