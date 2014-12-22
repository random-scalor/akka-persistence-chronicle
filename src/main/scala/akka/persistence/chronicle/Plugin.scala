/**
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.persistence.chronicle

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRefProvider
import akka.actor.ExtendedActorSystem
import akka.persistence.Persistence
import akka.persistence.SnapshotMetadata
import akka.serialization.SerializationExtension

/** Chronicle persistence plugin actors extend this trait. */
private[chronicle] trait PluginActor extends Actor with ActorLogging with Settings {
  import PluginActor._
  import ExtensionProtocol._
  import ReplicationProtocol._
  import context._

  override def chronicleConfig = system.settings.config.getConfig(chroniclePath)
  val settings = chronicleSettings
  import settings._

  /** System persistence. */
  implicit val persistence = Persistence(system)

  /** System serialization. */
  implicit val serializer = SerializationExtension(system)

  /** [[ActorRefProvider]] used for actor reference resolution. */
  val referenceProvider: ActorRefProvider = system.asInstanceOf[ExtendedActorSystem].provider

  /** Chronicle cluster replication extension. */
  val chronicleExtension = ChronicleExtension(system)

  /** Publish local content to remote replicas. */
  def payloadPublish(payload: Payload): Unit = {
    chronicleExtension.publish(PublishCommand(payload))
  }

  /** Publish [[PluginNotification]] to system stream. */
  def systemPublish(event: PluginNotification): Unit = {
    if (extension.exposeNotificationStream) system.eventStream.publish(event)
  }

}

private[chronicle] object PluginActor {
  import ChronicleJournal.JournalProvider

  /** [[Long]] counter. */
  case class Counter(private var _count: Long = 0) {
    def count: Long = _count
    def reset(): Unit = _count = 0
    def increment(): Unit = _count += 1
    def decrement(): Unit = _count -= 1
  }

  //

  /** Serialized key. */
  type Key = Array[Byte]

  /** Serialized value. */
  type Value = Array[Byte]

  /** Serialized content. */
  type Content = Array[Byte]

  //

  /** Journal/Snapshot actors coordination commands. */
  sealed trait CoordinationCommand

  /** Command to force journal actor restart. */
  final case object JournalRestartCommand extends CoordinationCommand

  /** Command to force snapshot actor restart. */
  final case object SnapshotRestartCommand extends CoordinationCommand

  /** Notification from Journal to self with rotated journal. */
  final case class JournalRotatedCommand(provider: JournalProvider) extends CoordinationCommand

  /** Notification from Snapshot actor to Journal actor. */
  final case class SnapshotCreatedCommand(metadata: SnapshotMetadata) extends CoordinationCommand

}
