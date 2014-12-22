/**
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.persistence.chronicle

import scala.annotation.varargs
import scala.collection.immutable

import akka.actor.ActorSelection
import akka.actor.ActorSelection.toScala
import akka.actor.Address
import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.ClusterEvent.MemberEvent
import akka.cluster.ClusterEvent.MemberExited
import akka.cluster.ClusterEvent.MemberRemoved
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.ClusterEvent.ReachabilityEvent
import akka.cluster.ClusterEvent.ReachableMember
import akka.cluster.ClusterEvent.UnreachableMember
import akka.cluster.Member
import akka.cluster.Member.addressOrdering
import akka.cluster.MemberStatus
import akka.persistence.PersistentRepr
import akka.persistence.SnapshotMetadata
import akka.serialization.Serialization

/** Persistence replication protocol. */
private[chronicle] object ReplicationProtocol {
  import PluginActor._
  import ExtensionSerializer._

  /** Marker for messages supported by [[ExtensionSerializer]]. */
  sealed trait Marker extends Serializable

  /** Replication payload command. */
  sealed trait Command extends Marker {
    /** Command payload content. */
    val payload: Payload
  }

  /** Outgoing publish command. */
  @SerialVersionUID(1L)
  final case class PublishCommand(payload: Payload) extends Command

  /** Incoming consume command. */
  @SerialVersionUID(1L)
  final case class ConsumeCommand(payload: Payload) extends Command

  /** Replication payload content. */
  sealed trait Payload extends Marker
  sealed trait JournalPayload extends Payload
  sealed trait SnapshotPayload extends Payload

  /** Remove journal content. */
  @SerialVersionUID(1L)
  final case object JournalClearPayload extends JournalPayload {
    /** Java API. */
    def getInstance = JournalClearPayload
  }

  /** Remove journal content for persistence id. */
  @SerialVersionUID(1L)
  final case class JournalClearPayload(persistenceId: String) extends JournalPayload

  /** Rotate journal segments for persistence id. */
  @SerialVersionUID(1L)
  final case class JournalRotatePayload(persistenceId: String) extends JournalPayload

  /** Append new journal entries. */
  @SerialVersionUID(1L)
  final case class JournalAppendPayload(list: Seq[Value]) extends JournalPayload {
    override def equals(o: Any): Boolean = o match {
      case that: JournalAppendPayload =>
        if (this.list.size != that.list.size) return false
        for {
          index <- 0 until this.list.size
          one = this.list(index)
          two = that.list(index)
          if (!java.util.Arrays.equals(one, two))
        } yield { return false }
        true
      case _ =>
        false
    }
  }
  final case object JournalAppendPayload {
    def apply(list: Seq[PersistentRepr], tester: Boolean = true): JournalAppendPayload = {
      ??? // TODO used in test
    }
  }

  /** Remove journal entries by criteria. */
  @SerialVersionUID(1L)
  final case class JournalDeletePayload(key: String, toSequenceNr: Long) extends JournalPayload

  /** Remove snapshot content. */
  @SerialVersionUID(1L)
  final case object SnapshotClearPayload extends SnapshotPayload {
    /** Java API. */
    def getInstance = SnapshotClearPayload
  }

  /** Remove snapshot content for persistence id. */
  @SerialVersionUID(1L)
  final case class SnapshotClearPayload(persistenceId: String) extends SnapshotPayload

  /** Create serialized snapshot by key. */
  @SerialVersionUID(1L)
  final case class SnapshotCreatePayload(key: Key, value: Value) extends SnapshotPayload {
    override def equals(o: Any): Boolean = o match {
      case that: SnapshotCreatePayload =>
        if (!java.util.Arrays.equals(this.key, that.key)) return false
        if (!java.util.Arrays.equals(this.value, that.value)) return false
        true
      case _ =>
        false
    }
  }

  final case object SnapshotCreatePayload {
    def apply(metadata: SnapshotMetadata, payload: AnyRef)(implicit extension: Serialization): SnapshotCreatePayload = {
      SnapshotCreatePayload(SnapshotMetadataSerializer.encode(metadata), SnapshotPayloadSerializer.encode(payload))
    }
    def unapply(payload: SnapshotCreatePayload)(implicit extension: Serialization): Option[(SnapshotMetadata, AnyRef)] = {
      Some((SnapshotMetadataSerializer.decode(payload.key), SnapshotPayloadSerializer.decode(payload.value)))
    }
  }

  /** Remove serialized snapshot by key. */
  @SerialVersionUID(1L)
  final case class SnapshotRemovePayload(key: Key) extends SnapshotPayload {
    override def equals(o: Any): Boolean = o match {
      case that: SnapshotRemovePayload =>
        if (!java.util.Arrays.equals(this.key, that.key)) return false
        true
      case _ =>
        false
    }
  }

  final case object SnapshotRemovePayload {
    def apply(metadata: SnapshotMetadata): SnapshotRemovePayload = {
      SnapshotRemovePayload(SnapshotMetadataSerializer.encode(metadata))
    }
    def unapply(payload: SnapshotRemovePayload, tester: Boolean = true): Option[SnapshotMetadata] = {
      Some((SnapshotMetadataSerializer.decode(payload.key)))
    }
  }

}

/** Cluster data replication manager. */
private[chronicle] class ReplicationCoordinator extends PluginActor {
  import ReplicationProtocol._
  import ReplicationCoordinator._
  import Member.addressOrdering
  import context._
  import settings._

  /** Cluster extension. */
  val cluster = Cluster(system)

  /** Current node cluster address. */
  val localAddress = cluster.selfAddress

  /** Remote cluster replication members. */
  var replicas: immutable.SortedMap[Address, Replica] = immutable.SortedMap.empty

  override def preStart() = {
    super.preStart()
    cluster.subscribe(self, classOf[MemberEvent], classOf[ReachabilityEvent])
  }

  override def postStop() = {
    cluster.unsubscribe(self)
    super.postStop()
  }

  override def receive: Receive = {
    case command: ConsumeCommand      => payloadConsume(command)
    case command: PublishCommand      => payloadPublish(command)
    case MemberUp(member)             => memberRegister(member)
    case ReachableMember(member)      => memberRegister(member)
    case MemberExited(member)         => memberUnregister(member)
    case MemberRemoved(member, _)     => memberUnregister(member)
    case UnreachableMember(member)    => memberUnregister(member)
    case message: CurrentClusterState => processState(message)
    case _                            => // Ignore.
  }

  /** Replica detection based on cluster node role. */
  def isReplica(member: Member): Boolean = {
    member.address != localAddress && member.roles.contains(extension.replicator.role)
  }

  /** Interested in live cluster nodes only. */
  def isStatusUp(member: Member): Boolean = {
    member.status == MemberStatus.Up
  }

  /** Access remote replicator as a sibling selection. */
  def selection(member: Member): ActorSelection = {
    context.actorSelection(self.path.toStringWithAddress(member.address))
  }

  /** Cluster replication member addition. */
  def memberRegister(member: Member): Unit = if (isReplica(member) && isStatusUp(member)) {
    replicas += member.address -> Replica(member.address, selection(member))
  }

  /** Cluster replication member removal. */
  def memberUnregister(member: Member): Unit = if (isReplica(member)) {
    replicas -= member.address
  }

  /** Apply initial subscriber cluster state. */
  def processState(message: CurrentClusterState): Unit = {
    message.members foreach { memberRegister(_) }
  }

  /** Transmit outgoing local content to remote replica members. */
  def payloadPublish(message: PublishCommand): Unit = {
    val delivery = ConsumeCommand(message.payload)
    replicas foreach { case (address, replica) => replica.selection ! delivery }
  }

  /** Distribute incoming remote content to local participants. */
  def payloadConsume(message: ConsumeCommand): Unit = {
    chronicleExtension.consume(message)
  }

}

/** Cluster data replication manager provider. */
private[chronicle] object ReplicationCoordinator {

  /** Descriptor of a cluster node running [[ReplicationCoordinator]]. */
  case class Replica(address: Address, selection: ActorSelection)

  /** [[ReplicationCoordinator]] actor provider. */
  def apply(): Props = Props[ReplicationCoordinator]

}
