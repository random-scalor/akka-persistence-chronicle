/**
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.persistence.chronicle

import scala.concurrent.Future
import akka.actor.actorRef2Scala
import akka.persistence.SelectedSnapshot
import akka.persistence.SnapshotMetadata
import akka.persistence.SnapshotSelectionCriteria
import akka.persistence.chronicle.ChronicleProvider.MapObserver
import akka.persistence.snapshot.SnapshotStore
import java.nio.ByteBuffer
import akka.persistence.Protocol
import akka.persistence.SaveSnapshotSuccess
import akka.persistence.SaveSnapshotFailure

/** Chronicle snapshot actor components. */
private[chronicle] object ChronicleSnapshot {
  import PluginActor._

  /** Apply snapshot selection criteria to snapshot metadata. */
  def selection(metadata: SnapshotMetadata, criteria: SnapshotSelectionCriteria): Boolean = {
    metadata.sequenceNr <= criteria.maxSequenceNr && metadata.timestamp <= criteria.maxTimestamp
  }

  /** Use reverse sort order by time and sequence. */
  val metadataOrdering = new Ordering[SnapshotMetadata] {
    override def compare(one: SnapshotMetadata, two: SnapshotMetadata): Int =
      if (one.timestamp < two.timestamp) +1
      else if (one.timestamp > two.timestamp) -1
      else if (one.sequenceNr < two.sequenceNr) +1
      else if (one.sequenceNr > two.sequenceNr) -1
      else 0
  }

}

/** Chronicle snapshot actor base trait. */
private[chronicle] trait ChronicleSnapshot extends SnapshotStore with PluginActor {
  import PluginActor._
  import ChronicleSnapshot._
  import ChronicleProvider._
  import ExtensionProtocol._
  import ReplicationProtocol._
  import ExtensionSerializer._
  import context._
  import settings._

  /** Runtime cache of stored snapshots metadata for given persistence id. */
  private val indexMap = new java.util.concurrent.ConcurrentHashMap[String, List[SnapshotMetadata]]()

  /** Underlying persistent snapshot content map by serialized metadata. */
  private val chronicleMap = {
    ChronicleProvider.createMap[Key, Value](snapshotStore.contentFile, snapshotStore.chronicleMap)
  }

  /** Observer of operations in underlying snapshot map. */
  protected val contentObserver = new MapObserver[Key, Value] {
    override def onGet(key: Key, value: Value) = ()
    override def onPut(key: Key, value: Value) = payloadPublish(SnapshotCreatePayload(key, value))
    override def onRemove(key: Key, value: Value) = payloadPublish(SnapshotRemovePayload(key))
  }

  /** Observed persistent snapshot content map. */
  protected val contentMap = if (chronicleExtension.clusterEnable)
    ObservedMap(chronicleMap, contentObserver)
  else
    UnobservedMap(chronicleMap)

  override def preStart() = {
    super.preStart()
    open()
    become(recieveFilter)
  }
  override def postStop() = {
    unbecome()
    close()
    super.postStop()
  }

  /** Count persistence id (number of serviced actors). */
  def count: Long = indexMap.size

  /** Count all stored snapshots for all actors.*/
  def chronicleCount: Long = chronicleMap.synchronized(chronicleMap.longSize)

  /** Count stored snapshots for actor persistence id.*/
  def chronicleCount(persistenceId: String): Long = {
    val metaList = indexMap.get(persistenceId)
    if (metaList == null) 0 else metaList.size
  }

  /** Differentiate replication vs persistence commands. */
  def recieveFilter: Receive = {
    case message: Protocol.Message    => receive(message)
    case message: CoordinationCommand => coordinate(message)
    case message: ConsumeCommand      => payloadConsume(message)
  }

  /** Apply Journal/Snapshot coordination command. */
  def coordinate(message: CoordinationCommand): Unit = message match {
    case SnapshotRestartCommand => throw new RuntimeException("SnapshotRestartCommand")
    case _                      => log.error(s"Wrong message: ${message}")
  }

  /** Apply incoming replication payload to unobserved underlying map. */
  def payloadConsume(message: ConsumeCommand) = {
    message.payload match {
      case payload: SnapshotCreatePayload      => consumeCreate(payload)
      case payload: SnapshotRemovePayload      => consumeRemove(payload)
      case SnapshotClearPayload(persistenceId) => clear(persistenceId)
      case SnapshotClearPayload                => clear()
      case _                                   => log.error(s"Wrong message: ${message}")
    }
  }

  /** React to snapshot create events. */
  protected def snapshotCreated(metadata: SnapshotMetadata) {
    if (rotationManager.rotateEnable) {
      chronicleExtension.journalActor ! SnapshotCreatedCommand(metadata)
    }
  }

  /** Unobservable content creation. */
  def consumeCreate(payload: SnapshotCreatePayload): Unit = {
    import payload._
    chronicleMap.put(key, value)
    val metadata = keyDeserialize(key)
    indexInsert(metadata)
    snapshotCreated(metadata)
  }

  /** Unobservable content removal. */
  def consumeRemove(payload: SnapshotRemovePayload): Unit = {
    import payload._
    chronicleMap.remove(key)
    val metadata = keyDeserialize(key)
    indexRemove(metadata)
  }

  /** Snapshot content map key serialization. */
  def keySerialize(key: SnapshotMetadata): Key = SnapshotMetadataSerializer.encode(key)
  def keyDeserialize(key: Key): SnapshotMetadata = SnapshotMetadataSerializer.decode(key)

  /** Snapshot content map value serialization. */
  def valueSerialize(value: AnyRef): Value = SnapshotPayloadSerializer.encode(value)
  def valueDeserialize(value: Value): AnyRef = SnapshotPayloadSerializer.decode(value)

  /** Build snapshot index. */
  def open(): Unit = {
    val iterator = chronicleMap.keySet.iterator
    while (iterator.hasNext) {
      val key = iterator.next
      indexInsert(keyDeserialize(key))
    }
    val size = indexMap.size
    log.info(s"Opened ${size} snapshots.")
    systemPublish(PluginSnapshotOpenNotification)
  }

  /** Release snapshot JVM resources. */
  def close(): Unit = {
    val size = indexMap.size
    indexMap.clear()
    chronicleMap.close()
    log.info(s"Closed ${size} snapshots.")
    systemPublish(PluginSnapshotCloseNotification)
  }

  /** Empty snapshots by marking entries deleted. */
  def clear(): Unit = {
    val size = indexMap.size
    indexMap.clear()
    chronicleMap.clear()
    //snapshotStore.contentFile.delete()
    log.warning(s"Cleared ${size} snapshots.")
    systemPublish(PluginSnapshotClearNotification)
  }

  /** Empty snapshot by marking selected entries as deleted. */
  def clear(persistenceId: String): Unit = {
    val metaList = indexMap.remove(persistenceId)
    if (metaList != null) {
      metaList.foreach { metadata =>
        val key = keySerialize(metadata)
        val value = contentMap.remove(key)
      }
      systemPublish(PluginSnapshotClearNotification(persistenceId))
    }
  }

  /** Find snapshot metadata for persistence id. */
  def indexLoad(persistenceId: String): List[SnapshotMetadata] = {
    val metaList = indexMap.get(persistenceId)
    if (metaList == null) Nil else metaList
  }

  /** Update snapshot metadata list for persistence id. */
  def indexSave(persistenceId: String, metaList: List[SnapshotMetadata]): Unit = {
    indexMap.put(persistenceId, metaList.sorted(metadataOrdering))
  }

  /** Prepend an entry into metadata list. */
  def indexInsert(metadata: SnapshotMetadata): Unit = {
    import metadata._
    indexSave(persistenceId, metadata :: indexLoad(persistenceId))
  }

  /** Remove an entry from metadata list. */
  def indexRemove(metadata: SnapshotMetadata): Unit = {
    import metadata._
    val update = indexLoad(persistenceId) filterNot { _ == metadata }
    indexSave(persistenceId, update)
  }

}

/** Chronicle snapshot actor with cluster data replication. */
private[chronicle] class ChronicleSnapshotStore extends ChronicleSnapshot {
  import ChronicleSnapshot._
  import context._
  import settings._

  /** Observable content creation. */
  def publishCreate(metadata: SnapshotMetadata, snapshot: Any): Unit = {
    val key = keySerialize(metadata)
    val value = valueSerialize(snapshot.asInstanceOf[AnyRef])
    contentMap.put(key, value)
  }

  /** Observable content deletion. */
  def publishDelete(metadata: SnapshotMetadata): Boolean = {
    val key = keySerialize(metadata)
    val value = contentMap.remove(key)
    null != value
  }

  /** Observable content extraction. */
  def publishExtract(metadata: SnapshotMetadata): SelectedSnapshot = {
    val key = keySerialize(metadata)
    val value = contentMap.get(key)
    require(value != null, s"Missing entry for key: ${key}")
    SelectedSnapshot(metadata, valueDeserialize(value))
  }

  //

  def delete(persistenceId: String, criteria: SnapshotSelectionCriteria): Unit = {
    val (delete, update) = indexLoad(persistenceId) partition { selection(_, criteria) }
    delete foreach { publishDelete(_) }
    indexSave(persistenceId, update)
  }

  def delete(metadata: SnapshotMetadata): Unit = {
    publishDelete(metadata)
    indexRemove(metadata)
  }

  def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = Future {
    indexLoad(persistenceId) find { selection(_, criteria) } map { publishExtract(_) }
  }

  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = Future {
    import metadata._
    val (update, delete) = indexLoad(persistenceId) splitAt { snapshotStore.limit - 1 }
    delete foreach { publishDelete(_) }
    publishCreate(metadata, snapshot)
    indexSave(persistenceId, metadata :: update)
  }

  def saved(metadata: SnapshotMetadata): Unit = {
    snapshotCreated(metadata)
  }

}
