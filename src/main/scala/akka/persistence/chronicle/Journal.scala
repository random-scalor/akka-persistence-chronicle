/**
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.persistence.chronicle

import java.io.File
import java.nio.ByteBuffer
import java.nio.ByteOrder
import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.collection.JavaConverters.mapAsScalaConcurrentMapConverter
import scala.collection.Seq
import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import akka.actor.ActorRef
import akka.actor.actorRef2Scala
import akka.persistence.PersistentRepr
import akka.persistence.SnapshotMetadata
import akka.persistence.journal.AsyncRecovery
//import akka.persistence.journal.SyncWriteJournal
import akka.serialization.Serialization
import net.openhft.chronicle.Excerpt
import net.openhft.chronicle.ExcerptAppender
import net.openhft.chronicle.ExcerptCommon
import net.openhft.chronicle.ExcerptTailer
import net.openhft.lang.io.ByteBufferBytes
import net.openhft.lang.io.Bytes
import akka.persistence.Protocol
import akka.persistence.journal.AsyncWriteJournal

/** Chronicle journal actor components. */
private[chronicle] object ChronicleJournal {
  import Settings._
  import ExtensionSerializer._
  import ChronicleSegmentedQueue._

  /** Resolve persistence id from serialized content. */
  def persistenceIdentifier(message: Content): String = {
    /** [[Bytes]] is using native order. */
    val buffer = ByteBuffer.wrap(message).order(ByteOrder.nativeOrder)
    /** Version 1. Compatible. */
    //    val entry = JournalBufferBytes(buffer)
    //    entry.sequenceNr
    //    entry.deleted
    //    entry.persistenceId
    /** Version 2. Performance. */
    buffer.getLong // sequenceNr
    buffer.get // deleted
    StringSerializer.decode(buffer) // persistenceId
  }

  /** Journal entry serialization on top of [[Bytes]]. */
  trait JournalBytes {

    /** Underlying queue entry resource provider. */
    val excerpt: Bytes

    /** Persistence message sequence */
    def sequenceNr: Long = excerpt.readLong()
    def sequenceNr_=(value: Long): Unit = excerpt.writeLong(value)

    /** Persistence message status. */
    def deleted: Boolean = excerpt.readBoolean()
    def deleted_=(value: Boolean): Unit = excerpt.writeBoolean(value)

    /** Persistence actor persistence id. */
    def persistenceId: String = stringDecode()
    def persistenceId_=(value: String): Unit = stringEncode(value)

    /** Persistence message sender. */
    def sender(implicit serializer: ActorReferenceSerializer): ActorRef = {
      serializer.decode(stringDecode())
    }
    def sender_=(value: ActorRef)(implicit serializer: ActorReferenceSerializer): Unit = {
      stringEncode(serializer.encode(value))
    }

    /** Persistence message payload. */
    def payload(implicit extension: Serialization): AnyRef = {
      val id = excerpt.readInt
      val size = excerpt.readInt
      val array = Array.ofDim[Byte](size); excerpt.read(array)
      val serializer = extension.serializerByIdentity(id)
      serializer.fromBinary(array)
    }
    def payload_=(value: AnyRef)(implicit extension: Serialization): Unit = {
      val serializer = extension.serializerFor(value.getClass)
      val array = serializer.toBinary(value)
      val size = array.length
      val id = serializer.identifier
      excerpt.writeInt(id)
      excerpt.writeInt(size)
      excerpt.write(array)
    }

    /** Serialized entry content. */
    def message: Content = {
      val size = excerpt.position.toInt
      val value = Array.ofDim[Byte](size)
      excerpt.position(0)
      excerpt.readFully(value)
      value
    }
    def message_=(value: Content): Unit = {
      excerpt.position(0)
      excerpt.write(value)
    }

    /** Fast string codec. */
    def stringDecode(): String = {
      val count = excerpt.readInt
      val array = Array.ofDim[Char](count)
      excerpt.readFully(array)
      new String(array)
    }

    /** Fast string codec. */
    def stringEncode(value: String): Unit = {
      excerpt.writeInt(value.length)
      excerpt.writeChars(value)
    }

  }

  /** Journal entry serialization on top of [[ByteBuffer]]. */
  case class JournalBufferBytes(buffer: ByteBuffer) extends JournalBytes {
    val excerpt = new ByteBufferBytes(buffer)
  }

  /** Journal entry serialization on top of [[ExcerptCommon]]. */
  trait JournalExcerpt extends JournalBytes {
    val excerpt: ExcerptCommon
    def size(): Long = excerpt.size()
    def index(): Long = excerpt.index()
    def finish(): Unit = excerpt.finish()
    def close(): Unit = excerpt.close()
    def isEmpty(): Boolean = size() == 0
  }

  /** Journal entry for write-only access. */
  case class JournalAppender(excerpt: ExcerptAppender) extends JournalExcerpt {
    def start(): Unit = excerpt.startExcerpt()
    def finalIndex(): Long = excerpt.lastWrittenIndex()
  }

  /** Journal entry for read-only access. */
  case class JournalScanner(excerpt: ExcerptTailer) extends JournalExcerpt {
    def toStart(): Long = { excerpt.toStart(); excerpt.index }
    def toEnd(): Long = { excerpt.toEnd(); excerpt.index() }
    def moveIndex(): Boolean = excerpt.nextIndex()
  }

  /** Journal entry for random read/write access. */
  case class JournalNavigator(excerpt: Excerpt) extends JournalExcerpt {
    import Segment._
    def index(position: Long): Boolean = excerpt.index(position)
    def toStart(): Long = { excerpt.toStart(); excerpt.index }
    def toEnd(): Long = { excerpt.toEnd(); excerpt.index() }
    def moveIndex(): Boolean = excerpt.nextIndex()
    def recoverSequence(tracker: SequenceTracker): Unit = if (!isEmpty()) {
      require(tracker.sequenceStart == InvalidPosition && tracker.sequenceFinish == InvalidPosition,
        s"Tracker sequence is already initialized: ${tracker.sequenceStart}/${tracker.sequenceFinish}")
      toStart(); moveIndex(); tracker.updateSequence(sequenceNr)
      toEnd(); tracker.updateSequence(sequenceNr)
    }
  }

  /** Updatable SnapshotMetadata store with sequence discriminator. */
  case class SnapshotMetadataWrapper(segment: Segment) {
    /** Placeholder metadata. */
    final val referenceMetadata = SnapshotMetadata("", Long.MaxValue, Long.MaxValue)
    /** Updatable [[SnapshotMetadata]]. */
    private var snapshotMetadata: SnapshotMetadata = referenceMetadata
    /** Reset snapshot metadata to reference value. */
    def snapshotReset(): Unit = snapshotUpdate(referenceMetadata)
    /** Latest snapshot notification from snapshot store actor. */
    def snapshotUpdate(metadata: SnapshotMetadata): Unit = snapshotMetadata = metadata
    /** Sequence difference: how much journal is in advance of snapshot. */
    def snapshotDifference: Long = segment.sequenceFinish - snapshotMetadata.sequenceNr
  }

  /** Segmented journal queue for single persistence id. */
  case class JournalProvider(persistenceId: String, repository: Repository) extends Clearable {
    import repository._

    /** Current segment appender. */
    val appender = JournalAppender(current.appender)

    /** Current segment navigator. */
    val currentNavigator = JournalNavigator(current.navigator)
    /** Previous segment navigator. */
    val previousNavigator = JournalNavigator(previous.navigator)

    /** Updatable SnapshotMetadata store. */
    val snapshotWrapper = SnapshotMetadataWrapper(current)

    /** Recover first and last sequence from persistence. */
    def recoverSequence(): Unit = {
      previousNavigator.recoverSequence(previous)
      currentNavigator.recoverSequence(current)
    }
    /** Update current sequence tracker. */
    def updateSequence(sequence: Long) = current.updateSequence(sequence)

    /** Entry count in active queue segments. */
    def count = repository.count

    /** Rotate underlying queue segments. */
    def rotate = JournalProvider(this)

    override def close(): Unit = repository.close()
    override def clear(): Unit = repository.clear()
  }

  /** Segmented journal queue for single persistence id. */
  case object JournalProvider {

    /** Recover from repository persistence. */
    def apply(persistenceId: String, journal: JournalSettings, mapper: NamingMapper): JournalProvider = {
      val folder = new File(journal.folder, mapper.encode(persistenceId))
      val repository = Repository(folder, journal.chronicleQueue)
      val provider = JournalProvider(persistenceId, repository)
      provider.recoverSequence()
      provider
    }

    /** Rotate from existing queue provider. */
    def apply(provider: JournalProvider): JournalProvider = {
      import provider._
      JournalProvider(persistenceId, repository.rotate)
    }
  }

}

/** Chronicle journal actor base trait. */
private[chronicle] trait ChronicleJournal extends PluginActor {
  import PluginActor._
  import ChronicleJournal._
  import ExtensionProtocol._
  import ReplicationProtocol._
  import ExtensionSerializer._
  import context._
  import settings._

  /** Registry of journal queues by persistent id. */
  private val journalMap = new java.util.concurrent.ConcurrentHashMap[String, JournalProvider]()

  /** Actor reference codec with optional erasure. */
  private implicit val actorCodec = ActorReferenceSerializer(referenceProvider, journal.persistSender)

  /** Map persistence id to queue folder name. */
  private implicit val namingMapper = NamingMapper(system, journal.namingMapper)

  /** Count persistence id (number of serviced actors). */
  def count: Long = journalMap.size

  /** Count all stored journal entries for all actors.*/
  def chronicleCount: Long = {
    journalMap.asScala.foldLeft(0L) { case (count, (_, provider)) => count + provider.count }
  }

  /** Count stored journal entries for actor persistence id.*/
  def chronicleCount(persistenceId: String): Long = {
    val provider = journalMap.get(persistenceId)
    if (provider == null) 0 else provider.count
  }

  /** Locate existing journal for persistence id. */
  def locateJournal(persistenceId: String): JournalProvider = {
    journalMap.get(persistenceId)
  }

  /** Locate or recover journal for persistence id. */
  def provideJournal(persistenceId: String): JournalProvider = {
    val existing = journalMap.get(persistenceId)
    if (existing != null) existing else {
      val provider = JournalProvider(persistenceId, journal, namingMapper)
      journalMap.put(persistenceId, provider)
      provider
    }
  }

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

  /** Differentiate replication vs persistence commands. */
  def recieveFilter: Receive = {
    case message: Protocol.Message    => receive(message)
    case message: CoordinationCommand => coordinate(message)
    case message: ConsumeCommand      => payloadConsume(message)
  }

  /** Apply Journal/Snapshot coordination command. */
  def coordinate(message: CoordinationCommand): Unit = message match {
    case SnapshotCreatedCommand(metadata) => snapshotCreated(metadata)
    case JournalRotatedCommand(provider)  => journalRotated(provider)
    case JournalRestartCommand            => throw new RuntimeException("JournalRestartCommand")
    case _                                => log.error(s"Wrong message: ${message}")
  }

  /** Apply incoming replication payload to journal queue. */
  def payloadConsume(message: ConsumeCommand): Unit = {
    message.payload match {
      case payload: JournalAppendPayload       => consumeAppend(payload.list)
      case payload: JournalDeletePayload       => messageDelete(payload.key, payload.toSequenceNr)
      case JournalRotatePayload(persistenceId) => rotate(persistenceId)
      case JournalClearPayload(persistenceId)  => clear(persistenceId)
      case JournalClearPayload                 => clear()
      case _                                   => log.error(s"Wrong message: ${message}")
    }
  }

  /** Load journals form disk. */
  def open(): Unit = if (journal.folder.exists) {
    val list = journal.folder.list
    val size = list.size
    list foreach { folder =>
      val persistenceId = namingMapper.decode(folder)
      provideJournal(persistenceId)
    }
    log.info(s"Opened ${size} journals.")
    systemPublish(PluginJournalOpenNotification)
  }

  /** Release journals JVM resources. */
  def close(): Unit = {
    val size = journalMap.size
    journalMap.values.asScala foreach { _.close }
    journalMap.clear()
    log.info(s"Closed ${size} journals.")
    systemPublish(PluginJournalCloseNotification)
  }

  /** Total journal append count. */
  private val appendNumber = Counter()
  def appendCount = appendNumber.count

  /** Total journal rotation count. */
  private val rotateNumber = Counter()
  def rotateCount = rotateNumber.count

  /** Empty journals by removing disk files. */
  def clear(): Unit = {
    val size = journalMap.size
    journalMap.values.asScala foreach { _.clear }
    journalMap.clear()
    appendNumber.reset()
    rotateNumber.reset()
    log.warning(s"Cleared ${size} journals.")
    systemPublish(PluginJournalClearNotification)
  }

  /** Empty queues by removing queue disk files. */
  def clear(persistenceId: String): Unit = {
    val provider = journalMap.get(persistenceId)
    if (provider != null) {
      provider.clear()
      systemPublish(PluginJournalClearNotification(persistenceId))
    }
  }

  /** Rotate journal queue segments for given persistence id. */
  def rotate(persistenceId: String): Unit = {
    val provider = journalMap.get(persistenceId)
    if (provider != null) rotate(provider)
  }

  /** Rotate journal segments off-band.*/
  private def rotate(provider: JournalProvider): Unit = {
    system.scheduler.scheduleOnce(Duration.Zero) {
      self ! JournalRotatedCommand(provider.rotate)
    }
  }

  /** React to journal append events. */
  protected def journalAppended(provider: JournalProvider): Unit = {
    import provider._; import rotationManager._; import provider.snapshotWrapper._
    appendNumber.increment()
    if (rotateEnable && snapshotDifference > messageCount) {
      snapshotReset()
      ChronicleJournal.this.rotate(provider)
    }
  }

  /** React to journal rotated commands. */
  protected def journalRotated(provider: JournalProvider): Unit = {
    import provider._
    journalMap.put(persistenceId, provider)
    rotateNumber.increment()
    systemPublish(PluginJournalRotateNotification(persistenceId))
  }

  /** React to snapshot create events. */
  protected def snapshotCreated(metadata: SnapshotMetadata): Unit = {
    val provider = provideJournal(metadata.persistenceId)
    provider.snapshotWrapper.snapshotUpdate(metadata)
  }

  /** Persist incoming replicated serialized journal events. */
  def consumeAppend(list: Seq[Content]): Unit = {
    var index = 0
    val count = list.size
    while (index < count) {
      val message = list(index)
      // Extract identity.
      val buffer = ByteBuffer.wrap(message).order(ByteOrder.nativeOrder)
      val sequenceNr = buffer.getLong
      val deleted = buffer.get
      val persistenceId = StringSerializer.decode(buffer)
      val provider = provideJournal(persistenceId)
      // Persist in the queue.
      val appender = provider.appender
      messagePersist(appender, message)
      // Update sequence.
      provider.updateSequence(sequenceNr)
      // Notify append.
      journalAppended(provider)
      //
      index += 1
    }
  }

  /** Append serialized message at the end of the queue. */
  def messagePersist(appender: JournalAppender, message: Content): Unit = {
    appender.start()
    appender.message = message
    appender.finish()
  }

  /** Persist outgoing journal events and publish replication. */
  def persistMesageList(list: immutable.Seq[PersistentRepr]): Unit = {
    var index = 0
    val count = list.size
    val clusterEnable = chronicleExtension.clusterEnable
    val payloadList = if (clusterEnable) Array.ofDim[Content](count) else null
    while (index < count) {
      val message = list(index); import message._
      val provider = provideJournal(persistenceId)
      // Persist in the queue.
      val appender = provider.appender
      messagePersist(appender, message)
      // Prepare replication.
      if (clusterEnable) payloadList(index) = appender.message
      // Update sequence.
      provider.updateSequence(sequenceNr)
      // Notify append.
      journalAppended(provider)
      //
      index += 1
    }
    if (clusterEnable) payloadPublish(JournalAppendPayload(payloadList))
  }

  /** Append [[PersistentRepr]] message at the end of the queue. */
  def messagePersist(appender: JournalAppender, message: PersistentRepr): Unit = {
    import message._
    appender.start()
    appender.sequenceNr = sequenceNr
    appender.deleted = deleted
    appender.persistenceId = persistenceId
    appender.sender = message.sender
    appender.payload = payload.asInstanceOf[AnyRef]
    appender.finish()
  }

  /** Delete messages by scanning the queue. */
  def messageDelete(navigator: JournalNavigator, toSequenceNr: Long): Unit = {
    navigator.toStart()
    var loop = navigator.moveIndex()
    while (loop) {
      val sequenceNr = navigator.sequenceNr
      if (sequenceNr <= toSequenceNr) {
        navigator.deleted = true
        loop = navigator.moveIndex()
      } else {
        loop = false
      }
      navigator.finish()
    }
  }

  /** Delete messages by scanning the queue. */
  def messageDelete(persistenceId: String, toSequenceNr: Long): Unit = {
    val provider = provideJournal(persistenceId)
    import provider._
    if (!previousNavigator.isEmpty())
      messageDelete(previousNavigator, toSequenceNr)
    if (!currentNavigator.isEmpty())
      messageDelete(currentNavigator, toSequenceNr)
  }

  /** Message replay contract. */
  def messageReplay(navigator: JournalNavigator, fromSequenceNr: Long, toSequenceNr: Long,
    maximum: Long)(replayCallback: PersistentRepr => Unit): Long = {
    var counter = 0L
    var loop = navigator.moveIndex()
    while (loop) {
      val sequenceNr = navigator.sequenceNr
      if (sequenceNr < fromSequenceNr) {
        // Skip before range start.
        loop = navigator.moveIndex()
      } else if (fromSequenceNr <= sequenceNr && sequenceNr <= toSequenceNr && counter < maximum) {
        // Replay while in range and limit.
        val deleted = navigator.deleted
        val persistenceId = navigator.persistenceId
        val sender = navigator.sender
        val payload = navigator.payload
        val manifest = "" // TODO
        replayCallback(PersistentRepr(payload, sequenceNr, persistenceId, manifest, deleted, sender))
        counter += 1
        loop = navigator.moveIndex()
      } else {
        // Terminate when out of range or limit.
        loop = false
      }
      navigator.finish()
    }
    counter
  }

}

/** Journal entry asynchronous replay provider. */
private[chronicle] trait ChronicleJournalRecovery extends AsyncRecovery with ChronicleJournal {
  import ChronicleJournal._
  import context._
  import settings._

  // Note: thread context in Future is different form the actor
  // Rule: use "synchronized" on core actor state
  // Rule: do not create here any instances

  override def asyncReadHighestSequenceNr(
    persistenceId: String, fromSequenceNr: Long): Future[Long] = Future {
    val provider = locateJournal(persistenceId)
    import provider.repository._
    if (provider == null) 0 else {
      val previousFinish = previous.synchronized(previous.sequenceFinish)
      val currentFinish = current.synchronized(current.sequenceFinish)
      if (currentFinish > 0) currentFinish
      else if (previousFinish > 0) previousFinish
      else 0
    }
  }

  override def asyncReplayMessages(
    persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long,
    maximum: Long)(replayCallback: PersistentRepr => Unit): Future[Unit] = Future {
    val provider = locateJournal(persistenceId)
    import provider._; import provider.repository._
    if (provider != null) {
      val (previousStart, previousFinish) = previous.synchronized(previous.sequenceRange)
      if (fromSequenceNr <= previousFinish) {
        val position = fromSequenceNr - previousStart - 1
        if (position < 0) currentNavigator.toStart() else previousNavigator.index(position)
        messageReplay(previousNavigator, fromSequenceNr, toSequenceNr, maximum)(replayCallback)
      }
      val (currentStart, currentFinish) = current.synchronized(current.sequenceRange)
      if (currentStart <= toSequenceNr) {
        val position = fromSequenceNr - currentStart - 1
        if (position < 0) currentNavigator.toStart() else currentNavigator.index(position)
        messageReplay(currentNavigator, fromSequenceNr, toSequenceNr, maximum)(replayCallback)
      }
    }
  }

}

/** Journal implementation. */
private[chronicle] class ChronicleSyncJournal extends AsyncWriteJournal with ChronicleJournalRecovery {
  import ChronicleJournal._
  import ReplicationProtocol._

  /*override*/ def deleteMessagesTo(persistenceId: String, toSequenceNr: Long, permanent: Boolean): Unit = {
    messageDelete(persistenceId, toSequenceNr)
  }

  /*override*/ def writeMessages(messages: immutable.Seq[PersistentRepr]): Unit = {
    persistMesageList(messages)
  }

  def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): scala.concurrent.Future[Unit] = ???

  def asyncWriteMessages(messages: scala.collection.immutable.Seq[akka.persistence.AtomicWrite]): scala.concurrent.Future[scala.collection.immutable.Seq[scala.util.Try[Unit]]] = ???
}
