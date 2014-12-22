/**
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.persistence.chronicle

import akka.persistence.SnapshotMetadata

import scala.annotation.tailrec
import scala.collection.JavaConverters._

import java.io.File
import java.io.Closeable
import java.text.SimpleDateFormat
import java.util.Arrays
import java.util.UUID

import net.openhft.lang.io.Bytes
import net.openhft.chronicle.Chronicle
import net.openhft.chronicle.ChronicleQueueBuilder
import net.openhft.chronicle.Excerpt
import net.openhft.chronicle.ExcerptAppender
import net.openhft.chronicle.map.ChronicleMap
import net.openhft.chronicle.map.ChronicleMapBuilder
import net.openhft.lang.io.serialization.BytesMarshaller

/** Native chronicle component factory. */
private[chronicle] object ChronicleProvider {
  import Settings._

  /** React on map operations. */
  trait MapObserver[K, V] {
    def onGet(key: K, value: V): Unit = ()
    def onPut(key: K, value: V): Unit = ()
    def onRemove(key: K, value: V): Unit = ()
  }

  /** Minimal set of map operations. */
  trait MapContract[K, V] {
    def get(key: K): V
    def put(key: K, value: V): V
    def remove(key: K): V
  }

  /** Observe chronicle map operations. */
  case class ObservedMap[K, V](map: ChronicleMap[K, V], observer: MapObserver[K, V])
      extends MapContract[K, V] {
    override def get(key: K): V = {
      val value = map.get(key)
      observer.onGet(key, value)
      value
    }
    override def put(key: K, value: V): V = {
      val previous = map.put(key, value)
      observer.onPut(key, value)
      previous
    }
    override def remove(key: K): V = {
      val value = map.remove(key)
      observer.onRemove(key, value)
      value
    }
  }

  /** Chronicle map wrapper with empty observer. */
  case class UnobservedMap[K, V](map: ChronicleMap[K, V])
      extends MapContract[K, V] {
    override def get(key: K): V = map.get(key)
    override def put(key: K, value: V): V = map.put(key, value)
    override def remove(key: K): V = map.remove(key)
  }

  /** Provide configured chronicle map. */
  def createMap[K, V](store: File, settings: MapSettings)(implicit keyType: Manifest[K], valueType: Manifest[V]): ChronicleMap[K, V] = {
    import settings._
    val builder = ChronicleMapBuilder
      .of(keyType.runtimeClass, valueType.runtimeClass)
      .averageKeySize(averageKeySize)
      .averageValueSize(averageValueSize)
      .maxChunksPerEntry(maxChunksPerEntry)
    builder.createPersistedTo(store).asInstanceOf[ChronicleMap[K, V]]
  }

  /** Provide configured chronicle queue. */
  def createQueue(store: File, settings: QueueSettings): Chronicle = {
    import settings._
    val builder = ChronicleQueueBuilder.indexed(store)
      .synchronous(synchronous)
      .useCheckedExcerpt(useCheckedExcerpt)
      .indexBlockSize(indexBlockSize)
      .dataBlockSize(dataBlockSize)
      .messageCapacity(messageCapacity)
      .cacheLineSize(cacheLineSize)
    builder.build
  }

}

/** Chronicle map entry serialization. */
private[chronicle] object ChronicleMapMarshaller {
  import ExtensionSerializer._

  /** Fast snapshot content serialization. */
  case class ContentWrapperMarshaller() extends BytesMarshaller[ContentWrapper] {
    override def write(bytes: Bytes, wrapper: ContentWrapper): Unit = {
      import wrapper._
      bytes.writeInt(identifier)
      bytes.writeInt(content.length)
      bytes.write(content)
    }
    override def read(bytes: Bytes): ContentWrapper = {
      val identifier = bytes.readInt
      val length = bytes.readInt
      val content = Array.ofDim[Byte](length)
      bytes.readFully(content)
      ContentWrapper(identifier, content)
    }
    override def read(bytes: Bytes, wrapper: ContentWrapper): ContentWrapper = read(bytes)
  }

}

/** Chronicle queue with segment rotation. */
private[chronicle] object ChronicleSegmentedQueue {
  import Settings._

  /** Value for invalid index or sequence. */
  final val InvalidPosition = -1L

  /** Prefix of segment files: queue.index and queue.data */
  final val SegmentBaseName = "queue"

  /** Resource release trait. */
  trait Clearable extends Closeable {
    /** Release JVM resources, keep file system resources. */
    def close(): Unit
    /** Release JVM resources and remove file system resources. */
    def clear(): Unit
  }

  /** Format of a queue segment folder. */
  private val FolderFormat = "yyyy-MM-dd_hh-mm-ss_SSS"

  /** Default placeholder segment folder name. */
  private val FolderZero = format(0)

  /** Use time to name and sort segment folders. */
  private def format(timestamp: Long): String = {
    new SimpleDateFormat(FolderFormat).format(timestamp)
  }

  /** Track logical sequence to facilitate sequence to index resolution. */
  trait SequenceTracker {
    /** Verify if segment sequence is valid. */
    def hasSequence: Boolean
    /** First sequence number in this segment. */
    def sequenceStart: Long
    /** Last sequence number in this segment. */
    def sequenceFinish: Long
    /** Sequence range as tuple. */
    def sequenceRange: (Long, Long)
    /** Update first and last sequence number in the segment. */
    def updateSequence(sequence: Long): Unit
  }

  /** Default implementation of [[SequenceTracker]] */
  trait SequenceTrackerProvider extends SequenceTracker {
    private var _sequenceStart: Long = InvalidPosition
    private var _sequenceFinish: Long = InvalidPosition
    override def hasSequence: Boolean = _sequenceStart != InvalidPosition
    override def sequenceStart = _sequenceStart
    override def sequenceFinish = _sequenceFinish
    override def sequenceRange = (_sequenceStart, _sequenceFinish)
    override def updateSequence(sequence: Long) = {
      if (!hasSequence) _sequenceStart = sequence; _sequenceFinish = sequence
    }
  }

  /** Single queue segment with sequence tracking. */
  trait Segment extends Clearable with SequenceTrackerProvider {
    /** Location of queue files. */
    def folder: File
    /** Random access to underlying queue. */
    def navigator: Excerpt
    /** Append-only access to underlying queue. */
    def appender: ExcerptAppender
    /** Verify if queue contains any entries. */
    def isEmpty: Boolean
    /** Report number of entries stored in queue. */
    def count: Long
  }

  /** Single queue segment placeholder. */
  private case object EmptySegment extends Segment {
    def folder = throw new UnsupportedOperationException("folder")
    def navigator = throw new UnsupportedOperationException("navigator")
    def appender = throw new UnsupportedOperationException("appender")
    def isEmpty = true
    def count = 0
    def clear() = ()
    def close() = ()
  }

  /** Single queue segment on top of [[Chronicle]]. */
  private case class DefaultSegment(folder: File, queue: Chronicle) extends Segment {
    val navigator = queue.createExcerpt()
    val appender = queue.createAppender()
    def isEmpty: Boolean = !folder.exists
    def count: Long = appender.size
    override def close(): Unit = {
      navigator.close(); appender.close(); queue.close()
    }
    override def clear(): Unit = {
      close(); queue.clear(); folder.delete()
      //      require(isEmpty, s"Failed to delete segment folder: ${folder}")
    }
  }

  /** Single queue segment provider. */
  case object Segment {
    /** Provide empty segment. */
    def apply(): Segment = EmptySegment
    /** Restore segment from persistence. */
    def apply(folder: File, settings: QueueSettings): Segment = {
      folder.mkdirs()
      val basePath = new File(folder, SegmentBaseName)
      DefaultSegment(folder, ChronicleProvider.createQueue(basePath, settings))
    }
  }

  /** Combined segmented queue. */
  case class Repository(folder: File, settings: QueueSettings,
      archive: Segment, previous: Segment, current: Segment) extends Clearable {
    def isEmpty: Boolean = !folder.exists
    def count: Long = previous.count + current.count
    def rotate: Repository = Repository(this)
    override def close(): Unit = {
      archive.close(); previous.close(); current.close()
    }
    override def clear(): Unit = {
      archive.clear(); previous.clear(); current.clear()
      //      require(archive.isEmpty && previous.isEmpty && current.isEmpty, "Failed to delete segments.")
      folder.delete()
    }
  }

  /** Combined segmented queue provider. */
  case object Repository {

    /** Segment folder naming convention. */
    def segmentPath(folder: File, timestamp: String): File = new File(folder, timestamp)

    /** Segment folder naming convention. */
    def segmentPath(folder: File, timestamp: Long): File = new File(folder, format(timestamp))

    /** Rotate segments and release resources. */
    def apply(repository: Repository): Repository = {
      import repository._
      archive.clear()
      val path = segmentPath(folder, System.currentTimeMillis)
      require(!path.exists, s"Duplicate segment path: ${path}")
      copy(archive = previous, previous = current, current = Segment(path, settings))
    }

    /** Delete folder content recursively. */
    def deletePath(path: File): Unit = if (path.exists) {
      if (path.isDirectory) path.listFiles foreach { deletePath(_) }; path.delete()
    }

    /** Restore repository from persistence. */
    def apply(folder: File, settings: QueueSettings): Repository = {
      folder.mkdirs()
      val (archive, previous, current) = {
        val zero = FolderZero
        val list = folder.list.toList.sorted.reverse
        val (latest, obsolete) = list splitAt 2
        val (result, itemzero) = latest span { _ != zero }
        (obsolete ++ itemzero) foreach { path => deletePath(segmentPath(folder, path)) }
        val time = System.currentTimeMillis
        val (past, next) = result match {
          case List()           => (zero, format(time))
          case List(curr)       => (zero, curr)
          case List(curr, prev) => (prev, curr)
        }
        require(past != next, s"Duplicate segment path: ${past}")
        (Segment(), Segment(segmentPath(folder, past), settings), Segment(segmentPath(folder, next), settings))
      }
      Repository(folder, settings, archive, previous, current)
    }
  }

}
