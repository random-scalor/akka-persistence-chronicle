/**
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.persistence.chronicle

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.Charset

import scala.collection.immutable.IntMap

import akka.actor.ActorRef
import akka.actor.ActorRefProvider
import akka.actor.ExtendedActorSystem
import akka.persistence.PersistentRepr
import akka.persistence.SnapshotMetadata

import akka.serialization.Serialization
import akka.serialization.Serializer
import akka.util.ByteStringBuilder

/** Serialization provider for [[ReplicationProtocol]] messages. */
private[chronicle] class ExtensionSerializer(system: ExtendedActorSystem) extends Serializer with Settings {
  import ExtensionSerializer._
  import ReplicationProtocol._

  override def chronicleConfig = system.settings.config.getConfig(chroniclePath)
  val settings = chronicleSettings

  override def identifier: Int = settings.serializer.identifier

  override def includeManifest: Boolean = false

  override def fromBinary(content: Content, manifest: Option[Class[_]]): AnyRef = {
    val buffer = ByteBuffer.wrap(content).order(order)
    val id = buffer.getInt
    val codec = locateCodec(id)
    codec.decode(buffer)
  }

  override def toBinary(instance: AnyRef): Content = {
    val rope = new ByteStringBuilder
    val id = classId(instance)
    val codec = locateCodec(id)
    rope.putInt(id)
    codec.encode(rope, instance)
    rope.result.toArray
  }

  //

  /** Message codec mapping. */
  val knownTypes: Seq[(Class[_], Codec)] = Seq( //
    (classOf[SnapshotMetadata], SnapshotMetadataCodec()),
    (JournalClearPayload.getClass, JournalClearPayloadCodec()),
    (SnapshotClearPayload.getClass, SnapshotClearPayloadCodec()),
    (classOf[JournalClearPayload], JournalClearSingleCodec()),
    (classOf[JournalRotatePayload], JournalRotatePayloadCodec()),
    (classOf[JournalAppendPayload], JournalAppendPayloadCodec()),
    (classOf[JournalDeletePayload], JournalDeletePayloadCodec()),
    (classOf[SnapshotClearPayload], SnapshotClearSingleCodec()),
    (classOf[SnapshotCreatePayload], SnapshotCreatePayloadCodec()),
    (classOf[SnapshotRemovePayload], SnapshotRemovePayloadCodec()),
    (classOf[PublishCommand], PublishCommandCodec()),
    (classOf[ConsumeCommand], ConsumeCommandCodec())
  )

  /** Store message codecs by class id. */
  val codecMap: IntMap[Codec] = IntMap {
    knownTypes.map { case (klaz, codec) => (classId(klaz), codec) }.toSeq: _*
  }

  /** Verify build result. */
  require(knownTypes.size == codecMap.size, "Message class id collision.")

  /** Find message codec by class id. */
  def locateCodec(id: Int) = try { codecMap(id) } catch {
    case e: Throwable => throw new IllegalArgumentException(s"Missing serialization codec for class id=${id}.")
  }

  /** Codec for [[ReplicationProtocol.Command]] */
  trait CommandCodec extends Codec {
    def encode(rope: ByteStringBuilder, instance: AnyRef) = {
      val payload = instance.asInstanceOf[Command].payload
      val id = classId(payload)
      val codec = locateCodec(id)
      rope.putInt(id)(order)
      codec.encode(rope, payload)
    }
    def decodePayload(buffer: ByteBuffer): Payload = {
      locateCodec(buffer.getInt).decode(buffer).asInstanceOf[Payload]
    }
  }

  /** Codec for [[ReplicationProtocol.PublishCommand]] */
  case class PublishCommandCodec() extends CommandCodec {
    def decode(buffer: ByteBuffer) = PublishCommand(decodePayload(buffer))
  }

  /** Codec for [[ReplicationProtocol.ConsumeCommand]] */
  case class ConsumeCommandCodec() extends CommandCodec {
    def decode(buffer: ByteBuffer) = ConsumeCommand(decodePayload(buffer))
  }

}

/** Serialization provider for [[Replication]] messages. */
private[chronicle] object ExtensionSerializer {
  import ReplicationProtocol._

  /** Serialized content. */
  type Content = Array[Byte]

  /** Encoding for [[ByteBuffer]] and [[ByteStringBuilder]] */
  implicit val order = ByteOrder.BIG_ENDIAN

  /** Default string encoding [[Charset]]. */
  implicit val UTF_8 = Charset.forName("UTF-8")

  /** Type serialization decoder/encoder. */
  trait Codec {
    /** Decode from [[ByteBuffer]] into instance. */
    def decode(buffer: ByteBuffer): AnyRef
    /** Encode from instance into [[ByteStringBuilder]] */
    def encode(rope: ByteStringBuilder, instance: AnyRef): Unit
  }

  /** Codec for case object instance. */
  trait ObjectCodec extends Codec {
    final override def encode(rope: ByteStringBuilder, instance: AnyRef) = ()
  }

  /** Use integer class identity representation. */
  def classId(instance: AnyRef): Int = classId(instance.getClass)

  /** Use integer class identity representation. */
  def classId(klaz: Class[_]): Int = klaz.getName.hashCode

  /** Array codec contract. */
  def arrayDecode(buffer: ByteBuffer): Content = {
    val array = Array.ofDim[Byte](buffer.getInt); buffer.get(array); array
  }

  /** Array codec contract. */
  def arrayEncode(rope: ByteStringBuilder, array: Content): Unit = {
    rope.putInt(array.length); rope.putBytes(array)
  }

  /** String codec contract. */
  def stringDecode(buffer: ByteBuffer): String = new String(arrayDecode(buffer), UTF_8)

  /** String codec contract. */
  def stringEncode(rope: ByteStringBuilder, text: String): Unit = arrayEncode(rope, text.getBytes(UTF_8))

  //

  /** Codec for [[SnapshotMetadata]] */
  case class SnapshotMetadataCodec() extends Codec {
    def decode(buffer: ByteBuffer) = {
      SnapshotMetadata(stringDecode(buffer), buffer.getLong, buffer.getLong)
    }
    def encode(rope: ByteStringBuilder, instance: AnyRef) = {
      val metadata = instance.asInstanceOf[SnapshotMetadata]
      stringEncode(rope, metadata.persistenceId)
      rope.putLong(metadata.sequenceNr)
      rope.putLong(metadata.timestamp)
    }
  }

  /** Codec for [[ReplicationProtocol.JournalClearPayload]] */
  case class JournalClearPayloadCodec() extends ObjectCodec {
    def decode(buffer: ByteBuffer) = JournalClearPayload
  }

  /** Codec for [[ReplicationProtocol.JournalClearPayload(persistenceId)]] */
  case class JournalClearSingleCodec() extends Codec {
    def decode(buffer: ByteBuffer) = {
      val persistenceId = stringDecode(buffer)
      JournalClearPayload(persistenceId)
    }
    def encode(rope: ByteStringBuilder, instance: AnyRef) = {
      val payload = instance.asInstanceOf[JournalClearPayload]
      stringEncode(rope, payload.persistenceId)
    }
  }

  /** Codec for [[ReplicationProtocol.JournalRotatePayloadCodec(persistenceId)]] */
  case class JournalRotatePayloadCodec() extends Codec {
    def decode(buffer: ByteBuffer) = {
      val persistenceId = stringDecode(buffer)
      JournalRotatePayload(persistenceId)
    }
    def encode(rope: ByteStringBuilder, instance: AnyRef) = {
      val payload = instance.asInstanceOf[JournalRotatePayload]
      stringEncode(rope, payload.persistenceId)
    }
  }

  /** Codec for [[ReplicationProtocol.JournalAppendPayload]] */
  case class JournalAppendPayloadCodec() extends Codec {
    def decode(buffer: ByteBuffer) = {
      var index = 0
      val count = buffer.getInt
      val list = Array.ofDim[Content](count)
      while (index < count) {
        list(index) = arrayDecode(buffer)
        index += 1
      }
      JournalAppendPayload(list)
    }
    def encode(rope: ByteStringBuilder, instance: AnyRef) = {
      val list = instance.asInstanceOf[JournalAppendPayload].list
      var index = 0
      val count = list.size
      rope.putInt(count)
      while (index < count) {
        arrayEncode(rope, list(index))
        index += 1
      }
    }
  }

  /** Codec for [[ReplicationProtocol.JournalDeletePayload]] */
  case class JournalDeletePayloadCodec() extends Codec {
    def decode(buffer: ByteBuffer) = {
      JournalDeletePayload(stringDecode(buffer), buffer.getLong)
    }
    def encode(rope: ByteStringBuilder, instance: AnyRef) = {
      val payload = instance.asInstanceOf[JournalDeletePayload]
      stringEncode(rope, payload.key); rope.putLong(payload.toSequenceNr)
    }
  }

  /** Codec for [[ReplicationProtocol.SnapshotClearPayload]] */
  case class SnapshotClearPayloadCodec() extends ObjectCodec {
    def decode(buffer: ByteBuffer) = SnapshotClearPayload
  }

  /** Codec for [[ReplicationProtocol.SnapshotClearPayload(persistenceId)]] */
  case class SnapshotClearSingleCodec() extends Codec {
    def decode(buffer: ByteBuffer) = {
      val persistenceId = stringDecode(buffer)
      SnapshotClearPayload(persistenceId)
    }
    def encode(rope: ByteStringBuilder, instance: AnyRef) = {
      val payload = instance.asInstanceOf[SnapshotClearPayload]
      stringEncode(rope, payload.persistenceId)
    }
  }

  /** Codec for [[ReplicationProtocol.SnapshotCreatePayload]] */
  case class SnapshotCreatePayloadCodec() extends Codec {
    def decode(buffer: ByteBuffer) = {
      SnapshotCreatePayload(arrayDecode(buffer), arrayDecode(buffer))
    }
    def encode(rope: ByteStringBuilder, instance: AnyRef) = {
      val payload = instance.asInstanceOf[SnapshotCreatePayload]
      arrayEncode(rope, payload.key); arrayEncode(rope, payload.value)
    }
  }

  /** Codec for [[ReplicationProtocol.SnapshotRemovePayload]] */
  case class SnapshotRemovePayloadCodec() extends Codec {
    def decode(buffer: ByteBuffer) = {
      SnapshotRemovePayload(arrayDecode(buffer))
    }
    def encode(rope: ByteStringBuilder, instance: AnyRef) = {
      val payload = instance.asInstanceOf[SnapshotRemovePayload]
      arrayEncode(rope, payload.key)
    }
  }

  /** Fast String codec contract. */
  object StringSerializer {
    def encode(buffer: ByteBuffer, text: String): Unit = {
      val size = text.length
      var index = 0
      buffer.putInt(size)
      while (index < size) {
        buffer.putChar(text.charAt(index)); index += 1
      }
    }
    def decode(buffer: ByteBuffer): String = {
      val size = buffer.getInt
      val array = Array.ofDim[Char](size)
      var index = 0
      while (index < size) {
        array(index) = buffer.getChar; index += 1
      }
      new String(array)
    }
  }

  /** Fast [[PersistentRepr]] codec. */
  object JournalMessageSerializer {
    final val FixedSize = 8 + 1 + 4
    def encode(message: PersistentRepr)(implicit extension: Serialization): Content = {
      import message._
      val sender = ""
      val payloadCodec = extension.serializerFor(payload.getClass)
      val binaryPayload = payloadCodec.toBinary(payload.asInstanceOf[AnyRef])
      val buffer = ByteBuffer.allocate(
        8 + 1 + 4 + persistenceId.length + 4 + sender.length + 4 + 4 + binaryPayload.length
      ).order(ByteOrder.nativeOrder)
      buffer.putLong(sequenceNr) // 8
      buffer.put(if (deleted) 1.toByte else 0.toByte) // 1
      StringSerializer.encode(buffer, persistenceId) // 4
      StringSerializer.encode(buffer, sender) // 4
      buffer.putInt(payloadCodec.identifier) // 4
      buffer.putInt(binaryPayload.length) // 4
      buffer.put(binaryPayload)
      buffer.array
    }
  }

  /** Fast [[SnapshotMetadata]] codec. */
  object SnapshotMetadataSerializer {
    final val FixedSize = 8 + 8 + 4
    def encode(metadata: SnapshotMetadata): Content = {
      val text = metadata.persistenceId
      val buffer = ByteBuffer.allocate(text.length * 2 + FixedSize)
      StringSerializer.encode(buffer, text)
      buffer.putLong(metadata.sequenceNr)
      buffer.putLong(metadata.timestamp)
      buffer.array
    }
    def decode(array: Content): SnapshotMetadata = {
      val buffer = ByteBuffer.wrap(array)
      SnapshotMetadata(StringSerializer.decode(buffer), buffer.getLong, buffer.getLong)
    }
  }

  /** Default Snapshot content serialization. */
  object SnapshotPayloadSerializer {
    def encode(payload: AnyRef)(implicit extension: Serialization): Content = {
      val serializer = extension.serializerFor(payload.getClass)
      val identifier = serializer.identifier
      val binaryPayload = serializer.toBinary(payload)
      val buffer = ByteBuffer.allocate(4 + binaryPayload.length)
      buffer.putInt(identifier)
      buffer.put(binaryPayload)
      buffer.array
    }
    def decode[T](content: Content)(implicit extension: Serialization): T = {
      val buffer = ByteBuffer.wrap(content)
      val identifier = buffer.getInt
      val binaryPayload = Array.ofDim[Byte](content.length - 4); buffer.get(binaryPayload)
      val serializer = extension.serializerByIdentity(identifier)
      serializer.fromBinary(binaryPayload).asInstanceOf[T]
    }
  }

  /** Actor reference codec with optional erasure. */
  case class ActorReferenceSerializer(provider: ActorRefProvider, persist: Boolean) {
    val defaultActorPath = ""
    val defaultActorReference = provider.resolveActorRef("resolve default sender to system dead letters")
    def encode(actor: ActorRef): String = {
      if (persist) actor.path.toSerializationFormat else defaultActorPath
    }
    def decode(actor: String): ActorRef = {
      if (persist) provider.resolveActorRef(actor) else defaultActorReference
    }
  }

  /** Bind serialized content with serialization id. */
  case class ContentWrapper(identifier: Int, content: Content)

  case object ContentWrapper {
    def apply(instance: AnyRef)(implicit extension: Serialization): ContentWrapper = {
      val serializer = extension.serializerFor(instance.getClass)
      ContentWrapper(serializer.identifier, serializer.toBinary(instance))
    }
    def unapply(wrapper: ContentWrapper)(implicit extension: Serialization): AnyRef = {
      val serializer = extension.serializerByIdentity(wrapper.identifier)
      serializer.fromBinary(wrapper.content)
    }
  }

}
