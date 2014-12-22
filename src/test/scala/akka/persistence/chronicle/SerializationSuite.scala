/**
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.persistence.chronicle

import akka.actor.ActorSystem
import akka.persistence.SnapshotMetadata
import akka.serialization.SerializationExtension

import org.scalatest.Matchers
import org.scalatest.WordSpecLike

import com.typesafe.config.ConfigFactory

import ExtensionSerializer.SnapshotMetadataSerializer
import ExtensionSerializer.SnapshotPayloadSerializer
import ReplicationProtocol.ConsumeCommand
import ReplicationProtocol.JournalAppendPayload
import ReplicationProtocol.JournalClearPayload
import ReplicationProtocol.JournalDeletePayload
import ReplicationProtocol.JournalRotatePayload
import ReplicationProtocol.PublishCommand
import ReplicationProtocol.SnapshotClearPayload
import ReplicationProtocol.SnapshotCreatePayload
import ReplicationProtocol.SnapshotRemovePayload

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class SerializerSpec extends WordSpecLike with Matchers {
  import ReplicationProtocol._

  val config = ConfigFactory.parseString(s"""
    # Disable plugin.
    akka.persistence {
      journal {
        plugin = "akka.persistence.journal.inmem"
      }
      snapshot-store {
        plugin = "akka.persistence.snapshot-store.local"
      }
    }
  """).withFallback(ConfigFactory.load)

  val system = ActorSystem("default", config)

  implicit val extension = SerializationExtension(system)

  val messageList = Seq(
    JournalClearPayload,
    SnapshotClearPayload,
    JournalClearPayload("meta-0"),
    JournalRotatePayload("meta-0"),
    JournalAppendPayload(Seq.empty),
    JournalAppendPayload(Array(Array[Byte](1, 2, 3))),
    JournalAppendPayload(Seq(Array[Byte](1, 2, 3), Array[Byte](4, 5, 6))),
    JournalDeletePayload("persistence-id", 123),
    SnapshotClearPayload("meta-0"),
    SnapshotCreatePayload("meta-1".getBytes, Array[Byte](1, 2, 3, 4, 5, 6, 7)),
    SnapshotRemovePayload("meta-2".getBytes),
    PublishCommand(JournalClearPayload),
    ConsumeCommand(SnapshotClearPayload),
    PublishCommand(JournalAppendPayload(Seq.empty)),
    ConsumeCommand(JournalAppendPayload(Seq.empty)),
    PublishCommand(JournalAppendPayload(Array(Array[Byte](1, 2, 3)))),
    ConsumeCommand(JournalAppendPayload(Array(Array[Byte](1, 2, 3)))),
    PublishCommand(JournalAppendPayload(Seq(Array[Byte](1, 2, 3), Array[Byte](4, 5, 6)))),
    ConsumeCommand(JournalAppendPayload(Seq(Array[Byte](1, 2, 3), Array[Byte](4, 5, 6)))),
    PublishCommand(JournalDeletePayload("meta-1", 123)),
    ConsumeCommand(JournalDeletePayload("meta-2", 123)),
    PublishCommand(SnapshotCreatePayload("meta-3".getBytes, Array[Byte](1, 2, 3, 4, 5, 6, 7))),
    ConsumeCommand(SnapshotCreatePayload("meta-4".getBytes, Array[Byte](1, 2, 3, 4, 5, 6, 7))),
    PublishCommand(SnapshotRemovePayload("meta-5".getBytes)),
    ConsumeCommand(SnapshotRemovePayload("meta-6".getBytes)),
    ConsumeCommand(JournalClearPayload)
  )

  def verify(source: AnyRef) = {
    val serializer = extension.findSerializerFor(source)
    val content = serializer.toBinary(source)
    val target = serializer.fromBinary(content)
    source should be(target)

  }

  "serializer" must {

    "verify command equality" in {

      PublishCommand(JournalAppendPayload(Seq(Array[Byte](1, 2, 3)))) should be(PublishCommand(JournalAppendPayload(List(Array[Byte](1, 2, 3)))))
      PublishCommand(JournalAppendPayload(Seq(Array[Byte](1, 2, 3)))) should not be (PublishCommand(JournalAppendPayload(List(Array[Byte](1, 4, 3)))))

      PublishCommand(SnapshotCreatePayload("meta-1".getBytes, Array[Byte](1, 2, 3, 4, 5, 6, 7))) should be(PublishCommand(SnapshotCreatePayload("meta-1".getBytes, Array[Byte](1, 2, 3, 4, 5, 6, 7))))
      PublishCommand(SnapshotCreatePayload("meta-2".getBytes, Array[Byte](1, 2, 3, 8, 5, 6, 7))) should not be (PublishCommand(SnapshotCreatePayload("meta-2".getBytes, Array[Byte](1, 2, 3, 4, 5, 6, 7))))

      PublishCommand(SnapshotRemovePayload("meta-1".getBytes)) should be(PublishCommand(SnapshotRemovePayload("meta-1".getBytes)))
      PublishCommand(SnapshotRemovePayload("meta-1".getBytes)) should not be (PublishCommand(SnapshotRemovePayload("meta-2".getBytes)))

    }

    "provide round trip serialization" in {
      messageList foreach { verify }
    }

    "verify SnapshotMetadataSerializer" in {

      val sourceList = Seq(
        SnapshotMetadata("", 0, 0),
        SnapshotMetadata("persistence id", 123, 456)
      )

      def verify(source: SnapshotMetadata) = {
        val content = SnapshotMetadataSerializer.encode(source)
        val target = SnapshotMetadataSerializer.decode(content)
        source should be(target)
      }

      sourceList foreach (verify(_))

    }

    "verify SnapshotPayloadSerializer" in {
      val source = "snapshot paylaod"
      val content = SnapshotPayloadSerializer.encode(source)
      val target = SnapshotPayloadSerializer.decode[String](content)
      source should be(target)
    }
  }

}
