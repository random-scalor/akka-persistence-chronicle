/**
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.persistence.chronicle;

import java.util.UUID

import akka.persistence.SaveSnapshotSuccess
import akka.persistence.SnapshotMetadata
import akka.persistence.SnapshotProtocol.SaveSnapshot
import akka.persistence.snapshot.SnapshotStoreSpec
import akka.testkit.TestProbe

import com.typesafe.config.ConfigFactory

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class ChronicleSnapshotStoreSpec extends SnapshotStoreSpec {

  final val snapshotLimit = 10

  override lazy val config = ConfigFactory.parseString(s"""
    akka.persistence {
      journal {
        plugin = "akka.persistence.journal.inmem"
      }
      snapshot-store {
        plugin = "akka.persistence.chronicle.snapshot-store"
      }
      chronicle {
        snapshot-store {
          folder = target/store/snapshot
          limit = ${snapshotLimit}
        }
      }
    }
  """).withFallback(ConfigFactory.load)

  "Snapshot store" must {
    "limit number of snapshots" in {

      val customPid = s"custom-${UUID.randomUUID().toString}"

      val senderProbe = new TestProbe(system)

      def writeCustomSnapshots(pid: String = "custom", prefix: String = "snapshot", count: Int = 10): Seq[SnapshotMetadata] = {
        val current = System.currentTimeMillis
        1 to count map { index =>
          val metadata = SnapshotMetadata(pid, index + 10, current + index)
          val snapshot = s"${prefix}-${index}"
          snapshotStore.tell(SaveSnapshot(metadata, snapshot), senderProbe.ref)
          senderProbe.expectMsgPF() { case SaveSnapshotSuccess(metainfo) => metainfo }
        }
      }

      val snapshotActor = PluginTest.extractActor[ChronicleSnapshotStore](snapshotStore)

      def indexSize = snapshotActor.count
      def contentSize = snapshotActor.chronicleCount

      val indexSize1 = indexSize
      val contentSize1 = contentSize

      writeCustomSnapshots(pid = customPid, count = snapshotLimit * 2)

      val indexSize2 = indexSize
      val contentSize2 = contentSize

      indexSize2 should be(indexSize1 + 1)
      contentSize2 should be(contentSize1 + snapshotLimit)

    }
  }

}
