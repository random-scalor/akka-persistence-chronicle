/**
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.persistence.chronicle

import java.io.File

import scala.language.postfixOps

import org.scalatest.Matchers
import org.scalatest.WordSpecLike

import com.typesafe.config.ConfigFactory

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class SettingsSpec extends WordSpecLike with Matchers with Settings {

  override def chronicleConfig = ConfigFactory.load("reference.conf").getConfig(chroniclePath)

  "configuration" must {

    "contain default settings" in {

      val settings = chronicleSettings
      import settings._

      val userDir = System.getProperty("user.dir")

      val K = 1024
      val M = K * K

      extension.clusterEnable should be(false)
      extension.exposeReplicationStream should be(false)
      extension.exposeNotificationStream should be(false)
      extension.replicator.name should be("chronicle-replicator")
      extension.replicator.role should be("chronicle-persistence")

      serializer.identifier should be(888)

      journal.provider should be(classOf[ChronicleSyncJournal].getName)
      journal.folder should be(new File(s"${userDir}/store/journal"))
      journal.persistSender should be(false)
      journal.namingMapper should be(classOf[DirectNamingMapper].getName)

      import journal.chronicleQueue

      chronicleQueue.synchronous should be(false)
      chronicleQueue.useCheckedExcerpt should be(false)
      chronicleQueue.messageCapacity should be(256)
      chronicleQueue.indexBlockSize should be(4 * M)
      chronicleQueue.dataBlockSize should be(16 * M)
      chronicleQueue.cacheLineSize should be(64)

      snapshotStore.provider should be(classOf[ChronicleSnapshotStore].getName)
      snapshotStore.folder should be(new File(s"${userDir}/store/snapshot"))
      snapshotStore.limit should be(2)

      import snapshotStore.chronicleMap

      chronicleMap.count should be(16 * K)
      chronicleMap.averageKeySize should be(128)
      chronicleMap.averageValueSize should be(64 * K)
      chronicleMap.maxChunksPerEntry should be(64)

      rotationManager.rotateEnable should be(true)
      rotationManager.messageCount should be(16 * K)

    }

  }

}
