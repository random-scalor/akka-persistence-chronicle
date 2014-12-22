/**
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.persistence.chronicle

import java.io.File

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

/** Chronicle plugin settings. */
trait Settings {

  import Settings._

  def chroniclePath = "akka.persistence.chronicle"

  def chronicleConfig = ConfigFactory.load().getConfig(chroniclePath)

  def chronicleSettings = PluginSettings(chronicleConfig)

}

/** Chronicle plugin settings. Please see 'src/main/resources/reference.conf' */
object Settings {

  case class PluginSettings(config: Config) {
    val extension = ExtensionSettings(config.getConfig("extension"))
    val serializer = SerializerSettings(config.getConfig("serializer"))
    val journal = JournalSettings(config.getConfig("journal"))
    val snapshotStore = SnapshotSettings(config.getConfig("snapshot-store"))
    val rotationManager = RotationManager(config.getConfig("rotation-manager"))
  }

  case class ExtensionSettings(config: Config) {
    val clusterEnable = config.getBoolean("cluster-enable")
    val exposeReplicationStream = config.getBoolean("expose-replication-stream")
    val exposeNotificationStream = config.getBoolean("expose-notification-stream")
    val replicator = ReplicatorSettings(config.getConfig("replicator"))
  }

  case class ReplicatorSettings(config: Config) {
    val name = config.getString("name")
    val role = config.getString("role")
  }

  case class SerializerSettings(config: Config) {
    val identifier = config.getInt("identifier")
  }

  case class RotationManager(config: Config) {
    val rotateEnable = config.getBoolean("rotate-enable")
    val messageCount = config.getBytes("message-count").toInt
  }

  trait CommonSettings {
    def config: Config
    val provider = config.getString("class")
    val folder = {
      val dir = new File(config.getString("folder"))
      dir.mkdirs()
      dir.getAbsoluteFile
    }
  }

  case class QueueSettings(config: Config) {
    val synchronous = config.getBoolean("synchronous")
    val useCheckedExcerpt = config.getBoolean("useCheckedExcerpt")
    val indexBlockSize = config.getBytes("indexBlockSize").toInt
    val dataBlockSize = config.getBytes("dataBlockSize").toInt
    val messageCapacity = config.getBytes("messageCapacity").toInt
    val cacheLineSize = config.getBytes("cacheLineSize").toInt
  }

  case class JournalSettings(config: Config) extends CommonSettings {
    val namingMapper = config.getString("naming-mapper")
    val persistSender = config.getBoolean("persist-sender")
    val chronicleQueue = QueueSettings(config.getConfig("chronicle-queue"))
  }

  case class MapSettings(config: Config) {
    val count = config.getBytes("count").toLong
    val averageKeySize = config.getBytes("averageKeySize").toInt
    val averageValueSize = config.getBytes("averageValueSize").toInt
    val maxChunksPerEntry = config.getInt("maxChunksPerEntry")
  }

  case class SnapshotSettings(config: Config) extends CommonSettings {
    val contentFile = new File(folder, "content.dat")
    val limit = config.getInt("limit")
    val chronicleMap = MapSettings(config.getConfig("chronicle-map"))
  }

}
