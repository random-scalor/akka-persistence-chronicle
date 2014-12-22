/**
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.persistence.chronicle

import java.io.File

import scala.language.postfixOps

import akka.persistence.chronicle.Settings.MapSettings
import akka.persistence.chronicle.Settings.QueueSettings

import org.apache.commons.io.FileUtils
import org.scalatest.Matchers
import org.scalatest.WordSpecLike

import com.typesafe.config.ConfigFactory

import ChronicleSegmentedQueue.Repository

//@org.scalatest.Ignore
@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class ChronicleBasicSpec extends WordSpecLike with Matchers {

  "chronicle" must {

    "perform map ops" in {

      val store = new File("target/store/chroncile-map.dat")
      store.getParentFile.mkdirs()
      val count = 16 * 1024
      val keySize = 128
      val valueSize = 256

      val config = ConfigFactory.parseString(s"""
      count = ${count}
      averageKeySize = ${keySize}
      averageValueSize = ${valueSize}
      maxChunksPerEntry = 64
      """)

      val settings = MapSettings(config)

      val map = ChronicleProvider.createMap[Array[Byte], Array[Byte]](store, settings)

      def keyOf(index: Int) = s"key:${index}".getBytes
      def valueOf(index: Int) = s"value:${index}".getBytes

      for {
        index <- (1 to count)
        key = keyOf(index)
        value = valueOf(index)
      } yield {
        map.put(key, value)
      }

      for {
        index <- (1 to count)
        key = keyOf(index)
        value = valueOf(index)
      } yield {
        map.get(key) should be(value)
      }

      map.size should be(count)

      map.clear()
      map.close()

    }

    "perform queue ops" in {

      val store = new File("target/store/chronicle-queue")
      store.getParentFile.mkdirs()

      val config = ConfigFactory.parseString("""
        synchronous = false
        useCheckedExcerpt = false
        indexBlockSize = 1M
        dataBlockSize = 4M
        messageCapacity = 256
        cacheLineSize = 64
      """)

      val count = 100 * 1024

      val settings = QueueSettings(config)

      val queue = ChronicleProvider.createQueue(store, settings)

      val appender = queue.createAppender
      appender.startExcerpt()
      appender.writeObject("initial")
      appender.finish()

      val index1 = queue.lastIndex

      for {
        index <- (1 to count)
        key = s"index:${index}"
        value = s"value:${key}"
      } yield {
        appender.startExcerpt()
        appender.writeObject(s"${key}=${value}")
        appender.finish()
      }

      appender.close()

      val index2 = queue.lastIndex

      (index2 - index1) should be(count)

      queue.close() // commit changes
      queue.clear() // remove folder

    }

  }

  import ChronicleSegmentedQueue._

  "Chronicle Repository" must {

    val config = ConfigFactory.parseString("""
       synchronous = false
       useCheckedExcerpt = false
       indexBlockSize = 1K
       dataBlockSize = 4K
       messageCapacity = 256
       cacheLineSize = 64
    """)

    val settings = QueueSettings(config)

    val folder = new File("target/store/chronicle-respository")

    FileUtils.deleteDirectory(folder)
    folder.exists should be(false)

    "create/delete segments" in {
      val repo = Repository(folder, settings)
      //
      folder.list.size should be(2)
      repo.archive.isEmpty should be(true)
      repo.previous.isEmpty should be(false)
      repo.current.isEmpty should be(false)
      repo.previous.folder.list.size should be(2)
      repo.current.folder.list.size should be(2)
      //
      repo.clear()
      repo.isEmpty should be(true)
      repo.archive.isEmpty should be(true)
      repo.previous.isEmpty should be(true)
      repo.current.isEmpty should be(true)
      //
      folder.exists should be(false)
    }

    "restore segments from disk" in {
      val repo1 = Repository(folder, settings)
      repo1.close()
      Thread.sleep(1)
      val repo2 = Repository(folder, settings)
      repo2.close()
      repo1.isEmpty should be(false)
      repo2.isEmpty should be(false)
      repo1.archive.isEmpty should be(true)
      repo2.archive.isEmpty should be(true)
      repo1.previous.folder should be(repo2.previous.folder)
      repo1.current.folder should be(repo2.current.folder)
      repo2.clear()
      repo1.isEmpty should be(true)
      repo2.isEmpty should be(true)
      //
      folder.exists should be(false)
    }

    "rotate segments in order" in {
      val repo1 = Repository(folder, settings)
      repo1.current.appender.startExcerpt()
      repo1.current.appender.writeLong(1)
      repo1.current.appender.finish()
      Thread.sleep(1)
      val repo2 = repo1.rotate
      repo2.current.appender.startExcerpt()
      repo2.current.appender.writeLong(2)
      repo2.current.appender.finish()
      //
      repo1.archive.isEmpty should be(true)
      repo2.archive.isEmpty should be(false)
      repo2.archive should be(repo1.previous)
      repo2.previous should be(repo1.current)
      repo2.current.folder should not be (repo1.current.folder)
      repo1.previous.folder.compareTo(repo2.previous.folder) should be < 0
      repo1.current.folder.compareTo(repo2.current.folder) should be < 0
      repo1.previous.folder.compareTo(repo1.current.folder) should be < 0
      repo2.previous.folder.compareTo(repo2.current.folder) should be < 0
      //
      Thread.sleep(1)
      val repo3 = repo2.rotate
      repo3.current.appender.startExcerpt()
      repo3.current.appender.writeLong(3)
      repo3.current.appender.finish()
      //
      repo1.archive.isEmpty should be(true)
      repo2.archive.isEmpty should be(true)
      repo3.archive.isEmpty should be(false)
      //
      repo1.clear() // 0 1 2
      repo2.clear() // 1 2 3
      repo3.clear() // 2 3 4
      //
      folder.exists should be(false)
    }

    "remove obsolete segments on recover" in {
      var temp = Repository(folder, settings)
      (1 to 5) foreach { index =>
        temp = temp.rotate; Thread.sleep(1)
      }
      temp.close()
      folder.list.size should be(3)
      val repo = Repository(folder, settings)
      folder.list.size should be(2)
      repo.clear()
      //
      folder.exists should be(false)
    }

  }

}
