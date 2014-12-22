/**
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.persistence.chronicle;

import java.io.File

import com.typesafe.config.ConfigFactory

import Settings.QueueSettings

object JournalMain {

  def main(args: Array[String]): Unit = {

    val config = ConfigFactory.parseString(
      """
        synchronous = false
        useCheckedExcerpt = false
        defaultMessageSize = 1K
        indexBlockSize = 1M
        dataBlockSize = 4M
        """
    )

    val count = 1 * 1000

    val settings = QueueSettings(config)

    val queueList = for {
      index <- (1 to count)
    } yield {
      val store = new File(s"target/store/chronicle-queue-${index}")
      store.mkdirs()
      val queue = ChronicleProvider.createQueue(store, settings)
      val accessor = queue.createExcerpt()
      val appender = queue.createAppender()
      val follower = queue.createTailer()
      (queue, accessor, appender, follower)
    }

    var index = 0L
    while (true) {
      queueList foreach {
        case (queue, accessor, appender, follower) =>
          appender.startExcerpt()
          appender.writeLong(index)
          appender.finish()
      }
      Thread.sleep(3 * 1000)
      System.gc()
      println(s"index ${index}")
      index += 1
    }

  }

}
