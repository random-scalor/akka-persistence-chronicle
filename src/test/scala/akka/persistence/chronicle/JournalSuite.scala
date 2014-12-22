/**
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.persistence.chronicle;

import java.io.File
import java.util.UUID

import scala.concurrent.duration.DurationInt

import akka.persistence.PersistentRepr
import akka.persistence.journal.JournalPerfSpec
import akka.persistence.journal.JournalSpec

import org.apache.commons.io.FileUtils

import com.typesafe.config.ConfigFactory

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class ChronicleSyncJournalSpec extends JournalSpec {

  final val journalFolder = "target/store/journal-spec"

  override lazy val config = ConfigFactory.parseString(s"""
    akka.persistence {
      journal {
        plugin = "akka.persistence.chronicle.journal"
      }
      snapshot-store {
        plugin = "akka.persistence.snapshot-store.local"
      }
      chronicle {
        journal {
          folder = "${journalFolder}"
          persist-sender = true // Required to pass JournalSpec
        }
      }
    }
  """).withFallback(ConfigFactory.load)

  def initialize() = {
    FileUtils.deleteDirectory(new File(journalFolder))
  }

  def terminate() = {
    val journalActor = PluginTest.extractActor[ChronicleJournal](journal)
    journalActor.clear()
    new File(journalFolder).list.length should be(0)
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    initialize()
  }

  override protected def afterAll(): Unit = {
    terminate()
    super.afterAll()
  }

  "Journal" must {
    type Content = Array[Byte]
    "decode persistence id from serialized content" in {
      val sourceId = UUID.randomUUID.toString
      val journalActor = PluginTest.extractActor[ChronicleJournal](journal)
      val appender = journalActor.provideJournal(sourceId).appender
      val source = PersistentRepr("message-payload", 123, sourceId, false, journal)
      journalActor.messagePersist(appender, source)
      val target: Content = appender.message
      val targetId = ChronicleJournal.persistenceIdentifier(target)
      sourceId should be(targetId)
    }
  }

}

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class ChronicleSyncJournalPerfSpec extends ChronicleSyncJournalSpec with JournalPerfSpec {

  override def eventsCount: Int = 1 * 1000

  override def awaitDurationMillis = 10.seconds.toMillis

  override def measurementIterations: Int = 10

}
