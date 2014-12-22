/**
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.persistence.chronicle

import java.util.UUID
import org.scalatest.Matchers
import org.scalatest.WordSpecLike
import akka.testkit.AkkaSpec
import com.typesafe.config.ConfigFactory
import akka.persistence.Persistence
import akka.testkit.TestProbe
import akka.actor.Identify
import akka.actor.ActorIdentity
import scala.concurrent.duration.DurationInt
import akka.actor.Kill

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class NamingMapperSpec extends WordSpecLike with Matchers with Settings {

  "DirectNamingMapper" must {
    "provide direct mapping" in {
      val mapper: NamingMapper = DirectNamingMapper()
      val source = UUID.randomUUID.toString
      val target = mapper.encode(source)
      mapper.decode(target) should be(source)
    }
  }

  "URLNamingMapper" must {
    "provide url mapping" in {
      val mapper: NamingMapper = URLNamingMapper()
      val source = "/some/../file/like:path.here"
      val target = mapper.encode(source)
      mapper.decode(target) should be(source)
    }
  }

}

object ExtensionSpec {

  val journalFolder = "target/store/extension-journal"
  val snapshotLimit = 10
  val snapshotFolder = "target/store/extension-snapshot"
  val rotateMessateCount = 100

  val config = ConfigFactory.parseString(s"""
    akka.persistence {
      journal {
        plugin = "akka.persistence.chronicle.journal"
      }
      snapshot-store {
        plugin = "akka.persistence.chronicle.snapshot-store"
      }
      chronicle {
        extension {
          expose-notification-stream = true
        }
        journal {
          folder = "${journalFolder}"
        }
        snapshot-store {
          folder = "${snapshotFolder}"
          limit = ${snapshotLimit}
        }
        rotation-manager {
          message-count = ${rotateMessateCount}
        }
      }
    }
  """).withFallback(ConfigFactory.load)

}

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class ExtensionSpec extends AkkaSpec(ExtensionSpec.config) with Settings {
  import PluginActor._
  import ExtensionSpec._
  import ExtensionProtocol._
  import system.eventStream

  override protected def afterTermination() {
    // Thread.sleep(1000)
  }

  PluginTest.saveApplicationConf(system, classOf[ExtensionSpec].getSimpleName)

  /** Persistence cluster extension. */
  val extension = ChronicleExtension(system)

  def journalActor = PluginTest.extractActor[ChronicleSyncJournal](extension.journalActor)
  def snapshotActor = PluginTest.extractActor[ChronicleSnapshotStore](extension.snapshotActor)

  /** Current rotated synchronized journal. */
  def locateJournal(persistenceId: String) = {
    val journal = journalActor.locateJournal(persistenceId)
    journal.synchronized(journal)
  }

  def journalActorCount = journalActor.synchronized(journalActor.count)
  def journalEntryCount = journalActor.synchronized(journalActor.chronicleCount)
  def journalAppendCount = journalActor.synchronized(journalActor.appendCount)
  def journalRotateCount = journalActor.synchronized(journalActor.rotateCount)

  def snapshotActorCount = snapshotActor.synchronized(snapshotActor.count)
  def snapshotEntryCount = snapshotActor.synchronized(snapshotActor.chronicleCount)

  def locateSnapshotIndex(persistenceId: String) = snapshotActor.indexLoad(persistenceId)

  val batch1 = (1 to rotateMessateCount * 2) map { index => s"batch-1-${index}" }
  val batch2 = (1 to rotateMessateCount * 3) map { index => s"batch-2-${index}" }
  val batch3 = (1 to rotateMessateCount * 4) map { index => s"batch-3-${index}" }
  val batch4 = (1 to rotateMessateCount * 5) map { index => s"batch-4-${index}" }
  val batch12 = batch1 ++ batch2
  val batch34 = batch3 ++ batch4
  val batch123 = batch1 ++ batch2 ++ batch3
  val batch1234 = batch1 ++ batch2 ++ batch3 ++ batch4

  val probe = TestProbe()
  eventStream.subscribe(probe.ref, classOf[PluginNotification])

  def clear(): Unit = {
    extension.command(PluginJournalClearCommand)
    probe.fishForMessage() { case PluginJournalClearNotification => true case _ => false }
    extension.command(PluginSnapshotClearCommand)
    probe.fishForMessage() { case PluginSnapshotClearNotification => true case _ => false }
    journalActorCount should be(0)
    journalEntryCount should be(0)
    journalAppendCount should be(0)
    journalRotateCount should be(0)
    snapshotActorCount should be(0)
    snapshotEntryCount should be(0)
  }

  def stoicActor(name: String): StoicActor = {
    val stoicRef = system.actorOf(StoicActor(name), name)
    probe.send(stoicRef, Identify(name))
    //    probe.expectMsgPF() { case ActorIdentity(`name`, _) => }
    probe.fishForMessage() { case ActorIdentity(`name`, _) => true case _ => false }
    PluginTest.extractActor[StoicActor](stoicRef)
  }

  "Chronicle extension" must {

    "open and clear stores" in {
      clear()
    }

    "create/restore journal and snapshot" in {
      clear()

      val name1 = "stoic-1"
      val name2 = "stoic-2"

      // Persistent actor journal, snapshot, rotation.
      val stoic1A = stoicActor(name1)
      batch1 foreach { stoic1A.self ! StoicActor.Command(_) }
      awaitCond(journalActorCount == 1)
      awaitCond(journalEntryCount == batch1.size)
      stoic1A.self ! "snapshot"
      awaitCond(snapshotActorCount == 1)
      awaitCond(snapshotEntryCount == 1)
      batch2 foreach { stoic1A.self ! StoicActor.Command(_) }
      awaitCond(journalActorCount == 1)
      awaitCond(journalEntryCount == batch12.size)
      probe.expectMsg(PluginJournalRotateNotification(name1))

      // Persistent actor journal, snapshot, rotation.
      val stoic2A = stoicActor("stoic-2")
      batch3 foreach { stoic2A.self ! StoicActor.Command(_) }
      awaitCond(journalActorCount == 2)
      awaitCond(journalEntryCount == batch123.size)
      stoic2A.self ! "snapshot"
      awaitCond(snapshotActorCount == 2)
      awaitCond(snapshotEntryCount == 2)
      batch4 foreach { stoic2A.self ! StoicActor.Command(_) }
      awaitCond(journalActorCount == 2)
      awaitCond(journalEntryCount == batch1234.size)
      probe.expectMsg(PluginJournalRotateNotification(name2))

      // Journal plugin crash/restore.
      journalActor.self ! JournalRestartCommand
      probe.expectMsg(PluginJournalCloseNotification)
      probe.expectMsg(PluginJournalOpenNotification)
      println(s"### journalActorCount ${journalActorCount}")
      awaitCond(journalActorCount == 2)
      awaitCond(journalEntryCount == batch1234.size)

      // Snapshot plugin crash/restore.
      snapshotActor.self ! SnapshotRestartCommand
      probe.expectMsg(PluginSnapshotCloseNotification)
      probe.expectMsg(PluginSnapshotOpenNotification)
      awaitCond(snapshotActorCount == 2)
      awaitCond(snapshotEntryCount == 2)

      // Persistent actor crash.
      probe.watch(stoic1A.self)
      stoic1A.self ! Kill
      probe.expectTerminated(stoic1A.self)

      // Persistent actor crash.
      probe.watch(stoic2A.self)
      stoic2A.self ! Kill
      probe.expectTerminated(stoic2A.self)

      // Persistent actor restore.
      val stoic1B = stoicActor(name1)
      awaitCond(stoic1B.eventCount == batch12.size)
      stoic1B.state.events.reverse should be(batch12)

      // Persistent actor restore.
      val stoic2B = stoicActor(name2)
      awaitCond(stoic2B.eventCount == batch34.size)
      stoic2B.state.events.reverse should be(batch34)

    }

    "coordinate journal/snapshot rotation" in {
      clear()

      val name = "stoic-3"

      val stoicOne = stoicActor(name)

      batch1 foreach { stoicOne.self ! StoicActor.Command(_) }
      awaitCond(journalActorCount == 1)
      awaitCond(journalAppendCount == batch1.size)

      stoicOne.self ! "snapshot"
      awaitCond(snapshotActorCount == 1)
      awaitCond(snapshotEntryCount == 1)

      batch2 foreach { stoicOne.self ! StoicActor.Command(_) }
      awaitCond(journalActorCount == 1)
      awaitCond(journalAppendCount == batch12.size)
      probe.expectMsg(PluginJournalRotateNotification(name))

      stoicOne.self ! "snapshot"
      awaitCond(snapshotActorCount == 1)
      awaitCond(snapshotEntryCount == 2)

      batch3 foreach { stoicOne.self ! StoicActor.Command(_) }
      awaitCond(journalActorCount == 1)
      awaitCond(journalAppendCount == batch123.size)
      probe.expectMsg(PluginJournalRotateNotification(name))

      stoicOne.self ! "snapshot"
      awaitCond(snapshotActorCount == 1)
      awaitCond(snapshotEntryCount == 3)

      batch4 foreach { stoicOne.self ! StoicActor.Command(_) }
      awaitCond(journalActorCount == 1)
      awaitCond(journalAppendCount == batch1234.size)
      probe.expectMsg(PluginJournalRotateNotification(name))

      journalAppendCount should be > journalEntryCount

      // Persistent actor crash.
      probe.watch(stoicOne.self)
      stoicOne.self ! Kill
      probe.expectTerminated(stoicOne.self)

      // Persistent actor restore.
      val stoicTwo = stoicActor(name)
      awaitCond(stoicTwo.eventCount == batch1234.size)
      stoicTwo.state.events.reverse should be(batch1234)

      journalRotateCount should be(3)

      journalAppendCount should be > journalEntryCount

      log.info(s"journalAppendCount=${journalAppendCount} journalEntryCount=${journalEntryCount}")
    }

  }

}
