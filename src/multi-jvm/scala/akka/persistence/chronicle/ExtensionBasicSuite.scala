/**
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.persistence.chronicle

import scala.collection.Seq
import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import akka.actor.actorRef2Scala
import akka.cluster.MemberStatus
import akka.cluster.MultiNodeClusterSpec
import akka.cluster.MultiNodeClusterSpec.clusterConfig
import akka.persistence.Persistence
import akka.persistence.SnapshotMetadata
import akka.persistence.chronicle.ReplicationProtocol.Marker
import akka.remote.testkit.MultiNodeSpec
import akka.serialization.SerializationExtension
import akka.testkit.LongRunningTest
import akka.testkit.TestProbe

/** Cluster test configuration. */
object ExtensionBasicConfig extends ExtensionConfig {
  import MultiNodeClusterSpec._
  override def rotateMessateCount = 20
  /** Configuration customizations.*/
  commonConfig {
    Seq(
      customLogging,
      exposeReplication,
      disableMetricsLegacy,
      debugConfig(false),
      clusterConfig(false)
    ).reduceLeft(_ withFallback _)
  }
}

// multi-jvm:test-only akka.persistence.chronicle.ExtensionBasic
class ExtensionBasicMultiJvmNode1 extends ExtensionBasicSpec
class ExtensionBasicMultiJvmNode2 extends ExtensionBasicSpec
class ExtensionBasicMultiJvmNode3 extends ExtensionBasicSpec
class ExtensionBasicMultiJvmNode4 extends ExtensionBasicSpec
class ExtensionBasicMultiJvmNode5 extends ExtensionBasicSpec

/** Basic plugin contract test sequence on every cluster node. */
abstract class ExtensionBasicSpec extends MultiNodeSpec(ExtensionBasicConfig) with MultiNodeClusterSpec {
  import ReplicationProtocol._
  import ExtensionBasicConfig._

  /** Export per-node settings for review. */
  PluginTest.saveApplicationConf(system, myself.name)

  /** Persistence extension. */
  val persistence = Persistence(system)

  /** Persistence cluster extension. */
  val extension = ChronicleExtension(system)

  /** System serialization provider. */
  implicit val serializer = SerializationExtension(system)

  val rotateTrigger = ExtensionBasicConfig.rotateMessateCount
  val rotatePresision = (rotateTrigger * 0.2).toInt

  def journalActor = PluginTest.extractActor[ChronicleSyncJournal](persistence.journalFor(null))
  def snapshotActor = PluginTest.extractActor[ChronicleSnapshotStore](persistence.snapshotStoreFor(null))
  def replicatorActor = PluginTest.extractActor[ReplicationCoordinator](extension.replicator)

  def journalCount = journalActor.count
  def journalEntryCount = journalActor.chronicleCount
  def journalEntryCount(id: String) = journalActor.chronicleCount(id)
  def snapshotCount = snapshotActor.count
  def snapshotEntryCount = snapshotActor.chronicleCount
  def snapshotEntryCount(id: String) = snapshotActor.chronicleCount(id)

  def awaitJournalCount(count: Int) = awaitAssert(journalCount should be(count))
  def awaitJournalEntryCount(count: Int) = awaitAssert(journalEntryCount should be(count))
  def awaitSnapshotCount(count: Int) = awaitAssert(snapshotCount should be(count))
  def awaitSnapshotEntryCount(count: Int) = awaitAssert(snapshotEntryCount should be(count))

  def journalAppendCount = journalActor.appendCount
  def awaitJournalAppendCount(count: Int) = awaitAssert(journalAppendCount should be(count))

  def journalRotateCount = journalActor.rotateCount
  def awaitJournalRotateCount(count: Int) = awaitAssert(journalRotateCount should be(count))

  def awaitMemberCount(count: Int) = awaitAssert(clusterView.members.count(_.status == MemberStatus.Up) should be(count))

  /** Command data set. */
  val payloadList = Seq(
    // Journal.
    JournalClearPayload,
    JournalClearPayload("meta-1"),
    JournalRotatePayload("meta-2"),
    //JournalAppendPayload(Seq(PersistentRepr("payload-3", 123, "meta-3", false, null))), // FIXME serialization
    JournalDeletePayload("meta-4", 123),
    // Snapshot.
    SnapshotClearPayload,
    SnapshotClearPayload("meta-5"),
    SnapshotCreatePayload(SnapshotMetadata("meta-6", 0, 0), "payload-6"),
    SnapshotRemovePayload(SnapshotMetadata("meta-7", 0, 0))
  )
  require(payloadList.size < rotateTrigger)
  /** Publish command set. */
  val payloadPublishList = payloadList map { PublishCommand(_) }
  /** Consume command set. */
  val payloadConsumeList = payloadList map { ConsumeCommand(_) }

  /** [[StoicActor]] first event batch. */
  val stoicBasicList = Seq("basic-1", "basic-2", "basic-3")
  /** [[StoicActor]] second event batch. */
  val stoicExtraList = Seq("extra-4", "extra-5", "extra-6", "extra-7")
  /** [[StoicActor]] combined event batch. */
  val stoicTotalList = stoicBasicList ++ stoicExtraList
  require(stoicTotalList.size < rotateTrigger)

  override def atStartup() = {
    journalActor.clear()
    snapshotActor.clear()
  }

  override def afterTermination() = {
  }

  /** Consume persistence clear command. */
  def issueClear() = {
    extension.consume(ConsumeCommand(JournalClearPayload))
    extension.consume(ConsumeCommand(SnapshotClearPayload))
  }

  /** Consume journal segment rotate command. */
  def issueRotate(persistenceId: String) = {
    extension.consume(ConsumeCommand(JournalRotatePayload(persistenceId)))
  }

  /** Provide unique test step count. */
  var step = 0
  val stepSet = mutable.Set[Int]()
  def makeStep(nextStep: Int): Unit =
    if (stepSet.contains(nextStep)) throw { new Error(s"Duplicate step ${nextStep}") }
    else { step = nextStep; stepSet += step }

  /** All nodes. */
  val nodeAll = roles
  /** Nodes active during test. */
  val nodeActive = Array(node1, node3, node4, node5)
  /** Nodes removed during test. */
  val nodeInactive = node2
  /** Publisher node. */
  val nodeMaster = Array(node1)
  /** Consuming node. */
  val nodeWorker = Array(node3, node4, node5)

  "Cluster extension" must {

    "discover replica members" taggedAs LongRunningTest in within(20 seconds) {
      makeStep(1)
      enterBarrier(s"${step}-start")
      awaitClusterUp(nodeAll: _*)
      awaitMemberCount(roles.size)
      runOn(nodeAll: _*) {
        replicatorActor.replicas.size should be(roles.size - 1)
        replicatorActor.replicas should not contain (cluster.selfAddress)
      }
      enterBarrier(s"${step}-finish")
    }

    "update replica membership" taggedAs LongRunningTest in within(5 seconds) {
      makeStep(2)
      enterBarrier(s"${step}-start")
      runOn(nodeMaster: _*) {
        cluster.leave(nodeInactive)
      }
      enterBarrier(s"${step}-leave")
      runOn(nodeActive: _*) {
        awaitMemberCount(roles.size - 1)
      }
      enterBarrier(s"${step}-removed")
      runOn(nodeActive: _*) {
        replicatorActor.replicas.size should be(roles.size - 2)
        replicatorActor.replicas should not contain (cluster.selfAddress)
      }
      enterBarrier(s"${step}-finish")
    }

    "deliver replication commands" taggedAs LongRunningTest in within(10 seconds) {
      makeStep(3)
      enterBarrier(s"${step}-start")
      val probe = TestProbe()
      system.eventStream.subscribe(probe.ref, classOf[Marker])
      enterBarrier(s"${step}-publish")
      runOn(nodeMaster: _*) {
        payloadPublishList foreach { extension.publish(_) }
      }
      enterBarrier(s"${step}-consume")
      runOn(nodeMaster: _*) {
        payloadPublishList foreach { probe.expectMsg(_) }
      }
      runOn(nodeWorker: _*) {
        payloadConsumeList foreach { probe.expectMsg(_) }
      }
      system.eventStream.unsubscribe(probe.ref)
      enterBarrier(s"${step}-finish")
    }

    "persist within cluster" taggedAs LongRunningTest in within(10 seconds) {
      makeStep(4)
      val name = s"stoic-${step}"
      enterBarrier(s"${step}-clean")
      runOn(nodeActive: _*) {
        issueClear()
        awaitJournalCount(0); awaitSnapshotCount(0)
        awaitJournalEntryCount(0); awaitSnapshotEntryCount(0)
      }
      enterBarrier(s"${step}-actor")
      val stoicRef = system.actorOf(StoicActor(name), name)
      enterBarrier(s"${step}-publish")
      runOn(nodeMaster: _*) {
        stoicBasicList foreach { stoicRef ! StoicActor.Command(_) }
        stoicRef ! "snapshot"
        stoicExtraList foreach { stoicRef ! StoicActor.Command(_) }
      }
      enterBarrier(s"${step}-consume")
      runOn(nodeActive: _*) {
        awaitJournalCount(1); awaitSnapshotCount(1)
        awaitJournalEntryCount(stoicTotalList.size); awaitSnapshotEntryCount(1)
      }
      enterBarrier(s"${step}-restart")
      runOn(nodeWorker: _*) {
        val probe = TestProbe()
        system.eventStream.subscribe(probe.ref, classOf[String])
        stoicRef ! "restart"
        probe.expectMsg("recover-complete")
        system.eventStream.unsubscribe(probe.ref)
      }
      enterBarrier(s"${step}-recover")
      val stoicActor = PluginTest.extractActor[StoicActor](stoicRef)
      runOn(nodeWorker: _*) {
        stoicActor.state.events.reverse should be(stoicTotalList)
      }
      enterBarrier(s"${step}-finish")
    }

    "rotate segments manually" taggedAs LongRunningTest in within(10 seconds) {
      makeStep(5)
      val name = s"stoic-${step}"
      enterBarrier(s"${step}-clean")
      runOn(nodeActive: _*) {
        issueClear()
        awaitJournalCount(0); awaitSnapshotCount(0)
        awaitJournalEntryCount(0); awaitSnapshotEntryCount(0)
      }
      enterBarrier(s"${step}-actor")
      val stoicRef = system.actorOf(StoicActor(name), name)
      enterBarrier(s"${step}-publish-basic")
      runOn(node1) {
        stoicBasicList foreach { stoicRef ! StoicActor.Command(_) }
      }
      enterBarrier(s"${step}-consume-basic")
      runOn(nodeActive: _*) {
        awaitJournalCount(1); awaitSnapshotCount(0)
        awaitJournalEntryCount(stoicBasicList.size); awaitSnapshotEntryCount(0)
      }
      enterBarrier(s"${step}-rotate")
      runOn(nodeActive: _*) {
        awaitJournalRotateCount(0)
      }
      issueRotate(name)
      runOn(node1, node3, node4, node5) {
        awaitJournalRotateCount(1)
      }
      enterBarrier(s"${step}-publish-extra")
      runOn(nodeMaster: _*) {
        stoicExtraList foreach { stoicRef ! StoicActor.Command(_) }
      }
      enterBarrier(s"${step}-consume-extra")
      runOn(nodeActive: _*) {
        awaitJournalCount(1); awaitSnapshotCount(0)
        awaitJournalEntryCount(stoicTotalList.size); awaitSnapshotEntryCount(0)
      }
      enterBarrier(s"${step}-restart")
      runOn(nodeWorker: _*) {
        val probe = TestProbe()
        system.eventStream.subscribe(probe.ref, classOf[String])
        stoicRef ! "restart"
        probe.expectMsg("recover-complete")
        system.eventStream.unsubscribe(probe.ref)
      }
      enterBarrier(s"${step}-recover")
      val stoicActor = PluginTest.extractActor[StoicActor](stoicRef)
      runOn(node3, node4, node5) {
        stoicActor.state.events.reverse should be(stoicTotalList)
      }
      enterBarrier(s"${step}-verify")
      runOn(nodeWorker: _*) {
        val journal = journalActor.provideJournal(name)
        journal.repository.count should be(stoicTotalList.size)
        journal.repository.previous.count should be(stoicBasicList.size)
        journal.repository.current.count should be(stoicExtraList.size)
      }
      enterBarrier(s"${step}-finish")
    }

    "rotate segments automatically" taggedAs LongRunningTest in within(10 seconds) {
      makeStep(6)
      val name = s"stoic-${step}"
      val batch1 = (1 to rotateTrigger * 2) map { index => s"batch-1-${index}" }
      val batch2 = (1 to rotateTrigger * 3) map { index => s"batch-2-${index}" }
      val batch3 = (1 to rotateTrigger * 4) map { index => s"batch-3-${index}" }
      val batch12 = batch1 ++ batch2
      val batch123 = batch1 ++ batch2 ++ batch3
      def keepPace() = Thread.sleep(10)
      enterBarrier(s"${step}-clean")
      runOn(nodeActive: _*) {
        issueClear()
        awaitJournalCount(0); awaitSnapshotCount(0)
        awaitJournalEntryCount(0); awaitSnapshotEntryCount(0)
        awaitJournalAppendCount(0); awaitJournalRotateCount(0)
      }
      enterBarrier(s"${step}-actor")
      val stoicRef = system.actorOf(StoicActor(name), name)
      enterBarrier(s"${step}-publish-1")
      runOn(nodeMaster: _*) {
        batch1 foreach { message => stoicRef ! StoicActor.Command(message) }
      }
      enterBarrier(s"${step}-consume-1")
      runOn(node1, node3, node4, node5) {
        awaitSnapshotEntryCount(0)
        awaitJournalAppendCount(batch1.size)
        awaitJournalRotateCount(0)
      }
      enterBarrier(s"${step}-snapshot-1")
      runOn(nodeMaster: _*) {
        stoicRef ! "snapshot"
      }
      enterBarrier(s"${step}-publish-2")
      runOn(nodeMaster: _*) {
        batch2 foreach { message => stoicRef ! StoicActor.Command(message); keepPace() }
      }
      enterBarrier(s"${step}-consume-2")
      runOn(nodeActive: _*) {
        awaitSnapshotEntryCount(1)
        awaitJournalAppendCount(batch12.size)
        awaitJournalRotateCount(1)
      }
      enterBarrier(s"${step}-verify-12")
      runOn(nodeActive: _*) {
        val journal = journalActor.provideJournal(name)
        import journal.repository._
        count should be(batch12.size)
        previous.count.toInt should be(batch1.size + rotateTrigger +- rotatePresision)
        current.count.toInt should be(batch2.size - rotateTrigger +- rotatePresision)
      }
      enterBarrier(s"${step}-snapshot-2")
      runOn(nodeMaster: _*) {
        stoicRef ! "snapshot"
      }
      enterBarrier(s"${step}-publish-3")
      runOn(nodeMaster: _*) {
        batch3 foreach { message => stoicRef ! StoicActor.Command(message); keepPace() }
      }
      enterBarrier(s"${step}-consume-3")
      runOn(nodeActive: _*) {
        awaitSnapshotEntryCount(2)
        awaitJournalAppendCount(batch123.size)
        awaitJournalRotateCount(2)
      }
      enterBarrier(s"${step}-verify-123")
      runOn(nodeActive: _*) {
        val journal = journalActor.provideJournal(name)
        import journal.repository._
        count.toInt should be <= batch123.size
        previous.count.toInt should be(batch2.size +- rotatePresision)
        current.count.toInt should be(batch3.size - rotateTrigger +- rotatePresision)
      }
      enterBarrier(s"${step}-finish")
    }

    "finsish with cleanup" taggedAs LongRunningTest in within(10 seconds) {
      makeStep(9)
      enterBarrier(s"${step}-clean")
      runOn(nodeActive: _*) {
        issueClear()
        awaitJournalCount(0); awaitSnapshotCount(0)
        awaitJournalEntryCount(0); awaitSnapshotEntryCount(0)
        awaitJournalAppendCount(0); awaitJournalRotateCount(0)
      }
      enterBarrier(s"${step}-finish")
    }

  }

}
