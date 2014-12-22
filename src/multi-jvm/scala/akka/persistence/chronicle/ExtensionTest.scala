/**
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.persistence.chronicle

import akka.actor.Props
import akka.persistence.PersistentActor
import akka.persistence.RecoveryCompleted
import akka.persistence.RecoveryFailure
import akka.persistence.SaveSnapshotFailure
import akka.persistence.SaveSnapshotSuccess
import akka.persistence.SnapshotOffer
import akka.remote.testkit.MultiNodeConfig

import com.typesafe.config.ConfigFactory.parseString

/** Cluster test configuration. */
trait ExtensionConfig extends MultiNodeConfig {

  /** Sequence difference: how much journal is in advance of snapshot. */
  def rotateMessateCount: Int

  val node1 = role("node-1")
  val node2 = role("node-2")
  val node3 = role("node-3")
  val node4 = role("node-4")
  val node5 = role("node-5")

  /** All cluster nodes. */
  def nodeList = Seq(node1, node2, node3, node4, node5)

  /** Apply per-node configuration. */
  nodeList foreach { role ⇒

    /** Use separate persistence store. */
    val store = s"target/${role.name}/store"

    nodeConfig(role) {
      parseString(s"""
      akka.persistence {
        journal {
          plugin = "akka.persistence.chronicle.journal"
        }
        snapshot-store {
          plugin = "akka.persistence.chronicle.snapshot-store"
        }
      }
      akka.persistence.chronicle {
        extension {
          cluster-enable = true
        }
        journal {
          folder = ${store}/journal
        }
        snapshot-store {
          folder = ${store}/snapshot
        }
        rotation-manager {
          message-count = ${rotateMessateCount}
        }
      }
      """)
    }
  }

  /** Disable legacy metrics in akka-cluster. */
  def disableMetricsLegacy = parseString("""
  akka.cluster.metrics.enabled = off
  """)

  /** Provide replication commands for review. */
  def exposeReplication = parseString("""
  akka.persistence.chronicle.extension.expose-replication-stream = true
  """)

  /** Activate slf4j logging along with test listener. */
  def customLogging = parseString("""
  akka.loglevel = INFO
  akka.loggers = [ "akka.testkit.TestEventListener","akka.event.slf4j.Slf4jLogger" ]
  """)
}
