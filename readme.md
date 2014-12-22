akka-persistence-chronicle
==========================

[![Build Status](https://api.travis-ci.org/carrot-garden/akka-persistence-chronicle.png)]
(https://travis-ci.org/carrot-garden/akka-persistence-chronicle)

[License](https://github.com/carrot-garden/akka-persistence-chronicle/blob/master/license.txt)

[Settings](https://github.com/carrot-garden/akka-persistence-chronicle/blob/master/src/main/resources/reference.conf)

[Maven Central](http://search.maven.org/#search%7Cga%7C1%7Cakka-persistence-chronicle)


### Purpose

Provides [Akka Persistence](http://doc.akka.io/docs/akka/snapshot/scala/persistence.html) plugin
built on top of [Chronicle Queue](https://github.com/OpenHFT/Chronicle-Queue) 
and [Chronicle Map](https://github.com/OpenHFT/Chronicle-Map)
with [Akka Cluster](http://doc.akka.io/docs/akka/snapshot/common/cluster.html)
as [CRDT](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type)
backing store.

### Design

[Persistence Journal](http://doc.akka.io/docs/akka/snapshot/scala/persistence.html#Journal_plugin_API)
is backed by the [Chronicle Queue](https://github.com/OpenHFT/Chronicle-Queue)
and [Persistence Snapshot Store](http://doc.akka.io/docs/akka/snapshot/scala/persistence.html#Snapshot_store_plugin_API)
is backed by the [Chronicle Map](https://github.com/OpenHFT/Chronicle-Map).

Internal plugin Journal/Snapshot persistence commands are replicated 
to all cluster members running the cluster extension provided by this plugin.

Both Journal and Snapshot are only eventually consistent in the cluster in the loose
[CRDT](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type)
sense.

### Use case

[Persistent Actor](http://doc.akka.io/api/akka/snapshot/index.html#akka.persistence.PersistentActor)
cluster node fail-over under control of
[Cluster Singleton](http://doc.akka.io/docs/akka/snapshot/contrib/cluster-singleton.html).

### Deployment

Declare dependency to the
[akka-persistence-chronicle](http://search.maven.org/#search%7Cga%7C1%7Cakka-persistence-chronicle)
artifact.

Provide configuration stanza in ```application.conf``` to activate persistence plugin actors.
```
      akka.persistence {
        journal {
          plugin = "akka.persistence.chronicle.journal" 
        }
        snapshot-store {
          plugin = "akka.persistence.chronicle.snapshot-store"
        }
      }
```
