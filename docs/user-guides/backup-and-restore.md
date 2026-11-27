# Backup and restore

Use Midge's backup directory APIs for local and `CloudSimulated` databases.
For provider-backed S3 deployments, see [AWS Backup for S3](#aws-backup-for-s3).

## Local and simulated cloud

Midge can capture a versioned backup directory from a local or
`CloudSimulated` engine. The capture briefly pauses mutations while it syncs
the current WAL and pins the durable file set. It copies and checksums the
files after releasing that barrier, so the full backup duration does not hold
the write pause. The WAL part of the pinned set is `wal/LEDGER` and the
segments it names. Orphaned segment files and the copies salvage set aside
under `wal/` are not captured, and a ledger that cannot be read fails the
backup.

```rust,no_run
use cntryl_midge::{Engine, OpenOptions};
use std::time::Duration;

let engine = Engine::open(OpenOptions::local("./data").build()?)?;
let inventory = engine.backup_to("./backups/run-001", Duration::from_secs(30))?;
println!("captured through sequence {}", inventory.durability_frontier);
# Ok::<(), Box<dyn std::error::Error>>(())
```

The destination must be new and outside the source database tree. Existing
symlink aliases and relative paths are resolved before this check. Publication
atomically refuses an existing destination, including one created during capture.
A completed artifact contains `backup.json` and
an `objects/` tree. The inventory records the backup format, database format,
capture frontier, engine version, storage kind, and each object's size and
CRC32C. Copy the complete directory as one unit and retain it until the backup
is no longer needed.

Restore accepts only a new target outside the artifact tree and a matching
storage kind. Each attempt uses its own staging directory. If the process aborts
before publication, retry the same artifact; abandoned or foreign stages do not
block the retry and are retained. Ordinary failures clean up only their own stage.
The final publication atomically refuses replacement of an existing target.
It validates
the inventory, all object paths and checksums, and strict database storage
state in a staging directory before publishing that directory. Open the
restored database normally afterward; startup obtains a new writer lease and
does not reuse lease state from the source.

```rust,no_run
# use cntryl_midge::{Engine, OpenOptions};
let options = OpenOptions::local("./restored").build()?;
Engine::restore_backup("./backups/run-001", options)?;
let restored = Engine::open(OpenOptions::local("./restored").build()?)?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

In-memory databases and provider-backed `Cloud` databases are not supported by
this API. `CloudSimulated` backups include its local cache and simulated object
store directories, so they can be restored only to another `CloudSimulated`
target. A checksum, missing object, unsupported artifact version, or failed
strict verification rejects the restore before the target becomes visible.

## AWS Backup for S3

AWS Backup captures S3 objects in a backup vault. Qualify the restore with the
engine version, database format, and application build you deploy. A recovery
point must include the selected metadata, writer lease and authority marker,
both WAL catalog copies, and the WAL and SST objects the database needs.
Keep the database in one bucket where practical. With separate control, WAL,
or SST buckets, establish and verify a common recovery point across them
before relying on the backup.

### Choose a recovery window

Enable versioning on the source bucket and follow the
[S3 lifecycle guidance](../operations/cloud-setup.md#s3-versioning-and-lifecycle).
S3 noncurrent-version retention controls how long earlier object versions stay
in the source bucket. The backup plan independently controls how long
recovery points stay in the vault.

| Goal | AWS Backup configuration |
| --- | --- |
| A dated recovery point each day | Schedule a daily snapshot rule. Set vault retention to the time you want to keep those points, for example 30 days. Periodic-only backups scan the bucket on each run. The first backup is full and later backups store changes incrementally. |
| Select a timestamp within the last seven days | Create a single backup rule for the bucket. Select **Enable continuous backups for supported resources** and set **Total retention period** to seven days. Keep S3 notifications to EventBridge enabled. |
| Seven days of timestamp selection plus older dated points | Use the continuous rule and a daily snapshot rule in the same vault. |

With daily snapshots, a failed or delayed job can leave the most recent
usable recovery point more than a day old. For a seven-day continuous window,
monitor the oldest and latest restorable times after the initial backup
completes. AWS documents second-level timestamp
selection and about 15 minutes of lag at the newest end of the S3 window.
AWS Backup can fall back to snapshots if continuous backup fails. Use
[vault notifications][aws-backup-notifications] for failed or expired backup
jobs, interrupted continuous backups, failed restore jobs, and S3 object
failures. Check each bucket's recovery point freshness so a missed job or a stale
continuous window triggers an alert even when no failure event arrives.
Cross-Region and cross-account copies of continuous recovery points restore as
dated snapshots. See [AWS backup rule settings][aws-backup-rules],
[S3 backup types][aws-s3-backups], and [point-in-time recovery][aws-pitr].

AWS Backup can capture earlier versions and delete markers that remain in the
source. Budget for backup storage and requests separately from live S3
storage. AWS applies a minimum billable backup size to small objects and delete
markers, so the live byte count alone can miss the cost of frequent lease and
WAL catalog rewrites. See [AWS Backup pricing][aws-backup-pricing].

### Restore and verify

For a point-in-time drill, wait for a `cloud_strict` commit to succeed and
record a UTC cutoff after that acknowledgement. Submit a second
`cloud_strict` commit for a different key after the cutoff. Wait for the
cutoff to enter the available recovery window.

1. Choose a completed snapshot or a timestamp within the continuous recovery
   window. In **AWS Backup → Protected resources**, open the bucket's recovery
   point and choose **Restore**. Select **Restore entire bucket** and the
   default latest version of each object at the selected recovery point.
   Restore into a new, empty, versioned bucket in the recovery point's Region.
   AWS Backup can skip occupied keys, so an in-place restore can mix states.
   See the [AWS S3 restore procedure][aws-s3-restore].
2. Check the restore job details for skipped or failed objects. Provision the
   destination permissions, encryption configuration, and lifecycle separately.
   Keep the database's original object prefix and use an empty local cache.
3. Start the deployment in isolation and open the restored database using
   normal recovery. Verify known application records at the selected point.
   For the drill, verify that the first key is present and the second absent.
   The requested cutoff describes S3 state, so writes that only reached the
   local cache may be missing.
4. Record the recovered data boundary and the time to application readiness.
   Before a live cutover, take the original writer out of service and verify
   the restored deployment's data and readiness. The restored namespace has
   an independent writer lease. Reconcile external consumer positions and
   work that may be replayed before resuming them.

[aws-backup-rules]: https://docs.aws.amazon.com/aws-backup/latest/devguide/plan-options-and-configuration.html
[aws-s3-backups]: https://docs.aws.amazon.com/aws-backup/latest/devguide/s3-backups.html
[aws-pitr]: https://docs.aws.amazon.com/aws-backup/latest/devguide/point-in-time-recovery.html
[aws-backup-pricing]: https://aws.amazon.com/backup/pricing/
[aws-backup-notifications]: https://docs.aws.amazon.com/aws-backup/latest/devguide/backup-notifications.html
[aws-s3-restore]: https://docs.aws.amazon.com/aws-backup/latest/devguide/restoring-s3.html
