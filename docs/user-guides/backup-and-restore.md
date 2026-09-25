# Backup and restore

Midge can capture a versioned backup directory from a local or
`CloudSimulated` engine. The capture briefly pauses mutations while it syncs
the current WAL and pins the durable file set. It copies and checksums the
files after releasing that barrier, so the full backup duration does not hold
the write pause.

```rust,no_run
use cntryl_midge::{Engine, OpenOptions};
use std::time::Duration;

let engine = Engine::open(OpenOptions::local("./data").build()?)?;
let inventory = engine.backup_to("./backups/run-001", Duration::from_secs(30))?;
println!("captured through sequence {}", inventory.durability_frontier);
# Ok::<(), Box<dyn std::error::Error>>(())
```

The destination must be new. A completed artifact contains `backup.json` and
an `objects/` tree. The inventory records the backup format, database format,
capture frontier, engine version, storage kind, and each object's size and
CRC32C. Copy the complete directory as one unit and retain it until the backup
is no longer needed.

Restore accepts only a new target and a matching storage kind. It validates
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
