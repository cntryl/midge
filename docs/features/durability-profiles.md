# Durability Profiles

Midge supports multiple **durability profiles** that control how aggressively data is persisted and synced to storage.  
Each embedding host configures the appropriate profile based on its reliability and performance needs.

- **Strict Durability** — used by the Fitz broker.  
  Every write is fully fsynced and replicated to blob storage for crash consistency.  
  Suitable for authoritative, production-grade data.

- **Weak Durability** — used by the Portia materialization engine.  
  WAL and manifest operations are buffered and flushed periodically, trading persistence for throughput.  
  Safe for ephemeral or rebuildable state.

Future profiles (e.g., *MemoryOnly*, *Relaxed*) may provide finer control between strict and weak modes.
