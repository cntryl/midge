# WAL Invariants (Draft)

- Frames are ordered by monotonically increasing LSN; no gaps after a committed frame.
- Each frame is checksummed; a bad checksum renders the frame and all later frames invalid.
- A frame is durable only after fsync of the WAL file and directory entry.
- Recovery replays frames in order and stops at the first checksum or length error.
- Zero-length or partial trailing frames must be ignored (crash safety).
