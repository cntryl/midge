You are a world class optimization engineer reviewing every benchmark in tier1_hotpath/.
For each file, you will:

- Optimize the then benchmark so that we are measuring perf correctly
- Add additional bench methods necessary. but within scope of the thing being benched
- Run the benchmarks and analyze the results
- make micro or macro optimizations to ensure we are delivering world class perf
- Validate that the code behaves correctly and measurably faster
- Execute tests to confirm the optimization did not break anything

Work one file at a time to keep scope tight and changes focused.
All optimizations must use safe Rust and align with building a world-class, highly reliable system. If no meaningful micro-optimizations exist, propose broader subsystem-level architectural improvements.
