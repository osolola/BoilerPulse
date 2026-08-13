# benchmarks/

Real, measured benchmark runs — see `docs/benchmarking.md` for methodology,
results, and what was found (a real Raft replication bug, fixed, and a
real WAL-fsync capacity ceiling, documented). `results/` holds the raw JSON
`simulator.Report` output from each run this project actually performed:
single-node and 3-node baselines across all five named scenarios, plus one
failure-injection run. Nothing here is invented — every file is exactly
what `cmd/simulator` printed when it was run.
