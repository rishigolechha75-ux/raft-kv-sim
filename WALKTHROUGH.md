# Walkthrough

A study guide for this repository: what problem it solves, how the pieces fit,
why each design choice was made, and what you should be able to explain in an
interview.

## 1. The problem

A single database server is a single point of failure. If you copy the data to
several machines you survive crashes, but now the copies can disagree: a
network partition can let two machines each believe they are in charge and
accept conflicting writes. **Consensus** is the problem of getting a group of
machines to agree on one sequence of operations even when some of them crash,
restart, run slowly, or cannot talk to each other.

Raft (Ongaro and Ousterhout, 2014) is the consensus algorithm behind etcd
(and therefore Kubernetes), Consul, CockroachDB, TiKV and many others. It was
designed to be easier to understand than Paxos, but implementations are still
notoriously easy to get subtly wrong. The bugs only appear under rare
interleavings of crashes and message delays, which ordinary unit tests and
manual testing almost never produce.

This project therefore has two halves of equal weight:

1. **A Raft-replicated key-value store** that runs as real processes over TCP.
2. **A deterministic simulator and two checkers** that run the same Raft code
   through thousands of randomised fault schedules and verify that nothing
   unsafe ever happens - plus a mutation-testing experiment showing that the
   checkers really do catch real Raft bugs.

## 2. Architecture and data flow

```
                       +-----------------------------+
   client  ----------> |  RaftServer (asyncio, TCP)  |  one per process
   (RaftClient)        |   - tick timer (10 ms)      |
                       |   - FileStorage (fsync)     |
                       |   - peer sockets            |
                       +--------------+--------------+
                                      |  tick() / step(msg) / propose(cmd)
                                      v
                       +-----------------------------+
                       |  RaftNode  (pure, no I/O)   |  same code in both worlds
                       |   election, replication,    |
                       |   commit, apply             |
                       +--------------+--------------+
                                      |  apply(cmd)
                                      v
                       +-----------------------------+
                       |  KVStateMachine + dedup     |
                       +-----------------------------+

   Simulation world (one thread, logical clock, seeded RNG):
     Simulation --> 3-5 RaftNodes + MemoryStorage
                --> simulated network (delay, drop, duplicate, partition)
                --> nemesis (crash/restart, pause, partition schedules)
                --> closed-loop clients that record a history
     SafetyMonitor checks invariants after every tick
     check_history() checks linearizability at the end
```

**Write path (real cluster):** the client sends `{"kind":"client","cmd":...}`
to the node it thinks is the leader. A follower answers `not_leader` with a
hint. The leader calls `node.propose(cmd)`, which appends to its log and
writes it to disk, then schedules a flush. The flush sends one
`AppendEntries` per follower containing every command proposed since the
last flush (this is the batching). Followers check that their log matches at
`prev_log_index`, append, persist, and reply with their `match_index`. Once a
majority hold the entry and the entry is from the leader's current term, the
leader advances `commit_index`, applies the command to the key-value store and
answers the client. Followers learn the new commit index on the next
`AppendEntries` and apply too.

**Read path:** reads go through the log exactly like writes. That is the
simplest way to make reads linearizable (see trade-offs).

## 3. File-by-file tour

| File | What to look at |
|---|---|
| `src/raftkv/messages.py` | The four Raft RPCs as dataclasses and JSON (de)serialisation. Note that every message carries a `term`. |
| `src/raftkv/storage.py` | What must be durable (`current_term`, `voted_for`, log) and why. `FileStorage` uses atomic rename for hard state and tolerates a torn final log line. |
| `src/raftkv/node.py` | The heart of the project. Read `step()`, then each `_on_*` handler. Key places: `_log_up_to_date` (election restriction), `_on_append` (consistency check, careful truncation, `min(leader_commit, last_new)`), `_maybe_advance_commit` (current-term rule), `_become_leader` (no-op entry), PreVote handling. The `bugs` flags are the mutation points. |
| `src/raftkv/kv.py` | The replicated state machine and the `(client, seq)` dedup table that gives exactly-once semantics for retried commands. |
| `src/raftkv/sim.py` | The deterministic simulator: `FaultProfile`s, the message heap, the nemesis, clients that retry, and the main loop. |
| `src/raftkv/checker/invariants.py` | Online checks of Raft's safety properties using the simulator's global view. |
| `src/raftkv/checker/linearizability.py` | Wing-Gong-Lowe search with memoisation, partitioned by key. |
| `src/raftkv/server.py` | asyncio host: tick loop, newline-delimited JSON over TCP, flush batching, pending client futures. |
| `src/raftkv/client.py` | Leader discovery, redirects, retries with the same sequence number. |
| `src/raftkv/cli.py` | `serve`, `cluster`, `put/get/cas/delete/status`, `sim`. |
| `scripts/sim_campaign.py` | Produces `reports/sim_campaign.json`, `ablation.json`, `mutation.json` and charts. |
| `scripts/bench_cluster.py` | Produces `reports/cluster_bench.json`: real-process throughput, latency and failover time. |
| `tests/` | Unit tests for every handler and rule, checker tests with hand-built histories, simulator integration tests, and real TCP integration tests (failover, restart from disk, no quorum). |

## 4. Key design decisions and trade-offs

**Pure state machine core (no I/O inside `RaftNode`).** etcd's `raft`
library uses the same pattern. Alternatives: an actor/thread-per-node
implementation that sleeps and calls sockets directly. That is simpler to
write but impossible to test deterministically, because the OS scheduler and
real clocks introduce randomness you cannot replay. The cost of the pure
design is a little plumbing (outbox, `take_applied`) in each driver.

**Deterministic simulation instead of (or before) Jepsen.** Jepsen tests
real binaries on real networks and is the industry gold standard, but each run
takes minutes and failures are hard to reproduce. Simulation runs a full
2,600-tick fault schedule in tens of milliseconds and any failing seed replays
exactly with `raftkv sim --seed N --trace`. The trade-off: the simulator only
tests what it models (it does not exercise the real TCP code or real disks),
so this repo also has real-process integration tests and benchmarks.

**No-op entry on election.** Raft only commits entries from the leader's
current term by counting replicas (Figure 8 of the paper shows why counting
old-term entries is unsafe). Without a no-op, a new leader with no new client
traffic could never commit leftovers from earlier terms. With a no-op, it
commits them as soon as the no-op is replicated.

**PreVote.** Without it, a node cut off by a partition keeps incrementing its
term; when it rejoins, its high term forces the healthy leader to step down
and causes an unnecessary election. With PreVote, a node first asks "would you
vote for me?" without changing any term, and peers that still hear from a
leader say no. Cost: one extra round trip before every real election. The
ablation report measures the effect.

**Reads through the log.** Simplest correct option. Alternatives: *ReadIndex*
(leader confirms it is still leader with a heartbeat round, then serves from
its state machine) or *leases* (leader serves locally while a time-based lease
is valid, which depends on bounded clock drift). Both are faster; both are
listed as future work.

**Client dedup table.** A client whose reply is lost must retry, and the
retry can land after the original was already committed. Without dedup, a
retried `cas` or `put` is applied twice, which can violate linearizability.
The mutation test `no_dedup` shows the checker catching exactly this. The
table keeps only the latest `(seq, result)` per client, which is sufficient
because each client has one outstanding request at a time.

**Optimistic pipelining.** After sending entries to a follower the leader
advances `next_index` immediately instead of waiting for the reply. If the
message is lost, the follower rejects the next one and the leader backs off
using `conflict_index`. This is how etcd's "replicate" state works.

**Batching via a coalesced flush.** All proposals that arrive in one event
loop iteration share a single `AppendEntries` per follower (and one fsync on
each follower). This is the main reason throughput grows with client
concurrency in the benchmark.

**No snapshots / log compaction.** The log grows forever and a restarted
node replays it from index 1. Real systems snapshot the state machine and send
`InstallSnapshot` to lagging followers. Left out to keep the core readable.

**No membership changes.** The cluster is fixed at start. Joint consensus or
single-server changes (thesis chapter 4) are future work.

## 5. How the evaluation works and how to read it

**Campaign (`reports/sim_campaign.json`).** For every fault profile and
cluster size, many seeds are run. Each run has 2,000 ticks of faults followed
by 600 ticks of healing, with 4 closed-loop clients on 3 keys (few keys means
lots of contention, which makes anomalies more likely to be visible). Read:

* `failing_runs` - must be 0 for the correct implementation. Any non-zero
  value would be a real bug, replayable by seed.
* `ops_per_1k_ticks` - availability. Lossy networks and partitions reduce it
  because clients time out and retry.
* `latency_ticks_p50/p99` - client-observed latency in simulated ticks.
* `linearizability_unknown` - runs where the search hit its state budget; the
  checker neither passed nor failed them.

**Ablation (`reports/ablation.json`).** Same seeds, three configurations.
Compare `mean_leaders_elected` (PreVote should reduce needless elections) and
`messages_per_completed_op` (batching should reduce message count).

**Mutation testing (`reports/mutation.json`).** Each bug in
`raftkv.node.KNOWN_BUGS` is a one-line deviation from the paper. For each bug,
500 seeds are run in two configurations and we count how many seeds produce a
violation. This measures the *power* of the test harness, not of Raft. The
`none` row is the false-positive check (must be 0). Low detection rates are
real information: some bugs need a precise interleaving (for example a crash
exactly between two vote requests), and the "adversarial" configuration
(3 nodes, PreVote off, batch size 1, frequent short crashes) is designed to
create those interleavings more often. The `first_signal` field shows which
checker fired first.

**Cluster benchmark (`reports/cluster_bench.json`).** Real processes on one
machine. The 1-node cluster is the no-replication baseline. On localhost the
network is nearly free, so the difference between 1 and 3 nodes is mostly the
extra round trip plus the follower's fsync; on a real network the gap would be
larger. Failover gap is roughly "time to notice the leader is gone" (election
timeout 150-300 ms) plus one election round plus client retry delay.

## 6. Likely interview questions

1. **Why does Raft need a majority, and why an odd number of nodes?**
   Any two majorities intersect, so at least one node in every new quorum has
   seen every committed entry. Four nodes tolerate one failure, same as three,
   but need more acknowledgements, so odd sizes are more efficient.

2. **What exactly must be persisted before replying to an RPC, and why?**
   `current_term`, `voted_for` and the log. If `voted_for` is lost on a crash
   the node can vote twice in one term and two leaders can be elected - the
   `no_vote_persist` mutation demonstrates this.

3. **What is the election restriction?** A node votes only for candidates
   whose log is at least as up to date (compare last term, then last index).
   Together with majority quorums, this guarantees a new leader has every
   committed entry (leader completeness). The `skip_log_check` mutation breaks
   it and the monitor sees leaders missing committed entries.

4. **Explain Figure 8.** A leader may see an entry from an earlier term on a
   majority, yet that entry can still be overwritten by a later leader that
   never had it. So leaders only commit current-term entries by counting;
   earlier entries become committed indirectly. In this code the no-op on
   election plus batching makes the `commit_old_term` bug almost unobservable
   in the default configuration, which is why it is only caught in the
   adversarial configuration.

5. **Why must a follower set `commit_index = min(leader_commit, index of last
   new entry)` and not `min(leader_commit, last_index)`?** Entries past the
   ones this message verified might be stale leftovers from an old leader. The
   `unbounded_follower_commit` mutation applies such entries.

6. **Why not truncate the log on every AppendEntries?** Messages can be
   delayed and reordered; an old, shorter AppendEntries would erase entries
   the follower already acknowledged and the leader may have committed.

7. **What is linearizability and how do you check it?** Every operation
   appears to take effect atomically at some instant between its call and its
   return. Checking is NP-complete in general; the Wing-Gong search with
   memoisation of `(linearised set, state)` and per-key partitioning makes it
   fast on realistic histories.

8. **How do you handle an operation whose reply never arrived?** Treat it as
   having an infinite return time and unknown output: it may take effect at
   any point after its invocation, or never.

9. **How does the system give exactly-once semantics?** At-least-once
   delivery from client retries plus a dedup table keyed by `(client, seq)`
   applied inside the replicated state machine, so every replica deduplicates
   identically.

10. **What does PreVote solve?** Disruption by a rejoining partitioned node
    with an inflated term. Peers refuse pre-votes while they hear from a live
    leader, so the partitioned node never bumps its term.

11. **Why is determinism so valuable for testing distributed systems?** A
    failing seed is a perfect reproduction. You can add logging and re-run the
    exact same schedule, which is impossible with real threads and clocks.

12. **What does the simulator not catch?** Bugs in the TCP layer, the real
    disk (e.g. fsync semantics, torn writes beyond the last line), real clock
    behaviour, and performance problems. That is why there are real-process
    tests and benchmarks too.

13. **How would you make reads faster?** ReadIndex (one heartbeat round to
    confirm leadership, no log write) or leader leases (no round trip, but
    relies on bounded clock drift). Follower reads with ReadIndex can spread
    read load.

14. **How would you add snapshots?** Periodically serialise the state
    machine together with `last_included_index/term`, discard the log prefix,
    and send `InstallSnapshot` to followers whose `next_index` falls before the
    snapshot.

15. **What limits throughput here?** Python's single-threaded event loop and
    JSON encoding on the leader, and fsync on every node. Batching amortises
    both; a production system would also pipeline disk writes with network
    sends (the leader can send before its own fsync completes).

## 7. Concepts to study further

* Raft paper (extended version): https://raft.github.io/raft.pdf
* Diego Ongaro's PhD thesis (PreVote, membership changes, snapshots): https://github.com/ongardie/dissertation
* The Raft website and visualisation: https://raft.github.io/
* Herlihy and Wing, "Linearizability: A Correctness Condition for Concurrent Objects" (1990): https://cs.brown.edu/~mph/HerlihyW90/p463-herlihy.pdf
* Lowe, "Testing for linearizability" (2017): https://www.cs.ox.ac.uk/people/gavin.lowe/LinearizabiltyTesting/
* Porcupine, a fast linearizability checker: https://github.com/anishathalye/porcupine
* Jepsen analyses and Knossos: https://jepsen.io/analyses
* FoundationDB's deterministic simulation testing (talk by Will Wilson): https://www.youtube.com/watch?v=4fFDFbi3toc
* TigerBeetle's VOPR simulator: https://docs.tigerbeetle.com/concepts/safety/
* etcd raft library design (the pure state-machine pattern): https://github.com/etcd-io/raft
* MIT 6.5840 Distributed Systems labs (Raft): https://pdos.csail.mit.edu/6.824/
