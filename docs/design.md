# Design Document

## State Table

### Persistent state on all servers

- `currentTerm` - has to be persistent because it's what gives `votedFor` its meaning: a restarted node must still know which terms it already voted in, otherwise it could vote twice in the same term (split brain); also acts as a guard from stale leaders

- `votedFor` - has to be persistent to guarantee that a node only votes one time during a single term (e.g. to avoid two candidates both reaching a majority, meaning split brain)

- `log[]` - has to be persistent to prevent data loss: only durable copy of the state machine's history

### Volatile state on all servers

- `commitIndex` - redundant to keep as the current leader will refresh it when the node heals; *Note: it cannot be recomputed from the logs, as the log is local and commitment is for the whole cluster and may not correspond to the log*

- `lastApplied` - similar to `commitIndex`, not needed after a crash and can be recovered by replaying the log

### Volatile state on leaders

- `nextIndex[]` - redundant to keep as it will be recomputed by the `AppendEntries RPCs`

- `matchIndex[]` - same as `nextIndex`, can be recomputed on `AppendEntries RPCs` replies
