// Package api holds the wire types shared by lcm-agent and its clients (the lcm CLI/controller,
// and the destination agent receiving staged images from a source agent).
package api

import "time"

// DefaultPort is where lcm-agent listens on every node.
const DefaultPort = 7790

// CheckpointRequest asks the agent on the source node to live-checkpoint one container and stage
// the result on the destination node's agent under Key.
type CheckpointRequest struct {
	MigrationID string `json:"migrationID"`
	Namespace   string `json:"namespace"`
	Pod         string `json:"pod"`
	PodUID      string `json:"podUID"` // the source pod's UID: its fence key
	Container   string `json:"container"`
	ContainerID string `json:"containerID"` // containerd id, without the "containerd://" prefix
	// Key under which the destination stages the restore (namespace/destination pod/container).
	DestKey string        `json:"destKey"`
	DestURL string        `json:"destURL"` // base URL of the destination agent, e.g. http://10.0.0.2:7790
	PreDump PreDumpPolicy `json:"preDump"`
	// LazyRestore restores with CRIU lazy pages (experimental): shorter blackout, memory
	// faulted in after the process resumes.
	LazyRestore bool `json:"lazyRestore,omitempty"`
	// Relay controls the in-container lcm-relay handshake: "auto" (quiesce if its control socket
	// exists), "required", or "off".
	Relay string `json:"relay,omitempty"`
}

// PreDumpPolicy controls iterative pre-copy: pre-dump and ship memory while the workload runs,
// until the dirty set is small enough or stops shrinking.
type PreDumpPolicy struct {
	Max         int   `json:"max"`         // maximum pre-dump rounds (0 = no pre-copy)
	TargetBytes int64 `json:"targetBytes"` // stop once a round dumps fewer bytes than this
}

// Event is one line of the NDJSON progress stream the checkpoint endpoint returns.
type Event struct {
	Time    time.Time `json:"time"`
	Phase   string    `json:"phase"` // predump, quiesce, fence, dump, rootfs, ship, ready, error, done
	Message string    `json:"message,omitempty"`
	Round   int       `json:"round,omitempty"`
	Bytes   int64     `json:"bytes,omitempty"`
	Millis  int64     `json:"ms,omitempty"`
	// Set on the final "done" event.
	Result *CheckpointResult `json:"result,omitempty"`
}

type CheckpointResult struct {
	FreezeStart  time.Time `json:"freezeStart"` // when the final dump began (the workload stopped)
	ReadyAt      time.Time `json:"readyAt"`     // when the destination had everything staged
	PreDumps     []Round   `json:"preDumps"`
	FinalDumpMS  int64     `json:"finalDumpMs"`
	FinalBytes   int64     `json:"finalBytes"`
	RootfsBytes  int64     `json:"rootfsBytes"`
	ShipMS       int64     `json:"shipMs"`
	FrozenMS     int64     `json:"frozenMs"` // FreezeStart..ReadyAt (restore time is added on the destination)
	SourceKilled bool      `json:"sourceKilled"`
}

type Round struct {
	DumpMS int64 `json:"dumpMs"`
	ShipMS int64 `json:"shipMs"`
	Bytes  int64 `json:"bytes"`
}

// RestoreResult is what lcm-runc records on the destination after a restore attempt.
type RestoreResult struct {
	ContainerID string    `json:"containerID"`
	Started     time.Time `json:"started"`
	Finished    time.Time `json:"finished"`
	// ReadyAt is when the staging was marked READY, on the same (destination) clock as Finished.
	ReadyAt     time.Time `json:"readyAt,omitzero"`
	RootfsMS    int64     `json:"rootfsMs"`
	RestoreMS   int64     `json:"restoreMs"`
	RelayResume string    `json:"relayResume,omitempty"`
	LazyPages   bool      `json:"lazyPages,omitempty"`
	ExitCode    int       `json:"exitCode"`
	Error       string    `json:"error,omitempty"`
}

// RestoreEvent is one line of /var/lib/lcm/events/restores.jsonl, appended by lcm-runc after
// every restore attempt and turned into metrics by the agent.
type RestoreEvent struct {
	Key string `json:"key"`
	RestoreResult
}

// MigrationReport is what the lcm CLI sends to the agents when a migration ends, for metrics and
// for `lcm history`.
type MigrationReport struct {
	ID         string    `json:"id"`
	Time       time.Time `json:"time"`
	Namespace  string    `json:"namespace"`
	SourcePod  string    `json:"sourcePod"`
	SourceNode string    `json:"sourceNode"`
	DestPod    string    `json:"destPod"`
	DestNode   string    `json:"destNode"`
	// Outcome: success, rolled_back (restore failed, workload back on the source node),
	// aborted (failed before the source stopped; it kept running), failed (needs an operator).
	Outcome    string  `json:"outcome"`
	Error      string  `json:"error,omitempty"`
	BlackoutMS int64   `json:"blackoutMs,omitempty"`
	FrozenMS   int64   `json:"frozenMs,omitempty"`
	RestoreMS  int64   `json:"restoreMs,omitempty"`
	PreDumps   []Round `json:"preDumps,omitempty"`
	TotalMS    int64   `json:"totalMs"`
}
