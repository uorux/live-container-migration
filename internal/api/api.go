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
	Container   string `json:"container"`
	ContainerID string `json:"containerID"` // containerd id, without the "containerd://" prefix
	// Key under which the destination stages the restore (namespace/destination pod/container).
	DestKey string        `json:"destKey"`
	DestURL string        `json:"destURL"` // base URL of the destination agent, e.g. http://10.0.0.2:7790
	PreDump PreDumpPolicy `json:"preDump"`
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
	RootfsMS    int64     `json:"rootfsMs"`
	RestoreMS   int64     `json:"restoreMs"`
	RelayResume string    `json:"relayResume,omitempty"`
	ExitCode    int       `json:"exitCode"`
	Error       string    `json:"error,omitempty"`
}
