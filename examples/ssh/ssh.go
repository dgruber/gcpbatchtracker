package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	_ "embed"

	"github.com/dgruber/drmaa2interface"
	gcpbatchtracker "github.com/dgruber/gcpbatchtrackertracker"
)

var (
	//go:embed jobscript.sh
	jobScript []byte
)

func main() {

	// gcloud auth application-default login

	if len(os.Args) != 2 {
		fmt.Printf("Please provide a Google project as first argument")
		os.Exit(1)
	}

	tracker, err := gcpbatchtracker.NewGCPBatchTracker(os.Args[1], "us-central1")
	if err != nil {
		panic(err)
	}
	jobID, err := tracker.AddJob(drmaa2interface.JobTemplate{
		JobCategory:       "ucdagru/bssh:latest",
		RemoteCommand:     "/bin/sh",
		Args:              []string{"-c", string(jobScript)},
		MinSlots:          4,
		MaxSlots:          4,
		Priority:          50, // from 0 to 100
		CandidateMachines: []string{"e2-standard-4"},
		ResourceLimits: map[string]string{
			"cpumilli":    "4000",  // 4 cores
			"bootdiskmib": "10000", // larger boot disk
			"runtime":     "15m",   // max. runtime 15 minutes
		},
		Extension: drmaa2interface.Extension{
			ExtensionList: map[string]string{
				gcpbatchtracker.ExtensionSpot: "true",
			},
		},
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("JobID: %s\n", jobID)
	state, substate, err := tracker.JobState(jobID)
	for state != drmaa2interface.Done &&
		state != drmaa2interface.Failed {
		fmt.Printf("Job is in state %s (%s)\n", state.String(), substate)
		state, substate, err = tracker.JobState(jobID)
		<-time.Tick(time.Second)
	}
	fmt.Printf("Job is in state %s (%s)\n", state.String(), substate)
	ji, err := tracker.JobInfo(jobID)
	if err != nil {
		panic(err)
	}
	formatted, _ := json.Marshal(ji)
	fmt.Printf("JobInfo: %s\n", string(formatted))

}
