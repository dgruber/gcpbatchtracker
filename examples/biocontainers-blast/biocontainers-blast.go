package main

import (
	"fmt"
	"os"

	_ "embed"

	"github.com/dgruber/drmaa2interface"
	"github.com/dgruber/drmaa2os"
	"github.com/dgruber/gcpbatchtracker"
)

//go:embed blast.sh
var jobScript string

// gcloud auth application-defaults login

func main() {
	js, err := GetJobSession("testsession")
	if err != nil {
		fmt.Printf("could not get job session: %v\n", err)
		os.Exit(1)
	}
	job, err := js.RunJob(drmaa2interface.JobTemplate{
		RemoteCommand:     "/bin/bash",
		Args:              []string{"-c", jobScript},
		JobCategory:       "biocontainers/blast:2.2.31",
		CandidateMachines: []string{"e2-standard-4"},
		MinSlots:          1,
		MaxSlots:          1,
		ResourceLimits: map[string]string{
			"cpumilli": "4000",
			"runtime":  "30m",
		},
		// StageOutFiles mounts the bucket to the VM and copies the file
		// from the VM to the bucket. This is like StageInFiles but in case
		// of a bucket it creates the bucket if it does not exist. For that
		// of course you need to have the right permissions.
		StageOutFiles: map[string]string{
			"/host": "gs://" + os.Getenv("GOOGLE_BUCKET_NAME"), // Please modify this to your bucket
		},
		// save money by using spot instances
		Extension: drmaa2interface.Extension{
			ExtensionList: map[string]string{
				gcpbatchtracker.ExtensionSpot: "true",
			},
		},
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("JobID: %s\n", job.GetID())
	fmt.Printf("Job state: %s\n", job.GetState())
	fmt.Printf("Waiting for job %s to start\n", job.GetID())
	startedJob, err := js.WaitAnyStarted([]drmaa2interface.Job{job},
		drmaa2interface.InfiniteTime)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Job %s started\n", startedJob.GetID())
	fmt.Printf("Waiting for job %s to finish\n", startedJob.GetID())
	termintedJob, err := js.WaitAnyTerminated([]drmaa2interface.Job{job},
		drmaa2interface.InfiniteTime)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Job %s terminated\n", termintedJob.GetID())
}

func GetJobSession(jobSessionName string) (drmaa2interface.JobSession, error) {
	sm, err := drmaa2os.NewGoogleBatchSessionManager(
		gcpbatchtracker.GoogleBatchTrackerParams{
			GoogleProjectID: os.Getenv("GOOGLE_PROJECT_ID"),
			Region:          "us-central1",
		},
		"jobsession.db",
	)
	if err != nil {
		return nil, fmt.Errorf("could not create session manager: %v", err)
	}
	js, err := sm.OpenJobSession("testsession")
	if err != nil {
		js, err = sm.CreateJobSession("testsession", "")
		if err != nil {
			return nil, fmt.Errorf("could not create job session: %v", err)
		}
	}
	return js, nil
}
