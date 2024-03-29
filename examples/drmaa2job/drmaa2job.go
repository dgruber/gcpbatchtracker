package main

import (
	"fmt"

	"github.com/dgruber/drmaa2interface"
	"github.com/dgruber/drmaa2os"
	"github.com/dgruber/gcpbatchtracker"
)

// gcloud auth application-default login

func main() {
	sm, err := drmaa2os.NewGoogleBatchSessionManager(
		gcpbatchtracker.GoogleBatchTrackerParams{
			GoogleProjectID: "googleprojectid",
			Region:          "us-central1",
		},
		"jobsession.db",
	)
	if err != nil {
		panic(err)
	}

	js, err := sm.OpenJobSession("testsession")
	if err != nil {
		js, err = sm.CreateJobSession("testsession", "")
		if err != nil {
			panic(err)
		}
	}
	defer js.Close()

	job, err := js.RunJob(drmaa2interface.JobTemplate{
		RemoteCommand:     "echo hello google batch",
		JobCategory:       gcpbatchtracker.JobCategoryScript,
		CandidateMachines: []string{"e2-standard-4"},
		MinSlots:          1,
		MaxSlots:          2,
		ResourceLimits: map[string]string{
			"cpumilli": "4000",
			"runtime":  "3m",
		},
		Extension: drmaa2interface.Extension{
			ExtensionList: map[string]string{
				gcpbatchtracker.ExtensionSpot: "true",
				gcpbatchtracker.ExtensionProlog: `#!/bin/bash
echo "hello from prolog"
`,
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

	// Get job template
	jt, err := job.GetJobTemplate()
	if err != nil {
		panic(err)
	}
	templateJSON, err := jt.MarshalJSON()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Job template: %s\n", string(templateJSON))
}
