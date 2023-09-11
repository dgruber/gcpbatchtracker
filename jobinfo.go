package gcpbatchtracker

import (
	"errors"
	"strings"
	"time"

	"cloud.google.com/go/batch/apiv1/batchpb"
	"github.com/dgruber/drmaa2interface"
)

func BatchJobToJobInfo(job *batchpb.Job) (drmaa2interface.JobInfo, error) {
	if job == nil {
		return drmaa2interface.JobInfo{}, errors.New("batch job is nil")
	}
	ji := drmaa2interface.JobInfo{
		ID: job.Name,
	}

	ji.SubmissionTime = job.CreateTime.AsTime()
	if job.Status != nil {
		ji.DispatchTime, ji.FinishTime = TimesFromStatusEvents(job.Status.StatusEvents)
		ji.WallclockTime = job.Status.RunDuration.AsDuration()
	}

	ji.Annotation = job.Labels["accounting"]

	// job template: max slots
	ji.Slots = job.TaskGroups[0].TaskCount

	if job.Status.State == batchpb.JobStatus_FAILED {
		ji.ExitStatus = 1
	}

	ji.State, ji.SubState, _ = ConvertJobState(job)

	return ji, nil
}

func TimesFromStatusEvents(events []*batchpb.StatusEvent) (dispatchTime, finishTime time.Time) {
	for _, event := range events {
		if event.Type != "STATUS_CHANGED" {
			continue
		}
		if strings.Contains(event.Description, "SCHEDULED to RUNNING") {
			dispatchTime = event.EventTime.AsTime()
		} else if strings.Contains(event.Description, "RUNNING to") {
			finishTime = event.EventTime.AsTime()
		}
	}
	return
}
