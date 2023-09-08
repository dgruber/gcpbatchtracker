package gcpbatchtracker

import (
	"errors"

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

	if job.Status != nil {
		ji.WallclockTime = job.Status.RunDuration.AsDuration()
		ji.FinishTime = job.CreateTime.AsTime().Add(job.Status.RunDuration.AsDuration())
	}
	ji.Slots = job.TaskGroups[0].Parallelism
	ji.State, ji.SubState, _ = ConvertJobState(job)

	if job.Status.State == batchpb.JobStatus_FAILED {
		// TODO how to get the exit status?
		ji.ExitStatus = 1
	}

	ji.State, ji.SubState, _ = ConvertJobState(job)
	ji.SubmissionTime = job.CreateTime.AsTime()

	return ji, nil
}
