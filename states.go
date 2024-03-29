package gcpbatchtracker

import (
	"fmt"

	"cloud.google.com/go/batch/apiv1/batchpb"
	"github.com/dgruber/drmaa2interface"
)

func ConvertJobState(job *batchpb.Job) (drmaa2interface.JobState, string, error) {
	switch job.Status.State {
	case batchpb.JobStatus_STATE_UNSPECIFIED:
		return drmaa2interface.Undetermined, batchpb.JobStatus_State_name[int32(batchpb.JobStatus_STATE_UNSPECIFIED)], nil
	case batchpb.JobStatus_QUEUED:
		return drmaa2interface.Queued, batchpb.JobStatus_State_name[int32(batchpb.JobStatus_QUEUED)], nil
	case batchpb.JobStatus_RUNNING:
		return drmaa2interface.Running, batchpb.JobStatus_State_name[int32(batchpb.JobStatus_RUNNING)], nil
	case batchpb.JobStatus_SUCCEEDED:
		return drmaa2interface.Done, batchpb.JobStatus_State_name[int32(batchpb.JobStatus_SUCCEEDED)], nil
	case batchpb.JobStatus_FAILED:
		return drmaa2interface.Failed, batchpb.JobStatus_State_name[int32(batchpb.JobStatus_FAILED)], nil
	case batchpb.JobStatus_SCHEDULED:
		return drmaa2interface.Queued, batchpb.JobStatus_State_name[int32(batchpb.JobStatus_SCHEDULED)], nil
	case batchpb.JobStatus_DELETION_IN_PROGRESS:
		return drmaa2interface.Running, batchpb.JobStatus_State_name[int32(batchpb.JobStatus_DELETION_IN_PROGRESS)], nil
	}
	fmt.Printf("internal error: unknown state (please report): %s", batchpb.JobStatus_State_name[int32(job.Status.State)])
	return drmaa2interface.Undetermined, fmt.Sprintf("unknown state: %v", job.Status.State), nil
}
