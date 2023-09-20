package gcpbatchtracker

import (
	"context"
	"fmt"

	"cloud.google.com/go/batch/apiv1/batchpb"
	"github.com/dgruber/drmaa2interface"
)

// Implements the job tracker "JobTemplater" interface which is used
// by the DRMAA2 integration to retrieve job templates. The job template
// needs to be stored somewhere. The easiest way is to store is as env
// variable of the job so that it can be retrieved directly from the
// backend and does not need to be stored in a local database.

func (t *GCPBatchTracker) JobTemplate(jobID string) (drmaa2interface.JobTemplate, error) {
	// get job template from env variables
	job, err := t.client.GetJob(context.Background(),
		&batchpb.GetJobRequest{
			Name: jobID,
		})
	if err != nil {
		return drmaa2interface.JobTemplate{},
			fmt.Errorf("could not get job %s: %v", jobID, err)
	}

	for _, group := range job.GetTaskGroups() {
		if group.TaskSpec != nil && group.TaskSpec.Environment != nil &&
			group.TaskSpec.Environment.Variables != nil {
			value, exists := group.TaskSpec.Environment.Variables[EnvJobTemplate]
			if exists {
				return GetJobTemplateFromBase64(value)
			}
		}
	}

	return drmaa2interface.JobTemplate{},
		fmt.Errorf("could not find job template in env variables")
}
