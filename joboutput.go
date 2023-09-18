package gcpbatchtracker

import (
	"context"
	"fmt"

	"cloud.google.com/go/batch/apiv1/batchpb"
	"cloud.google.com/go/logging/logadmin"
	"google.golang.org/api/iterator"
)

const (
	BatchTaskLogs = "batch_task_logs"
)

func GetJobOutput(projectID, jobUid string) ([]string, error) {
	ctx := context.Background()

	adminClient, err := logadmin.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("Failed to create logadmin client: %w", err)
	}
	defer adminClient.Close()

	iter := adminClient.Entries(ctx,
		logadmin.Filter(fmt.Sprintf(`logName = "projects/%s/logs/%s" AND labels.job_uid=%s`,
			projectID, BatchTaskLogs, jobUid)),
	)

	lines := make([]string, 0, 64)

	for {
		// how to distinguish between stdout and stderr?
		logEntry, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("could not fetch log entry: %w", err)
		}
		lines = append(lines, logEntry.Payload.(string))
	}

	return lines, nil
}

func (t *GCPBatchTracker) JobOutput(jobID string) ([]string, error) {
	job, err := t.client.GetJob(context.Background(), &batchpb.GetJobRequest{
		Name: jobID,
	})
	if err != nil {
		return nil, err
	}
	return GetJobOutput(t.project, job.Uid)
}
