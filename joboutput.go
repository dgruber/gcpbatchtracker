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

func GetJobOutput(projectID, jobUid string, limit int64) ([]string, error) {
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

	var lines []string
	if limit != 0 {
		lines = make([]string, 0, limit)
	} else {
		lines = make([]string, 0, 64)
	}

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
		if limit != 0 && int64(len(lines)) > limit {
			lines = lines[1:]
		}
	}

	return lines, nil
}

// JobOutput is not part of JobTracker interface but it could be a future
// JobOutputer interface extension. This would be also useful for k8s,
// Docker, and other JobTracker which currently store the output as
// JobInfo extension.
// If lastNLines is 0 then all lines are returned.
func (t *GCPBatchTracker) JobOutput(jobID string, lastNLines int64) ([]string, error) {
	job, err := t.client.GetJob(context.Background(), &batchpb.GetJobRequest{
		Name: jobID,
	})
	if err != nil {
		return nil, err
	}
	return GetJobOutput(t.project, job.Uid, lastNLines)
}
