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
		logadmin.NewestFirst(), // reverse order
	)

	var lines []string
	if limit > 0 {
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
		if limit > 0 && int64(len(lines)) >= limit {
			break
		}
	}

	// reverse order of lines (change to slices.Reverse() in 1.21)
	// https://cs.opensource.google/go/go/+/master:src/slices/slices.go;l=488
	for i, j := 0, len(lines)-1; i < j; i, j = i+1, j-1 {
		lines[i], lines[j] = lines[j], lines[i]
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
