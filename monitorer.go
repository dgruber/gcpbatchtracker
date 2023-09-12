package gcpbatchtracker

import (
	"context"
	"fmt"

	"cloud.google.com/go/batch/apiv1/batchpb"
	"github.com/dgruber/drmaa2interface"
)

// All methods required for the DRMAA2 MonitoringSession

func (t *GCPBatchTracker) OpenMonitoringSession(name string) error {
	// DRMAA2 sessions are just labels in Google Batch
	return nil
}

func (t *GCPBatchTracker) GetAllJobIDs(filter *drmaa2interface.JobInfo) ([]string, error) {
	// don't filter for session names
	return listJobs(t, false)
}

func (t *GCPBatchTracker) GetAllQueueNames(filter []string) ([]string, error) {
	// no queues in Google Batch
	return []string{}, nil
}

func (t *GCPBatchTracker) GetAllMachines(filter []string) ([]drmaa2interface.Machine, error) {
	// TODO: get machine types from Google Cloud API
	return nil, fmt.Errorf("not yet implemented")
}

func (t *GCPBatchTracker) CloseMonitoringSession(name string) error {
	return nil
}

// JobInfoFromMonitor might collect job state and job info in a
// different way as a JobSession with persistent storage does
func (t *GCPBatchTracker) JobInfoFromMonitor(jobID string) (drmaa2interface.JobInfo, error) {
	job, err := t.client.GetJob(context.Background(), &batchpb.GetJobRequest{
		Name: jobID,
	})
	if err != nil {
		return drmaa2interface.JobInfo{}, err
	}
	return BatchJobToJobInfo(t.project, job)
}
