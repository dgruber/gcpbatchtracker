package gcpbatchtracker

import (
	"context"
	"errors"
	"fmt"
	"time"

	batch "cloud.google.com/go/batch/apiv1"
	"cloud.google.com/go/batch/apiv1/batchpb"
	"github.com/dgruber/drmaa2interface"
	"github.com/dgruber/drmaa2os/pkg/helper"
	"github.com/dgruber/drmaa2os/pkg/jobtracker"
	"google.golang.org/api/iterator"
)

// GCPBatchTracker implements the JobTracker interface so that it can be
// used as backend in drmaa2os project.
type GCPBatchTracker struct {
	client *batch.Client
	// GCP project ID
	project string
	// GCP location
	location string
	// job session name
	drmaa2session string
}

// NewGCPBatchTracker returns a new GCPBatchTracker instance which is used
// for managing jobs in Google Batch. The project and location parameters
// define the Google Cloud project and the location (like "us-central1").
// The drmaa2session parameter is optional and can be used to filter for
// jobs which are in the same job session. If the job session is "" then
// all jobs are made visible.
// GCPBatchTracker implements the JobTracker interface so that it can be
// used as backend in drmaa2os project and wfl.
func NewGCPBatchTracker(drmaa2session string, project, location string) (*GCPBatchTracker, error) {
	ctx := context.Background()
	c, err := batch.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	return &GCPBatchTracker{
		client:        c,
		project:       project,
		location:      location,
		drmaa2session: drmaa2session,
	}, nil
}

// ListJobs returns all visible job IDs or an error.
func (t *GCPBatchTracker) ListJobs() ([]string, error) {
	return listJobs(t, true)
}

// listJobs returns all visible job IDs or an error. If useJobSessionFilter
// is true then only jobs which are in the same job session are returned.
func listJobs(t *GCPBatchTracker, useJobSessionFilter bool) ([]string, error) {
	jobs := make([]string, 0)
	req := &batchpb.ListJobsRequest{
		Parent: fmt.Sprintf("projects/%s/locations/%s", t.project, t.location),
	}
	iter := t.client.ListJobs(context.Background(), req)
	for {
		job, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		// filter for jobsession, if job session is "" then all jobs are returned
		if useJobSessionFilter && t.drmaa2session != "" {
			if job.Labels["drmaa2session"] != t.drmaa2session {
				continue
			}
		}
		jobs = append(jobs, job.Name)
	}
	return jobs, nil
}

// ListArrayJobs returns all job IDs an job array ID (or array job ID)
// represents or an error.
func (t *GCPBatchTracker) ListArrayJobs(arrayjobID string) ([]string, error) {
	// TODO implement real job arrays
	return helper.ArrayJobID2GUIDs(arrayjobID)
}

// AddJob creates a Google Batch job which is defined by the DRMAA2 job
// template.
// Job names must be unique in Google Batch hence it is automatically created
// by the backend. The CandidateMachines field is used to define the machine
// type (like "n2-standard-2") to be used. Exactly one machine type must be
// specified. The ResourceLimits field is used to define the CPU and runtime
// limits.
// On success the job ID (job name) is returned.
func (t *GCPBatchTracker) AddJob(jt drmaa2interface.JobTemplate) (string, error) {
	req, err := ConvertJobTemplateToJobRequest(t.drmaa2session, t.project, t.location, jt)
	if err != nil {
		return "", err
	}
	// do some init: in case the stage out bucket does not exist, create it
	if err := CreateMissingStageOutBuckets(t.project, jt.StageOutFiles); err != nil {
		return "", fmt.Errorf("could not create stage out buckets: %v", err)
	}
	job, err := t.client.CreateJob(context.Background(), req)
	if err != nil {
		return "", err
	}
	return job.Name, nil
}

// AddArrayJob makes a mass submission of jobs defined by the same job template.
// Many HPC workload manager support job arrays for submitting 10s of thousands
// of similar jobs by one call. The additional parameters define how many jobs
// are submitted by defining a TASK_ID range. Begin is the first task ID (like 1),
// end is the last task ID (like 10), step is a positive integeger which defines
// the increments from one task ID to the next task ID (like 1). maxParallel is
// an arguments representating an optional functionality which instructs the
// backend to limit maxParallel tasks of this job arary to run in parallel.
// Note, that jobs use the TASK_ID environment variable to identifiy which
// task they are and determine that way what to do (like which data set is
// accessed).
//
// With Google Batch job arrays can be created by using MinSlots and MaxSlots
// in AddJob(). MaxSlots defines the number of tasks in the job array. MinSlots
// defines the number of tasks which are run in parallel (for MPI).
func (t *GCPBatchTracker) AddArrayJob(jt drmaa2interface.JobTemplate, begin int, end int, step int, maxParallel int) (string, error) {
	// TODO: translate to Google Batch instead using a simple wrapper
	return helper.AddArrayJobAsSingleJobs(jt, t, begin, end, step)
}

// JobState returns the DRMAA2 state and substate (free form string) of the job.
func (t *GCPBatchTracker) JobState(jobID string) (drmaa2interface.JobState, string, error) {
	job, err := t.client.GetJob(context.Background(), &batchpb.GetJobRequest{
		Name: jobID,
	})
	if err != nil {
		return drmaa2interface.Undetermined, "", err
	}
	if t.drmaa2session != "" {
		if job.Labels["drmaa2session"] != t.drmaa2session {
			return drmaa2interface.Undetermined, "", errors.New("job not found in job session")
		}
	}
	return ConvertJobState(job)
}

// JobInfo returns the job status of a job in form of a JobInfo struct or an error.
func (t *GCPBatchTracker) JobInfo(jobID string) (drmaa2interface.JobInfo, error) {
	job, err := t.client.GetJob(context.Background(), &batchpb.GetJobRequest{
		Name: jobID,
	})
	if err != nil {
		return drmaa2interface.JobInfo{}, err
	}
	if t.drmaa2session != "" && !IsInDRMAA2Session(t.client, t.drmaa2session, jobID) {
		return drmaa2interface.JobInfo{}, errors.New("job not found in job session")
	}

	return BatchJobToJobInfo(t.project, job)
}

// JobControl sends a request to the backend to either "terminate", "suspend",
// "resume", "hold", or "release" a job. The strings are fixed and are defined
// by the JobControl constants. This could change in the future to be limited
// only to constants representing the actions. When the request is not accepted
// by the system the function must return an error.
func (t *GCPBatchTracker) JobControl(jobID string, action string) error {
	switch action {
	case jobtracker.JobControlSuspend:
		return errors.New("unsupported operation")
	case jobtracker.JobControlResume:
		return errors.New("unsupported operation")
	case jobtracker.JobControlHold:
		// can a Google Batch job be put in hold?
		return errors.New("unsupported operation")
	case jobtracker.JobControlRelease:
		// can a Google Batch job be released from hold?
		return errors.New("unsupported operation")
	case jobtracker.JobControlTerminate:
		// TODO: that reaps the job and should be DeleteJob()
		// any Google Batch equivalent?
		if t.drmaa2session != "" && !IsInDRMAA2Session(t.client, t.drmaa2session, jobID) {
			return errors.New("job not found in job session")
		}
		_, err := t.client.DeleteJob(context.Background(), &batchpb.DeleteJobRequest{
			Name:   jobID,
			Reason: "job terminated by user",
		})
		return err
	}
	return fmt.Errorf("undefined job operation")
}

// Wait blocks until the job is either in one of the given states, the max.
// waiting time (specified by timeout) is reached or an other internal
// error occured (like job was not found). In case of a timeout also an
// error must be returned.
func (t *GCPBatchTracker) Wait(jobID string, timeout time.Duration, state ...drmaa2interface.JobState) error {
	if t.drmaa2session != "" && !IsInDRMAA2Session(t.client, t.drmaa2session, jobID) {
		return errors.New("job not found in job session")
	}
	return helper.WaitForState(t, jobID, timeout, state...)
}

// DeleteJob removes a job from a potential internal database. It does not stop
// a job. A job must be in an endstate (terminated, failed) in order to call
// DeleteJob. In case of an error or the job is not in an end state error must be
// returned. If the backend does not support cleaning up resources for a finished
// job nil should be returned.
func (t *GCPBatchTracker) DeleteJob(jobID string) error {
	// here it does not need to be in an end state
	if t.drmaa2session != "" && !IsInDRMAA2Session(t.client, t.drmaa2session, jobID) {
		return fmt.Errorf("job not found in job session %s", t.drmaa2session)
	}
	_, err := t.client.DeleteJob(context.Background(), &batchpb.DeleteJobRequest{
		Name:   jobID,
		Reason: "job deleted by user",
	})
	return err
}

// ListJobCategories returns a list of job categories which can be used in the
// JobCategory field of the job template. The list is informational. An example
// is returning a list of supported container images. AddJob() and AddArrayJob()
// processes a JobTemplate and hence also the JobCategory field.
//
// JobCategories supported by Google Batch are all container images which can be
// used by the service. Hence the list of job categories is empty.
func (t *GCPBatchTracker) ListJobCategories() ([]string, error) {
	// list available container images?
	return []string{}, nil
}

func IsInDRMAA2Session(client *batch.Client, session string, jobID string) bool {
	// job ID might be long or short
	//name := strings.Split(jobID, "/")[len(strings.Split(jobID, "/"))-1]
	job, err := client.GetJob(context.Background(), &batchpb.GetJobRequest{
		Name: jobID,
	})
	if err != nil {
		fmt.Printf("get job error: %v", err)
		return false
	}
	return IsInJobSession(session, job)
}

func IsInJobSession(session string, job *batchpb.Job) bool {
	return job.Labels["drmaa2session"] == session
}
