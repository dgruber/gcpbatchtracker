package gcpbatchtracker

import (
	"errors"

	"github.com/dgruber/drmaa2os"
	"github.com/dgruber/drmaa2os/pkg/jobtracker"
)

// This file contains all functions for allowing to using the JobTracker
// implemenation in a drmaa2os session.

// init registers the GoogleBatch tracker at the SessionManager
func init() {
	drmaa2os.RegisterJobTracker(drmaa2os.GoogleBatchSession, NewAllocator())
}

// GoogleBatchTrackerParams provide parameters which can be passed
// to the SessionManager in order to pass things like Google project
// or region into the job tracker. It needs to be that complicated
// in order to be used but not tightly integrated with the SessionManager
// of the DRMAA2OS project, so that not all depenedencies have to
// be compiled in.
type GoogleBatchTrackerParams struct {
	GoogleProjectID string
	Region          string
}

type allocator struct{}

func NewAllocator() *allocator {
	return &allocator{}
}

// New is called by the SessionManager when a new DRMAA2 JobSession is allocated.
func (a *allocator) New(jobSessionName string, jobTrackerInitParams interface{}) (jobtracker.JobTracker, error) {
	if jobTrackerInitParams != nil {
		googleBatchParams, ok := jobTrackerInitParams.(GoogleBatchTrackerParams)
		if !ok {
			return nil, errors.New("jobTrackerInitParams for podman has not PodmanTrackerParams type")
		}
		return NewGCPBatchTracker(jobSessionName,
			googleBatchParams.GoogleProjectID,
			googleBatchParams.Region)
	}
	return nil, errors.New("GoogleBatchTrackerParams{} not specified")
}
