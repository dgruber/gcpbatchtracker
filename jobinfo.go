package gcpbatchtracker

import (
	"errors"
	"strings"
	"time"

	"cloud.google.com/go/batch/apiv1/batchpb"
	"github.com/dgruber/drmaa2interface"
)

func BatchJobToJobInfo(project string, job *batchpb.Job) (drmaa2interface.JobInfo, error) {
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

	ji.ExtensionList = make(map[string]string)
	ji.ExtensionList[ExtensionJobInfoJobUID] = job.Uid

	// store job template in extension
	for _, group := range job.GetTaskGroups() {
		if group.TaskSpec != nil && group.TaskSpec.Environment != nil &&
			group.TaskSpec.Environment.Variables != nil {
			value, exists := group.TaskSpec.Environment.Variables[EnvJobTemplate]
			if exists {
				ji.ExtensionList[ExtensionJobInfoJobTemplate] = value
			}
		}
	}

	// too slow
	/*
		out, err := GetJobOutput(project, job.Uid)
		if err != nil {
			// skip output
		} else {
			ji.ExtensionList["output"] = strings.Join(out, "\n")
		}
	*/
	machineType := "unknown"

	instances := job.AllocationPolicy.GetInstances()
	if len(instances) > 0 && instances[0].GetPolicy() != nil {
		machineType = instances[0].GetPolicy().MachineType
		ji.ExtensionList["min_cpu_platform"] =
			instances[0].GetPolicy().MinCpuPlatform
		ji.ExtensionList["boot_disk"] =
			instances[0].GetPolicy().BootDisk.String()
		// accelerators / disks / ...
	}
	ji.AllocatedMachines = []string{machineType}

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
