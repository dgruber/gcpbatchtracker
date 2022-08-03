package gcpbatchtracker

import (
	"fmt"
	"log"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/dgruber/drmaa2interface"
	batchpb "google.golang.org/genproto/googleapis/cloud/batch/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	defaultCPUMilli    = 2000      // 2 cores default
	defaultBootDiskMib = 50 * 1024 // 50GB boot disk default
	// job categories (otherwise it is a container image)
	JobCategoryScriptPath = "$scriptpath$" // treats RemoteCommand as path to script and ignores args
	JobCategoryScript     = "$script$"     // treats RemoteCommand as script and ignores args
)

// https://cloud.google.com/go/docs/reference/cloud.google.com/go/batch/latest/apiv1#example-usage

func ConvertJobTemplateToJobRequest(session, project, location string, jt drmaa2interface.JobTemplate) (batchpb.CreateJobRequest, error) {
	var jobRequest batchpb.CreateJobRequest

	jt, err := ValidateJobTemplate(jt)
	if err != nil {
		return jobRequest, err
	}

	jobRequest.Parent = "projects/" + project + "/locations/" + location
	jobRequest.JobId = jt.JobName
	if jobRequest.JobId == "" {
		jobRequest.JobId = fmt.Sprintf("drmaa2-job-%d", time.Now().Unix())
	}

	prolog, _ := GetMachinePrologExtension(jt)
	if prolog == "" {
		prolog = `#!/bin/sh
echo 'Prolog'
`
	}

	tasksPerNode, _ := GetTasksPerNodeExtension(jt)

	jobRequest.Job = &batchpb.Job{
		Priority: int64(jt.Priority),
		TaskGroups: []*batchpb.TaskGroup{
			{
				Name:             "default",
				TaskCount:        int64(jt.MaxSlots),
				Parallelism:      int64(jt.MinSlots),
				TaskCountPerNode: tasksPerNode,
				// sets $BATCH_HOSTS_FILE
				RequireHostsFile: true,
				// what is with containers?
				PermissiveSsh: true,
				TaskSpec: &batchpb.TaskSpec{
					Environments: jt.JobEnvironment,
					ComputeResource: &batchpb.ComputeResource{
						CpuMilli:    defaultCPUMilli,
						BootDiskMib: defaultBootDiskMib,
						MemoryMib:   jt.MinPhysMemory,
					},
					//MaxRunDuration: ,
					Runnables: []*batchpb.Runnable{
						{
							IgnoreExitStatus: false,
							Background:       false,
							Executable: &batchpb.Runnable_Barrier_{
								Barrier: &batchpb.Runnable_Barrier{
									Name: "barrier",
								},
							},
						},
						{
							IgnoreExitStatus: false,
							Background:       false,
							Executable: &batchpb.Runnable_Script_{
								Script: &batchpb.Runnable_Script{
									Command: &batchpb.Runnable_Script_Text{
										Text: prolog,
									},
								},
							},
						},
						{
							IgnoreExitStatus: false,
							Background:       false,
							Executable: &batchpb.Runnable_Barrier_{
								Barrier: &batchpb.Runnable_Barrier{
									Name: "barrier2",
								},
							},
						},
						{
							IgnoreExitStatus: false,
							Background:       false,
							// Executable: set below
						},
					},
				},
			},
		},
		AllocationPolicy: &batchpb.AllocationPolicy{
			/*
				Network: &batchpb.AllocationPolicy_NetworkPolicy{
					NetworkInterfaces: []*batchpb.AllocationPolicy_NetworkInterface{
						{
							Network: "global/networks/default",
						},
				},
			*/
			Location: &batchpb.AllocationPolicy_LocationPolicy{
				AllowedLocations: []string{},
			},
			Labels: map[string]string{
				"origin":     "go-drmaa2",
				"accounting": jt.AccountingID,
			},
		},
		// job labels
		Labels: map[string]string{
			"origin":        "go-drmaa2",
			"accounting":    jt.AccountingID,
			"drmaa2session": session,
		},
		// default logging is cloud logging
		LogsPolicy: &batchpb.LogsPolicy{
			Destination: batchpb.LogsPolicy_CLOUD_LOGGING,
		},
	}

	// apply resource limits
	if jt.ResourceLimits != nil {
		rt, exists := jt.ResourceLimits["runtime"]
		if exists {
			if maxRunDuration, err := time.ParseDuration(rt); err != nil {
				log.Printf("Invalid max run duration: %s (%v)", rt, err)
			} else {
				jobRequest.Job.TaskGroups[0].TaskSpec.MaxRunDuration = durationpb.New(maxRunDuration)
			}
		}
		bootDiskMib, exists := jt.ResourceLimits["bootdiskmib"]
		if exists {
			bootdisk, err := strconv.ParseInt(bootDiskMib, 10, 64)
			if err != nil {
				log.Printf("Invalid boot disk size: %s (%v)", bootDiskMib, err)
			} else {
				if jobRequest.Job.TaskGroups[0].TaskSpec.ComputeResource == nil {
					jobRequest.Job.TaskGroups[0].TaskSpec.ComputeResource = &batchpb.ComputeResource{}
				}
				jobRequest.Job.TaskGroups[0].TaskSpec.ComputeResource.BootDiskMib = bootdisk
			}
		}
		cpuMili, exists := jt.ResourceLimits["cpumilli"]
		if exists {
			cpu, err := strconv.ParseInt(cpuMili, 10, 64)
			if err != nil {
				log.Printf("Invalid cpu milli: %s (%v)", cpuMili, err)
			} else {
				if jobRequest.Job.TaskGroups[0].TaskSpec.ComputeResource == nil {
					jobRequest.Job.TaskGroups[0].TaskSpec.ComputeResource = &batchpb.ComputeResource{}
				}
				jobRequest.Job.TaskGroups[0].TaskSpec.ComputeResource.CpuMilli = cpu
			}
		}
	}

	// set executable
	switch jt.JobCategory {
	case JobCategoryScriptPath:
		jobRequest.Job.TaskGroups[0].TaskSpec.Runnables[3].Executable = &batchpb.Runnable_Script_{
			Script: &batchpb.Runnable_Script{
				Command: &batchpb.Runnable_Script_Path{
					Path: jt.RemoteCommand,
				},
			},
		}
	case JobCategoryScript:
		jobRequest.Job.TaskGroups[0].TaskSpec.Runnables[3].Executable = &batchpb.Runnable_Script_{
			Script: &batchpb.Runnable_Script{
				Command: &batchpb.Runnable_Script_Text{
					Text: jt.RemoteCommand,
				},
			},
		}
	default:
		// is container image
		jobRequest.Job.TaskGroups[0].TaskSpec.Runnables[3].Executable = &batchpb.Runnable_Container_{
			Container: &batchpb.Runnable_Container{
				ImageUri:   jt.JobCategory,
				Username:   "",
				Password:   "",
				Entrypoint: jt.RemoteCommand,
				Commands:   jt.Args,
				Volumes: []string{
					"/etc/cloudbatch-taskgroup-hosts:/etc/cloudbatch-taskgroup-hosts",
					"/etc/ssh:/etc/ssh",
					"/root/.ssh:/root/.ssh",
					//"/etc/hosts:/etc/hosts",
				},
				Options: "--network=host",
			},
		}
	}

	// jt.ErrorPath is not respected / must be same as output path if not empty
	if jt.OutputPath != "" {
		// store logs on disk
		jobRequest.Job.LogsPolicy.Destination = batchpb.LogsPolicy_PATH
		jobRequest.Job.LogsPolicy.LogsPath = jt.OutputPath
	}

	// CandiateMachines must be set
	if len(jt.CandidateMachines) < 1 {
		return jobRequest, fmt.Errorf("CandidateMachines must be set to the machine type or template:<instancetemplatename>")
	}
	if strings.HasPrefix(jt.CandidateMachines[0], "template:") {
		jobRequest.Job.AllocationPolicy.Instances = []*batchpb.AllocationPolicy_InstancePolicyOrTemplate{
			{
				/*
						gcloud compute instance-templates create ubercloud-base
					 	--image-family=hpc-centos-7 --image-project=cloud-hpc-image-public
					 	--machine-type=c2-standard-60
				*/
				PolicyTemplate: &batchpb.AllocationPolicy_InstancePolicyOrTemplate_InstanceTemplate{
					InstanceTemplate: strings.Split(jt.CandidateMachines[0], ":")[1],
				},
			},
		}
	} else {
		// it is a specific machine type

		provisioningModel := batchpb.AllocationPolicy_STANDARD
		if spot, _ := GetSpotExtension(jt); spot {
			provisioningModel = batchpb.AllocationPolicy_SPOT
		}

		var accelerators []*batchpb.AllocationPolicy_Accelerator
		if t, count, exists := GetAcceleratorsExtension(jt); exists {
			accelerators = []*batchpb.AllocationPolicy_Accelerator{
				{
					Type:  t,
					Count: count,
				},
			}
		}

		jobRequest.Job.AllocationPolicy.Instances = []*batchpb.AllocationPolicy_InstancePolicyOrTemplate{
			{
				PolicyTemplate: &batchpb.AllocationPolicy_InstancePolicyOrTemplate_Policy{
					Policy: &batchpb.AllocationPolicy_InstancePolicy{
						MachineType:       jt.CandidateMachines[0],
						MinCpuPlatform:    jt.MachineArch,
						ProvisioningModel: provisioningModel,
						Accelerators:      accelerators,
					},
				},
			},
		}
	}

	// stage in files

	for destination, source := range jt.StageInFiles {
		if strings.HasPrefix(source, "gs://") {

			jobRequest.Job.TaskGroups[0].TaskSpec.Volumes = append(
				jobRequest.Job.TaskGroups[0].TaskSpec.Volumes,
				&batchpb.Volume{
					Source: &batchpb.Volume_Gcs{
						Gcs: &batchpb.GCS{
							RemotePath: strings.TrimPrefix(source, "gs://"),
						},
					},
					MountPath: destination,
				},
			)

			if container, isContainer := jobRequest.Job.TaskGroups[0].TaskSpec.
				Runnables[3].Executable.(*batchpb.Runnable_Container_); isContainer {
				// job runs in container
				// mount from host into container
				container.Container.Volumes = append(container.Container.Volumes,
					fmt.Sprintf("%s:%s", destination, destination))
			}
		} else if strings.HasPrefix(source, "nfs:") {
			nfs := strings.Split(source, ":")
			if len(nfs) != 3 {
				return jobRequest, fmt.Errorf("invalid NFS source (nfs:server:remotepath): %s", source)
			}
			// if remote path is file then we need to mount the directory
			// to the host and from there the file to the container

			// expect path ends always with / !
			dir, file := filepath.Split(nfs[2])

			// single files can be mounted inside the container since
			// we first mount the directory to the host
			if container, isContainer := jobRequest.Job.TaskGroups[0].TaskSpec.
				Runnables[3].Executable.(*batchpb.Runnable_Container_); isContainer {

				// check if dir is already mounted
				if hasNFSVolume(jobRequest.Job.TaskGroups[0].TaskSpec.Volumes, nfs[1], dir) {
					// already mounted
				} else {
					jobRequest.Job.TaskGroups[0].TaskSpec.Volumes = append(
						jobRequest.Job.TaskGroups[0].TaskSpec.Volumes,
						&batchpb.Volume{
							Source: &batchpb.Volume_Nfs{
								Nfs: &batchpb.NFS{
									Server:     nfs[1],
									RemotePath: dir,
								},
							},
							MountPath: "/mnt" + dir,
						},
					)
				}
				// mount from host into container
				container.Container.Volumes = append(container.Container.Volumes,
					fmt.Sprintf("/mnt%s%s:%s", dir, file, destination))
			} else {
				// not running in a container
				jobRequest.Job.TaskGroups[0].TaskSpec.Volumes = append(
					jobRequest.Job.TaskGroups[0].TaskSpec.Volumes,
					&batchpb.Volume{
						Source: &batchpb.Volume_Nfs{
							Nfs: &batchpb.NFS{
								Server:     nfs[1],
								RemotePath: dir,
							},
						},
						MountPath: "/mnt" + dir,
					},
				)
			}
		} else if strings.HasPrefix(source, "b64data:") {
			// first copy data to a bucket and then mount it?
		}
	}

	return jobRequest, nil
}

func hasNFSVolume(volumes []*batchpb.Volume, server, path string) bool {
	for _, v := range volumes {
		if nfs, hasType := v.Source.(*batchpb.Volume_Nfs); hasType {
			if nfs.Nfs.Server == server && nfs.Nfs.RemotePath == path {
				return true
			}
		}
	}
	return false
}

func ValidateJobTemplate(jt drmaa2interface.JobTemplate) (drmaa2interface.JobTemplate, error) {
	if jt.MaxSlots == 0 {
		return jt, fmt.Errorf("MaxSlots is 0")
	}
	if jt.MinSlots == 0 {
		jt.MinSlots = 1
	}
	if jt.MinSlots > jt.MaxSlots {
		return jt, fmt.Errorf("MinSlots > MaxSlots")
	}
	if jt.JobCategory == "" {
		return jt, fmt.Errorf("JobCategory is empty - should be the container image")
	}
	if len(jt.CandidateMachines) == 0 {
		return jt, fmt.Errorf("CandidateMachines must contain exactly the machine or image type")
	}
	if jt.ErrorPath != "" && jt.OutputPath != "" {
		if jt.ErrorPath != jt.OutputPath {
			return jt, fmt.Errorf("ErrorPath and OutputPath must be the same or one unset")
		}
	}
	return jt, nil
}
