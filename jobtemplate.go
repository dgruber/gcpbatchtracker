package gcpbatchtracker

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/batch/apiv1/batchpb"
	"github.com/dgruber/drmaa2interface"
	"github.com/mitchellh/copystructure"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	defaultCPUMilli    = 2000      // 2 cores default
	defaultBootDiskMib = 50 * 1024 // 50GB boot disk default
	// job categories (otherwise it is a container image)
	JobCategoryScriptPath = "$scriptpath$" // treats RemoteCommand as path to script and ignores args
	JobCategoryScript     = "$script$"     // treats RemoteCommand as script and ignores args
	// Env variable name container job template
	EnvJobTemplate = "DRMAA2_JOB_TEMPLATE"
)

// https://cloud.google.com/go/docs/reference/cloud.google.com/go/batch/latest/apiv1#example-usage

func ConvertJobTemplateToJobRequest(session, project, location string, jt drmaa2interface.JobTemplate) (*batchpb.CreateJobRequest, error) {
	var jobRequest batchpb.CreateJobRequest

	jt, err := ValidateJobTemplate(jt)
	if err != nil {
		return nil, err
	}

	jobRequest.Parent = "projects/" + project + "/locations/" + location
	jobRequest.JobId = jt.JobName
	if jobRequest.JobId == "" {
		rand.Seed(time.Now().UnixNano())
		jobRequest.JobId = fmt.Sprintf("drmaa2-%d-%d",
			time.Now().Unix(), rand.Int()%10000)
	}

	prolog, _ := GetMachinePrologExtension(jt)
	if prolog == "" {
		prolog = `#!/bin/sh
echo 'Prolog'
`
	}

	epilog, _ := GetMachineEpilogExtension(jt)

	tasksPerNode, _ := GetTasksPerNodeExtension(jt)

	barries := true
	// barrier seem to be only allowed for parallel jobs:
	// "Barriers require task_count = parallelism"
	if jt.MaxSlots != jt.MinSlots {
		barries = false
	}

	// set job template as environment variable, so that
	// we can access it later; unfortunately, we cannot
	// store it as a label as labels are limited to 63
	// characters.
	env, err := JobTemplateToEnv(jt)

	if jt.JobEnvironment == nil {
		jt.JobEnvironment = make(map[string]string)
	}
	jobEnvironment, err := copystructure.Copy(jt.JobEnvironment)
	if err != nil {
		return nil,
			fmt.Errorf("failed to copy job environment: %s", err)
	}
	jobEnvironment.(map[string]string)[EnvJobTemplate] = env

	// environment variables coming from google secret manager
	secrets, exists := GetSecretEnvironmentVariables(jt)
	if !exists {
		secrets = nil
	}

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
					Environment: &batchpb.Environment{
						Variables:       jobEnvironment.(map[string]string),
						SecretVariables: secrets,
					},
					ComputeResource: &batchpb.ComputeResource{
						CpuMilli:    defaultCPUMilli,
						BootDiskMib: defaultBootDiskMib,
						MemoryMib:   jt.MinPhysMemory,
					},
					//MaxRunDuration: ,
					Runnables: CreateRunnables(barries, prolog),
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
				"origin":        "go-drmaa2",
				"accounting":    jt.AccountingID,
				"drmaa2session": session,
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

	// if epilog is set, add it to the job
	if epilog != "" {
		if barries {
			jobRequest.Job.TaskGroups[0].TaskSpec.Runnables = append(
				jobRequest.Job.TaskGroups[0].TaskSpec.Runnables,
				&batchpb.Runnable{
					IgnoreExitStatus: true,
					Background:       false,
					Executable: &batchpb.Runnable_Barrier_{
						Barrier: &batchpb.Runnable_Barrier{
							Name: "after_job_barrier",
						},
					},
				},
			)
		}
		jobRequest.Job.TaskGroups[0].TaskSpec.Runnables = append(jobRequest.Job.TaskGroups[0].TaskSpec.Runnables,
			&batchpb.Runnable{
				IgnoreExitStatus: false,
				Background:       false,
				AlwaysRun:        true,
				Executable: &batchpb.Runnable_Script_{
					Script: &batchpb.Runnable_Script{
						Command: &batchpb.Runnable_Script_Text{
							Text: epilog,
						},
					},
				},
			})
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
	execPosition := 3
	if !barries {
		execPosition = 1
	}

	switch jt.JobCategory {
	case JobCategoryScriptPath:
		jobRequest.Job.TaskGroups[0].TaskSpec.Runnables[execPosition].Executable = &batchpb.Runnable_Script_{
			Script: &batchpb.Runnable_Script{
				Command: &batchpb.Runnable_Script_Path{
					Path: jt.RemoteCommand,
				},
			},
		}
	case JobCategoryScript:
		jobRequest.Job.TaskGroups[0].TaskSpec.Runnables[execPosition].Executable = &batchpb.Runnable_Script_{
			Script: &batchpb.Runnable_Script{
				Command: &batchpb.Runnable_Script_Text{
					Text: jt.RemoteCommand,
				},
			},
		}
	default:
		// is container image

		// in case of a GPU job we need to add the --gpus all option
		additionalOption := ""
		if t, count, exists := GetAcceleratorsExtension(jt); exists && count > 0 && strings.HasPrefix(t, "nvidia") {
			additionalOption = " --gpus all --device /dev/nvidiactl --device /dev/nvidia-uvm --device /dev/nvidia-uvm-tools"
			for i := 0; i < int(count); i++ {
				additionalOption += fmt.Sprintf(" --device /dev/nvidia%d", i)
			}
		}

		jobRequest.Job.TaskGroups[0].TaskSpec.Runnables[execPosition].Executable = &batchpb.Runnable_Container_{
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
				Options: "--network=host --ipc=host --pid=host --privileged --uts=host" + additionalOption,
			},
		}

	}

	dockerOptionsExtension, exists := GetDockerOptionsExtension(jt)
	if exists {
		// override docker extensions
		if _, ok := jobRequest.Job.TaskGroups[0].TaskSpec.Runnables[execPosition].Executable.(*batchpb.Runnable_Container_); ok {
			jobRequest.Job.TaskGroups[0].TaskSpec.Runnables[execPosition].Executable.(*batchpb.Runnable_Container_).Container.Options = dockerOptionsExtension
		} else {
			return nil, fmt.Errorf("docker option extensions set but no container image set")
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
		return nil, fmt.Errorf("CandidateMachines must be set to the machine type or template:<instancetemplatename>")
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
		installGPUDriver := false
		if t, count, exists := GetAcceleratorsExtension(jt); exists {
			if strings.HasPrefix(t, "nvidia") {
				installGPUDriver = true
			}
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
				InstallGpuDrivers: installGPUDriver,
			},
		}

	}

	// stage in files

	for destination, source := range jt.StageInFiles {
		if strings.HasPrefix(source, "gs://") {
			jobRequest = *MountBucket(&jobRequest, execPosition, destination, source)
		} else if strings.HasPrefix(source, "locahost:") {
			// only valid in container mode; mounts from host into container
			if container, isContainer := jobRequest.Job.TaskGroups[0].TaskSpec.
				Runnables[execPosition].Executable.(*batchpb.Runnable_Container_); isContainer {
				container.Container.Volumes = append(container.Container.Volumes,
					fmt.Sprintf("%s:%s", source, destination))
			} else {
				return nil, fmt.Errorf("localhost: only valid when container is used")
			}
		} else if strings.HasPrefix(source, "nfs:") {
			nfs := strings.Split(source, ":")
			if len(nfs) != 3 {
				return nil, fmt.Errorf("invalid NFS source (nfs:server:remotepath): %s", source)
			}
			// if remote path is file then we need to mount the directory
			// to the host and from there the file to the container

			// expect path ends always with / !
			dir, file := filepath.Split(nfs[2])

			// single files can be mounted inside the container since
			// we first mount the directory to the host
			if container, isContainer := jobRequest.Job.TaskGroups[0].TaskSpec.
				Runnables[execPosition].Executable.(*batchpb.Runnable_Container_); isContainer {

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

	// stage out files (same as stage in files, but in case of bucket
	// we need to try to create the bucket first if it does not exist)
	for destination, source := range jt.StageOutFiles {
		if strings.HasPrefix(source, "gs://") {
			for _, bucket := range jt.StageInFiles {
				if bucket == source {
					// bucket already mounted from stage in
					continue
				}
			}
			jobRequest = *MountBucket(&jobRequest, execPosition, destination, source)
		}
	}

	return &jobRequest, nil
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

func CreateRunnables(barriers bool, prolog string) []*batchpb.Runnable {
	var runnable []*batchpb.Runnable
	if barriers {
		runnable = append(runnable, &batchpb.Runnable{
			IgnoreExitStatus: false,
			Background:       false,
			Executable: &batchpb.Runnable_Barrier_{
				Barrier: &batchpb.Runnable_Barrier{
					Name: "before_job_barrier",
				},
			},
		})
	}
	runnable = append(runnable, &batchpb.Runnable{
		IgnoreExitStatus: false,
		Background:       false,
		Executable: &batchpb.Runnable_Script_{
			Script: &batchpb.Runnable_Script{
				Command: &batchpb.Runnable_Script_Text{
					Text: prolog,
				},
			},
		},
	})

	if barriers {
		runnable = append(runnable, &batchpb.Runnable{
			IgnoreExitStatus: false,
			Background:       false,
			Executable: &batchpb.Runnable_Barrier_{
				Barrier: &batchpb.Runnable_Barrier{
					Name: "after_prolog_barrier",
				},
			},
		})
	}

	runnable = append(runnable, &batchpb.Runnable{
		IgnoreExitStatus: false,
		Background:       false,
		// Executable: set below
	})

	return runnable
}

func ValidateJobTemplate(jt drmaa2interface.JobTemplate) (drmaa2interface.JobTemplate, error) {
	if jt.MaxSlots == 0 {
		jt.MaxSlots = 1
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

// Implements JobTemplater interface

func (t *GCPBatchTracker) JobTemplate(jobID string) (drmaa2interface.JobTemplate, error) {
	// get job template from env variables
	job, err := t.client.GetJob(context.Background(), &batchpb.GetJobRequest{
		Name: jobID,
	})
	if err != nil {
		return drmaa2interface.JobTemplate{},
			fmt.Errorf("could not get job %s: %v", jobID, err)
	}

	for _, group := range job.GetTaskGroups() {
		for _, envs := range group.GetTaskEnvironments() {
			value, exists := envs.GetVariables()[EnvJobTemplate]
			if exists {
				jt, err := GetJobTemplateFromBase64(value)
				if err != nil {
					continue
				}
				return jt, nil
			}
		}
	}

	return drmaa2interface.JobTemplate{},
		fmt.Errorf("could not find job template in env variables")
}

func JobTemplateToEnv(jt drmaa2interface.JobTemplate) (string, error) {
	jtBytes, err := json.Marshal(jt)
	if err != nil {
		return "", fmt.Errorf("could not marshal job template: %v", err)
	}
	return base64.StdEncoding.EncodeToString(jtBytes), nil
}

func GetJobTemplateFromBase64(base64encondedJT string) (drmaa2interface.JobTemplate, error) {
	jt := drmaa2interface.JobTemplate{}
	decodedJT, err := base64.StdEncoding.DecodeString(base64encondedJT)
	if err != nil {
		return jt, fmt.Errorf("could not decode job template: %v", err)
	}
	err = json.Unmarshal(decodedJT, &jt)
	if err != nil {
		return jt, fmt.Errorf("could not unmarshal job template: %v", err)
	}
	return jt, nil
}
