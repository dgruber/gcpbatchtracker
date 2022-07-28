# gcpbatchtracker

DRMAA2 JobTracker implementation for Google Batch

Experimental [Google Batch](https://cloud.google.com/blog/products/compute/new-batch-service-processes-batch-jobs-on-google-cloud) support for [DRMAA2os](https://github.com/dgruber/drmaa2os).

## How gcpbatchtracker Works

The project is created for embedding it as a backend in https://github.com/dgruber/drmaa2os

## What gcpbatchtracker is

It is a basic DRMAA2 implementation for Google Batch for Go. The DRMAA2 [JobTemplate](https://github.com/dgruber/drmaa2interface/blob/master/jobtemplate.go) can be used for submitting
Google Batch jobs. The DRMAA2 [JobInfo](https://github.com/dgruber/drmaa2interface/blob/master/jobinfo.go) struct 
is used for getting the status of a job. The job state model is converted to the DRMAA2 spec.

## How to use it

See examples directory which uses the interface directly.

## Converting a DRMAA2 Job Template to an Google Batch Job

| DRMAA2 JobTemplate   | Google Batch Job            |
| :-------------------:|:-------------------------------:|
| RemoteCommand        | Command to execute in container or script or script path |
| Args                 | In case of container the arguments of the command (if RemoteCommand empty then the arguments of entrypoint) |
| CandidateMachines[0] | Machine type or when prefixed with "template:" it uses an instance template with that name    |
| JobCategory          | Container image or $script$ or $scriptpath$ for other runnables which interpretes then RemoteCommand as script or script path |
| JobName              | JobID |
| MinSlots/MaxSlots (can't be different) | defines parallelism (always 1 task per node) |
| AccountingID | Sets a tag "accounting" |

In case of a container following files are always mounted from host:

````go
    "/etc/cloudbatch-taskgroup-hosts:/etc/cloudbatch-taskgroup-hosts",
    "/etc/ssh:/etc/ssh",
    "/root/.ssh:/root/.ssh",
````

For a container the following runtime options are set:

- "--network=host"

Default output path is cloud logging. If "OutputPath" is set it is changed to
LogsPolicy_PATH with the OutputPath as destination.

## JobInfo Fields

| DRMAA2 JobInfo               | Batch Job             |
| :---------------------------:|:---------------------:|
| Slots                        | Parallelism           |

## Job Control Mapping

Did not yet find some way to put a job in hold, suspend, or release a job.
Terminating a job deletes it...

## Job State Mapping

| DRMAA2 State                  | Batch Job State       |
| :----------------------------:|:---------------------:|
| Done                          | JobStatus_SUCCEEDED   |
| Failed                        | JobStatus_FAILED      |
| Suspended                     | -                     |
| Running                       | JobStatus_RUNNING JobStatus_DELETION_IN_PROGRESS |
| Queued                        | JobStatus_QUEUED JobStatus_SCHEDULED             |
| Undetermined                  | JobStatus_STATE_UNSPECIFIED                      |

## File staging using the Job Template

Currently only for container based jobs (JobCategory is a container image).
Here NFS is supported (Google Filestore) for both files and directories.
In case of files, the directory is mounted to the host and from there the
file inside the container as specified in key.

````go
    StageInFiles: map[string]string{
            "/etc/script.sh": "nfs:10.20.30.40:/filestore/user/dir/script.sh",
            "/mnt/dir": "nfs:10.20.30.40:/filestore/user/dir/",
        },
````

## Examples

See examples directory.
