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
| AccountingID | Sets a tag "accounting" |
| MinSlots | Specifies the parallelism (how many tasks to run in parallel)|
| MaxSlots | Specifies the amount of tasks to run. For MPI set MinSlots = MaxSlots. |

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

### JobTemplate Extensions

| DRMAA2 JobTemplate Extension Key | DRMAA2 JobTemplate Extension Value      |
| :-------------------------------:|:---------------------------------------:|
| ExtensionProlog / "prolog"       | String which contains prolog script executed on machine level before the job starts |
| ExtensionEpilog / "epilog"       | String which contains epilog script executed on machine level after the job ends successfully |
| ExtensionSpot / "spot"          |  "true"/"t"/... when machine should be spot |
| ExctensionAccelerators / "accelerators"  | Accelerator name for machine |
| ExtensionTasksPerNode / "tasks_per_node" | Amount of tasks per node |
| ExtensionDockerOptions / "docker_options" | Override of docker run options in case a container image is used|

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

NFS (Google Filestore) and GCS is supported.

For NFS in containers besides directories also files can be specified.
In case of files, the directory is mounted to the host and from there the
file inside the container as specified in key. For the directory case
a leading "/" is required.

````go
    StageInFiles: map[string]string{
            "/etc/script.sh": "nfs:10.20.30.40:/filestore/user/dir/script.sh",
            "/mnt/dir": "nfs:10.20.30.40:/filestore/user/dir/",
            "/somedir": "gs://benchmarkfiles", // mount a bucket into container or host
        },
````

## Examples

See examples directory.
