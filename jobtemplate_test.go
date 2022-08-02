package gcpbatchtracker_test

import (
	"time"

	. "github.com/dgruber/gcpbatchtracker"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchpb "google.golang.org/genproto/googleapis/cloud/batch/v1"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/dgruber/drmaa2interface"
)

var _ = Describe("Jobtemplate", func() {

	Context("Basic", func() {

		It("should convert a jobtemplate to a jobrequest", func() {
			jt := drmaa2interface.JobTemplate{
				RemoteCommand:     "echo",
				Args:              []string{"hello", "world"},
				JobCategory:       "ubuntu:18.04",
				MaxSlots:          1, // one machine
				CandidateMachines: []string{"e2-standard-4"},
			}

			jobRequest, err := ConvertJobTemplateToJobRequest("", "", jt)
			Expect(err).To(BeNil())

			container := (jobRequest.Job.TaskGroups[0].TaskSpec.Runnables[3].Executable).(*batchpb.Runnable_Container_)

			Expect(container.Container.ImageUri).To(Equal("ubuntu:18.04"))
			Expect(container.Container.Entrypoint).To(Equal("echo"))
			Expect(container.Container.Commands).To(Equal([]string{"hello", "world"}))
			Expect(container.Container.Options).To(Equal("--network=host"))
		})
	})

	Context("Stage in files", func() {

		It("should set NFS for dirs and files in containers", func() {
			jt := drmaa2interface.JobTemplate{
				JobCategory:       "ubuntu:18.04",
				MaxSlots:          1, // one machine
				CandidateMachines: []string{"e2-standard-4"},
			}
			jt.StageInFiles = map[string]string{
				"/containermnt/share": "nfs:myserver:/share/", // path needs to end with / !!!
				"/home/user/file.sh":  "nfs:myserver:/share/file.sh",
			}
			req, err := ConvertJobTemplateToJobRequest("project", "location", jt)
			Expect(err).To(BeNil())
			Expect(len(req.Job.TaskGroups[0].TaskSpec.Volumes)).To(Equal(int(1)))
			Expect(req.Job.TaskGroups[0].TaskSpec.Volumes[0].MountPath).To(Equal("/mnt/share/"))
			cvolumes := req.Job.TaskGroups[0].TaskSpec.Runnables[3].Executable.(*batchpb.Runnable_Container_).Container.Volumes
			Expect(len(cvolumes)).To(Equal(int(5))) // there are 3 files mounted from host into container before
			Expect(cvolumes[3]).To(Or(Equal("/mnt/share/:/containermnt/share"),
				Equal("/mnt/share/file.sh:/home/user/file.sh")))
			Expect(cvolumes[4]).To(Or(Equal("/mnt/share/:/containermnt/share"),
				Equal("/mnt/share/file.sh:/home/user/file.sh")))
		})

	})

	Context("Resource limits", func() {

		It("should set the cpu core count, boot disk size, and runtime limit", func() {
			jt := drmaa2interface.JobTemplate{
				JobCategory:       "ubuntu:18.04",
				MaxSlots:          1, // one machine
				CandidateMachines: []string{"e2-standard-4"},
			}
			jt.ResourceLimits = map[string]string{
				"cpumilli":    "4000",  // 4 cores
				"bootdiskmib": "10240", // 10 GB
				"runtime":     "1h",    // 1 hour
			}
			req, err := ConvertJobTemplateToJobRequest("project", "location", jt)
			Expect(err).To(BeNil())
			Expect(req.Job.TaskGroups[0].TaskSpec.ComputeResource.CpuMilli).To(Equal(int64(4000)))
			Expect(req.Job.TaskGroups[0].TaskSpec.ComputeResource.BootDiskMib).To(Equal(int64(10240)))
			Expect(req.Job.TaskGroups[0].TaskSpec.MaxRunDuration).To(Equal(durationpb.New(time.Hour)))
		})

	})

})
