package gcpbatchtracker_test

import (
	"fmt"
	"os"
	"time"

	"github.com/dgruber/drmaa2interface"
	. "github.com/dgruber/gcpbatchtracker"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// make sure you are logged in with gcloud auth application-default login

func credentialsCheck() bool {
	if os.Getenv("GCPBATCHTRACKER_PROJECT") == "" {
		return false
	}
	if os.Getenv("GCPBATCHTRACKER_LOCATION") == "" {
		return false
	}
	return true
}

var _ = Describe("GCP Batch tracker", func() {

	Context("Job lifecycle", func() {

		It("should submit a job", func() {
			if !credentialsCheck() {
				Skip("Credentials not set")
			}
			t, err := NewGCPBatchTracker(
				"testsession",
				os.Getenv("GCPBATCHTRACKER_PROJECT"),
				os.Getenv("GCPBATCHTRACKER_LOCATION")) // like "us-central1"
			Expect(err).ToNot(HaveOccurred())
			jobTemplate := drmaa2interface.JobTemplate{
				RemoteCommand: "/bin/sleep",
				CandidateMachines: []string{
					"n2-standard-2",
				},
				Args:        []string{"1"},
				JobCategory: "busybox",
			}
			fmt.Printf("Submitting job\n")
			jobID, err := t.AddJob(jobTemplate)
			Expect(err).ToNot(HaveOccurred())
			Expect(jobID).ToNot(Equal(""))

			// wait for job to finish
			fmt.Printf("Waiting for job %s to finish\n", jobID)
			for {
				ji, err := t.JobInfo(jobID)
				Expect(err).ToNot(HaveOccurred())
				if ji.State == drmaa2interface.Done {
					break
				}
				time.Sleep(1 * time.Second)
			}
			fmt.Printf("Job %s finished\n", jobID)

			ji, err := t.JobInfo(jobID)
			Expect(err).ToNot(HaveOccurred())
			Expect(ji.State).To(Equal(drmaa2interface.Done))
			Expect(ji.ExitStatus).To(Equal(0))

			Expect(ji.SubmissionTime).To(BeTemporally("<", ji.DispatchTime))
			Expect(ji.DispatchTime).To(BeTemporally("<", ji.FinishTime))

		})

	})

	Context("List jobs", func() {

		It("should list jobs", func() {
			if !credentialsCheck() {
				Skip("Credentials not set")
			}

			t, err := NewGCPBatchTracker(
				"testsession",
				os.Getenv("GCPBATCHTRACKER_PROJECT"),
				os.Getenv("GCPBATCHTRACKER_LOCATION")) // like "us-central1"
			Expect(err).ToNot(HaveOccurred())

			jobs, err := t.ListJobs()
			Expect(err).ToNot(HaveOccurred())
			Expect(jobs).ToNot(BeNil())
			Expect(len(jobs)).To(BeNumerically(">=", 0))

			for _, job := range jobs {
				ji, err := t.JobInfo(job)
				Expect(err).ToNot(HaveOccurred())
				Expect(ji.ID).To(Equal(job))

				Expect(ji.ExtensionList).NotTo(BeNil())
				Expect(ji.ExtensionList).To(HaveKey("uid"))

				Expect(ji.AllocatedMachines).NotTo(BeNil())
			}

		})

	})

	Context("Jobtemplate", func() {

		It("should get the job template from job env", func() {
			if !credentialsCheck() {
				Skip("Credentials not set")
			}
			t, err := NewGCPBatchTracker(
				"testsession",
				os.Getenv("GCPBATCHTRACKER_PROJECT"),
				os.Getenv("GCPBATCHTRACKER_LOCATION")) // like "us-central1"
			Expect(err).ToNot(HaveOccurred())

			jobTemplate := drmaa2interface.JobTemplate{
				RemoteCommand: "/bin/sleep",
				CandidateMachines: []string{
					"n2-standard-2",
				},
				Args:        []string{"0"},
				JobCategory: "busybox",
				JobEnvironment: map[string]string{
					"TEST_ENV": "test",
				},
			}

			jobID, err := t.AddJob(jobTemplate)
			Expect(err).ToNot(HaveOccurred())
			Expect(jobID).ToNot(Equal(""))

			jt, err := t.JobTemplate(jobID)
			Expect(err).ToNot(HaveOccurred())
			Expect(jt.JobEnvironment).NotTo(BeNil())
			Expect(jt.JobEnvironment["TEST_ENV"]).To(Equal("test"))

		})

	})

})
