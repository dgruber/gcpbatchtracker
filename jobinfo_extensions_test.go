package gcpbatchtracker_test

import (
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	. "github.com/dgruber/gcpbatchtracker"

	"github.com/dgruber/drmaa2interface"
)

var _ = Describe("JobinfoExtensions", func() {

	Context("Standard JobInfo extensions", func() {

		It("should return the job template and UID from the job info extension", func() {

			if !credentialsCheck() {
				Skip("Credentials not set")
			}

			t, err := NewGCPBatchTracker(
				"testsession",
				os.Getenv("GCPBATCHTRACKER_PROJECT"),
				os.Getenv("GCPBATCHTRACKER_LOCATION"))
			Expect(err).ToNot(HaveOccurred())

			jobTemplate := drmaa2interface.JobTemplate{
				RemoteCommand: "/bin/sleep",
				CandidateMachines: []string{
					"n2-standard-2",
				},
				Args:        []string{"0"},
				MinSlots:    1,
				MaxSlots:    1,
				JobCategory: "busybox",
				JobEnvironment: map[string]string{
					"SOMEENV": "with some value",
				},
			}

			jobID, err := t.AddJob(jobTemplate)
			Expect(err).ToNot(HaveOccurred())
			Expect(jobID).ToNot(Equal(""))

			jobInfo, err := t.JobInfo(jobID)
			Expect(err).ToNot(HaveOccurred())

			jt, exists := GetJobTemplateExtensionFromJobInfo(jobInfo)
			Expect(exists).To(BeTrue())
			Expect(jt).To(Equal(jobTemplate))

			uid, exists := GetUIDExtensionFromJobInfo(jobInfo)
			Expect(exists).To(BeTrue())
			Expect(uid).ToNot(Equal(""))
		})

	})

})
