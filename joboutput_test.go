package gcpbatchtracker_test

import (
	"fmt"
	"os"

	"github.com/dgruber/drmaa2interface"

	. "github.com/dgruber/gcpbatchtracker"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Joboutput", func() {

	Context("Basic tests", func() {

		It("should return the full and line limited job output", func() {

			if !credentialsCheck() {
				Skip("Credentials not set")
			}
			t, err := NewGCPBatchTracker(
				"testsession",
				os.Getenv("GCPBATCHTRACKER_PROJECT"),
				os.Getenv("GCPBATCHTRACKER_LOCATION"))
			Expect(err).ToNot(HaveOccurred())
			jobTemplate := drmaa2interface.JobTemplate{
				RemoteCommand: "/bin/sh",
				CandidateMachines: []string{
					"n2-standard-2",
				},
				Args:        []string{"-c", `for i in $(seq 1 20); do echo "line $i"; done`},
				JobCategory: "busybox",
			}
			fmt.Printf("Submitting job\n")
			jobID, err := t.AddJob(jobTemplate)
			Expect(err).ToNot(HaveOccurred())
			Expect(jobID).ToNot(Equal(""))
			err = t.Wait(jobID, drmaa2interface.InfiniteTime,
				drmaa2interface.Done, drmaa2interface.Failed)
			Expect(err).ToNot(HaveOccurred())

			lines, err := t.JobOutput(jobID, 0)
			Expect(err).ToNot(HaveOccurred())
			for i, line := range lines {
				fmt.Printf("%d %s\n", i, line)
			}
			Expect(len(lines)).To(Equal(20))

			lines, err = t.JobOutput(jobID, 10)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(lines)).To(Equal(10))

			Expect(lines[9]).To(Equal("line 20"))
			Expect(lines[8]).To(Equal("line 19"))
		})

	})

})
