package gcpbatchtracker_test

import (
	"os"

	. "github.com/dgruber/gcpbatchtracker"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

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
			Expect(len(jobs)).To(BeNumerically(">", 0))
		})

	})

})
