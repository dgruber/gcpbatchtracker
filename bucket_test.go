package gcpbatchtracker_test

import (
	"fmt"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dgruber/gcpbatchtracker"
)

var _ = Describe("Bucket", func() {

	testBucketName := "gs://dg-testbucket-creation-test"

	Context("Bucket tests with mock", func() {

		It("should create a bucket", func() {

			if os.Getenv("GCPBATCHTRACKER_BUCKET_TEST") != "true" {
				Skip("GCPBATCHTRACKER_BUCKET_TEST not set")
			}
			fmt.Printf("GCPBATCHTRACKER_BUCKET_TEST: %s\n",
				os.Getenv("GCPBATCHTRACKER_BUCKET_TEST"))

			if os.Getenv("GCPBATCHTRACKER_PROJECT") == "" {
				Skip("GCPBATCHTRACKER_PROJECT not set")
			}
			fmt.Printf("GCPBATCHTRACKER_PROJECT: %s\n",
				os.Getenv("GCPBATCHTRACKER_PROJECT"))

			// bucket should not exist hence we expect an error
			err := gcpbatchtracker.CopyFileToBucket(testBucketName,
				"testdata/bucket_test", "bucket_test.test")
			Expect(err).To(HaveOccurred())

			// create bucket
			err = gcpbatchtracker.CreateMissingStageOutBuckets(
				os.Getenv("GCPBATCHTRACKER_PROJECT"),
				map[string]string{"/output": testBucketName})
			Expect(err).ToNot(HaveOccurred())

			// copy file to bucket
			err = gcpbatchtracker.CopyFileToBucket(testBucketName,
				"testdata/bucket_test.test", "bucket_test.test")
			Expect(err).NotTo(HaveOccurred())

			// copy file from bucket to local
			err = gcpbatchtracker.CopyFileFromBucket(testBucketName,
				"testdata/bucket_test.test", "bucket_test.test_copy")
			Expect(err).NotTo(HaveOccurred())

			// check if file exists
			fi, err := os.Stat("./bucket_test.test")
			Expect(err).NotTo(HaveOccurred())
			Expect(fi.Size()).To(BeNumerically(">", 0))

			// delete file in bucket
			err = gcpbatchtracker.DeleteFileInBucket(testBucketName,
				"testdata/bucket_test.test")
			Expect(err).NotTo(HaveOccurred())

			// delete bucket
			err = gcpbatchtracker.DeleteBucket(testBucketName)
			Expect(err).NotTo(HaveOccurred())
		})

	})

})
