package gcpbatchtracker

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"cloud.google.com/go/batch/apiv1/batchpb"
	"cloud.google.com/go/storage"
)

func getStorageClient() (*storage.Client, error) {
	return storage.NewClient(context.Background())
}

func CreateMissingStageOutBuckets(project string, stageOutFiles map[string]string) error {
	storageClient, err := getStorageClient()
	if err != nil {
		return fmt.Errorf("could not create storage client: %v", err)
	}
	for _, destinationBucket := range stageOutFiles {
		if !strings.HasPrefix(destinationBucket, "gs://") {
			continue
		}
		bucketName := strings.TrimPrefix(destinationBucket, "gs://")

		// check if bucket exists
		bucket := storageClient.Bucket(bucketName)
		_, err = bucket.Attrs(context.Background())
		if err != nil {
			// create bucket as it does not exist
			if err := bucket.Create(context.Background(), project, nil); err != nil {
				return fmt.Errorf("could not create bucket %s: %v",
					bucketName, err)
			}
		}
	}
	return nil
}

// DeleteFileInBucket deletes a file in a bucket. It expects the bucket
// name prefixed with gs://. The file is the name of the file in the
// bucket (could be like testpath/testfile.txt).
func DeleteFileInBucket(bucket, file string) error {
	if !strings.HasPrefix(bucket, "gs://") {
		return fmt.Errorf("source %s is not a GCS bucket (has no gs:// prefix)",
			bucket)
	}
	bucketName := strings.TrimPrefix(bucket, "gs://")
	storageClient, err := getStorageClient()
	if err != nil {
		return fmt.Errorf("could not create storage client: %v", err)
	}
	bucketHandle := storageClient.Bucket(bucketName)
	obj := bucketHandle.Object(file)
	if err := obj.Delete(context.Background()); err != nil {
		return fmt.Errorf("could not delete file %s in bucket %s: %v",
			file, bucketName, err)
	}
	return nil
}

// DeleteBucket deletes a bucket. It expects the bucket name prefixed
// with gs://. The bucket must be empty to be deleted.
func DeleteBucket(bucket string) error {
	if !strings.HasPrefix(bucket, "gs://") {
		return fmt.Errorf("source %s is not a GCS bucket (has no gs:// prefix)",
			bucket)
	}
	bucketName := strings.TrimPrefix(bucket, "gs://")
	storageClient, err := getStorageClient()
	if err != nil {
		return fmt.Errorf("could not create storage client: %v", err)
	}
	bucketHandle := storageClient.Bucket(bucketName)
	if err := bucketHandle.Delete(context.Background()); err != nil {
		return fmt.Errorf("could not delete bucket %s: %v", bucketName, err)
	}
	return nil
}

// MountBucket mounts a bucket into the job request. The source is the
// bucket name prefixed with gs:// and the destination is the mount
// path inside the host or container.
func MountBucket(jobRequest *batchpb.CreateJobRequest, execPosition int, destination, source string) *batchpb.CreateJobRequest {
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
		Runnables[execPosition].Executable.(*batchpb.Runnable_Container_); isContainer {
		// job runs in container: mount from host into container
		container.Container.Volumes = append(container.Container.Volumes,
			fmt.Sprintf("%s:%s", destination, destination))
	}
	return jobRequest
}

func getBucketObjectHandle(bucket, file string) (*storage.ObjectHandle, error) {
	if !strings.HasPrefix(bucket, "gs://") {
		return nil, fmt.Errorf("source %s is not a GCS bucket (has no gs:// prefix)",
			bucket)
	}
	bucketName := strings.TrimPrefix(bucket, "gs://")
	storageClient, err := getStorageClient()
	if err != nil {
		return nil, fmt.Errorf("could not create storage client: %v", err)
	}
	bucketHandle := storageClient.Bucket(bucketName)

	// check if bucket exists
	_, err = bucketHandle.Attrs(context.Background())
	if err != nil {
		return nil, fmt.Errorf("bucket %s does not exist: %v", bucket, err)
	}

	return bucketHandle.Object(file), nil
}

// ReadFromBucket reads the content of an object from a bucket.
// This is a convenience function to read files, like output files
// from a bucket. The bucket name must be prefixed with gs:// and
// must not contain any other slashes.
func ReadFromBucket(bucket string, file string) ([]byte, error) {
	obj, err := getBucketObjectHandle(bucket, file)
	if err != nil {
		return nil, err
	}
	reader, err := obj.NewReader(context.Background())
	if err != nil {
		return nil, fmt.Errorf("could not read object %s from bucket %s: %v",
			file, bucket, err)
	}
	defer reader.Close()
	return ioutil.ReadAll(reader)
}

// CopyFileFromBucket reads the content of an object from a bucket
// and writes it to a local file. It expects the bucket name to be
// prefixed with gs:// and not contain any other slashes.
func CopyFileFromBucket(bucket string, file string, localFile string) error {
	obj, err := getBucketObjectHandle(bucket, file)
	if err != nil {
		return err
	}
	reader, err := obj.NewReader(context.Background())
	if err != nil {
		return fmt.Errorf("could not read object %s from bucket %s: %v",
			file, bucket, err)
	}
	defer reader.Close()
	f, err := os.Create(localFile)
	if err != nil {
		return fmt.Errorf("could not create file %s: %v", localFile, err)
	}
	defer f.Close()
	if _, err := io.Copy(f, reader); err != nil {
		return fmt.Errorf("could not copy object %s from bucket %s to file %s: %v",
			file, bucket, localFile, err)
	}
	return nil
}

// WriteToBucket writes the content of a file to a bucket. It expects
// the bucket name to be prefixed with gs:// and not contain any other slashes.
func WriteToBucket(bucket string, file string, content []byte) error {
	obj, err := getBucketObjectHandle(bucket, file)
	if err != nil {
		return err
	}
	writer := obj.NewWriter(context.Background())
	defer writer.Close()
	if _, err := writer.Write(content); err != nil {
		return fmt.Errorf("could not write object %s to bucket %s: %v",
			file, bucket, err)
	}
	return nil
}

// CopyFileToBucket writes the content of a local file to a bucket. It expects
// the bucket name to be prefixed with gs:// and not contain any other slashes.
func CopyFileToBucket(bucket string, file string, localFile string) error {
	obj, err := getBucketObjectHandle(bucket, file)
	if err != nil {
		return err
	}
	writer := obj.NewWriter(context.Background())
	defer writer.Close()

	localFileReader, err := os.Open(localFile)
	if err != nil {
		return fmt.Errorf("could not create/open local file %s: %v", localFile, err)
	}
	defer localFileReader.Close()
	if _, err := io.Copy(writer, localFileReader); err != nil {
		return fmt.Errorf("could not write object %s to bucket %s: %v",
			file, bucket, err)
	}
	return nil
}
