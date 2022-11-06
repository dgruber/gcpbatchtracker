package gcpbatchtracker

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/storage"
	batchpb "google.golang.org/genproto/googleapis/cloud/batch/v1"
)

func CreateMissingStageOutBuckets(project string, stageOutFiles map[string]string) error {
	for _, destinationBucket := range stageOutFiles {
		if !strings.HasPrefix(destinationBucket, "gs://") {
			continue
		}
		storageClient, err := GetStorageClient()
		if err != nil {
			return fmt.Errorf("could not create storage client: %v", err)
		}
		// check if bucket exists
		bucket := storageClient.Bucket(strings.TrimPrefix(destinationBucket, "gs://"))
		_, err = bucket.Attrs(context.Background())
		if err != nil {
			// create bucket
			if err := bucket.Create(context.Background(), project, nil); err != nil {
				return fmt.Errorf("could not create bucket %s: %v", destinationBucket, err)
			}
		}
	}
	return nil
}

func GetStorageClient() (*storage.Client, error) {
	ctx := context.Background()
	return storage.NewClient(ctx)
}

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
		// job runs in container
		// mount from host into container
		container.Container.Volumes = append(container.Container.Volumes,
			fmt.Sprintf("%s:%s", destination, destination))
	}
	return jobRequest
}
