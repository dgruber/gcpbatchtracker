# gcpbatchtracker
DRMAA2 JobTracker implementation for Google Batch

Experimental [Google Batch](https://cloud.google.com/blog/products/compute/new-batch-service-processes-batch-jobs-on-google-cloud) support for [DRMAA2os](https://github.com/dgruber/drmaa2os). This project evaluates the usefulness of DRMAA2 for managing Google Batch jobs running on Google Cloud.

## How gcpbatchtracker Works

The project is created for embedding it as a backend in https://github.com/dgruber/drmaa2os

## What gcpbatchtracker is

It is a basic DRMAA2 implementation for Google Batch for Go. The DRMAA2 [JobTemplate](https://github.com/dgruber/drmaa2interface/blob/master/jobtemplate.go) can be used for submitting
Google Batch jobs. The DRMAA2 [JobInfo](https://github.com/dgruber/drmaa2interface/blob/master/jobinfo.go) struct 
is used for getting the status of a job. The job state model is converted to the DRMAA2 spec.

## How to use it

## Converting a DRMAA2 Job Template to an Google Batch Job

## JobInfo Fields

## Job Control Mapping

## Job State Mapping

## Examples
