package gcpbatchtracker

import "github.com/dgruber/drmaa2interface"

const (
	// ExtensionJobInfoJobTemplate is the job template stored in the job info
	// extension list as base64 encoded string
	ExtensionJobInfoJobTemplate = "jobtemplate_base64"
	// ExtensionJobInfoJobUID is the Google Batch internal job UID
	ExtensionJobInfoJobUID = "uid"
)

// GetJobTemplateExtensionFromJobInfo returns the job template which is stored
// in the job info extension list. If the job info does not contain a job
// template extension it returns false.
func GetJobTemplateExtensionFromJobInfo(ji drmaa2interface.JobInfo) (drmaa2interface.JobTemplate, bool) {
	if ji.ExtensionList == nil {
		return drmaa2interface.JobTemplate{}, false
	}
	jt, hasExtension := ji.ExtensionList[ExtensionJobInfoJobTemplate]
	if !hasExtension {
		return drmaa2interface.JobTemplate{}, false
	}
	jobTemplate, err := GetJobTemplateFromBase64(jt)
	if err != nil {
		return drmaa2interface.JobTemplate{}, false
	}
	return jobTemplate, true
}

// GetUIDExtensionFromJobInfo returns the Google Batch Job UID which is
// stored in the job info extension list. If the job info does not contain
// a UID extension it returns false.
func GetUIDExtensionFromJobInfo(ji drmaa2interface.JobInfo) (string, bool) {
	if ji.ExtensionList == nil {
		return "", false
	}
	uid, hasExtension := ji.ExtensionList[ExtensionJobInfoJobUID]
	if !hasExtension {
		return "", false
	}
	return uid, true
}
