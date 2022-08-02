package gcpbatchtracker

import (
	"strconv"
	"strings"

	"github.com/dgruber/drmaa2interface"
)

const (
	ExtensionProlog        = "prolog"
	ExtensionSpot          = "spot"
	ExctensionAccelerators = "accelerators"
	ExtensionTasksPerNode  = "tasks_per_node"
)

func GetMachinePrologExtension(jt drmaa2interface.JobTemplate) (string, bool) {
	if jt.ExtensionList == nil {
		return "", false
	}
	extension, hasExtensions := jt.ExtensionList[ExtensionProlog]
	return extension, hasExtensions
}

func SetMachinePrologExtension(jt drmaa2interface.JobTemplate, prolog string) drmaa2interface.JobTemplate {
	if jt.ExtensionList == nil {
		jt.ExtensionList = make(map[string]string)
	}
	jt.ExtensionList[ExtensionProlog] = prolog
	return jt
}

func GetSpotExtension(jt drmaa2interface.JobTemplate) (bool, bool) {
	if jt.ExtensionList == nil {
		return false, false
	}
	extension, hasExtensions := jt.ExtensionList[ExtensionSpot]
	if hasExtensions {
		if isSpot, _ := strconv.ParseBool(extension); isSpot {
			return true, true
		}
		return false, true
	}
	return false, false
}

func SetSpotExtension(jt drmaa2interface.JobTemplate, isSpot bool) drmaa2interface.JobTemplate {
	if jt.ExtensionList == nil && isSpot {
		jt.ExtensionList = make(map[string]string)
	}
	if isSpot {
		jt.ExtensionList[ExtensionSpot] = "true"
	} else if jt.ExtensionList != nil {
		delete(jt.ExtensionList, ExtensionSpot)
	}
	return jt
}

func GetAcceleratorsExtension(jt drmaa2interface.JobTemplate) (string, int64, bool) {
	if jt.ExtensionList == nil {
		return "", 0, false
	}
	extension, hasExtensions := jt.ExtensionList[ExctensionAccelerators]
	if hasExtensions {
		a := strings.Split(extension, "*")
		if len(a) >= 2 {
			count, _ := strconv.ParseInt(a[0], 10, 64)
			return a[1], count, true
		}
		return extension, 1, true
	}
	return "", 0, false
}

func SetAcceleratorsExtension(jt drmaa2interface.JobTemplate, count int64, accelerator string) drmaa2interface.JobTemplate {
	if count > 0 {
		if jt.ExtensionList == nil {
			jt.ExtensionList = make(map[string]string)
		}
		jt.ExtensionList[ExctensionAccelerators] = strconv.FormatInt(count, 10) + "*" + accelerator
	}
	return jt
}

func GetTasksPerNodeExtension(jt drmaa2interface.JobTemplate) (int64, bool) {
	if jt.ExtensionList == nil {
		return 1, false
	}
	extension, hasExtensions := jt.ExtensionList[ExtensionTasksPerNode]
	if hasExtensions {
		count, _ := strconv.ParseInt(extension, 10, 64)
		return count, true
	}
	return 1, false
}

func SetTasksPerNodeExtension(jt drmaa2interface.JobTemplate, count int64) drmaa2interface.JobTemplate {
	if count > 0 {
		if jt.ExtensionList == nil {
			jt.ExtensionList = make(map[string]string)
		}
		jt.ExtensionList[ExtensionTasksPerNode] = strconv.FormatInt(count, 10)
	}
	return jt
}
