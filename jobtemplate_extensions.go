package gcpbatchtracker

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"encoding/json"

	"github.com/dgruber/drmaa2interface"
)

const (
	ExtensionProlog          = "prolog"
	ExtensionEpilog          = "epilog"
	ExtensionSpot            = "spot"
	ExtensionAccelerators    = "accelerators"
	ExtensionTasksPerNode    = "tasks_per_node"
	ExtensionDockerOptions   = "docker_options"
	ExtensionGoogleSecretEnv = "secret_env"
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

func GetMachineEpilogExtension(jt drmaa2interface.JobTemplate) (string, bool) {
	if jt.ExtensionList == nil {
		return "", false
	}
	extension, hasExtensions := jt.ExtensionList[ExtensionEpilog]
	return extension, hasExtensions
}

func SetMachineEpilogExtension(jt drmaa2interface.JobTemplate, epilog string) drmaa2interface.JobTemplate {
	if jt.ExtensionList == nil {
		jt.ExtensionList = make(map[string]string)
	}
	jt.ExtensionList[ExtensionEpilog] = epilog
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
	extension, hasExtensions := jt.ExtensionList[ExtensionAccelerators]
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
		jt.ExtensionList[ExtensionAccelerators] = strconv.FormatInt(count, 10) + "*" + accelerator
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

func GetDockerOptionsExtension(jt drmaa2interface.JobTemplate) (string, bool) {
	if jt.ExtensionList == nil {
		return "", false
	}
	extension, hasExtensions := jt.ExtensionList[ExtensionDockerOptions]
	return extension, hasExtensions
}

func SetDockerOptionsExtension(jt drmaa2interface.JobTemplate, dockerOptions string) drmaa2interface.JobTemplate {
	if jt.ExtensionList == nil {
		jt.ExtensionList = make(map[string]string)
	}
	jt.ExtensionList[ExtensionDockerOptions] = dockerOptions
	return jt
}

// SetSecretEnvironmentVariables sets environment variables which are
// retrieved from Google Secret Manager as JobTemplate extenion.
// The map key is the environment variable name and the value is the
// path to the secret (like "projects/dev/secrets/secret_message/versions/1")
func SetSecretEnvironmentVariables(jt drmaa2interface.JobTemplate, secretEnv map[string]string) (drmaa2interface.JobTemplate, error) {
	if jt.ExtensionList == nil {
		jt.ExtensionList = make(map[string]string)
	}
	// convert to JSON
	encoded, err := json.Marshal(secretEnv)
	if err != nil {
		return jt, fmt.Errorf("could not encode secret environment variables: %v", err)
	}
	// base64 envoding for encoded
	b64Secrets := base64.StdEncoding.EncodeToString(encoded)
	jt.ExtensionList[ExtensionGoogleSecretEnv] = b64Secrets
	return jt, nil
}

func GetSecretEnvironmentVariables(jt drmaa2interface.JobTemplate) (map[string]string, bool) {
	if jt.ExtensionList == nil {
		return nil, false
	}
	secretEnv, hasSecretEnv := jt.ExtensionList[ExtensionGoogleSecretEnv]
	if !hasSecretEnv {
		return nil, false
	}
	// decode base64
	decoded, err := base64.StdEncoding.DecodeString(secretEnv)
	if err != nil {
		return nil, false
	}
	// convert from JSON
	var secretEnvMap map[string]string
	err = json.Unmarshal(decoded, &secretEnvMap)
	if err != nil {
		return nil, false
	}
	return secretEnvMap, true
}
