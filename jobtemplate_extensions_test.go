package gcpbatchtracker_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dgruber/drmaa2interface"
	. "github.com/dgruber/gcpbatchtracker"
)

var _ = Describe("JobtemplateExtensions", func() {

	Context("Set and get extensions with helper functions", func() {

		It("should set a machine level prolog", func() {
			jt := drmaa2interface.JobTemplate{}
			jt = SetMachinePrologExtension(jt, "#!/bin/bash")
			Expect(jt.ExtensionList).To(HaveKey(ExtensionProlog))
			Expect(jt.ExtensionList[ExtensionProlog]).To(Equal("#!/bin/bash"))
			prolog, exists := GetMachinePrologExtension(jt)
			Expect(exists).To(BeTrue())
			Expect(prolog).To(Equal("#!/bin/bash"))
		})

		It("should set a spot instance request", func() {
			jt := drmaa2interface.JobTemplate{}
			jt = SetSpotExtension(jt, true)
			Expect(jt.ExtensionList).To(HaveKey(ExtensionSpot))
			Expect(jt.ExtensionList[ExtensionSpot]).To(Equal("true"))
			isSpot, exists := GetSpotExtension(jt)
			Expect(exists).To(BeTrue())
			Expect(isSpot).To(BeTrue())
		})

		It("should set the accelerators request", func() {
			jt := drmaa2interface.JobTemplate{}
			jt = SetAcceleratorsExtension(jt, 1, "nvidia-tesla-k80")
			Expect(jt.ExtensionList).To(HaveKey(ExctensionAccelerators))
			Expect(jt.ExtensionList[ExctensionAccelerators]).To(Equal("1*nvidia-tesla-k80"))
			accelerators, count, exists := GetAcceleratorsExtension(jt)
			Expect(exists).To(BeTrue())
			Expect(accelerators).To(Equal("nvidia-tesla-k80"))
			Expect(count).To(Equal(int64(1)))
		})

		It("should set tasks per node", func() {
			jt := drmaa2interface.JobTemplate{}
			defaultTasksPerNode, exists := GetTasksPerNodeExtension(jt)
			Expect(exists).To(BeFalse())
			Expect(defaultTasksPerNode).To(Equal(int64(1)))
			jt = SetTasksPerNodeExtension(jt, 2)
			Expect(jt.ExtensionList).To(HaveKey(ExtensionTasksPerNode))
			Expect(jt.ExtensionList[ExtensionTasksPerNode]).To(Equal("2"))
			count, exists := GetTasksPerNodeExtension(jt)
			Expect(exists).To(BeTrue())
			Expect(count).To(Equal(int64(2)))
		})

	})

})
