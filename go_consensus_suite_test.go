package consensus

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestGoConsensus(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "GoConsensus Suite")
}
