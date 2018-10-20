package consensus

import (
	"github.com/blocktop/go-spec"
)

// Competition isolates the data that needs to be communicated
// to the blockchain for competition from the rest of the consensus
// system.
type Competition struct {
	branches map[int]spec.CompetingBranch
}

// Enforce interface at compile-time.
var _ spec.Competition = (*Competition)(nil)

func newCompetition() *Competition {
	return &Competition{
		branches: make(map[int]spec.CompetingBranch)}
}

func (c *Competition) addBranch(rootID int, branch *CompetingBranch) {
	c.branches[rootID] = branch
}

// Branches returns the current competing branches being tracked
// in the consensus system indexed by RootID.
func (c *Competition) Branches() map[int]spec.CompetingBranch {
	return c.branches
}
