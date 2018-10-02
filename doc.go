// Copyright © 2018 J. Strobus White.
// This file is part of the blocktop blockchain development kit.
//
// Blocktop is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Blocktop is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with blocktop. If not, see <http://www.gnu.org/licenses/>.

/*
Package consensus provides a blockchain consensus finding system. It allows
client programs to submit blocks to be tracked and to retrieve the 'best' head block
for the next round of block computation. A lightweight block interface is provided
to allow the package to retrieve basic information about the block.

The default criteria for branch comparison is blockchain height. Client programs may
provide a comparator function to inject their own criteria for evaluation.

Example pseudo code of client program:
		var c *Consensus = consensus.NewConsensusConsensus(0, nil)
		...

		// This function is called whenever a block is received from the network
		// MyBlock implements the consensus.Block interface
		func receiveBlockFromNetwork(b MyBlock) {
			if !c.WasSeen(b) {
				// preform any validation or processing required to
				// ensure block is worthy of submission
				if validate(b) {
					c.AddBlock(b)
				}
			}
		}

		// This function continuously competes for the next block
		func competeForBlocks() {
			for {
				// don't peg the CPU
				// MyBlock implements the consensus.Block interface

				branch := c.GetBestBranch()
				if branch == nil {
					// no best branch is available, wait for more blocks
					time.Sleep(50)
					// to come in from the network
					continue
				}

				// generate the next block based on the head of the best branch
				var head MyBlock = branch[0].(MyBlock)
				var nextBlock MyBlock = generateNextBlock(head)
				c.setCompeted(head)
				sendBlockToNetwork(nextBlock)
				c.AddBlock(nextBlock)
			}
		}
*/
package consensus
