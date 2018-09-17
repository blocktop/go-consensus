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

package consensus

import (
	"errors"
	"math/rand"
	"net/http"
	"time"

	rpcconsensus "github.com/blckit/go-rpc-client/consensus"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func (h *RPC) GetTree(r *http.Request, args *rpcconsensus.GetTreeArgs, reply *rpcconsensus.GetTreeReply) error {
	switch args.Format {
	case "text":
		reply.Tree = tree.getText()

	case "json":
		j, err := tree.getJSON()
		if err != nil {
			return err
		}
		reply.Tree = j

	default:
		return errors.New("Format must be either text or json")
	}

	return nil
}
