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
	"strings"
	"time"

	rpcconsensus "github.com/blckit/go-rpc-client/consensus"
	mtr "github.com/rcrowley/go-metrics"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type RPC struct {
}

func (h *RPC) GetMetrics(r *http.Request, args *rpcconsensus.GetMetricsArgs, reply *rpcconsensus.GetMetricsReply) error {
	switch args.Format {
	case "text":
		builder := &strings.Builder{}
		mtr.WriteOnce(metrics.registry, builder)
		reply.Metrics = builder.String()

	case "json":
		builder := &strings.Builder{}
		mtr.WriteJSONOnce(metrics.registry, builder)
		reply.Metrics = builder.String()

	default:
		return errors.New("Format must be either text or json")
	}

	return nil
}
