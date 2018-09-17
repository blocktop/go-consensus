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
