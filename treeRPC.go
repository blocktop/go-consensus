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
