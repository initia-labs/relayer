package cclient

import (
	rpchttp "github.com/cometbft/cometbft/rpc/client/http"
)

// CometRPCClient wraps our slimmed down CometBFT client and converts the returned types to the upstream CometBFT types.
// This is useful so that it can be used in any function calls that expect the upstream types.
type CometRPCClient struct {
	*rpchttp.HTTP
}

func NewCometRPCClient(c *rpchttp.HTTP) CometRPCClient {
	return CometRPCClient{HTTP: c}
}
