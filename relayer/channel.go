package relayer

import (
	"context"
	"fmt"
	"time"

	sdk "github.com/cosmos/cosmos-sdk/types"
	govv1 "github.com/cosmos/cosmos-sdk/x/gov/types/v1"
	chantypes "github.com/cosmos/ibc-go/v8/modules/core/04-channel/types"
	host "github.com/cosmos/ibc-go/v8/modules/core/24-host"
	"github.com/cosmos/relayer/v2/relayer/chains/cosmos"
	"github.com/cosmos/relayer/v2/relayer/processor"
	"github.com/cosmos/relayer/v2/relayer/provider"
	"go.uber.org/zap"
)

// CreateOpenChannels runs the channel creation messages on timeout until they pass.
func (c *Chain) CreateOpenChannels(
	ctx context.Context,
	dst *Chain,
	maxRetries uint64,
	timeout time.Duration,
	srcPortID, dstPortID, order, version string,
	override bool,
	memo string,
	pathName string,
) error {
	// client and connection identifiers must be filled in
	if err := ValidateConnectionPaths(c, dst); err != nil {
		return err
	}

	// port identifiers and channel ORDER must be valid
	if err := ValidateChannelParams(srcPortID, dstPortID, order); err != nil {
		return err
	}

	if !override {
		channel, err := QueryPortChannel(ctx, c, srcPortID)
		if err == nil && channel != nil {
			return fmt.Errorf("channel {%s} with port {%s} already exists on chain {%s}", channel.ChannelId, channel.PortId, c.ChainID())
		}

		channel, err = QueryPortChannel(ctx, dst, dstPortID)
		if err == nil && channel != nil {
			return fmt.Errorf("channel {%s} with port {%s} already exists on chain {%s}", channel.ChannelId, channel.PortId, dst.ChainID())
		}
	}

	// Timeout is per message. Four channel handshake messages, allowing maxRetries for each.
	processorTimeout := timeout * 4 * time.Duration(maxRetries)

	ctx, cancel := context.WithTimeout(ctx, processorTimeout)
	defer cancel()

	pp := processor.NewPathProcessor(
		c.log,
		processor.NewPathEnd(pathName, c.PathEnd.ChainID, c.PathEnd.ClientID, "", []processor.ChainChannelKey{}),
		processor.NewPathEnd(pathName, dst.PathEnd.ChainID, dst.PathEnd.ClientID, "", []processor.ChainChannelKey{}),
		nil,
		memo,
		DefaultClientUpdateThreshold,
		DefaultFlushInterval,
		DefaultMaxMsgLength,
		0,
		0,
	)

	c.log.Info("Starting event processor for channel handshake",
		zap.String("src_chain_id", c.PathEnd.ChainID),
		zap.String("src_port_id", srcPortID),
		zap.String("dst_chain_id", dst.PathEnd.ChainID),
		zap.String("dst_port_id", dstPortID),
	)

	return processor.NewEventProcessor().
		WithChainProcessors(
			c.chainProcessor(c.log, nil),
			dst.chainProcessor(c.log, nil),
		).
		WithPathProcessors(pp).
		WithInitialBlockHistory(0).
		WithMessageLifecycle(&processor.ChannelMessageLifecycle{
			Initial: &processor.ChannelMessage{
				ChainID:   c.PathEnd.ChainID,
				EventType: chantypes.EventTypeChannelOpenInit,
				Info: provider.ChannelInfo{
					PortID:             srcPortID,
					CounterpartyPortID: dstPortID,
					ConnID:             c.PathEnd.ConnectionID,
					Version:            version,
					Order:              OrderFromString(order),
				},
			},
			Termination: &processor.ChannelMessage{
				ChainID:   dst.PathEnd.ChainID,
				EventType: chantypes.EventTypeChannelOpenConfirm,
				Info: provider.ChannelInfo{
					PortID:             dstPortID,
					CounterpartyPortID: srcPortID,
				},
			},
		}).
		Build().
		Run(ctx)
}

// CloseChannel runs the channel closing messages on timeout until they pass.
func (c *Chain) CloseChannel(
	ctx context.Context,
	dst *Chain,
	maxRetries uint64,
	timeout time.Duration,
	srcChanID,
	srcPortID string,
	memo string,
	pathName string,
) error {
	// Timeout is per message. Two close channel handshake messages, allowing maxRetries for each.
	processorTimeout := timeout * 2 * time.Duration(maxRetries)

	// Perform a flush first so that any timeouts are cleared.
	flushCtx, flushCancel := context.WithTimeout(ctx, processorTimeout)
	defer flushCancel()

	flushProcessor := processor.NewEventProcessor().
		WithChainProcessors(
			c.chainProcessor(c.log, nil),
			dst.chainProcessor(c.log, nil),
		).
		WithPathProcessors(processor.NewPathProcessor(
			c.log,
			processor.NewPathEnd(pathName, c.PathEnd.ChainID, c.PathEnd.ClientID, "", []processor.ChainChannelKey{}),
			processor.NewPathEnd(pathName, dst.PathEnd.ChainID, dst.PathEnd.ClientID, "", []processor.ChainChannelKey{}),
			nil,
			memo,
			DefaultClientUpdateThreshold,
			DefaultFlushInterval,
			DefaultMaxMsgLength,
			0,
			0,
		)).
		WithInitialBlockHistory(0).
		WithMessageLifecycle(&processor.FlushLifecycle{}).
		Build()

	c.log.Info("Starting event processor for flush before channel close",
		zap.String("src_chain_id", c.PathEnd.ChainID),
		zap.String("src_port_id", srcPortID),
		zap.String("dst_chain_id", dst.PathEnd.ChainID),
	)

	if err := flushProcessor.Run(flushCtx); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, processorTimeout)
	defer cancel()

	c.log.Info("Starting event processor for channel close",
		zap.String("src_chain_id", c.PathEnd.ChainID),
		zap.String("src_port_id", srcPortID),
		zap.String("dst_chain_id", dst.PathEnd.ChainID),
	)

	return processor.NewEventProcessor().
		WithChainProcessors(
			c.chainProcessor(c.log, nil),
			dst.chainProcessor(c.log, nil),
		).
		WithPathProcessors(processor.NewPathProcessor(
			c.log,
			processor.NewPathEnd(pathName, c.PathEnd.ChainID, c.PathEnd.ClientID, "", []processor.ChainChannelKey{}),
			processor.NewPathEnd(pathName, dst.PathEnd.ChainID, dst.PathEnd.ClientID, "", []processor.ChainChannelKey{}),
			nil,
			memo,
			DefaultClientUpdateThreshold,
			DefaultFlushInterval,
			DefaultMaxMsgLength,
			0,
			0,
		)).
		WithInitialBlockHistory(0).
		WithMessageLifecycle(&processor.ChannelCloseLifecycle{
			SrcChainID:   c.PathEnd.ChainID,
			SrcChannelID: srcChanID,
			SrcPortID:    srcPortID,
			SrcConnID:    c.PathEnd.ConnectionID,
			DstConnID:    dst.PathEnd.ConnectionID,
		}).
		Build().
		Run(ctx)
}

// UpgradeChannel runs the channel upgrading messages on timeout until they pass.
func (c *Chain) UpgradeChannel(
	ctx context.Context,
	dst *Chain,
	timeout time.Duration,
	maxRetries uint64,
	srcChanID,
	srcPortID string,
	order string,
	version string,
	connectionHops []string,
	deposit string,
	memo string,
	pathName string,
) error {
	if err := ValidateConnectionPaths(c, dst); err != nil {
		return err
	}

	if err := host.PortIdentifierValidator(srcPortID); err != nil {
		return err
	}

	orderType := OrderFromString(order)
	if orderType == chantypes.NONE {
		return fmt.Errorf("invalid order input (%s), order must be 'ordered' or 'unordered'", order)
	}
	if len(connectionHops) == 0 {
		return fmt.Errorf("connection hops cannot be empty")
	}

	if timeout != 0 || maxRetries != 0 {
		c.log.Debug("timeout and max-retries parameters are ignored when submitting a governance proposal")
	}

	cosmosProvider, ok := c.ChainProvider.(*cosmos.CosmosProvider)
	if !ok {
		return fmt.Errorf("channel upgrade governance proposals are currently only supported for cosmos chains (got %T)", c.ChainProvider)
	}

	srcChannel, err := QueryChannel(ctx, c, srcChanID)
	if err != nil {
		return err
	}

	if srcChannel.PortId != srcPortID {
		return fmt.Errorf("source port mismatch: expected %s, channel bound to %s", srcPortID, srcChannel.PortId)
	}

	if srcChannel.Counterparty.ChannelId == "" {
		return fmt.Errorf("counterparty channel ID not found for %s", srcChanID)
	}

	dstChannel, err := QueryChannel(ctx, dst, srcChannel.Counterparty.ChannelId)
	if err != nil {
		return err
	}

	proposedFields := chantypes.NewUpgradeFields(orderType, connectionHops, version)

	initialInfo := provider.ChannelInfo{
		PortID:                srcChannel.PortId,
		ChannelID:             srcChannel.ChannelId,
		CounterpartyPortID:    srcChannel.Counterparty.PortId,
		CounterpartyChannelID: srcChannel.Counterparty.ChannelId,
		ConnID:                c.PathEnd.ConnectionID,
		CounterpartyConnID:    dst.PathEnd.ConnectionID,
		Order:                 orderType,
		Version:               version,
		Upgrade: chantypes.Upgrade{
			Fields: proposedFields,
		},
		UpgradeSequence: srcChannel.UpgradeSequence,
	}
	if initialInfo.ConnID == "" && len(srcChannel.ConnectionHops) > 0 {
		initialInfo.ConnID = srcChannel.ConnectionHops[0]
	}
	if initialInfo.CounterpartyConnID == "" && len(dstChannel.ConnectionHops) > 0 {
		initialInfo.CounterpartyConnID = dstChannel.ConnectionHops[0]
	}

	govAddress, err := cosmosProvider.QueryGovAddress(ctx)
	if err != nil {
		return err
	}

	relayerMsg, err := cosmosProvider.MsgChannelUpgradeInit(initialInfo, govAddress)
	if err != nil {
		return err
	}

	sdkMsg := cosmos.CosmosMsg(relayerMsg)
	if sdkMsg == nil {
		return fmt.Errorf("unexpected provider message type for channel upgrade init: %T", relayerMsg)
	}

	depositCoins := sdk.NewCoins()
	if deposit != "" {
		depositCoins, err = sdk.ParseCoinsNormalized(deposit)
		if err != nil {
			return fmt.Errorf("invalid deposit %q: %w", deposit, err)
		}
	}

	proposer, err := cosmosProvider.Address()
	if err != nil {
		return fmt.Errorf("failed to fetch proposer address: %w", err)
	}

	title := fmt.Sprintf("IBC channel upgrade %s", srcChanID)
	summary := fmt.Sprintf("Upgrade channel %s (%s/%s) to version %s on path %s", srcChanID, srcPortID, srcChannel.Counterparty.PortId, version, pathName)
	proposalMsg, err := govv1.NewMsgSubmitProposal([]sdk.Msg{sdkMsg}, depositCoins, proposer, "", title, summary, false)
	if err != nil {
		return fmt.Errorf("failed to build channel upgrade proposal: %w", err)
	}

	govRelayerMsg := cosmos.NewCosmosMessage(proposalMsg, nil)
	if _, _, err := cosmosProvider.SendMessages(ctx, []provider.RelayerMessage{govRelayerMsg}, memo); err != nil {
		return fmt.Errorf("failed to submit channel upgrade governance proposal: %w", err)
	}

	c.log.Info("Submitted channel upgrade governance proposal",
		zap.String("chain_id", c.PathEnd.ChainID),
		zap.String("channel_id", srcChanID),
		zap.String("port_id", srcPortID),
		zap.String("version", version),
		zap.Strings("connection_hops", connectionHops),
		zap.String("deposit", depositCoins.String()),
	)

	return nil
}

func (c *Chain) CancelChannelUpgrade(
	ctx context.Context,
	srcChanID,
	srcPortID string,
	deposit string,
	memo string,
	pathName string,
) error {
	if err := host.PortIdentifierValidator(srcPortID); err != nil {
		return err
	}

	cosmosProvider, ok := c.ChainProvider.(*cosmos.CosmosProvider)
	if !ok {
		return fmt.Errorf("channel upgrade cancel proposals are currently only supported for cosmos chains (got %T)", c.ChainProvider)
	}

	srcChannel, err := QueryChannel(ctx, c, srcChanID)
	if err != nil {
		return err
	}

	if srcChannel.State == chantypes.FLUSHCOMPLETE {
		return fmt.Errorf("channel upgrade cancel proposals through governance are currently only supported for channels not in the flushing complete state")
	}

	if srcChannel.PortId != srcPortID {
		return fmt.Errorf("source port mismatch: expected %s, channel bound to %s", srcPortID, srcChannel.PortId)
	}

	info := provider.ChannelInfo{
		PortID:                srcChannel.PortId,
		ChannelID:             srcChannel.ChannelId,
		CounterpartyPortID:    srcChannel.Counterparty.PortId,
		CounterpartyChannelID: srcChannel.Counterparty.ChannelId,
		ConnID:                c.PathEnd.ConnectionID,
		UpgradeSequence:       srcChannel.UpgradeSequence,
	}
	if info.ConnID == "" && len(srcChannel.ConnectionHops) > 0 {
		info.ConnID = srcChannel.ConnectionHops[0]
	}

	height, err := c.ChainProvider.QueryLatestHeight(ctx)
	if err != nil {
		return err
	}

	proof, err := cosmosProvider.ChannelUpgradeErrorProof(ctx, info, uint64(height))
	if err != nil {
		return fmt.Errorf("failed to query channel upgrade proof: %w", err)
	}

	govAddress, err := cosmosProvider.QueryGovAddress(ctx)
	if err != nil {
		return err
	}

	msg, err := cosmosProvider.MsgChannelUpgradeCancel(info, proof, govAddress)
	if err != nil {
		return fmt.Errorf("failed to build channel upgrade cancel message: %w", err)
	}

	sdkMsg := cosmos.CosmosMsg(msg)
	if sdkMsg == nil {
		return fmt.Errorf("unexpected provider message type for channel upgrade cancel: %T", msg)
	}

	depositCoins := sdk.NewCoins()
	if deposit != "" {
		depositCoins, err = sdk.ParseCoinsNormalized(deposit)
		if err != nil {
			return fmt.Errorf("invalid deposit %q: %w", deposit, err)
		}
	}

	proposer, err := cosmosProvider.Address()
	if err != nil {
		return fmt.Errorf("failed to fetch proposer address: %w", err)
	}

	title := fmt.Sprintf("Cancel IBC channel upgrade %s", srcChanID)
	summary := fmt.Sprintf("Cancel the in-flight upgrade for channel %s (%s/%s) on path %s", srcChanID, srcPortID, srcChannel.Counterparty.PortId, pathName)
	proposalMsg, err := govv1.NewMsgSubmitProposal([]sdk.Msg{sdkMsg}, depositCoins, proposer, "", title, summary, false)
	if err != nil {
		return fmt.Errorf("failed to build channel upgrade cancel proposal: %w", err)
	}

	govRelayerMsg := cosmos.NewCosmosMessage(proposalMsg, nil)
	if _, _, err := cosmosProvider.SendMessages(ctx, []provider.RelayerMessage{govRelayerMsg}, memo); err != nil {
		return fmt.Errorf("failed to submit channel upgrade cancel governance proposal: %w", err)
	}

	c.log.Info("Submitted channel upgrade cancel governance proposal",
		zap.String("chain_id", c.PathEnd.ChainID),
		zap.String("channel_id", srcChanID),
		zap.String("port_id", srcPortID),
		zap.String("deposit", depositCoins.String()),
	)

	return nil
}

// ValidateChannelParams validates a set of port-ids as well as the order.
func ValidateChannelParams(srcPortID, dstPortID, order string) error {
	if err := host.PortIdentifierValidator(srcPortID); err != nil {
		return err
	}
	if err := host.PortIdentifierValidator(dstPortID); err != nil {
		return err
	}
	if (OrderFromString(order) == chantypes.ORDERED) || (OrderFromString(order) == chantypes.UNORDERED) {
		return nil
	}
	return fmt.Errorf("invalid order input (%s), order must be 'ordered' or 'unordered'", order)
}
