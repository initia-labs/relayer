package cosmos

import (
	"context"
	"errors"
	"fmt"

	clienttypes "github.com/cosmos/ibc-go/v8/modules/core/02-client/types"
	chantypes "github.com/cosmos/ibc-go/v8/modules/core/04-channel/types"
	"github.com/cosmos/relayer/v2/relayer/provider"
)

// ChannelUpgradeProof collects the proofs required for assembling channel
// upgrade handshake messages.
func (cc *CosmosProvider) ChannelUpgradeProof(ctx context.Context, info provider.ChannelInfo, height uint64) (provider.ChannelUpgradeProof, error) {
	channelProof, err := cc.ChannelProof(ctx, info, height)
	if err != nil {
		return provider.ChannelUpgradeProof{}, err
	}

	upgradeRes, err := cc.QueryChannelUpgrade(ctx, int64(height), info.PortID, info.ChannelID)
	if err != nil {
		return provider.ChannelUpgradeProof{}, err
	}

	proof := provider.ChannelUpgradeProof{
		ChannelProof:       channelProof,
		Upgrade:            upgradeRes.Upgrade,
		UpgradeProof:       upgradeRes.Proof,
		UpgradeProofHeight: upgradeRes.ProofHeight,
	}

	if chanRes, err := cc.QueryChannel(ctx, int64(height), info.ChannelID, info.PortID); err == nil && chanRes.Channel != nil {
		channelCopy := *chanRes.Channel
		proof.CounterpartyChannel = &channelCopy
	}

	if errRes, err := cc.QueryChannelUpgradeError(ctx, int64(height), info.PortID, info.ChannelID); err == nil {
		receiptCopy := errRes.ErrorReceipt
		proof.ErrorReceipt = &receiptCopy
		proof.ErrorProof = errRes.Proof
		proof.ErrorProofHeight = errRes.ProofHeight
	} else if !errors.Is(err, chantypes.ErrUpgradeErrorNotFound) {
		return provider.ChannelUpgradeProof{}, err
	}

	return proof, nil
}

func (cc *CosmosProvider) ChannelUpgradeErrorProof(ctx context.Context, info provider.ChannelInfo, height uint64) (provider.ChannelUpgradeErrorProof, error) {
	errRes, err := cc.QueryChannelUpgradeError(ctx, int64(height), info.PortID, info.ChannelID)
	if err != nil {
		return provider.ChannelUpgradeErrorProof{}, err
	}
	return provider.ChannelUpgradeErrorProof{
		Receipt:     &errRes.ErrorReceipt,
		Proof:       errRes.Proof,
		ProofHeight: errRes.ProofHeight,
	}, nil
}

func (cc *CosmosProvider) MsgChannelUpgradeTry(info provider.ChannelInfo, proof provider.ChannelUpgradeProof) (provider.RelayerMessage, error) {
	signer, err := cc.Address()
	if err != nil {
		return nil, err
	}

	msg := &chantypes.MsgChannelUpgradeTry{
		PortId:                        info.CounterpartyPortID,
		ChannelId:                     info.CounterpartyChannelID,
		ProposedUpgradeConnectionHops: []string{info.CounterpartyConnectionHop0},
		CounterpartyUpgradeFields:     info.Upgrade.Fields,
		CounterpartyUpgradeSequence:   info.UpgradeSequence,
		ProofChannel:                  proof.ChannelProof.Proof,
		ProofUpgrade:                  proof.UpgradeProof,
		ProofHeight:                   proof.UpgradeProofHeight,
		Signer:                        signer,
	}

	return NewCosmosMessage(msg, func(signer string) {
		msg.Signer = signer
	}), nil
}

func (cc *CosmosProvider) MsgChannelUpgradeAck(info provider.ChannelInfo, proof provider.ChannelUpgradeProof) (provider.RelayerMessage, error) {
	signer, err := cc.Address()
	if err != nil {
		return nil, err
	}

	msg := &chantypes.MsgChannelUpgradeAck{
		PortId:              info.CounterpartyPortID,
		ChannelId:           info.CounterpartyChannelID,
		CounterpartyUpgrade: proof.Upgrade,
		ProofChannel:        proof.ChannelProof.Proof,
		ProofUpgrade:        proof.UpgradeProof,
		ProofHeight:         proof.UpgradeProofHeight,
		Signer:              signer,
	}

	return NewCosmosMessage(msg, func(signer string) {
		msg.Signer = signer
	}), nil
}

func (cc *CosmosProvider) MsgChannelUpgradeConfirm(info provider.ChannelInfo, proof provider.ChannelUpgradeProof) (provider.RelayerMessage, error) {
	signer, err := cc.Address()
	if err != nil {
		return nil, err
	}

	msg := &chantypes.MsgChannelUpgradeConfirm{
		PortId:                   info.CounterpartyPortID,
		ChannelId:                info.CounterpartyChannelID,
		CounterpartyChannelState: info.ChannelState,
		CounterpartyUpgrade:      proof.Upgrade,
		ProofChannel:             proof.ChannelProof.Proof,
		ProofUpgrade:             proof.UpgradeProof,
		ProofHeight:              proof.UpgradeProofHeight,
		Signer:                   signer,
	}

	return NewCosmosMessage(msg, func(signer string) {
		msg.Signer = signer
	}), nil
}

func (cc *CosmosProvider) MsgChannelUpgradeOpen(info provider.ChannelInfo, proof provider.ChannelProof) (provider.RelayerMessage, error) {
	signer, err := cc.Address()
	if err != nil {
		return nil, err
	}

	msg := &chantypes.MsgChannelUpgradeOpen{
		PortId:                      info.CounterpartyPortID,
		ChannelId:                   info.CounterpartyChannelID,
		CounterpartyChannelState:    info.ChannelState,
		CounterpartyUpgradeSequence: info.UpgradeSequence,
		ProofChannel:                proof.Proof,
		ProofHeight:                 proof.ProofHeight,
		Signer:                      signer,
	}

	return NewCosmosMessage(msg, func(signer string) {
		msg.Signer = signer
	}), nil
}

func (cc *CosmosProvider) MsgChannelUpgradeTimeout(info provider.ChannelInfo, proof provider.ChannelUpgradeProof) (provider.RelayerMessage, error) {
	signer, err := cc.Address()
	if err != nil {
		return nil, err
	}

	if proof.CounterpartyChannel == nil {
		return nil, fmt.Errorf("counterparty channel state not available for upgrade timeout")
	}

	msg := &chantypes.MsgChannelUpgradeTimeout{
		PortId:              info.CounterpartyPortID,
		ChannelId:           info.CounterpartyChannelID,
		CounterpartyChannel: *proof.CounterpartyChannel,
		ProofChannel:        proof.ChannelProof.Proof,
		ProofHeight:         proof.ChannelProof.ProofHeight,
		Signer:              signer,
	}

	return NewCosmosMessage(msg, func(signer string) {
		msg.Signer = signer
	}), nil
}

func (cc *CosmosProvider) MsgChannelUpgradeCancel(info provider.ChannelInfo, proof provider.ChannelUpgradeErrorProof, govAddress string) (provider.RelayerMessage, error) {
	var msg *chantypes.MsgChannelUpgradeCancel
	if govAddress != "" {
		msg = &chantypes.MsgChannelUpgradeCancel{
			PortId:            info.CounterpartyPortID,
			ChannelId:         info.CounterpartyChannelID,
			ErrorReceipt:      chantypes.ErrorReceipt{},
			ProofErrorReceipt: []byte{},
			ProofHeight:       clienttypes.Height{},
			Signer:            govAddress,
		}
	} else {
		if proof.Receipt == nil {
			return nil, fmt.Errorf("upgrade error receipt not available for cancellation")
		}

		signer, err := cc.Address()
		if err != nil {
			return nil, err
		}

		msg = &chantypes.MsgChannelUpgradeCancel{
			PortId:            info.CounterpartyPortID,
			ChannelId:         info.CounterpartyChannelID,
			ErrorReceipt:      *proof.Receipt,
			ProofErrorReceipt: proof.Proof,
			ProofHeight:       proof.ProofHeight,
			Signer:            signer,
		}
	}
	return NewCosmosMessage(msg, func(signer string) {
		msg.Signer = signer
	}), nil
}

func (cc *CosmosProvider) MsgChannelUpgradeInit(info provider.ChannelInfo, govAddress string) (provider.RelayerMessage, error) {
	fields := info.Upgrade.Fields
	if len(fields.ConnectionHops) == 0 {
		if info.ConnID != "" {
			fields.ConnectionHops = []string{info.ConnID}
		}
	}
	if fields.Ordering == chantypes.NONE {
		fields.Ordering = info.Order
	}
	if fields.Version == "" {
		fields.Version = info.Version
	}

	msg := chantypes.NewMsgChannelUpgradeInit(info.PortID, info.ChannelID, fields, govAddress)

	return NewCosmosMessage(msg, func(signer string) {
		msg.Signer = signer
	}), nil
}
