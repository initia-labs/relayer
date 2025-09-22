package penumbra

import (
	"context"
	"errors"

	"github.com/cosmos/relayer/v2/relayer/provider"
)

var errChannelUpgradeUnsupported = errors.New("channel upgrades are not supported for penumbra chains")

func (pp *PenumbraProvider) ChannelUpgradeProof(context.Context, provider.ChannelInfo, uint64) (provider.ChannelUpgradeProof, error) {
	return provider.ChannelUpgradeProof{}, errChannelUpgradeUnsupported
}

func (pp *PenumbraProvider) ChannelUpgradeErrorProof(context.Context, provider.ChannelInfo, uint64) (provider.ChannelUpgradeErrorProof, error) {
	return provider.ChannelUpgradeErrorProof{}, errChannelUpgradeUnsupported
}

func (pp *PenumbraProvider) MsgChannelUpgradeTry(provider.ChannelInfo, provider.ChannelUpgradeProof) (provider.RelayerMessage, error) {
	return nil, errChannelUpgradeUnsupported
}

func (pp *PenumbraProvider) MsgChannelUpgradeAck(provider.ChannelInfo, provider.ChannelUpgradeProof) (provider.RelayerMessage, error) {
	return nil, errChannelUpgradeUnsupported
}

func (pp *PenumbraProvider) MsgChannelUpgradeConfirm(provider.ChannelInfo, provider.ChannelUpgradeProof) (provider.RelayerMessage, error) {
	return nil, errChannelUpgradeUnsupported
}

func (pp *PenumbraProvider) MsgChannelUpgradeOpen(provider.ChannelInfo, provider.ChannelProof) (provider.RelayerMessage, error) {
	return nil, errChannelUpgradeUnsupported
}

func (pp *PenumbraProvider) MsgChannelUpgradeTimeout(provider.ChannelInfo, provider.ChannelUpgradeProof) (provider.RelayerMessage, error) {
	return nil, errChannelUpgradeUnsupported
}

func (pp *PenumbraProvider) MsgChannelUpgradeCancel(provider.ChannelInfo, provider.ChannelUpgradeErrorProof, string) (provider.RelayerMessage, error) {
	return nil, errChannelUpgradeUnsupported
}

func (pp *PenumbraProvider) MsgChannelUpgradeInit(provider.ChannelInfo, string) (provider.RelayerMessage, error) {
	return nil, errChannelUpgradeUnsupported
}
