package cmd

import (
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"strconv"

	rpchttp "github.com/cometbft/cometbft/rpc/client/http"
	"github.com/cosmos/relayer/v2/relayer/chains/cosmos"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func attestorsCmd(a *appState) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "attestors",
		Aliases: []string{"at"},
		Short:   "Manage attestor configurations",
	}

	cmd.AddCommand(
		attestorsAddCmd(a),
		attestorsSetThresholdCmd(a),
	)

	return cmd
}

func attestorsAddCmd(a *appState) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "add [chain-name] [attestor-rpc-addr]",
		Aliases: []string{"a"},
		Short:   "Add a new attestor to the configuration file by fetching attestor public key from given RPC address",
		Args:    withUsage(cobra.MinimumNArgs(0)),
		Example: fmt.Sprintf(` $ %s attestors add minimove https://127.0.0.1:26657`, appName),
		RunE: func(cmd *cobra.Command, args []string) error {
			if ok := a.config; ok == nil {
				return errors.New("config not initialized, consider running `rly config init`")
			}

			chainName := args[0]
			attestorRpcAddr := args[1]

			if a.config.Chains[chainName] == nil {
				return fmt.Errorf("chain %s not found; consider running `rly chains add %s`", chainName, chainName)
			}

			return a.performConfigLockingOperation(cmd.Context(), func() error {
				rpchttpClient, err := rpchttp.New(attestorRpcAddr, "/websocket")
				if err != nil {
					return err
				}
				res, err := rpchttpClient.AttestorPubKey(cmd.Context())
				if err != nil {
					return err
				}

				cp, ok := a.config.Chains[chainName].ChainProvider.(*cosmos.CosmosProvider)
				if !ok {
					return fmt.Errorf("chain %s is not a cosmos chain", chainName)
				}

				if cp.PCfg.AttestorConfiguration == nil {
					cp.PCfg.AttestorConfiguration = &cosmos.AttestorConfiguration{}
				}

				cp.PCfg.ClientType = "07-tendermint-attestor"
				cp.PCfg.AttestorConfiguration.AttestorRpcAddrs = append(cp.PCfg.AttestorConfiguration.AttestorRpcAddrs, attestorRpcAddr)
				cp.PCfg.AttestorConfiguration.AttestorPubKeys = append(cp.PCfg.AttestorConfiguration.AttestorPubKeys, base64.StdEncoding.EncodeToString(res.PubKey))
				a.log.Info("added attestor",
					zap.String("chain", chainName),
					zap.String("attestor_rpc_addr", attestorRpcAddr),
					zap.String("attestor_pub_key", base64.StdEncoding.EncodeToString(res.PubKey)),
				)
				return nil
			})
		},
	}
	return cmd
}

func attestorsSetThresholdCmd(a *appState) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "set-threshold [chain-name] [threshold]",
		Aliases: []string{"t"},
		Short:   "Set the threshold for the attestors",
		Args:    withUsage(cobra.MinimumNArgs(0)),
		Example: fmt.Sprintf(` $ %s attestors set-threshold minimove 2`, appName),
		RunE: func(cmd *cobra.Command, args []string) error {
			if ok := a.config; ok == nil {
				return errors.New("config not initialized, consider running `rly config init`")
			}

			chainName := args[0]
			threshold, err := strconv.Atoi(args[1])
			if err != nil {
				return fmt.Errorf("invalid threshold: %w", err)
			}
			if threshold < 0 {
				return fmt.Errorf("threshold must be greater than 0")
			} else if threshold >= math.MaxUint32 {
				return fmt.Errorf("threshold must be less than %d", math.MaxUint32)
			}

			if a.config.Chains[chainName] == nil {
				return fmt.Errorf("chain %s not found; consider running `rly chains add %s`", chainName, chainName)
			}

			return a.performConfigLockingOperation(cmd.Context(), func() error {
				cp, ok := a.config.Chains[chainName].ChainProvider.(*cosmos.CosmosProvider)
				if !ok {
					return fmt.Errorf("chain %s is not a cosmos chain", chainName)
				}

				if cp.PCfg.AttestorConfiguration == nil {
					cp.PCfg.AttestorConfiguration = &cosmos.AttestorConfiguration{}
				}
				cp.PCfg.AttestorConfiguration.Threshold = uint32(threshold)
				a.log.Info("set threshold",
					zap.String("chain", chainName),
					zap.Int("threshold", threshold),
				)
				return nil
			})
		},
	}
	return cmd
}
