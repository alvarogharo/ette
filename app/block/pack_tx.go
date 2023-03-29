package block

import (
	"fmt"
	"math/big"

	"github.com/GeoDB-Limited/go-ethereum/common"
	"github.com/GeoDB-Limited/go-ethereum/core/types"
	c "github.com/itzmeanjan/ette/app/common"
	"github.com/itzmeanjan/ette/app/db"
)

// BuildPackedTx - Putting all information, `ette` will keep for one tx
// into a single structure, so that it becomes easier to pass to & from functions
func BuildPackedTx(tx *types.Transaction, sender common.Address, receipt *types.Receipt) *db.PackedTransaction {

	packedTx := &db.PackedTransaction{}

	gasPriceBigInt := new(big.Int)
	gasPriceBigInt, okGasPrice := gasPriceBigInt.SetString(tx.GasPriceString, 10)
	if !okGasPrice {
		fmt.Println("SetString: error")
	}

	cost := new(big.Int)
	cost.Mul(gasPriceBigInt, new(big.Int).SetUint64(receipt.GasUsed))
	cost.Add(cost, tx.Value())

	if tx.To() == nil {

		packedTx.Tx = &db.Transactions{
			Hash:      tx.HashString,
			From:      sender.Hex(),
			Contract:  receipt.ContractAddress.Hex(),
			Value:     tx.Value().String(),
			Data:      tx.Data(),
			Gas:       receipt.GasUsed,
			GasPrice:  tx.GasPriceString,
			Cost:      cost.String(),
			Nonce:     tx.Nonce(),
			State:     receipt.Status,
			BlockHash: receipt.BlockHash.Hex(),
		}

	} else {

		packedTx.Tx = &db.Transactions{
			Hash:      tx.HashString,
			From:      sender.Hex(),
			To:        tx.To().Hex(),
			Value:     tx.Value().String(),
			Data:      tx.Data(),
			Gas:       receipt.GasUsed,
			GasPrice:  tx.GasPriceString,
			Cost:      cost.String(),
			Nonce:     tx.Nonce(),
			State:     receipt.Status,
			BlockHash: receipt.BlockHash.Hex(),
		}

	}

	packedTx.Events = make([]*db.Events, len(receipt.Logs))

	for k, v := range receipt.Logs {

		packedTx.Events[k] = &db.Events{
			Origin:          v.Address.Hex(),
			Index:           v.Index,
			Topics:          c.StringifyEventTopics(v.Topics),
			Data:            v.Data,
			TransactionHash: v.TxHash.Hex(),
			BlockHash:       v.BlockHash.Hex(),
		}

	}

	return packedTx

}
