package block

import (
	"encoding/hex"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/itzmeanjan/ette/app/db"
	"github.com/jackc/pgtype"
)

func processTxBalances(blockNumber uint64, txs []*db.PackedTransaction) []*db.BlockBalance {

	const EthTokenId = "0x"
	const TranferEventName = "Transfer"
	const TransferTopicId = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

	if len(txs) == 0 {
		return nil
	}

	blockBalances := make(map[string]map[string]big.Int)
	blockBalanceSize := 0

	for _, tx := range txs {
		//Eth balance
		//From
		_, okFrom := blockBalances[tx.Tx.From]

		if !okFrom {
			blockBalances[tx.Tx.From] = make(map[string]big.Int)
		}

		_, okToken := blockBalances[tx.Tx.From][EthTokenId]

		if !okToken {
			blockBalances[tx.Tx.From][EthTokenId] = *big.NewInt(0)
			blockBalanceSize += 1
		}
		value := new(big.Int)
		value, okValue := value.SetString(tx.Tx.Value, 10)
		if !okValue {
			fmt.Println("SetString: error")
		}
		cost := new(big.Int)
		cost, okCost := cost.SetString(tx.Tx.Cost, 10)
		if !okCost {
			fmt.Println("SetString: error")
		}
		currentBalance := blockBalances[tx.Tx.From][EthTokenId]
		currentBalance.Sub(&currentBalance, cost)
		blockBalances[tx.Tx.From][EthTokenId] = currentBalance

		//To
		if tx.Tx.To != "" {
			_, okTo := blockBalances[tx.Tx.To]

			if !okTo {
				blockBalances[tx.Tx.To] = make(map[string]big.Int)
			}

			_, okTokenTo := blockBalances[tx.Tx.To][EthTokenId]

			if !okTokenTo {
				blockBalances[tx.Tx.To][EthTokenId] = *big.NewInt(0)
				blockBalanceSize += 1
			}

			currentBalanceTo := blockBalances[tx.Tx.To][EthTokenId]
			currentBalanceTo.Add(&currentBalanceTo, value)
			blockBalances[tx.Tx.To][EthTokenId] = currentBalanceTo
		}

		//Token balance
		for _, event := range tx.Events {

			if len(event.Topics) == 3 && event.Topics[0] == TransferTopicId {

				amount := new(big.Int)
				amount, okAmount := amount.SetString(hex.EncodeToString(event.Data), 16)
				if !okAmount {
					fmt.Println("SetString: error")
				}

				from := common.HexToAddress(event.Topics[1]).Hex()
				to := common.HexToAddress(event.Topics[2]).Hex()

				// From
				_, okFrom := blockBalances[from]

				if !okFrom {
					blockBalances[from] = make(map[string]big.Int)
				}

				_, okToken := blockBalances[from][event.Origin]

				if !okToken {
					blockBalances[from][event.Origin] = *big.NewInt(0)
					blockBalanceSize += 1
				}

				currentBalanceFrom := blockBalances[from][event.Origin]
				currentBalanceFrom.Sub(&currentBalanceFrom, amount)
				blockBalances[from][event.Origin] = currentBalanceFrom

				// To
				_, okTo := blockBalances[to]

				if !okTo {
					blockBalances[to] = make(map[string]big.Int)
				}

				_, okTokenTo := blockBalances[to][event.Origin]

				if !okTokenTo {
					blockBalances[to][event.Origin] = *big.NewInt(0)
					blockBalanceSize += 1
				}

				currentBalanceTo := blockBalances[to][event.Origin]
				currentBalanceTo.Add(&currentBalanceTo, amount)
				blockBalances[to][event.Origin] = currentBalanceTo
			}
		}
	}

	balances := make([]*db.BlockBalance, blockBalanceSize)

	iter := 0
	for user, balance := range blockBalances {
		for token, amount := range balance {
			x := new(pgtype.Numeric)
			x.Set(amount.String())
			balances[iter] = &db.BlockBalance{
				Token:       token,
				User:        user,
				BlockNumber: blockNumber,
				Amount:      *x,
			}
			iter += 1
		}
	}

	return balances
}

// BuildPackedBlock - Builds struct holding whole block data i.e.
// block header, block body i.e. tx(s), event log(s)
func BuildPackedBlock(block *types.Block, txs []*db.PackedTransaction) *db.PackedBlock {

	packedBlock := &db.PackedBlock{}

	packedBlock.Block = &db.Blocks{
		Hash:                block.HashString,
		Number:              block.NumberU64(),
		Time:                block.Time(),
		ParentHash:          block.ParentHash().Hex(),
		Difficulty:          block.Difficulty().String(),
		GasUsed:             block.GasUsed(),
		GasLimit:            block.GasLimit(),
		Nonce:               hexutil.EncodeUint64(block.Nonce()),
		Miner:               block.Coinbase().Hex(),
		Size:                float64(block.Size()),
		StateRootHash:       block.Root().Hex(),
		UncleHash:           block.UncleHash().Hex(),
		TransactionRootHash: block.TxHash().Hex(),
		ReceiptRootHash:     block.ReceiptHash().Hex(),
		ExtraData:           block.Extra(),
	}
	packedBlock.Transactions = txs

	packedBlock.Balances = processTxBalances(block.NumberU64(), txs)

	return packedBlock
}
