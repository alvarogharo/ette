package balance

import (
	"log"
	"math/big"
	"strconv"
	"time"

	cfg "github.com/itzmeanjan/ette/app/config"
	"github.com/itzmeanjan/ette/app/db"
	"github.com/jackc/pgtype"
	"gorm.io/gorm"
)

func CompressedBalanceService(_db *gorm.DB) {
	compressionSize, _ := strconv.Atoi(cfg.Get("CompressionSize"))
	intervalCheckCfg, _ := strconv.Atoi(cfg.Get("CompresionCheckIntervalSecs"))
	compresionCheckIntervalSecs := time.Duration(intervalCheckCfg)
	ticker := time.NewTicker(compresionCheckIntervalSecs * time.Second)

	for range ticker.C {
		checkForCompressionAndPersist(_db, compressionSize)
	}

}

func checkForCompressionAndPersist(_db *gorm.DB, compressionSize int) {
	lastCompressedBalanceBlock := db.GetLastCompressedBalaceBlock(_db)

	lastToBlock := 0
	if lastCompressedBalanceBlock != nil {
		lastToBlock = int(lastCompressedBalanceBlock.ToBlock)
	}

	fromBlock := uint64(lastToBlock)
	toBlock := uint64(lastToBlock + compressionSize)

	blockNumbers := db.GetAllBlockNumbersInRangeExclusive(_db, fromBlock, toBlock)
	if len(blockNumbers) != compressionSize {
		log.Printf("🔆 Not enougth blocks for balance compression. Target size %d current uncompressed size %d", compressionSize, len(blockNumbers))
		return
	}

	blockBalances := db.GetBlocksBlockBlancesByRange(_db, fromBlock, toBlock)
	if len(blockBalances) == 0 {
		return
	}

	compressedBalances := processBlockBalances(blockBalances, toBlock)

	if err := db.StoreCompressedBalances(_db, compressedBalances); err != nil {

		log.Printf("❗️ Failed to sotre compressed balance range %d - %d : %s\n", fromBlock, toBlock, err.Error())
		return
	}
	log.Printf("📎 Published balances compression from %d - %d\n", fromBlock, toBlock-1)
}

func processBlockBalances(blockBalances []*db.BlockBalanceOut, toBlock uint64) []*db.CompressedBalance {
	compressedBalancesMap := make(map[string]map[string]big.Int)
	compressedBlanceSize := 0
	compressed := 0

	for _, blockBalance := range blockBalances {
		_, okUser := compressedBalancesMap[blockBalance.User]

		if !okUser {
			compressedBalancesMap[blockBalance.User] = make(map[string]big.Int)
		}

		_, okToken := compressedBalancesMap[blockBalance.User][blockBalance.Token]

		if !okToken {
			compressedBalancesMap[blockBalance.User][blockBalance.Token] = *big.NewInt(0)
			compressedBlanceSize += 1
		} else {
			compressed++
		}
		amount := new(big.Int)
		amount.SetString(blockBalance.Amount, 10)
		currentBalance := compressedBalancesMap[blockBalance.User][blockBalance.Token]
		currentBalance.Add(&currentBalance, amount)
		compressedBalancesMap[blockBalance.User][blockBalance.Token] = currentBalance
	}

	compressedBalances := make([]*db.CompressedBalance, compressedBlanceSize)

	iter := 0
	for user, balance := range compressedBalancesMap {
		for token, amount := range balance {
			x := new(pgtype.Numeric)
			x.Set(amount.String())
			compressedBalances[iter] = &db.CompressedBalance{
				Token:   token,
				User:    user,
				ToBlock: toBlock - 1,
				Amount:  *x,
			}
			iter += 1
		}
	}

	return compressedBalances
}
