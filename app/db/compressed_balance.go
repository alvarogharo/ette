package db

import (
	"errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func GetLastCompressedBalaceBlock(_db *gorm.DB) *CompressedBalance {
	var compressedBalance CompressedBalance

	if err := _db.Last(&compressedBalance).Error; err != nil {
		return nil
	}

	return &compressedBalance
}

func UpsertCompressedBalance(dbWTx *gorm.DB, balance *CompressedBalance) error {

	if balance == nil {
		return errors.New("empty block received while attempting to persist")
	}

	return dbWTx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "token"}, {Name: "user"}, {Name: "toblock"}},
		DoUpdates: clause.Assignments(map[string]interface{}{"amount": gorm.Expr("compressed_balance.amount + EXCLUDED.amount")}),
	}).Create(balance).Error

}

func StoreCompressedBalances(dbWOTx *gorm.DB, balances []*CompressedBalance) error {

	if balances == nil {
		return errors.New("empty balances received while attempting to persist")
	}

	// -- Starting DB transaction
	return dbWOTx.Transaction(func(dbWTx *gorm.DB) error {

		for _, bal := range balances {

			if err := UpsertCompressedBalance(dbWTx, bal); err != nil {
				return err
			}

		}

		return nil
	})
	// -- Ending DB transaction

}
