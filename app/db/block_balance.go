package db

import (
	"errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func UpsertBlockBalance(dbWTx *gorm.DB, balance *BlockBalance) error {

	if balance == nil {
		return errors.New("empty block received while attempting to persist")
	}

	return dbWTx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "token"}, {Name: "user"}, {Name: "blocknumber"}},
		DoUpdates: clause.Assignments(map[string]interface{}{"amount": gorm.Expr("block_balance.amount + EXCLUDED.amount")}),
	}).Create(balance).Error

}

func RemoveBlockBalance(dbWTx *gorm.DB, blockHash string) error {

	return dbWTx.Where("blockhash = ?", blockHash).Delete(&Transactions{}).Error

}
