package data

import "github.com/GeoDB-Limited/go-ethereum/common"

// APIKey - Payload to be sent in POST request
// when either enabling or disabling state of API Key
type APIKey struct {
	APIKey common.Hash `json:"apiKey"`
}
