package models

import "uuid"

const (
	TransactionCreatedType = "TransactionCreated"
	TransactionsTopic      = "transactions"
)

type TransactionCreatedEvent struct {
	TransactionID uuid.UUID `json:"transaction_id"`
	Sender        string    `json:"sender"`
	Recipient     string    `json:"recipient"`
	Amount        int64     `json:"amount"`
}
