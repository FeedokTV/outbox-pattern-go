package transaction

import (
	"time"
	"uuid"
)

type Transaction struct {
	ID        uuid.UUID `json:"id"`
	Sender    string    `json:"sender"`
	Recipient string    `json:"recipient"`
	Amount    int       `json:"amount"`
	SendedAt  time.Time `json:"sended_at"`
}

const (
	TransactionCreatedType = "TransactionCreated"
	TransactionsTopic      = "transactions"
)

type TransactionCreatedEvent struct {
	TransactionID uuid.UUID `json:"transaction_id"`
	Sender        string    `json:"sender"`
	Recipient     string    `json:"recipient"`
	Amount        int       `json:"amount"`
}
