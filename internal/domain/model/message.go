package model

import "github.com/google/uuid"

type Message struct {
	ID       uuid.UUID
	Key      string
	Payload  []byte
	Offset   uint64
	Topic    string
	PartID   int
	Producer string
}
