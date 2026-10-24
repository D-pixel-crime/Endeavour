package models

import (
	"github.com/google/uuid"
)

type Seat struct {
	ID                 uuid.UUID  `db:"id" json:"id"`
	TrainID            string     `db:"train_id" json:"train_id"`
	SeatNumber         string     `db:"seat_number" json:"seat_number"`
	Cost               int        `db:"cost" json:"cost"`
	IsBooked           bool       `db:"is_booked" json:"is_booked"`
	BookedByWorkflowID *uuid.UUID `db:"booked_by_workflow_id" json:"booked_by_workflow_id,omitempty"`
}
