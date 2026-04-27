package models

import "time"

type Sighting struct {
	ID          string    `json:"id"`
	Species     string    `json:"species"`
	Location    string    `json:"location"`
	Latitude    float64   `json:"latitude"`
	Longitude   float64   `json:"longitude"`
	ObservedBy  string    `json:"observed_by"`
	ObservedAt  time.Time `json:"observed_at"`
	Notes       []Note    `json:"notes,omitempty"`
	Verified    bool      `json:"verified"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type Note struct {
	Author    string    `json:"author"`
	Text      string    `json:"text"`
	CreatedAt time.Time `json:"created_at"`
}

type CreateSightingRequest struct {
	Species    string    `json:"species"`
	Location   string    `json:"location"`
	Latitude   float64   `json:"latitude"`
	Longitude  float64   `json:"longitude"`
	ObservedBy string    `json:"observed_by"`
	ObservedAt time.Time `json:"observed_at"`
}

type UpdateSightingRequest struct {
	Species    *string    `json:"species,omitempty"`
	Location   *string    `json:"location,omitempty"`
	Latitude   *float64   `json:"latitude,omitempty"`
	Longitude  *float64   `json:"longitude,omitempty"`
	ObservedBy *string    `json:"observed_by,omitempty"`
	ObservedAt *time.Time `json:"observed_at,omitempty"`
}

type AddNoteRequest struct {
	Author string `json:"author"`
	Text   string `json:"text"`
}
