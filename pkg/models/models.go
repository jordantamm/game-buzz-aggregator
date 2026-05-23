package models

import "time"

// Game is the canonical game entity used across services.
type Game struct {
	ID            string
	CanonicalName string
	SteamAppID    *int
	ReleasedAt    *time.Time
	Aliases       []string
}

// Trending is returned by the /v1/trending endpoint.
type Trending struct {
	GameID        string    `json:"game_id"`
	CanonicalName string    `json:"canonical_name"`
	MentionCount  int       `json:"mention_count"`
	AvgSentiment  *float64  `json:"avg_sentiment"`
	WindowStart   time.Time `json:"window_start"`
}

// MentionRow is a hydrated mention row from the DB.
type MentionRow struct {
	ID                 string
	Source             string
	NativeID           string
	AuthorHash         string
	Text               string
	CreatedAt          time.Time
	IngestedAt         time.Time
	SentimentScore     *float64
	SentimentMagnitude *float64
}
