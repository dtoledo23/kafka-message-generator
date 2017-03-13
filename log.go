package main

// Log defines a model for the logs received.
type Log struct {
  Timestamp int64  `json:"timestamp"`
  Stage     int    `json:"stage"`
  Message   string `json:"message"`
  SessionID string `json:"sessionId"`
}
