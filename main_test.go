package main

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestGetEnv(t *testing.T) {
	const envKey = "TEST_ENV_VAR"
	const expectedValue = "test_value"

	// Set an environment variable for testing.
	os.Setenv(envKey, expectedValue)
	defer os.Unsetenv(envKey) // Clean up after the test

	// Call getEnv and check the result.
	result := getEnv(envKey)
	if result != expectedValue {
		t.Errorf("getEnv(%q) = %q, want %q", envKey, result, expectedValue)
	}
}

type MockKafkaReader struct {
	Messages  []kafka.Message
	ReadIndex int // Tracks the current read position
}

func (m *MockKafkaReader) ReadMessage(ctx context.Context) (kafka.Message, error) {
	if m.ReadIndex >= len(m.Messages) {
		return kafka.Message{}, io.EOF // Simulate end of message queue
	}
	msg := m.Messages[m.ReadIndex]
	m.ReadIndex++
	return msg, nil
}

type MockKafkaWriter struct {
	WrittenMessages []kafka.Message
}

func (m *MockKafkaWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	m.WrittenMessages = append(m.WrittenMessages, msgs...)
	return nil
}
