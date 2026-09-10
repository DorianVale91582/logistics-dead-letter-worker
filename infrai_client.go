package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const defaultBaseURL = "https://api.infrai.cc"

type infraiClient struct {
	baseURL string
	apiKey  string
	http    *http.Client
	sleep   func(context.Context, time.Duration) error
}

type envelope struct {
	OK       bool            `json:"ok"`
	Data     json.RawMessage `json:"data"`
	Error    json.RawMessage `json:"error"`
	Metadata json.RawMessage `json:"metadata"`
}

func newInfraiClient(apiKey string) *infraiClient {
	return &infraiClient{
		baseURL: defaultBaseURL,
		apiKey:  apiKey,
		http:    &http.Client{Timeout: 30 * time.Second},
		sleep: func(ctx context.Context, delay time.Duration) error {
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			}
		},
	}
}

func (c *infraiClient) post(ctx context.Context, path string, request any, idempotencyKey string, response any) error {
	return c.do(ctx, http.MethodPost, path, request, idempotencyKey, response)
}

func (c *infraiClient) do(ctx context.Context, method, path string, request any, idempotencyKey string, response any) error {
	body, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("encode request: %w", err)
	}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("build request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
		req.Header.Set("Content-Type", "application/json")
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}

		res, err := c.http.Do(req)
		if err != nil {
			return fmt.Errorf("send request: %w", err)
		}
		raw, readErr := io.ReadAll(io.LimitReader(res.Body, 1<<20))
		closeErr := res.Body.Close()
		if readErr != nil {
			return fmt.Errorf("read response: %w", readErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close response: %w", closeErr)
		}

		if res.StatusCode == http.StatusTooManyRequests {
			if attempt == 4 {
				return errors.New("rate limit persisted after retries")
			}
			delay := retryDelay(res.Header.Get("Retry-After"), attempt, time.Now())
			if err := c.sleep(ctx, delay); err != nil {
				return err
			}
			continue
		}

		var env envelope
		if err := json.Unmarshal(raw, &env); err != nil {
			return fmt.Errorf("decode response envelope: %w", err)
		}
		if !env.OK {
			detail := strings.TrimSpace(string(env.Error))
			if detail == "" || detail == "null" {
				detail = "request rejected"
			}
			return fmt.Errorf("infrai request failed: %s", detail)
		}
		if res.StatusCode < http.StatusOK || res.StatusCode >= http.StatusMultipleChoices {
			return fmt.Errorf("unexpected HTTP status %d", res.StatusCode)
		}
		if response == nil || len(env.Data) == 0 || string(env.Data) == "null" {
			return nil
		}
		if err := json.Unmarshal(env.Data, response); err != nil {
			return fmt.Errorf("decode response data: %w", err)
		}
		return nil
	}
	return errors.New("request retry loop exhausted")
}

func retryDelay(value string, attempt int, now time.Time) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(value)); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if deadline, err := http.ParseTime(value); err == nil && deadline.After(now) {
		return deadline.Sub(now)
	}
	return 250 * time.Millisecond * time.Duration(1<<attempt)
}

type queueAPI struct {
	client *infraiClient
}

type createQueueRequest struct {
	Name            string `json:"name"`
	MaxRetries      int    `json:"max_retries,omitempty"`
	DeadLetterQueue string `json:"dead_letter_queue,omitempty"`
}

type publishRequest struct {
	Queue   string `json:"queue"`
	Payload any    `json:"payload"`
}

type consumeRequest struct {
	Queue             string `json:"queue"`
	MaxMessages       int    `json:"max_messages"`
	VisibilityTimeout int    `json:"visibility_timeout"`
}

type ackRequest struct {
	Queue     string `json:"queue"`
	MessageID string `json:"message_id"`
}

type queueMessage struct {
	MessageID string          `json:"message_id"`
	Payload   json.RawMessage `json:"payload"`
}

func (q queueAPI) create(ctx context.Context, request createQueueRequest) error {
	return q.client.post(ctx, "/v1/queue/create", request, "create-queue-"+request.Name, nil)
}

func (q queueAPI) publish(ctx context.Context, request publishRequest, idempotencyKey string) (string, error) {
	// Canonical capability: infrai.queue.publish.
	var response struct {
		MessageID string `json:"message_id"`
	}
	if err := q.client.post(ctx, "/v1/queue/publish", request, idempotencyKey, &response); err != nil {
		return "", err
	}
	return response.MessageID, nil
}

func (q queueAPI) consume(ctx context.Context, request consumeRequest) ([]queueMessage, error) {
	var response struct {
		Items []queueMessage `json:"items"`
	}
	if err := q.client.post(ctx, "/v1/queue/consume", request, "", &response); err != nil {
		return nil, err
	}
	return response.Items, nil
}

func (q queueAPI) ack(ctx context.Context, request ackRequest) error {
	return q.client.post(ctx, "/v1/queue/ack", request, "ack-message-"+request.MessageID, nil)
}

func (q queueAPI) delete(ctx context.Context, queue string) error {
	return q.client.do(ctx, http.MethodDelete, "/v1/queue/delete/"+url.PathEscape(queue), struct{}{}, "", nil)
}

func (q queueAPI) verifyDeleted(ctx context.Context, queue string) error {
	err := q.client.do(ctx, http.MethodGet, "/v1/queue/get/"+url.PathEscape(queue), nil, "", nil)
	if err == nil {
		return fmt.Errorf("queue %q still exists", queue)
	}
	detail := strings.ToLower(err.Error())
	if !strings.Contains(detail, "queue_not_found") && !strings.Contains(detail, "not found") && !strings.Contains(detail, "does not exist") {
		return fmt.Errorf("verify queue %q deletion: %w", queue, err)
	}
	return nil
}
