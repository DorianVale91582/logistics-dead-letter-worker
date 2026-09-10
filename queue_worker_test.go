package main

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

type fakeQueue struct {
	messages []queueMessage
	acked    []string
}

func (q *fakeQueue) consume(context.Context, consumeRequest) ([]queueMessage, error) {
	return q.messages, nil
}

func (q *fakeQueue) ack(_ context.Context, request ackRequest) error {
	q.acked = append(q.acked, request.MessageID)
	return nil
}

func TestRunBatchAcknowledgesOnlyCompletedShipments(t *testing.T) {
	good, _ := json.Marshal(shipmentJob{ShipmentID: "ship-100", Carrier: "northline", Route: "SHA-NKG"})
	poison, _ := json.Marshal(shipmentJob{ShipmentID: "ship-101", Carrier: "northline"})
	queue := &fakeQueue{messages: []queueMessage{
		{MessageID: "msg-good", Payload: good},
		{MessageID: "msg-poison", Payload: poison},
	}}

	err := runBatch(context.Background(), queue, func(_ context.Context, job shipmentJob) error {
		if job.Route == "" {
			return errors.New("route missing")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"msg-good"}; !reflect.DeepEqual(queue.acked, want) {
		t.Fatalf("acked %v, want %v", queue.acked, want)
	}
}
