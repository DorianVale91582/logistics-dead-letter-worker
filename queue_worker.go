package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
)

const (
	logisticsQueue  = "logistics-jobs"
	deadLetterQueue = "logistics-jobs-dead"
)

type shipmentJob struct {
	ShipmentID string `json:"shipment_id"`
	Carrier    string `json:"carrier"`
	Route      string `json:"route"`
}

type queueOperations interface {
	consume(context.Context, consumeRequest) ([]queueMessage, error)
	ack(context.Context, ackRequest) error
}

type shipmentProcessor func(context.Context, shipmentJob) error

func runBatch(ctx context.Context, queue queueOperations, process shipmentProcessor) error {
	messages, err := queue.consume(ctx, consumeRequest{
		Queue:             logisticsQueue,
		MaxMessages:       10,
		VisibilityTimeout: 60,
	})
	if err != nil {
		return err
	}

	for _, message := range messages {
		var job shipmentJob
		if err := json.Unmarshal(message.Payload, &job); err != nil {
			log.Printf("message %s retained for retry: invalid logistics payload", message.MessageID)
			continue
		}
		if err := process(ctx, job); err != nil {
			log.Printf("shipment %s retained for retry: %v", job.ShipmentID, err)
			continue
		}
		if err := queue.ack(ctx, ackRequest{Queue: logisticsQueue, MessageID: message.MessageID}); err != nil {
			return fmt.Errorf("ack shipment %s: %w", job.ShipmentID, err)
		}
	}
	return nil
}

func dispatchShipment(_ context.Context, job shipmentJob) error {
	if job.ShipmentID == "" || job.Carrier == "" || job.Route == "" {
		return fmt.Errorf("shipment_id, carrier, and route are required")
	}
	log.Printf("dispatched shipment=%s carrier=%s route=%s", job.ShipmentID, job.Carrier, job.Route)
	return nil
}
