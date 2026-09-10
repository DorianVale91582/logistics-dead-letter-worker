package main

import (
	"context"
	"fmt"
	"log"
	"os"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, args []string) error {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		return fmt.Errorf("set INFRAI_API_KEY")
	}

	client := newInfraiClient(apiKey)
	infrai := struct{ queue queueAPI }{queue: queueAPI{client: client}}
	if len(args) == 0 {
		return fmt.Errorf("command required: setup, publish, or work")
	}

	switch args[0] {
	case "setup":
		if err := infrai.queue.create(ctx, createQueueRequest{Name: deadLetterQueue}); err != nil {
			return err
		}
		return infrai.queue.create(ctx, createQueueRequest{
			Name:            logisticsQueue,
			MaxRetries:      4,
			DeadLetterQueue: deadLetterQueue,
		})

	case "publish":
		if len(args) != 4 {
			return fmt.Errorf("publish requires: shipment-id carrier route")
		}
		job := shipmentJob{ShipmentID: args[1], Carrier: args[2], Route: args[3]}
		messageID, err := infrai.queue.publish(ctx, publishRequest{
			Queue:   logisticsQueue,
			Payload: job,
		}, "shipment-"+job.ShipmentID)
		if err != nil {
			return err
		}
		fmt.Println("published logistics message:", messageID)
		return nil

	case "work":
		return runBatch(ctx, queueAPI{client: client}, dispatchShipment)

	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}
