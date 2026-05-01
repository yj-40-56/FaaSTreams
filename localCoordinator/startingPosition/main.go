package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

type SensorData struct {
	ShipID    string  `json:"ship_id"`
	Timestamp string  `json:"timestamp"`
	Lat       float64 `json:"lat"`
	Lon       float64 `json:"lon"`
}

func main() {
	ctx := context.Background()
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		panic(err)
	}

	for i := 1; i <= 10; i++ {
		shipID := fmt.Sprintf("%d", i)

		data := SensorData{
			ShipID:    shipID,
			Timestamp: time.Now().Format(time.RFC3339),
			Lat:       53.5 + (rand.Float64() * 2),
			Lon:       9.9 + (rand.Float64() * 2),
		}

		payload, _ := json.Marshal(data)

		resp, err := cli.ContainerCreate(ctx, client.ContainerCreateOptions{
			Config: &container.Config{
				Image: "faastreams_coordinator",
				Env: []string{
					"SHIP_ID=" + shipID,
					"PUBSUB_PAYLOAD=" + string(payload),
				},
			},
			HostConfig: &container.HostConfig{
				AutoRemove:  true,
				NetworkMode: "faastreams_net",
			},
		})
		if err != nil {
			fmt.Printf("Error while creating container %s: %v\n", shipID, err)
			continue
		}

		_, err = cli.ContainerStart(ctx, resp.ID, client.ContainerStartOptions{})
		if err != nil {
			fmt.Printf("Error while starting container %s: %v\n", shipID, err)
			continue
		}

		fmt.Printf("Schiff %s auf die Reise geschickt (ID: %s)\n", shipID, resp.ID[:8])
		time.Sleep(2 * time.Second)
	}
}

func pearls(v string) string { return v }
