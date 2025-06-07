package main

// Containerd agent to monitor Nextflow containers and send events to a Prometheus host.

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/events"
	"github.com/docker/docker/client"
	"github.com/sirupsen/logrus"
)

// Regex to match Nextflow container names.
var re = regexp.MustCompile(`^/nxf-[a-zA-Z0-9-]+$`)

const staticPort = "42"

type NextflowContainer struct {
	WorkerIP       string    `json:"node"`
	ContainerEvent string    `json:"event"`
	StartTime      time.Time `json:"start_time"`
	DieTime        time.Time `json:"die_time"`
	Name           string    `json:"name"`
	LifeTime       string    `json:"life_time"`
	PID            int       `json:"pid"`
	ContainerID    string    `json:"container_id"`
	WorkDir        string    `json:"work_dir"`
}

func (c *NextflowContainer) GetContainerEvents(prometheusHost string) {
	logrus.Info("Initializing Docker client to watch for container events...")

	// Container Client.
	apiClient, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		logrus.Fatalf("Failed to create Docker client: %v", err)
	}
	defer apiClient.Close()

	logrus.Info("Docker client initialized successfully. Listening for container events...")

	eventChan, errChan := apiClient.Events(context.Background(), events.ListOptions{})

	processedStarts := make(map[string]bool) // Track started containers
	processedDies := make(map[string]bool)   // Track died containers
	containerPIDs := make(map[string]int)    // Track container PIDs
	var mu sync.Mutex

	var conn net.Conn

	go func() {
		for {
			select {
			case event := <-eventChan:
				if event.Type == events.ContainerEventType {

					// Attempt to reopen the connection if it is nil
					if conn == nil {
						conn, err = openTCPConnection(prometheusHost)
						if err != nil {
							logrus.Errorf("Failed to open TCP connection: %v", err)
							continue
						}
					}
					switch event.Action {
					case "start":
						logrus.Infof("Processing 'start' event for container ID: %s", event.Actor.ID)
						processContainerEvent(conn, event, apiClient, re, &mu, processedStarts, containerPIDs, true, prometheusHost)
					case "die":
						logrus.Infof("Processing 'die' event for container ID: %s", event.Actor.ID)
						processContainerEvent(conn, event, apiClient, re, &mu, processedDies, containerPIDs, false, prometheusHost)
					}
				}
			case err := <-errChan:
				logrus.Errorf("Error while watching for events: %v", err)
			}
		}
	}()
}

func openTCPConnection(prometheusHost string) (net.Conn, error) {
	logrus.Infof("Opening TCP connection to %s on static port %s...", prometheusHost, staticPort)
	address := fmt.Sprintf("%s:%s", prometheusHost, staticPort)

	// Use a Dialer to bind the client to a specific local port
	dialer := &net.Dialer{
		LocalAddr: &net.TCPAddr{
			IP:   net.IPv4zero,
			Port: 0,
		},
	}

	conn, err := dialer.Dial("tcp", address)
	if err != nil {
		logrus.Errorf("Failed to open TCP connection to %s: %v", address, err)
		return nil, err
	}
	logrus.Info("TCP connection established successfully.")
	return conn, nil
}

func processContainerEvent(con net.Conn, event events.Message, apiClient *client.Client, re *regexp.Regexp, mu *sync.Mutex, processed map[string]bool, containerPIDs map[string]int, isStartEvent bool, prometheusHost string) {
	mu.Lock()
	if processed[event.Actor.ID] {
		logrus.Warnf("Event for container ID %s already processed. Skipping...", event.Actor.ID)
		mu.Unlock()
		return
	}
	processed[event.Actor.ID] = true
	mu.Unlock()

	go func() {
		containerInfo, err := apiClient.ContainerInspect(context.Background(), event.Actor.ID)
		if err != nil {
			logrus.Printf("Error inspecting container %s: %v", event.Actor.ID, err)
			return
		}

		if len(containerInfo.Name) > 0 && re.MatchString(containerInfo.Name) {
			eventType := "[STARTED]"
			if !isStartEvent {
				eventType = "[DIED]"
			}
			logrus.Infof("%s nextflow container: %s\n", eventType, containerInfo.Name)
			pid := containerInfo.State.Pid
			if !isStartEvent {
				mu.Lock()
				pid = containerPIDs[event.Actor.ID]
				if pid == 0 {
					logrus.Error("Container PID is zero.")
				}
				mu.Unlock()
			}

			nextflowContainer := createNextflowContainer(containerInfo, pid, eventType)
			if nextflowContainer.PID == 0 {
				logrus.Error("Container PID is zero.")
			}

			if isStartEvent {
				mu.Lock()
				containerPIDs[event.Actor.ID] = pid
				mu.Unlock()
				WriteToSocket(con, nextflowContainer, prometheusHost)
			} else {
				mu.Lock()
				containerPIDs[event.Actor.ID] = pid
				mu.Unlock()
				WriteToSocket(con, nextflowContainer, prometheusHost)
			}

		} else {
			logrus.Warnf("Container name does not match Nextflow pattern. Skipping container ID: %s", event.Actor.ID)
		}
	}()
}

func WriteToSocket(con net.Conn, container NextflowContainer, prometheusHost string) {
	logrus.Info("Serializing container data to JSON...")
	jsonData, err := json.Marshal(container)
	if err != nil {
		logrus.Errorf("Error serializing container data: %v", err)
		return
	}

	logrus.Infof("Writing container data to socket for container ID: %s", container.ContainerID)
	_, err = con.Write(jsonData)
	if err != nil {
		// Reopen the connection using the correct host
		newConn, connErr := openTCPConnection(prometheusHost)
		if connErr != nil {
			logrus.Errorf("Failed to reopen connection: %v", connErr)
			return
		}
		// Retry writing to the new connection
		logrus.Infof("Retrying to write container data to socket for container ID: %s", container.ContainerID)
		_, err = newConn.Write(jsonData)
		if err != nil {
			logrus.Errorf("Error writing to reopened socket: %v", err)
			return
		}
	} else {
		logrus.Infof("Container data sent to socket successfully: %s", string(jsonData))
	}
}

func createNextflowContainer(containerInfo types.ContainerJSON, pid int, eventType string) NextflowContainer {
	return NextflowContainer{
		WorkerIP:       containerInfo.NetworkSettings.IPAddress,
		ContainerEvent: eventType,
		StartTime: func() time.Time {
			t, _ := time.Parse(time.RFC3339, containerInfo.State.StartedAt)
			return t
		}(),
		DieTime: func() time.Time {
			t, _ := time.Parse(time.RFC3339, containerInfo.State.FinishedAt)
			return t
		}(),
		Name: strings.TrimPrefix(containerInfo.Name, "/"),
		LifeTime: func() string {
			startTime, _ := time.Parse(time.RFC3339, containerInfo.State.StartedAt)
			dieTime, _ := time.Parse(time.RFC3339, containerInfo.State.FinishedAt)
			return dieTime.Sub(startTime).String()
		}(),
		PID:         pid,
		ContainerID: containerInfo.ID,
		WorkDir:     containerInfo.Config.WorkingDir,
	}
}

func EscapeContainerName(containerName string) string {
	// Remove the leading '/' if present
	containerName = strings.TrimPrefix(containerName, "/")
	// Escape remaining '/' characters
	return fmt.Sprintf("Cleaned Container Name for Query: %s", strings.ReplaceAll(containerName, "/", `\/`))
}
func main() {
	logrus.Info("Starting Monitoring Agent...")

	// Prometheus host address
	prometheusHost := "130.149.248.100"

	// Watch for container events
	logrus.Info("Initializing container event watcher...")
	workflowContainer := NextflowContainer{}
	workflowContainer.GetContainerEvents(prometheusHost)

	// Block the main function to keep the program running
	logrus.Info("Monitoring Agent is now running. Waiting for events...")
	select {}
}
