package main

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
					// logrus.Infof("Received container event: Action=%s, ID=%s", event.Action, event.Actor.ID)

					// Attempt to reopen the connection if it is nil
					if conn == nil {
						conn, err = openTCPConnection(prometheusHost)
						if err != nil {
							logrus.Errorf("Failed to open TCP connection: %v", err)
							continue
						}
					}

					if conn != nil {
						switch event.Action {
						case "start":
							logrus.Infof("Processing 'start' event for container ID: %s", event.Actor.ID)
							processContainerEvent(conn, event, apiClient, re, &mu, processedStarts, containerPIDs, true)
						case "die":
							logrus.Infof("Processing 'die' event for container ID: %s", event.Actor.ID)
							processContainerEvent(conn, event, apiClient, re, &mu, processedDies, containerPIDs, false)
						}
					}
				}
			case err := <-errChan:
				logrus.Errorf("Error while watching for events: %v", err)
			}
		}
	}()
}

func openTCPConnection(prometheusHost string) (net.Conn, error) {
	logrus.Infof("Opening TCP connection to %s...", prometheusHost)
	conn, err := net.Dial("tcp", prometheusHost)
	if err != nil {
		logrus.Errorf("Failed to connect to Prometheus host: %v", err)
		return nil, err
	}
	logrus.Info("TCP connection established successfully.")
	return conn, nil
}

func processContainerEvent(con net.Conn, event events.Message, apiClient *client.Client, re *regexp.Regexp, mu *sync.Mutex, processed map[string]bool, containerPIDs map[string]int, isStartEvent bool) {
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
			logrus.Errorf("Error inspecting container %s: %v", event.Actor.ID, err)
			return
		}

		if len(containerInfo.Name) > 0 && re.MatchString(containerInfo.Name) {
			eventType := "start"
			if !isStartEvent {
				eventType = "die"
			}
			logrus.Infof("[%s] nextflow container: %s", strings.ToUpper(eventType), containerInfo.Name)

			pid := containerInfo.State.Pid
			if !isStartEvent {
				mu.Lock()
				pid = containerPIDs[event.Actor.ID]
				mu.Unlock()
			}

			nextflowContainer := createNextflowContainer(containerInfo, pid, eventType)

			// if !isStartEvent { // Only send data for `die` events
			logrus.Infof("Sending container data to server for container ID: %s", event.Actor.ID)
			WriteToSocket(con, nextflowContainer)
			// }
		} else {
			logrus.Warnf("Container name does not match Nextflow pattern. Skipping container ID: %s", event.Actor.ID)
		}
	}()
}

func WriteToSocket(con net.Conn, container NextflowContainer) {
	logrus.Info("Serializing container data to JSON...")
	jsonData, err := json.Marshal(container)
	if err != nil {
		logrus.Errorf("Error serializing container data: %v", err)
		return
	}

	logrus.Infof("Writing container data to socket for container ID: %s", container.ContainerID)
	_, err = con.Write(jsonData)
	if err != nil {
		logrus.Errorf("Error writing to socket: %v", err)
		logrus.Info("Attempting to reopen the connection...")

		// Reopen the connection
		newConn, connErr := openTCPConnection(con.RemoteAddr().String())
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

		logrus.Infof("Container data sent to socket successfully after reopening: %s", string(jsonData))
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

// func manageConnectionTimer(conn *net.Conn, timeout time.Duration) chan struct{} {
// 	resetChan := make(chan struct{})
// 	timer := time.NewTimer(timeout)

// 	go func() {
// 		for {
// 			select {
// 			case <-timer.C:
// 				// Timer expired, close the connection
// 				if *conn != nil {
// 					logrus.Info("Closing TCP connection due to inactivity...")
// 					(*conn).Close()
// 					*conn = nil
// 				}
// 			case <-resetChan:
// 				// Reset the timer
// 				if !timer.Stop() {
// 					<-timer.C
// 				}
// 				timer.Reset(timeout)
// 			}
// 		}
// 	}()

// 	return resetChan
// }

func EscapeContainerName(containerName string) string {
	// Remove the leading '/' if present
	containerName = strings.TrimPrefix(containerName, "/")
	// Escape remaining '/' characters
	return fmt.Sprintf("Cleaned Container Name for Query: %s", strings.ReplaceAll(containerName, "/", `\/`))
}
func main() {
	logrus.Info("Starting Monitoring Agent...")

	// Prometheus host address
	// prometheusHost := "130.149.248.100:42"
	prometheusHost := "127.0.0.1:42"

	// Watch for container events
	logrus.Info("Initializing container event watcher...")
	workflowContainer := NextflowContainer{}
	workflowContainer.GetContainerEvents(prometheusHost)

	// Block the main function to keep the program running
	logrus.Info("Monitoring Agent is now running. Waiting for events...")
	select {}
}
