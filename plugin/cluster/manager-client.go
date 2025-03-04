package plugin_claster

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"m7s.live/v5/pkg/task"
	"m7s.live/v5/plugin/cluster/pb"
)

// ManagerClient handles communication with the manager node
type ManagerClient struct {
	task.TickTask
	plugin     *ClusterPlugin
	conn       *grpc.ClientConn
	client     pb.ApiClient
	managerURL string
}

// NewManagerClient creates a new manager client
func NewManagerClient(plugin *ClusterPlugin, managerURL string) *ManagerClient {
	return &ManagerClient{
		plugin:     plugin,
		managerURL: managerURL,
	}
}

// Start initializes the manager client
func (mc *ManagerClient) Start() error {
	mc.Info("Starting manager client", "managerURL", mc.managerURL)
	conn, err := grpc.NewClient(mc.managerURL, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to manager: %v", err)
	}

	mc.conn = conn
	mc.client = pb.NewApiClient(conn)
	mc.Info("Successfully connected to manager")

	return mc.TickTask.Start()
}

// GetTickInterval returns the heartbeat interval
func (mc *ManagerClient) GetTickInterval() time.Duration {
	return mc.plugin.HeartbeatInterval
}

// Tick sends heartbeat to manager
func (mc *ManagerClient) Tick(any) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Prepare active streams list
	var activeStreams []string
	mc.plugin.streams.Range(func(stream *StreamInfo) bool {
		if stream.PublisherNodeID == mc.plugin.NodeID {
			activeStreams = append(activeStreams, stream.StreamPath)
		}
		return true
	})

	// Send heartbeat
	req := &pb.HeartbeatRequest{
		NodeId:        mc.plugin.NodeID,
		Timestamp:     time.Now().UnixMilli(),
		ActiveStreams: activeStreams,
		ResourceUsage: &pb.ResourceUsage{
			CpuPercent:             mc.plugin.nodeAgent.nodeInfo.CurrentLoad.CPUPercent,
			MemoryGb:               mc.plugin.nodeAgent.nodeInfo.CurrentLoad.MemoryGB,
			BandwidthMbps:          float64(mc.plugin.nodeAgent.nodeInfo.CurrentLoad.BandwidthMbps),
			ConcurrentStreams:      int32(mc.plugin.nodeAgent.nodeInfo.CurrentLoad.ConcurrentStreams),
			ActiveTranscodingTasks: int32(mc.plugin.nodeAgent.nodeInfo.CurrentLoad.TranscodingLoad),
		},
		LatencyMs: mc.plugin.nodeAgent.nodeInfo.CurrentLoad.NetworkLatencyMs,
	}

	resp, err := mc.client.SendHeartbeat(ctx, req)
	if err != nil {
		mc.Error("Failed to send heartbeat", "error", err)
		return
	}

	// Handle any required actions from manager
	if len(resp.ActionRequired) > 0 {
		mc.handleManagerActions(resp.ActionRequired)
	}

	// Update local time based on server time
	if resp.ServerTime > 0 {
		// TODO: Implement time synchronization if needed
	}

	// Handle node updates
	if len(resp.NodeUpdates) > 0 {
		mc.handleNodeUpdates(resp.NodeUpdates)
	}
}

// handleManagerActions processes actions required by the manager
func (mc *ManagerClient) handleManagerActions(actions []string) {
	for _, action := range actions {
		switch action {
		case "sync_streams":
			// TODO: Implement stream synchronization
			mc.Info("Stream sync requested by manager")
		default:
			mc.Warn("Unknown action required by manager", "action", action)
		}
	}
}

// handleNodeUpdates processes updates about other nodes in the cluster
func (mc *ManagerClient) handleNodeUpdates(updates []*pb.NodeUpdate) {
	for _, update := range updates {
		// Skip updates about our own node
		if update.NodeId == mc.plugin.NodeID {
			continue
		}

		// Update node info in the plugin's node map
		if node, exists := mc.plugin.nodes.Get(update.NodeId); exists {
			nodeInfo := node
			if update.ResourceUsage != nil {
				nodeInfo.CurrentLoad = ResourceUsage{
					CPUPercent:        update.ResourceUsage.CpuPercent,
					MemoryGB:          update.ResourceUsage.MemoryGb,
					BandwidthMbps:     int(update.ResourceUsage.BandwidthMbps),
					ConcurrentStreams: int(update.ResourceUsage.ConcurrentStreams),
					TranscodingLoad:   int(update.ResourceUsage.ActiveTranscodingTasks),
					NetworkLatencyMs:  update.LatencyMs,
				}
			}
			nodeInfo.StreamCount = int(update.StreamCount)
			nodeInfo.LastHeartbeat = time.Now()
		}
	}
}

// RegisterStream registers a stream with the manager
func (mc *ManagerClient) RegisterStream(ctx context.Context, req *pb.RegisterStreamRequest) (*pb.RegisterStreamResponse, error) {
	return mc.client.RegisterStream(ctx, req)
}

// UnregisterStream unregisters a stream from the manager
func (mc *ManagerClient) UnregisterStream(ctx context.Context, req *pb.UnregisterStreamRequest) (*pb.UnregisterStreamResponse, error) {
	return mc.client.UnregisterStream(ctx, req)
}

// Dispose cleans up resources
func (mc *ManagerClient) Dispose() {
	if mc.conn != nil {
		// Close connection
		mc.conn.Close()
	}
}
