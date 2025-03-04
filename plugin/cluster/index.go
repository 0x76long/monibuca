package plugin_claster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"m7s.live/v5"
	"m7s.live/v5/pkg"
	"m7s.live/v5/pkg/task"
	"m7s.live/v5/plugin/cluster/pb"
)

var _ = m7s.InstallPlugin[ClusterPlugin](&pb.Api_ServiceDesc, pb.RegisterApiHandler)
var _ pb.ApiServer = (*ClusterPlugin)(nil)

// ClusterPlugin 集群插件
type ClusterPlugin struct {
	m7s.Plugin
	pb.UnimplementedApiServer
	NodeID              string
	Role                string
	Region              string
	SyncInterval        time.Duration `default:"5s"`
	HeartbeatInterval   time.Duration `default:"2s"`
	HealthCheckInterval time.Duration `default:"2s"`
	ClusterSecret       string
	AuthToken           string
	ManagerAddress      string

	OfflineThreshold time.Duration `default:"5s"`  // 离线判定阈值
	FailoverDelay    time.Duration `default:"10s"` // 故障转移延迟时间

	Resources     ResourceConfig
	LoadBalancing LoadBalancingConfig
	Monitoring    MonitoringConfig
	Sync          SyncConfig
	Etcd          EtcdConfig

	nodes         task.Manager[string, *NodeInfo]
	streams       task.Manager[string, *StreamInfo]
	nodeAgent     *NodeAgent
	loadBalancer  ILoadBalancer
	managerClient *ManagerClient

	// Components
	resourceOptimizer *ResourceOptimizer
	healthMonitor     *HealthMonitor
	stateMonitor      *ClusterStateMonitor

	// 集群状态
	ClusterState string
	TotalNodes   int
	HealthyNodes int
	RoleCounts   map[string]int
}

// OnInit 初始化插件
func (p *ClusterPlugin) OnInit() error {
	if p.Role == "" {
		p.Role = "manager"
	}
	if p.NodeID == "" {
		p.NodeID = generateNodeID(p)
	}
	p.Info("Initializing cluster plugin",
		"role", p.Role,
		"nodeId", p.NodeID,
		"etcdEnabled", p.Etcd.Enabled,
	)
	p.streams.L = &sync.RWMutex{}
	p.nodes.L = &sync.RWMutex{}
	// 初始化集群状态
	p.ClusterState = "normal"
	p.TotalNodes = 0
	p.HealthyNodes = 0
	p.RoleCounts = make(map[string]int)

	// 初始化组件
	p.loadBalancer = NewLoadBalancer(p)
	p.resourceOptimizer = NewResourceOptimizer(p)
	p.healthMonitor = NewHealthMonitor(p)
	p.stateMonitor = NewClusterStateMonitor(p)

	// 启动组件
	p.AddTask(p.resourceOptimizer)
	p.AddTask(p.healthMonitor)
	p.AddTask(p.stateMonitor)

	// 如果启用了 etcd
	if p.Etcd.Enabled {
		// 如果是 etcd 模式且是管理节点，启动 etcd 服务器
		if p.Role == "manager" {
			// 初始化 etcd 服务器
			server, err := NewEtcdServer(p)
			if err != nil {
				p.Error("Failed to create etcd server", "error", err)
				return fmt.Errorf("failed to create etcd server: %v", err)
			}

			err = p.AddTask(server).WaitStarted()
			if err != nil {
				p.Error("Failed to start etcd server", "error", err)
				return err
			}
		}

		// 初始化节点代理
		p.nodeAgent = NewNodeAgent(p)
		err := p.AddTask(p.nodeAgent).WaitStarted()
		if err != nil {
			p.Error("Failed to start node agent", "error", err)
			return err
		}
		p.Info("Node agent initialized successfully")

		// 创建 etcd 发现实例
		discovery, err := NewEtcdDiscovery(p)
		if err != nil {
			p.Error("Failed to create etcd discovery", "error", err)
			return fmt.Errorf("failed to create etcd discovery: %v", err)
		}
		err = p.AddTask(discovery).WaitStarted()
		if err != nil {
			p.Error("Failed to start etcd discovery", "error", err)
			return err
		}
		p.Info("Etcd discovery instance created")

		// 如果不是管理节点，创建并启动 manager client
		if p.Role != "manager" && p.ManagerAddress != "" {
			managerClient := NewManagerClient(p, p.ManagerAddress)
			err = p.AddTask(managerClient).WaitStarted()
			if err != nil {
				p.Error("Failed to start manager client", "error", err)
				return err
			}
			p.Info("Manager client initialized successfully")
		}
	}

	p.Info("Cluster plugin initialized successfully")
	return nil
}

// RegisterStreamInternal registers a new stream with the cluster
func (p *ClusterPlugin) RegisterStreamInternal(streamInfo *StreamInfo) error {
	p.Info("Registering stream", "streamPath", streamInfo.StreamPath)

	// Check if the stream already exists
	existingStream, exists := p.streams.Get(streamInfo.StreamPath)
	if exists {
		// Check for conflicting publisher nodes
		if existingStream.PublisherNodeID != streamInfo.PublisherNodeID {
			p.Warn("Stream already published by different node",
				"streamPath", streamInfo.StreamPath,
				"existingPublisher", existingStream.PublisherNodeID)

			// Check vector clocks to resolve conflict
			if p.IsNewerStreamVersion(streamInfo, existingStream) {
				p.Info("New publisher has newer version, updating stream info",
					"streamPath", streamInfo.StreamPath)
			} else {
				// Reject the new publisher if the existing one has precedence
				errMsg := fmt.Sprintf("Stream %s already published by node %s with higher precedence",
					streamInfo.StreamPath, existingStream.PublisherNodeID)
				p.Error("Stream registration rejected",
					"streamPath", streamInfo.StreamPath,
					"existingPublisher", existingStream.PublisherNodeID,
					"reason", errMsg)
				return fmt.Errorf(errMsg)
			}
		}

		// Update stream information
		streamInfo.LastUpdated = time.Now()
		// Preserve replicated nodes list if not provided in the new info
		if len(streamInfo.ReplicatedTo) == 0 && len(existingStream.ReplicatedTo) > 0 {
			streamInfo.ReplicatedTo = existingStream.ReplicatedTo
		}
	} else {
		// Initialize new stream
		streamInfo.LastUpdated = time.Now()
		streamInfo.State = "active"
		if streamInfo.VectorClock == nil {
			streamInfo.VectorClock = make(map[string]uint64)
		}
		streamInfo.VectorClock[streamInfo.PublisherNodeID] = 1
	}

	// Store the stream
	p.streams.Add(streamInfo)

	// Notify components
	p.loadBalancer.OnStreamAdded(streamInfo)
	p.resourceOptimizer.OnStreamAdded(streamInfo)

	// Update node's stream list
	nodeInfo, nodeExists := p.nodes.Get(streamInfo.PublisherNodeID)
	if nodeExists {
		nodeInfo.Streams[streamInfo.StreamPath] = *streamInfo
		nodeInfo.StreamCount = len(nodeInfo.Streams)
	}

	p.Info("Stream registered successfully", "streamPath", streamInfo.StreamPath)
	return nil
}

// IsNewerStreamVersion compares vector clocks to determine if the new stream info is newer
func (p *ClusterPlugin) IsNewerStreamVersion(newStream, existingStream *StreamInfo) bool {
	// If the new stream has no vector clock, it can't be newer
	if len(newStream.VectorClock) == 0 {
		return false
	}

	// If the existing stream has no vector clock, the new one is newer
	if len(existingStream.VectorClock) == 0 {
		return true
	}

	// Check if any entry in the new clock is greater than in the existing clock
	hasGreater := false
	for node, count := range newStream.VectorClock {
		existingCount, exists := existingStream.VectorClock[node]
		if !exists || count > existingCount {
			hasGreater = true
		} else if count < existingCount {
			// If any entry is less, there's a conflict
			return false
		}
	}

	return hasGreater
}

// UnregisterStreamInternal removes a stream from the cluster
func (p *ClusterPlugin) UnregisterStreamInternal(streamPath string) error {
	p.Info("Unregistering stream", "streamPath", streamPath)

	// Get stream info
	streamInfo, ok := p.streams.Get(streamPath)
	if !ok {
		return fmt.Errorf("stream not found: %s", streamPath)
	}

	// Notify components
	p.loadBalancer.OnStreamRemoved(streamInfo)
	p.resourceOptimizer.OnStreamRemoved(streamInfo)

	// Remove from streams collection
	p.streams.RemoveByKey(streamPath)

	return nil
}

// UpdateNodeStatusInternal updates the status of a node
func (p *ClusterPlugin) UpdateNodeStatusInternal(nodeID string, status string) error {
	// Get node info
	nodeInfo, ok := p.nodes.Get(nodeID)
	if !ok {
		return fmt.Errorf("node not found: %s", nodeID)
	}

	// Update status
	oldStatus := nodeInfo.Status
	nodeInfo.Status = status

	// Log status change
	if oldStatus != status {
		p.Info("Node status changed", "nodeID", nodeID, "oldStatus", oldStatus, "newStatus", status)
	}

	return nil
}

// GetOptimalNodeForPublisher returns the optimal node for a new publisher
func (p *ClusterPlugin) GetOptimalNodeForPublisher(ctx context.Context, req *pb.GetOptimalNodeForPublisherRequest) (*pb.GetOptimalNodeForPublisherResponse, error) {
	node, err := p.loadBalancer.GetOptimalNodeForPublisher()
	if err != nil {
		return nil, err
	}
	return &pb.GetOptimalNodeForPublisherResponse{
		Success: true,
		Message: "success",
		OptimalNode: &pb.NodeInfo{
			Id:            node.ID,
			Role:          node.Role,
			Region:        node.Region,
			Status:        node.Status,
			LastHeartbeat: node.LastHeartbeat.UnixMilli(),
			Capacity:      convertInternalResourceCapacityToProto(&node.Capacity),
			StreamCount:   int32(node.StreamCount),
		},
		FallbackNodes: nil,
	}, nil
}

// GetOptimalNodeForSubscriber returns the optimal node for a new subscriber
func (p *ClusterPlugin) GetOptimalNodeForSubscriber(ctx context.Context, req *pb.GetOptimalNodeForSubscriberRequest) (*pb.GetOptimalNodeForSubscriberResponse, error) {
	node, err := p.loadBalancer.GetOptimalNodeForSubscriber(req.StreamPath)
	if err != nil {
		return nil, err
	}
	return &pb.GetOptimalNodeForSubscriberResponse{
		Success: true,
		Message: "success",
		OptimalNode: &pb.NodeInfo{
			Id:            node.ID,
			Role:          node.Role,
			Region:        node.Region,
			Status:        node.Status,
			LastHeartbeat: node.LastHeartbeat.UnixMilli(),
			Capacity:      convertInternalResourceCapacityToProto(&node.Capacity),
			StreamCount:   int32(node.StreamCount),
		},
	}, nil
}

// OnPublish handles stream publish events
func (p *ClusterPlugin) OnPublish(pub *m7s.Publisher) {
	// Get stream info from publisher
	streamInfo := &StreamInfo{
		StreamPath:      pub.StreamPath,
		PublisherNodeID: p.NodeID,
		ReplicatedTo:    make([]string, 0),
		SubscriberCount: pub.Subscribers.Length,
		State:           "active",
		MediaInfo: MediaInfo{
			VideoCodec:    pub.VideoTrack.AVTrack.FourCC().String(),
			AudioCodec:    pub.AudioTrack.AVTrack.FourCC().String(),
			Resolution:    fmt.Sprintf("%dx%d", pub.VideoTrack.AVTrack.ICodecCtx.(pkg.IVideoCodecCtx).Width(), pub.VideoTrack.AVTrack.ICodecCtx.(pkg.IVideoCodecCtx).Height()),
			Framerate:     float64(pub.VideoTrack.AVTrack.FPS),
			VideoWidth:    pub.VideoTrack.AVTrack.ICodecCtx.(pkg.IVideoCodecCtx).Width(),
			VideoHeight:   pub.VideoTrack.AVTrack.ICodecCtx.(pkg.IVideoCodecCtx).Height(),
			VideoEnabled:  pub.VideoTrack.AVTrack != nil,
			AudioEnabled:  pub.AudioTrack.AVTrack != nil,
			StartTime:     pub.StartTime.Unix(),
			BandwidthMbps: float64(pub.VideoTrack.AVTrack.BPS+pub.AudioTrack.AVTrack.BPS) / 1000000,
		},
		ClientInfo: ClientInfo{
			ClientID:    fmt.Sprintf("%d", pub.ID),
			ClientIP:    pub.RemoteAddr,
			ConnectTime: pub.StartTime,
			UserAgent:   string(pub.Args.Get("User-Agent")[0]),
			Metadata:    make(map[string]string),
		},
		CreationTime: time.Now(),
		LastUpdated:  time.Now(),
		VectorClock:  make(map[string]uint64),
		Tags:         make(map[string]string),
		StartTime:    pub.StartTime,
	}

	// Convert url.Values to map[string]string
	for k, v := range pub.Args {
		streamInfo.ClientInfo.Metadata[k] = v[0]
		streamInfo.Tags[k] = v[0]
	}

	// Add to local streams map
	p.streams.Add(streamInfo)

	// Register stream with manager
	req := &pb.RegisterStreamRequest{
		StreamInfo: convertInternalStreamInfoToProto(streamInfo),
		AuthToken:  p.AuthToken,
	}

	// Send to manager
	if p.managerClient != nil {
		resp, err := p.managerClient.RegisterStream(context.Background(), req)
		if err != nil {
			p.Error("Failed to register stream with manager", "error", err)
			return
		}
		if !resp.Success {
			p.Error("Manager rejected stream registration", "message", resp.Message)
			return
		}
		if resp.ConflictDetected {
			p.Info("Stream registration conflict detected, using resolved stream info")
			// Update local stream info with resolved version
			streamInfo = convertProtoStreamInfoToInternal(resp.ResolvedStream)
			p.streams.Add(streamInfo)
			// Set up dispose handler
			pub.OnDispose(func() {
				p.streams.Remove(streamInfo)
				// Unregister stream with manager
				req := &pb.UnregisterStreamRequest{
					StreamPath: streamInfo.StreamPath,
					NodeId:     p.NodeID,
					AuthToken:  p.AuthToken,
				}
				resp, err := p.managerClient.UnregisterStream(context.Background(), req)
				if err != nil {
					p.Error("Failed to unregister stream with manager", "error", err)
					return
				}
				if !resp.Success {
					p.Error("Manager rejected stream unregistration", "message", resp.Message)
					return
				}
			})
		}
	}
}
