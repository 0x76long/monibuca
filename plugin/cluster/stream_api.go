package plugin_claster

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"m7s.live/v5/plugin/cluster/pb"
)

// RegisterStreamAPI 实现流注册接口
func (p *ClusterPlugin) RegisterStreamAPI(ctx context.Context, req *pb.RegisterStreamRequest) (*pb.RegisterStreamResponse, error) {
	// 验证授权令牌
	if req.AuthToken != "" && req.AuthToken != p.AuthToken {
		return &pb.RegisterStreamResponse{
			Success:          false,
			Message:          "授权失败：无效的令牌",
			ConflictDetected: false,
		}, nil
	}

	if req.StreamInfo == nil {
		return &pb.RegisterStreamResponse{
			Success:          false,
			Message:          "无效的请求：缺少流信息",
			ConflictDetected: false,
		}, nil
	}

	// 将 proto StreamInfo 转换为内部 StreamInfo
	streamInfo := convertProtoStreamInfoToInternal(req.StreamInfo)
	if streamInfo == nil {
		return &pb.RegisterStreamResponse{
			Success:          false,
			Message:          "无效的请求：转换流信息失败",
			ConflictDetected: false,
		}, nil
	}

	// 如果配置为管理器模式，则使用集群管理器注册流
	if p.Role == "manager" {
		err := p.RegisterStreamInternal(streamInfo)
		if err != nil {
			return &pb.RegisterStreamResponse{
				Success:          false,
				Message:          "注册流失败：" + err.Error(),
				ConflictDetected: strings.Contains(err.Error(), "already published"),
			}, nil
		}
	} else {
		// 直接存储流信息
		p.streams.Add(streamInfo)
	}

	return &pb.RegisterStreamResponse{
		Success:          true,
		Message:          "流注册成功",
		ConflictDetected: false,
		ResolvedStream:   req.StreamInfo, // 返回原始流信息
	}, nil
}

// UnregisterStream 实现流注销接口
func (p *ClusterPlugin) UnregisterStream(ctx context.Context, req *pb.UnregisterStreamRequest) (*pb.UnregisterStreamResponse, error) {
	// 验证授权令牌
	if req.AuthToken != p.AuthToken {
		return &pb.UnregisterStreamResponse{
			Success: false,
			Message: "授权失败：无效的令牌",
		}, nil
	}

	// 获取流信息
	streamInfo, exists := p.streams.Get(req.StreamPath)
	if !exists {
		return &pb.UnregisterStreamResponse{
			Success: false,
			Message: "流不存在: " + req.StreamPath,
		}, nil
	}

	// 如果指定了节点ID，检查是否匹配
	if req.NodeId != "" && streamInfo.PublisherNodeID != req.NodeId {
		return &pb.UnregisterStreamResponse{
			Success: false,
			Message: "流不属于该节点",
		}, nil
	}

	// 如果有集群管理器，通知流移除事件
	if p.Role == "manager" {
		err := p.UnregisterStreamInternal(req.StreamPath)
		if err != nil {
			return &pb.UnregisterStreamResponse{
				Success: false,
				Message: "流注销失败: " + err.Error(),
			}, nil
		}
	}

	// 如果有负载均衡器, 通知流移除事件
	if p.loadBalancer != nil {
		p.loadBalancer.OnStreamRemoved(streamInfo)
	}

	// 从流集合中移除
	p.streams.RemoveByKey(req.StreamPath)

	return &pb.UnregisterStreamResponse{
		Success: true,
		Message: "流注销成功",
	}, nil
}

// GetStreamInfo 实现获取流信息接口
func (p *ClusterPlugin) GetStreamInfo(ctx context.Context, req *pb.GetStreamInfoRequest) (*pb.GetStreamInfoResponse, error) {
	// 获取流信息
	streamInfo, exists := p.streams.Get(req.StreamPath)
	if !exists {
		return &pb.GetStreamInfoResponse{
			Success: false,
			Message: "流不存在: " + req.StreamPath,
		}, nil
	}

	// 转换为 proto StreamInfo
	protoStreamInfo := convertInternalStreamInfoToProto(streamInfo)

	return &pb.GetStreamInfoResponse{
		Success:    true,
		Message:    "获取流信息成功",
		StreamInfo: protoStreamInfo,
	}, nil
}

// GetClusterStreams 实现获取集群流列表接口
func (p *ClusterPlugin) GetClusterStreams(ctx context.Context, req *pb.GetClusterStreamsRequest) (*pb.GetClusterStreamsResponse, error) {
	// 准备响应
	response := &pb.GetClusterStreamsResponse{
		Success: true,
		Message: "获取集群流列表成功",
		Streams: make([]*pb.StreamInfo, 0),
	}

	// 筛选和分页参数
	nodeFilter := req.NodeFilter   // 节点ID过滤器
	stateFilter := req.StateFilter // 流状态过滤器
	pathPrefix := req.PathPrefix   // 路径前缀过滤器
	limit := int(req.Limit)        // 返回数量限制
	offset := int(req.Offset)      // 分页偏移

	// 如果限制为0，设置一个默认值
	if limit <= 0 {
		limit = 100 // 默认最多返回100个流
	}

	// 总数计数器
	totalCount := 0
	currentCount := 0
	matchedStreams := make([]*pb.StreamInfo, 0, limit)

	// 收集所有流信息
	p.streams.Range(func(stream *StreamInfo) bool {
		totalCount++

		// 应用过滤器
		if nodeFilter != "" && stream.PublisherNodeID != nodeFilter {
			return true // 继续遍历
		}

		if stateFilter != "" && stream.State != stateFilter {
			return true // 继续遍历
		}

		if pathPrefix != "" && !strings.HasPrefix(stream.StreamPath, pathPrefix) {
			return true // 继续遍历
		}

		// 应用分页
		if currentCount < offset {
			currentCount++
			return true // 继续遍历
		}

		// 检查是否达到限制
		if len(matchedStreams) >= limit {
			return false // 停止遍历
		}

		// 转换为 proto StreamInfo 并添加到响应中
		protoStream := convertInternalStreamInfoToProto(stream)
		if protoStream != nil {
			matchedStreams = append(matchedStreams, protoStream)
		}

		currentCount++
		return true // 继续遍历
	})

	// 填充响应
	response.Streams = matchedStreams
	response.TotalCount = int32(totalCount)

	return response, nil
}

// GetOptimalNodeForSubscriberAPI 实现获取订阅者最优节点接口
func (p *ClusterPlugin) GetOptimalNodeForSubscriberAPI(ctx context.Context, req *pb.GetOptimalNodeForSubscriberRequest) (*pb.GetOptimalNodeForSubscriberResponse, error) {
	// 如果没有负载均衡器，无法提供最优节点
	if p.loadBalancer == nil {
		return &pb.GetOptimalNodeForSubscriberResponse{
			Success: false,
			Message: "节点选择服务不可用",
		}, nil
	}

	// 获取最优节点
	optimalNode, err := p.loadBalancer.GetOptimalNodeForSubscriber(req.StreamPath)
	if err != nil {
		return &pb.GetOptimalNodeForSubscriberResponse{
			Success: false,
			Message: fmt.Sprintf("获取最优节点失败: %v", err),
		}, nil
	}

	// 转换为 proto NodeInfo
	protoOptimalNode := convertInternalNodeInfoToProto(optimalNode)

	// 创建响应
	response := &pb.GetOptimalNodeForSubscriberResponse{
		Success:     true,
		Message:     "获取订阅者最优节点成功",
		OptimalNode: protoOptimalNode,
	}

	// 获取所有节点评分
	nodeScores := p.loadBalancer.GetAllNodeScores()

	// 获取流信息
	streamInfo, exists := p.streams.Get(req.StreamPath)
	if !exists {
		return response, nil // 直接返回最优节点
	}

	// 添加备选节点（最多3个）
	var candidates []*NodeInfo
	for _, nodeID := range streamInfo.ReplicatedTo {
		if nodeID != optimalNode.ID {
			if node, exists := p.nodes.Get(nodeID); exists && node.Status == "healthy" && node.Online {
				candidates = append(candidates, node)
			}
		}
	}

	// 根据节点评分排序候选节点
	sort.Slice(candidates, func(i, j int) bool {
		scoreI := nodeScores[candidates[i].ID]
		scoreJ := nodeScores[candidates[j].ID]
		return scoreI > scoreJ
	})

	// 添加前3个候选节点作为备选节点
	for i := 0; i < len(candidates) && i < 3; i++ {
		protoNode := convertInternalNodeInfoToProto(candidates[i])
		if protoNode != nil {
			response.FallbackNodes = append(response.FallbackNodes, protoNode)
		}
	}

	return response, nil
}

// UpdateStreamStateAPI 实现更新流状态接口
func (p *ClusterPlugin) UpdateStreamStateAPI(ctx context.Context, req *pb.UpdateStreamStateRequest) (*pb.UpdateStreamStateResponse, error) {
	// 验证授权令牌
	if req.AuthToken != p.AuthToken {
		return &pb.UpdateStreamStateResponse{
			Success: false,
			Message: "授权失败：无效的令牌",
		}, nil
	}

	// 获取流信息
	streamInfo, exists := p.streams.Get(req.StreamPath)
	if !exists {
		return &pb.UpdateStreamStateResponse{
			Success: false,
			Message: "流不存在: " + req.StreamPath,
		}, nil
	}

	// 更新流状态
	streamInfo.State = req.State
	streamInfo.LastUpdated = time.Now()

	// 如果有集群管理器，通知流状态更新
	if p.Role == "manager" {
		// 更新流信息
		p.streams.Add(streamInfo)
	}

	return &pb.UpdateStreamStateResponse{
		Success: true,
		Message: "流状态更新成功",
	}, nil
}

// 辅助函数：将 proto StreamInfo 转换为内部 StreamInfo
func convertProtoStreamInfoToInternal(protoStream *pb.StreamInfo) *StreamInfo {
	if protoStream == nil {
		return nil
	}

	// 创建内部StreamInfo对象
	streamInfo := &StreamInfo{
		StreamPath:      protoStream.StreamPath,
		PublisherNodeID: protoStream.PublisherNodeId,
		ReplicatedTo:    protoStream.ReplicatedTo,
		SubscriberCount: int(protoStream.SubscriberCount),
		State:           protoStream.State,
		MediaInfo: MediaInfo{
			BandwidthMbps: protoStream.BandwidthMbps,
		},
		VectorClock:     protoStream.VectorClock,
		CreationTime:    time.Now(), // proto没有这个字段，设置为当前时间
		LastUpdated:     time.Now(), // proto没有这个字段，设置为当前时间
		Tags:            protoStream.Metadata,
	}

	// 媒体信息
	streamInfo.MediaInfo = MediaInfo{
		VideoCodec:    protoStream.Codec,
		AudioCodec:    "",
		Resolution:    protoStream.Resolution,
		Framerate:     protoStream.Fps,
		VideoWidth:    0, // 需要从 Resolution 解析
		VideoHeight:   0, // 需要从 Resolution 解析
		VideoEnabled:  true,
		AudioEnabled:  false,
		StartTime:     0,
		BandwidthMbps: protoStream.BandwidthMbps,
	}

	// 解析分辨率，假设格式为 "宽x高"，如 "1920x1080"
	parts := strings.Split(protoStream.Resolution, "x")
	if len(parts) == 2 {
		width, err := strconv.Atoi(parts[0])
		if err == nil {
			streamInfo.MediaInfo.VideoWidth = width
		}

		height, err := strconv.Atoi(parts[1])
		if err == nil {
			streamInfo.MediaInfo.VideoHeight = height
		}
	}

	// 客户端信息在 proto 中不存在
	streamInfo.ClientInfo = ClientInfo{
		ClientID:    "",
		ClientIP:    "",
		ConnectTime: time.Time{},
		UserAgent:   "",
		Metadata:    make(map[string]string),
	}

	return streamInfo
}

// 辅助函数：将内部 StreamInfo 转换为 proto StreamInfo
func convertInternalStreamInfoToProto(stream *StreamInfo) *pb.StreamInfo {
	if stream == nil {
		return nil
	}

	// 如果分辨率为空，则从宽高构建
	resolution := stream.MediaInfo.Resolution
	if resolution == "" && stream.MediaInfo.VideoWidth > 0 && stream.MediaInfo.VideoHeight > 0 {
		resolution = fmt.Sprintf("%dx%d", stream.MediaInfo.VideoWidth, stream.MediaInfo.VideoHeight)
	}

	return &pb.StreamInfo{
		StreamPath:      stream.StreamPath,
		PublisherNodeId: stream.PublisherNodeID,
		ReplicatedTo:    stream.ReplicatedTo,
		SubscriberCount: int32(stream.SubscriberCount),
		State:           stream.State,
		BandwidthMbps:   stream.MediaInfo.BandwidthMbps, // 使用BandwidthMbps字段
		Codec:           stream.MediaInfo.VideoCodec,
		Resolution:      resolution,
		Fps:             stream.MediaInfo.Framerate,
		VectorClock:     stream.VectorClock,
		Metadata:        stream.Tags,
	}
}
