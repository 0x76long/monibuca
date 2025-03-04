#!/usr/bin/env node

/**
 * Cluster 插件 Etcd 功能测试运行器
 * 此脚本用于测试 Cluster 插件的 Etcd 集成功能
 */

const path = require('path');
const { spawn } = require('child_process');
const CDP = require('chrome-remote-interface');
const fetch = require('node-fetch');
const { execSync } = require('child_process');

// 测试配置
const TEST_PORT = 9222;
const HTTP_PORTS = {
  node1: 8080,
  node2: 8081,
  node3: 8082
};
const CONFIG_DIR = path.join(__dirname, '.');
const CONFIG_FILES = {
  node1: path.join(CONFIG_DIR, 'etcd-node1.yaml'),
  node2: path.join(CONFIG_DIR, 'etcd-node2.yaml'),
  node3: path.join(CONFIG_DIR, 'etcd-node3.yaml')
};

// 启动服务器进程
const servers = {};

// 一个简单的延迟函数
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

// 启动 Cluster 服务器
async function startServers() {
  console.log('启动 Cluster 服务器 (Etcd 模式)...');

  // 设置环境变量
  const env = {
    ...process.env,
    GODEBUG: 'protobuf=2',  // 启用详细的 protobuf 调试信息
    GO_TESTMODE: '1'        // 启用测试模式
  };

  // 首先启动管理节点
  servers.node1 = spawn('go', ['run', '-tags', 'sqlite,dummy', 'etcd-main.go', '-c', CONFIG_FILES.node1], {
    cwd: path.join(__dirname, '.'),
    stdio: ['ignore', 'pipe', 'pipe'],  // 只保留 stdout 和 stderr
    env
  });

  // 处理输出
  servers.node1.stdout.on('data', (data) => {
    console.log(`[Node1] ${data.toString().trim()}`);
  });

  servers.node1.stderr.on('data', (data) => {
    console.error(`[Node1 Error] ${data.toString().trim()}`);
  });

  // 等待管理节点和 etcd 启动
  console.log('等待管理节点和内嵌 etcd 启动...');
  await delay(10000);

  // 启动工作节点
  servers.node2 = spawn('go', ['run', '-tags', 'sqlite,dummy', 'etcd-main.go', '-c', CONFIG_FILES.node2], {
    cwd: path.join(__dirname, '.'),
    stdio: ['ignore', 'pipe', 'pipe'],
    env
  });

  // 处理输出
  servers.node2.stdout.on('data', (data) => {
    console.log(`[Node2] ${data.toString().trim()}`);
  });

  servers.node2.stderr.on('data', (data) => {
    console.error(`[Node2 Error] ${data.toString().trim()}`);
  });

  // 等待工作节点2启动
  await delay(5000);

  servers.node3 = spawn('go', ['run', '-tags', 'sqlite,dummy', 'etcd-main.go', '-c', CONFIG_FILES.node3], {
    cwd: path.join(__dirname, '.'),
    stdio: ['ignore', 'pipe', 'pipe'],
    env
  });

  // 处理输出
  servers.node3.stdout.on('data', (data) => {
    console.log(`[Node3] ${data.toString().trim()}`);
  });

  servers.node3.stderr.on('data', (data) => {
    console.error(`[Node3 Error] ${data.toString().trim()}`);
  });

  // 等待所有节点启动
  console.log('等待所有节点启动和同步...');
  await delay(8000);
  console.log('所有 Cluster 服务器已启动');
}

// 连接到 CDP
async function connectToCDP() {
  try {
    console.log(`连接到 Chrome DevTools Protocol (端口 ${TEST_PORT})...`);

    // 等待Chrome启动并开始接收连接
    let retries = 10;
    while (retries > 0) {
      try {
        // 尝试获取可用的调试目标
        const targets = await CDP.List({ port: TEST_PORT });
        if (targets && targets.length > 0) {
          console.log(`找到 ${targets.length} 个可调试目标`);
          break;
        }
        console.log('没有找到可调试目标，等待Chrome启动...');
      } catch (e) {
        console.log(`等待Chrome启动 (${retries} 次尝试剩余): ${e.message}`);
      }

      await delay(1000);
      retries--;

      if (retries === 0) {
        console.log('无法找到可调试目标，尝试打开一个新页面...');
        // 打开一个新标签页
        try {
          execSync(`open -a "Google Chrome" http://localhost:${HTTP_PORTS.node1}/`);
          await delay(2000);
        } catch (e) {
          console.error('打开新页面失败:', e);
        }
      }
    }

    const client = await CDP({ port: TEST_PORT });
    const { Network, Page, Runtime } = client;

    await Promise.all([
      Network.enable(),
      Page.enable()
    ]);

    console.log('CDP 连接成功');
    return { client, Network, Page, Runtime };
  } catch (err) {
    console.error('无法连接到 Chrome:', err);
    throw err;
  }
}

// 测试集群状态
async function testClusterStatus() {
  try {
    console.log('\n测试: 集群状态');
    const response = await fetch(`http://localhost:${HTTP_PORTS.node1}/api/cluster/cluster/status`);
    const text = await response.text();
    console.log('原始响应:', text);

    try {
      const status = JSON.parse(text);
      console.log('集群状态:', JSON.stringify(status, null, 2));

      // 检查基本状态字段
      if (status.status.totalNodes === 3) {
        console.log('✅ 测试通过: 集群包含所有预期的节点');
      } else {
        console.error(`❌ 测试失败: 集群应该包含 3 个节点，但实际有 ${status.status.totalNodes} 个`);
      }

      // 检查健康节点数量
      if (status.status.healthyNodes === 3) {
        console.log('✅ 测试通过: 所有节点都处于健康状态');
      } else {
        console.error(`❌ 测试失败: 健康节点数量不正确，期望 3，实际 ${status.status.healthyNodes}`);
      }

      // 检查集群状态
      if (status.status.clusterState === "normal") {
        console.log('✅ 测试通过: 集群状态正常');
      } else {
        console.error(`❌ 测试失败: 集群状态异常，期望 "normal"，实际 "${status.status.clusterState}"`);
      }

      // 获取节点列表进行详细检查
      const nodesResponse = await fetch(`http://localhost:${HTTP_PORTS.node1}/api/cluster/nodes`);
      const nodesData = await nodesResponse.json();

      if (nodesData.nodes && nodesData.nodes.length === 3) {
        console.log('✅ 测试通过: 节点列表包含所有预期的节点');

        // 检查是否包含所有预期的节点ID
        const nodeIds = nodesData.nodes.map(node => node.id);
        const expectedNodeIds = ['etcd-node1', 'etcd-node2', 'etcd-node3'];
        const allNodesPresent = expectedNodeIds.every(id => nodeIds.includes(id));

        if (allNodesPresent) {
          console.log('✅ 测试通过: 找到所有预期的节点 ID');
        } else {
          console.error('❌ 测试失败: 缺少一个或多个预期的节点');
        }
      } else {
        console.error(`❌ 测试失败: 节点列表数量不正确，期望 3，实际 ${nodesData.nodes ? nodesData.nodes.length : 0}`);
      }

    } catch (parseErr) {
      console.error('解析响应失败:', parseErr);
      return false;
    }

    return true;
  } catch (err) {
    console.error('集群状态测试出错:', err);
    return false;
  }
}

// 测试节点故障和自动恢复
async function testNodeFailureRecovery() {
  try {
    console.log('\n测试: 节点故障和自动恢复');

    // 获取初始状态
    console.log('获取初始集群状态...');
    const initialResponse = await fetch(`http://localhost:${HTTP_PORTS.node1}/api/cluster/nodes`);
    const initialNodes = await initialResponse.json();

    // 检查节点2的初始状态
    const node2 = initialNodes.nodes.find(node => node.id === 'etcd-node2');
    if (node2 && node2.status === 'healthy') {
      console.log('✅ 确认 etcd-node2 初始状态为健康');
    } else {
      console.error('❌ 测试失败: etcd-node2 初始状态不是健康');
      return false;
    }

    // 关闭节点2模拟故障
    console.log('关闭 etcd-node2 模拟故障...');
    if (servers.node2) {
      servers.node2.kill();
      console.log('etcd-node2 已关闭');
    }

    // 等待故障检测（略长于故障检测阈值）
    console.log('等待故障检测...');
    await delay(10000);

    // 检查节点2是否被标记为离线
    const failureResponse = await fetch(`http://localhost:${HTTP_PORTS.node1}/api/cluster/nodes`);
    const failureNodes = await failureResponse.json();
    const failedNode2 = failureNodes.nodes.find(node => node.id === 'etcd-node2');

    if (failedNode2 && failedNode2.status === 'offline') {
      console.log('✅ 测试通过: etcd-node2 被正确标记为离线');
    } else {
      console.error('❌ 测试失败: etcd-node2 没有被标记为离线');
      return false;
    }

    // 重启节点2
    console.log('重启 etcd-node2...');
    servers.node2 = spawn('go', ['run', '-tags', 'sqlite,dummy', 'etcd-main.go', '-c', CONFIG_FILES.node2], {
      cwd: path.join(__dirname, '.'),
      stdio: ['ignore', 'pipe', 'pipe'],
      env
    });

    // 等待节点恢复
    console.log('等待节点恢复...');
    await delay(15000);

    // 检查节点2是否重新上线
    const recoveryResponse = await fetch(`http://localhost:${HTTP_PORTS.node1}/api/cluster/nodes`);
    const recoveryNodes = await recoveryResponse.json();
    const recoveredNode2 = recoveryNodes.nodes.find(node => node.id === 'etcd-node2');

    if (recoveredNode2 && recoveredNode2.status === 'healthy') {
      console.log('✅ 测试通过: etcd-node2 成功恢复上线');
    } else {
      console.error('❌ 测试失败: etcd-node2 没有恢复上线');
      return false;
    }

    return true;

  } catch (err) {
    console.error('节点故障恢复测试出错:', err);
    return false;
  }
}

// 测试 etcd watcher
async function testEtcdWatcher() {
  try {
    console.log('\n测试: Etcd Watcher 功能');

    // 在节点1上注册一个流，然后检查节点3是否通过 watcher 收到更新
    const streamPath = 'test-stream-' + Date.now();
    const streamInfo = {
      streamPath: streamPath,
      publisherNodeID: 'etcd-node1',
      startTime: new Date().toISOString(),
      lastUpdated: new Date().toISOString(),
      bitrateMbps: 1.0
    };

    // 在节点1上注册流
    const registerResponse = await fetch(`http://localhost:${HTTP_PORTS.node1}/api/cluster/streams`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(streamInfo)
    });

    const registerResult = await registerResponse.json();
    if (!registerResult.success) {
      console.error('❌ 测试失败: 无法注册流');
      return false;
    }

    // 等待流信息同步和 watcher 触发
    console.log('等待 watcher 触发...');
    await delay(3000);

    // 从节点3获取流信息，验证是否与注册的相同
    const getResponse = await fetch(`http://localhost:${HTTP_PORTS.node3}/api/cluster/streams/${streamPath}`);
    const getResult = await getResponse.json();

    if (getResult.success && getResult.streamInfo && getResult.streamInfo.streamPath === streamPath) {
      console.log('✅ 测试通过: 节点3成功通过 watcher 更新流信息');
    } else {
      console.error(`❌ 测试失败: 节点3的流信息不匹配`);
      return false;
    }

    // 清理测试数据
    const unregisterResponse = await fetch(`http://localhost:${HTTP_PORTS.node1}/api/cluster/streams/${streamPath}`, {
      method: 'DELETE'
    });

    const unregisterResult = await unregisterResponse.json();
    if (!unregisterResult.success) {
      console.error('❌ 测试失败: 无法清理测试数据');
      return false;
    }

    return true;
  } catch (err) {
    console.error('Etcd Watcher 测试出错:', err);
    return false;
  }
}

// 清理函数
function cleanup() {
  console.log('清理资源...');

  // 终止所有服务器进程
  Object.values(servers).forEach(server => {
    if (server && !server.killed) {
      try {
        // 发送 SIGTERM 信号
        server.kill('SIGTERM');

        // 如果进程还在运行，强制结束
        if (!server.killed) {
          server.kill('SIGKILL');
        }
      } catch (err) {
        console.error(`终止服务器进程失败:`, err);
      }
    }
  });

  // 使用 pkill 确保所有相关进程都被终止
  try {
    execSync('pkill -f "etcd-main"');
  } catch (err) {
    // 忽略错误，因为可能没有进程需要终止
  }

  console.log('所有服务器已停止');
}

// 确保在程序退出时清理
process.on('exit', cleanup);
process.on('SIGTERM', () => {
  console.log('\n接收到 SIGTERM 信号，正在清理...');
  cleanup();
  process.exit(0);
});

// 处理进程终止信号
process.on('SIGINT', () => {
  console.log('\n接收到 SIGINT 信号，正在清理...');
  cleanup();
  process.exit(0);
});

// 主函数
async function main() {
  try {
    // 尝试启动服务器
    await startServers();

    // 连接到CDP（由用户预先打开的Chrome浏览器）
    const cdp = await connectToCDP();
    console.log('已成功连接到Chrome浏览器');

    // 运行各种测试
    const tests = [
      { name: "集群状态", fn: testClusterStatus },
      { name: "节点故障和自动恢复", fn: testNodeFailureRecovery },
      { name: "Etcd Watcher 功能", fn: testEtcdWatcher }
    ];

    let passedCount = 0;
    let failedCount = 0;

    for (const test of tests) {
      console.log(`\n====== 执行测试: ${test.name} ======`);
      const passed = await test.fn();
      if (passed) {
        passedCount++;
      } else {
        failedCount++;
      }
    }

    // 输出测试结果摘要
    console.log("\n====== 测试结果摘要 ======");
    console.log(`通过: ${passedCount}`);
    console.log(`失败: ${failedCount}`);
    console.log(`总共: ${tests.length}`);

    if (failedCount === 0) {
      console.log("\n✅ 所有测试通过!");
    } else {
      console.log("\n❌ 有测试失败!");
    }

    // 关闭CDP连接
    if (cdp && cdp.client) {
      await cdp.client.close();
      console.log('已关闭CDP连接');
    }

  } catch (err) {
    console.error('测试过程中出错:', err);
  } finally {
    // 清理资源
    cleanup();
  }
}

// 如果直接运行此脚本
if (require.main === module) {
  console.log('开始运行测试...');
  console.log('当前工作目录:', process.cwd());

  main().catch(err => {
    console.error('未处理的错误:', err);
    console.error('错误堆栈:', err.stack);
    cleanup();
    process.exit(1);
  });
} 