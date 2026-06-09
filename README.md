# QUE Engine

**Q**uery **U**nderstanding & **E**xecution Engine — 通用查询编排中间件

QUE Engine 是 RAG 知识库系统的智能调度中枢，采用 **Agentic RAG** 与 **Plan-then-Execute** 范式，将用户原始问题转化为结构化检索计划并执行，最终汇总为 LLM-ready 上下文字符串。

---

## 目录

- [1. 架构总览](#1-架构总览)
- [2. 演进历程](#2-演进历程)
- [3. 项目结构](#3-项目结构)
- [4. 快速开始](#4-快速开始)
- [5. 部署指南](#5-部署指南)
  - [5.1 Docker 部署](#51-docker-部署)
  - [5.2 裸机部署](#52-裸机部署)
  - [5.3 Kubernetes 部署](#53-kubernetes-部署)
- [6. 接入指南](#6-接入指南)
  - [6.1 Python SDK（进程内调用）](#61-python-sdk进程内调用)
  - [6.2 gRPC 接入（Java/Go/其他语言）](#62-grpc-接入javago其他语言)
  - [6.3 REST API 接入（任何语言）](#63-rest-api-接入任何语言)
  - [6.4 Kafka 消息接入（异步）](#64-kafka-消息接入异步)
  - [6.5 在 RAG 系统中使用](#65-在-rag-系统中使用)
- [7. 配置参考](#7-配置参考)
- [8. 可观测性](#8-可观测性)
- [9. 自定义后端开发](#9-自定义后端开发)
- [10. 多租户配置](#10-多租户配置)

---

## 1. 架构总览

### 1.1 核心定位

```
                         ┌────────────────────────────┐
  Java / Go / Python /   │     QUE Engine :50055       │
  任何业务系统            │     REST API :8080          │
       │                 │     Kafka :que.requests     │
       │                 │                             │
       ▼                 │  ┌─────────────────────┐    │
  ┌──────────┐           │  │  ICAR Pipeline        │    │
  │QueRequest│           │  │  Intent → Rewrite →   │    │
  │ · query  │           │  │  Plan → Route → Exec  │    │
  │ · context│           │  │  → Synthesize         │    │
  └──────────┘           │  └─────────┬───────────┘    │
       │                 │            │                 │
       ▼                 │  ┌─────────▼───────────┐    │
  ┌──────────┐           │  │  Backend Registry    │    │
  │QueResponse│          │  │  (Plugin System)    │    │
  │ · context│           │  ├─────────────────────┤    │
  │ · results│           │  │ RAG │ LLM │ Memory  │    │
  │ · trace  │           │  │ 检索  直接  记忆     │    │
  └──────────┘           │  └─────────────────────┘    │
                         └────────────────────────────┘
```

QUE Engine 是**纯中间件**——不存储数据、不直接访问数据库、不做最终回答。只做四件事：**理解 → 规划 → 调度 → 综合**。

### 1.2 ICAR 五段式管线

```
用户原始输入
    │
    ▼
Stage 1: Intent     意图识别（规则引擎 <1ms，低置信度时 LLM 兜底）
Stage 2: Clarify    指代消解 + 查询补全（结合 Memory 工作记忆）
Stage 3: Augment    多查询扩展 + HyDE（假设文档生成，并行执行）
Stage 4: Retrieve   DAG 规划 + Wave 并行执行（三通道路由）
Stage 5: Synthesize 按意图模式汇总（COMPARE/CHAIN/AGGREGATE）
    │
    ▼
结构化上下文 → 传给 LLM 生成最终回答
```

### 1.3 三通道路由

| 路由类型 | 目标后端 | 触发条件 |
|----------|---------|---------|
| `rag_retrieval` | RAG-PYTHON:50051 | 默认，常规知识检索 |
| `direct_llm` | LLM 直接推理 | 定义/观点/翻译/计算类 |
| `memory_lookup` | RAG-MEMORY:50054 | 包含"我/我的/上次/之前"等 |

### 1.4 DAG 并行执行

```
DAG Plan:
  Wave 0: [SQ_A, SQ_B]     ← ThreadPoolExecutor 并行
  Wave 1: [SQ_C]           ← 等待 Wave 0，注入依赖结果
  Wave 2: [SQ_D]           ← 等待 Wave 1
```

- 同 Wave 内无依赖节点并发执行
- 前序 Wave 结果自动拼入后续子查询
- 单节点失败不传播，超时自动标记

---

## 2. 演进历程

### 版本时间线

```
v0.1.0          v1.0.0           v2.0.0 (Phase 1)   v2.1.0 (Phase 2)   v2.2.0 (Phase 3)
基础搭建        6项核心优化       基础解耦             插件化              产品化
756dd22         10b8f00          fd04796              ff2c1d0             fc0d9e4
```

### v0.1.0 — 基础搭建

**提交**：`756dd22` ~ `b5587e8`（8 commits）

```
feat: scaffold QUE Engine project with build configs
feat: define que.proto and compile gRPC stubs
feat: implement infrastructure adapters (RAG/Memory/LLM)
feat: implement QUE core engine (6 subsystems)
feat: implement gRPC service and main entry point
feat: add E2E tests
docs: add QUE Engine architecture design document
```

**内容**：
- 项目骨架搭建（`pyproject.toml`, `config/settings.yaml`）
- gRPC 协议定义（`que.proto`，依赖 `retrieval.proto`）
- 6 大核心子系统首次实现（intent/rewrite/plan/route/execute/synthesize）
- gRPC 基础设施适配器（RAG-PYTHON 客户端、Memory 客户端、LLM 适配器）
- gRPC 服务入口（端口 50055）
- 端到端测试

### v1.0.0 — 6 项核心优化

**提交**：`10b8f00`（1 commit，+486/-52，13 files）

| 优化项 | 说明 |
|--------|------|
| HyDE + Multi-query 并行化 | ThreadPoolExecutor 并发执行，延迟降低 ~50% |
| Redis 分布式缓存 | 跨实例缓存共享，无 Redis 时自动降级内存缓存 |
| LLM 指数退避重试 | 3次重试 (1s/2s/4s)，处理 RateLimit/Timeout/Connection |
| N-gram Jaccard 去重 | 3-gram 相似度 (threshold=0.85) 替代前100字符 hash |
| gRPC 服务端流式输出 | ExecuteStream RPC，管线每阶段实时推送进度 |
| 双语 Prompt 自适应 | 根据中文字符占比自动选择中/英文 Prompt |

### v2.0.0 — Phase 1：基础解耦

**提交**：`fd04796`（1 commit，+621/-195，9 files）

| 改造项 | 说明 |
|--------|------|
| Self-contained proto | 消除 `import retrieval.proto` 依赖，定义自包含 `SearchResult` |
| context map | `user_id/session_id/kb_ids` 替换为 `map<string,string> context` |
| BackendRegistry | 插件接口 `SearchBackend` + 注册中心 + `resolve()` 方法 |
| Built-in backends | RAGRetrievalBackend / LLMBackend / MemoryBackend |
| dag_executor 重构 | `_execute_one()` 从 if-elif 改为 BackendRegistry 分发 |
| HealthCheck 动态化 | 固定 `rag_*_status` 字段替换为 `BackendHealth[]` |
| rag_backend 同步适配 | QueServiceClient + QaServiceImpl 改为 QUE 编排 |

**Breaking Changes**：
- `DocumentChunk` 类型替换为 `SearchResult`（含 `metadata` map）
- `QueRequest` 字段变更：`user_id/session_id/kb_ids` → `context`

### v2.1.0 — Phase 2：插件化

**提交**：`ff2c1d0`（1 commit，+707/-48，5 files）

| 子系统 | 核心能力 |
|--------|---------|
| **BackendRegistry v2** | `resolve_with_scores()` 多因子路由打分（capability 30% + pattern 25% + hint 35% + health 10%）|
| | `load_from_config()` YAML 驱动后端注册 |
| | 健康检查缓存 (15s TTL) |
| **QuePipeline** | `PipelineStage` 数据类（required/timeout/skip_if） |
| | 动态 add/remove/replace/reorder stage |
| | 3 种预设：`icar` / `retrieval_only` / `intent_then_retrieve` |
| | `PipelineResult` 含 per-stage outputs + errors + latency |
| **TenantManager** | `TenantConfig`：QPS 配额 / 最大子查询 / 超时 / 后端白名单 |
| | `TokenBucket` 令牌桶限流器 |
| | `filter_backends()` 租户级后端过滤 |
| | 默认租户自动注册 (qps=100) |

### v2.2.0 — Phase 3：产品化

**提交**：`fc0d9e4`（1 commit，+942，10 files）

| 子系统 | 核心能力 |
|--------|---------|
| **REST API Gateway** | FastAPI 服务 :8080，6 个端点（execute/health/backends/tenants/presets/metrics）|
| | 租户感知：请求级 `tenant_id` + 速率限制检查 |
| | OpenTelemetry Span 覆盖每个 pipeline stage |
| **Kafka Consumer** | `QueKafkaConsumer` 类，异步消息处理 |
| | JSON 格式，支持 `reply_topic` 回复 |
| | 后台线程 + 优雅关闭 |
| **Python SDK** | `que-sdk` 包，`pip install sdk/` 安装 |
| | `QueClient` 三模式：`inprocess` / `grpc` / `rest` |
| | `QueResult` 类型化返回 + `health_check()` 工具 |
| **OpenTelemetry** | OTel Tracer（无 OTel 时零开销 No-Op 降级） |
| | Prometheus 指标：`que_requests_total` / `que_latency_ms` / `que_errors_total` |
| | `/metrics` 端点（Prometheus text format） |

---

## 3. 项目结构

```
rag_que/
├── config/
│   └── settings.yaml              # 全局配置（YAML + ${ENV:default}）
├── proto/
│   └── que.proto                  # v2 自包含协议定义
├── scripts/
│   └── compile_proto.py           # proto 编译脚本
├── src/
│   ├── main.py                    # gRPC 服务入口 (:50055)
│   ├── common/                    # 共享层
│   │   ├── enums.py               # IntentCategory, RouteType, SynthesizerMode
│   │   ├── config_loader.py       # 配置加载器
│   │   ├── logger.py              # loguru 日志
│   │   ├── exceptions.py          # 异常体系
│   │   └── result.py              # 通用结果封装
│   ├── engine/                    # 核心引擎
│   │   ├── models.py              # 7 个 dataclass
│   │   ├── intent_recognizer.py   # 意图识别（规则 + LLM 兜底）
│   │   ├── query_rewriter.py      # 查询重写（核心指代 + 多查询 + HyDE）
│   │   ├── query_planner.py       # DAG 规划（按意图拆分）
│   │   ├── execution_router.py    # 三通道路由
│   │   ├── dag_executor.py        # Wave 并行执行器
│   │   ├── result_synthesizer.py  # 结果综合（4 种模式）
│   │   ├── backend_registry.py    # 后端注册中心 + 多因子路由
│   │   ├── builtin_backends.py    # 3 个内置后端
│   │   ├── pipeline.py            # 可编排管线引擎
│   │   └── tenant.py              # 多租户管理 + 令牌桶限流
│   ├── infrastructure/            # 基础设施
│   │   ├── llm/
│   │   │   ├── adapter.py         # OpenAI 兼容接口（含重试）
│   │   │   └── prompts.py         # 双语 Prompt 模板
│   │   ├── rag_client/
│   │   │   └── retrieval_client.py    # gRPC → RAG-PYTHON:50051
│   │   ├── memory_client/
│   │   │   └── search_client.py       # gRPC → RAG-MEMORY:50054
│   │   ├── redis/
│   │   │   └── client.py              # Redis 缓存客户端
│   │   └── telemetry.py               # OTel + Prometheus
│   ├── communication/              # 对外通信
│   │   └── grpc_server/
│   │       ├── que_service.py      # gRPC Servicer（ICAR 编排）
│   │       └── generated/          # Proto 编译桩
│   └── api/                        # REST API + Kafka
│       ├── rest_server.py          # FastAPI 网关 (:8080)
│       └── kafka_consumer.py       # Kafka 消费者
├── sdk/                            # Python SDK 包
│   ├── pyproject.toml
│   ├── README.md
│   └── que_sdk/
│       ├── __init__.py
│       └── client.py
├── pyproject.toml
└── Dockerfile
```

---

## 4. 快速开始

### 前置条件

- Python 3.10+
- LLM API Key（OpenAI 兼容接口）
- （可选）RAG-PYTHON 服务 :50051
- （可选）RAG-MEMORY 服务 :50054
- （可选）Redis 服务 :6379

### 安装

```bash
git clone https://github.com/LJ-Agent/rag_que.git
cd rag_que

# 安装核心依赖
pip install -e .

# 安装全部可选依赖（REST API + Kafka + OTel）
pip install -e ".[all]"
```

### 启动

```bash
# 设置 LLM
export LLM_API_KEY=sk-xxx
export LLM_BASE_URL=https://api.openai.com/v1

# 启动 gRPC 服务（主服务）
python src/main.py
# → QUE Engine gRPC server started on 0.0.0.0:50055

# 启动 REST API（另一个终端）
pip install -e ".[api]"
uvicorn api.rest_server:app --host 0.0.0.0 --port 8080
# → REST API Gateway started on :8080

# 启动 Kafka Consumer（另一个终端，可选）
pip install -e ".[kafka]"
export KAFKA_ENABLED=true
export KAFKA_BOOTSTRAP=localhost:9092
python -c "from api.kafka_consumer import QueKafkaConsumer; QueKafkaConsumer().start()"
```

### 验证

```bash
# gRPC 健康检查
grpcurl -plaintext localhost:50055 que.v2.QueEngineService/HealthCheck

# REST 健康检查
curl http://localhost:8080/api/v1/health

# 验证一个查询
curl -X POST http://localhost:8080/api/v1/execute \
  -H "Content-Type: application/json" \
  -d '{"query": "什么是RAG?", "tenant_id": "default"}'
```

---

## 5. 部署指南

### 5.1 Docker 部署

```bash
# 构建镜像
docker build -t que-engine:latest .

# 运行
docker run -d \
  --name que-engine \
  -p 50055:50055 \
  -p 8080:8080 \
  -e LLM_API_KEY=sk-xxx \
  -e LLM_BASE_URL=https://api.openai.com/v1 \
  -e GRPC_RETRIEVAL_HOST=rag-python \
  -e GRPC_MEMORY_HOST=rag-memory \
  -e REDIS_ENABLED=true \
  -e REDIS_HOST=redis \
  que-engine:latest
```

### 5.2 裸机部署

```bash
# 1. 安装依赖
pip install -e ".[all]"
pip install uvicorn

# 2. 配置环境变量
cat > .env << EOF
LLM_API_KEY=sk-xxx
LLM_BASE_URL=https://api.openai.com/v1
GRPC_RETRIEVAL_HOST=localhost
GRPC_RETRIEVAL_PORT=50051
GRPC_MEMORY_HOST=localhost
GRPC_MEMORY_SEARCH_PORT=50054
REDIS_ENABLED=true
REDIS_HOST=localhost
EOF

# 3. 启动 gRPC 服务（systemd 示例）
cat > /etc/systemd/system/que-engine.service << 'EOF'
[Unit]
Description=QUE Engine gRPC Service
After=network.target

[Service]
Type=simple
User=que
WorkingDirectory=/opt/que-engine
EnvironmentFile=/opt/que-engine/.env
ExecStart=/usr/bin/python src/main.py
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF

systemctl enable --now que-engine

# 4. 启动 REST API
cat > /etc/systemd/system/que-api.service << 'EOF'
[Unit]
Description=QUE Engine REST API
After=que-engine.service

[Service]
Type=simple
User=que
WorkingDirectory=/opt/que-engine
EnvironmentFile=/opt/que-engine/.env
ExecStart=/usr/bin/uvicorn api.rest_server:app --host 0.0.0.0 --port 8080
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF

systemctl enable --now que-api
```

### 5.3 Kubernetes 部署

```yaml
# que-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: que-engine
spec:
  replicas: 3
  selector:
    matchLabels:
      app: que-engine
  template:
    metadata:
      labels:
        app: que-engine
    spec:
      containers:
        - name: que-engine
          image: que-engine:latest
          ports:
            - containerPort: 50055
              name: grpc
            - containerPort: 8080
              name: http
          env:
            - name: LLM_API_KEY
              valueFrom:
                secretKeyRef:
                  name: que-secrets
                  key: llm-api-key
            - name: LLM_BASE_URL
              value: "https://api.openai.com/v1"
            - name: GRPC_RETRIEVAL_HOST
              value: "rag-python"
            - name: GRPC_MEMORY_HOST
              value: "rag-memory"
            - name: REDIS_ENABLED
              value: "true"
            - name: REDIS_HOST
              value: "redis"
          livenessProbe:
            httpGet:
              path: /api/v1/health
              port: 8080
            initialDelaySeconds: 10
            periodSeconds: 15
          readinessProbe:
            httpGet:
              path: /api/v1/health
              port: 8080
            initialDelaySeconds: 5
            periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: que-engine
spec:
  selector:
    app: que-engine
  ports:
    - name: grpc
      port: 50055
      targetPort: 50055
    - name: http
      port: 8080
      targetPort: 8080
```

---

## 6. 接入指南

### 6.1 Python SDK（进程内调用）

**适用场景**：Python 项目，QUE Engine 源码在同一进程内，零网络开销。

```bash
pip install /path/to/rag_que/sdk/
```

```python
from que_sdk import QueClient

# 初始化（mode="inprocess" 为默认值）
client = QueClient(mode="inprocess")

# 执行查询
result = client.execute(
    query="RAG 与微调有什么区别？",
    context={"kb_ids": "1,2,3"},
    tenant_id="my-project",
)

# 获取结果
print(f"意图: {result.intent}")
print(f"子查询数: {result.sub_queries_executed}")
print(f"延迟: {result.total_latency_ms:.0f}ms")
print(f"上下文:\n{result.synthesized_context}")
```

### 6.2 gRPC 接入（Java / Go / 其他语言）

**适用场景**：远程调用，需要跨语言访问 QUE Engine。

#### Step 1：获取 proto 文件

```bash
# 从仓库获取 que.proto
wget https://raw.githubusercontent.com/LJ-Agent/rag_que/master/proto/que.proto
```

#### Step 2：生成对应语言的 stub

**Java**：
```xml
<!-- pom.xml -->
<dependency>
    <groupId>io.grpc</groupId>
    <artifactId>grpc-protobuf</artifactId>
    <version>1.60.0</version>
</dependency>
```

```bash
protoc --java_out=src/main/java \
       --grpc-java_out=src/main/java \
       --proto_path=. \
       que.proto
```

**Go**：
```bash
protoc --go_out=. --go-grpc_out=. que.proto
```

#### Step 3：调用 QUE Engine

**Java 示例**：
```java
import com.rag.communication.grpc.proto.que.v2.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

// 建立连接
ManagedChannel channel = ManagedChannelBuilder
    .forAddress("que-engine", 50055)
    .usePlaintext()
    .build();

QueEngineServiceGrpc.QueEngineServiceBlockingStub stub =
    QueEngineServiceGrpc.newBlockingStub(channel);

// 构建请求
QueRequest request = QueRequest.newBuilder()
    .setQuery("什么是 RAG？")
    .putContext("kb_ids", "1,2,3")
    .putContext("user_id", "42")
    .setEnableHyde(true)
    .setEnableMultiQuery(true)
    .setTimeoutMs(30000)
    .build();

// 执行
QueResponse response = stub.withDeadlineAfter(30, TimeUnit.SECONDS)
    .execute(request);

// 使用结果
String context = response.getSynthesizedContext();
int subQueries = response.getPlan().getTotalQueries();
float latency = response.getTotalLatencyMs();

for (SubQueryResult r : response.getSubResultsList()) {
    for (SearchResult sr : r.getResultsList()) {
        System.out.printf("[%s] %s (score=%.2f)%n",
            sr.getSourceBackend(), sr.getContent(), sr.getScore());
    }
}

channel.shutdown();
```

**Go 示例**：
```go
import (
    pb "your-project/proto/que/v2"
    "google.golang.org/grpc"
)

conn, _ := grpc.Dial("que-engine:50055", grpc.WithInsecure())
defer conn.Close()

client := pb.NewQueEngineServiceClient(conn)
resp, _ := client.Execute(ctx, &pb.QueRequest{
    Query: "什么是 RAG？",
    Context: map[string]string{
        "kb_ids":  "1,2,3",
        "user_id": "42",
    },
    EnableHyde:       true,
    EnableMultiQuery: true,
    TimeoutMs:        30000,
})

fmt.Printf("意图: %s\n", resp.Plan.PrimaryIntent)
fmt.Printf("上下文: %s\n", resp.SynthesizedContext)
```

**Python（远程 gRPC）**：
```python
from que_sdk import QueClient

client = QueClient(mode="grpc", host="que-engine", port=50055)
result = client.execute("什么是 RAG？", context={"kb_ids": "1,2,3"})
print(result.synthesized_context)
```

### 6.3 REST API 接入（任何语言）

**适用场景**：不需要 gRPC 依赖，任何能发 HTTP 请求的语言都可以接入。

#### API 端点

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/api/v1/execute` | 执行 QUE 管线 |
| `GET` | `/api/v1/health` | 健康检查 |
| `GET` | `/api/v1/backends` | 列出注册的后端 |
| `GET` | `/api/v1/tenants` | 列出租户及配额 |
| `GET` | `/api/v1/presets` | 列出管线预设 |
| `GET` | `/metrics` | Prometheus 指标 |

#### 请求/响应格式

**请求**：
```json
{
  "query": "RAG 与微调有什么区别？",
  "context": {
    "kb_ids": "1,2,3",
    "user_id": "42",
    "session_id": "sess-001"
  },
  "tenant_id": "my-project",
  "enable_hyde": true,
  "enable_multi_query": true,
  "timeout_ms": 30000,
  "trace_id": "req-abc-123"
}
```

**响应**：
```json
{
  "original_query": "RAG 与微调有什么区别？",
  "rewritten_queries": ["RAG方法的特点", "模型微调的特点"],
  "synthesized_context": "[COMPARISON CONTEXT]\n=== Aspect A ===\n...",
  "sub_results": [
    {
      "query_id": "uuid-1",
      "query_text": "...",
      "route": "rag_retrieval",
      "results_count": 5,
      "success": true,
      "latency_ms": 120.5
    }
  ],
  "plan_summary": {
    "total_queries": 2,
    "parallel_waves": 1,
    "primary_intent": "comparison",
    "complexity": "compound"
  },
  "total_latency_ms": 1450.3,
  "trace_id": "req-abc-123",
  "tenant_id": "my-project"
}
```

#### 调用示例

```bash
# cURL
curl -X POST http://que-engine:8080/api/v1/execute \
  -H "Content-Type: application/json" \
  -d '{"query": "什么是RAG?", "tenant_id": "default"}'

# JavaScript/Node.js
const resp = await fetch("http://que-engine:8080/api/v1/execute", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({ query: "什么是RAG?", tenant_id: "default" })
});
const data = await resp.json();
console.log(data.synthesized_context);

# Python httpx
import httpx
resp = httpx.post("http://que-engine:8080/api/v1/execute", json={
    "query": "什么是RAG?", "tenant_id": "default"
}, timeout=30)
print(resp.json()["synthesized_context"])
```

### 6.4 Kafka 消息接入（异步）

**适用场景**：异步高吞吐场景，将查询请求通过消息队列发送给 QUE Engine。

#### 消息格式

**请求**（发送到 `que.requests`）：
```json
{
  "query": "最新财报的营收数据是多少？",
  "context": {"kb_ids": "5,6", "user_id": "100"},
  "tenant_id": "finance-team",
  "reply_topic": "que.results.finance-team",
  "trace_id": "trace-xyz-456",
  "params": {}
}
```

**响应**（发送到 `reply_topic`）：
```json
{
  "trace_id": "trace-xyz-456",
  "original_query": "最新财报的营收数据是多少？",
  "synthesized_context": "...",
  "intent": "fact_lookup",
  "sub_queries": 1,
  "ok_results": 1,
  "total_latency_ms": 850.2
}
```

#### 启动消费者

```bash
export KAFKA_ENABLED=true
export KAFKA_BOOTSTRAP=localhost:9092
export KAFKA_REQUEST_TOPIC=que.requests

python -c "
from api.kafka_consumer import QueKafkaConsumer
consumer = QueKafkaConsumer()
consumer.on_result = lambda msg, resp: print(f'Processed: {resp[\"trace_id\"]}')
consumer.start()
# Ctrl+C to stop
"
```

### 6.5 在 RAG 系统中使用

QUE Engine 在 RAG 系统中的标准位置：

```
用户问题 → QUE Engine → 结构化上下文 → LLM Generation → 最终回答
             │    │
             │    ├── RAG-PYTHON (向量检索)
             │    ├── RAG-MEMORY (用户记忆)
             │    └── LLM Direct (直接推理)
```

**Java 集成示例**（完整 RAG 后端）：

```java
// QueServiceClient.java — gRPC 客户端封装
@Component
public class QueServiceClient {
    private final QueEngineServiceBlockingStub stub;

    public QueResponse execute(String query, Long userId,
                                Long sessionId, List<Long> kbIds) {
        Map<String, String> context = new HashMap<>();
        context.put("user_id", String.valueOf(userId));
        context.put("session_id", String.valueOf(sessionId));
        context.put("kb_ids", kbIds.stream()
            .map(String::valueOf).collect(Collectors.joining(",")));

        QueRequest req = QueRequest.newBuilder()
            .setQuery(query)
            .putAllContext(context)
            .setTimeoutMs(30000)
            .build();

        return stub.withDeadlineAfter(30, TimeUnit.SECONDS).execute(req);
    }
}

// QaServiceImpl.java — 在业务服务中使用
@Service
public class QaServiceImpl {
    private final QueServiceClient queClient;
    private final GenerationServiceClient genClient;

    public AnswerVO chat(QuestionDTO dto, Long userId) {
        // Step 1: QUE Engine 查询理解 + 检索编排
        QueResponse queResp = queClient.execute(
            dto.getQuestion(), userId, dto.getSessionId(), dto.getKbIds());

        // Step 2: 提取上下文
        List<String> contexts = queResp.getSubResultsList().stream()
            .filter(r -> r.getSuccess())
            .flatMap(r -> r.getResultsList().stream())
            .map(SearchResult::getContent)
            .collect(Collectors.toList());

        // Step 3: LLM 生成回答
        GenerationResponse genResp = genClient.generate(
            dto.getQuestion(), contexts);

        // Step 4: 返回结果（含 QUE 执行追踪）
        AnswerVO vo = new AnswerVO();
        vo.setAnswer(genResp.getContent());
        vo.setLatencyMs(genResp.getLatencyMs() + queResp.getTotalLatencyMs());
        // ... 保存记录、缓存等
        return vo;
    }
}
```

---

## 7. 配置参考

所有配置通过 `config/settings.yaml` 和环境变量管理。

### 核心配置

```yaml
# gRPC 服务
grpc:
  port: ${GRPC_QUE_PORT:50055}        # gRPC 监听端口
  max_workers: 10

# 下游客户端
clients:
  rag_python:
    host: ${GRPC_RETRIEVAL_HOST:localhost}
    port: ${GRPC_RETRIEVAL_PORT:50051}
    timeout: 30
  rag_memory:
    host: ${GRPC_MEMORY_HOST:localhost}
    port: ${GRPC_MEMORY_SEARCH_PORT:50054}
    timeout: 10

# LLM
llm:
  api_key: ${LLM_API_KEY:}
  base_url: ${LLM_BASE_URL:}
  chat_model: ${CHAT_MODEL:gpt-4o-mini}
  timeout: 60

# 引擎参数
engine:
  max_sub_queries: 8
  max_dag_depth: 4
  timeout_ms: 30000
  hyde_enabled: true
  multi_query_enabled: true
  cache_ttl: 300
  rule_confidence_threshold: 0.7
  max_workers: 10

# Redis 缓存（可选）
redis:
  enabled: ${REDIS_ENABLED:false}
  host: ${REDIS_HOST:localhost}
  port: ${REDIS_PORT:6379}

# 管线
pipeline:
  default_preset: icar               # icar | retrieval_only | intent_then_retrieve

# 多租户
tenants:
  enabled: ${TENANTS_ENABLED:false}
  default:
    max_qps: 100
    max_sub_queries: 8
    timeout_ms: 30000

# Kafka（可选）
kafka:
  enabled: ${KAFKA_ENABLED:false}
  bootstrap_servers: ${KAFKA_BOOTSTRAP:localhost:9092}
  request_topic: ${KAFKA_REQUEST_TOPIC:que.requests}

# 可观测性
telemetry:
  tracing:
    enabled: ${OTEL_ENABLED:false}
    service_name: ${OTEL_SERVICE_NAME:que-engine}
    otlp_endpoint: ${OTEL_EXPORTER_OTLP_ENDPOINT:}
```

### 环境变量速查

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `LLM_API_KEY` | - | **必需**，LLM API Key |
| `LLM_BASE_URL` | - | **必需**，LLM API 地址 |
| `CHAT_MODEL` | `gpt-4o-mini` | 对话模型 |
| `GRPC_QUE_PORT` | `50055` | QUE gRPC 端口 |
| `GRPC_RETRIEVAL_HOST` | `localhost` | RAG-PYTHON 地址 |
| `GRPC_RETRIEVAL_PORT` | `50051` | RAG-PYTHON 端口 |
| `GRPC_MEMORY_HOST` | `localhost` | RAG-MEMORY 地址 |
| `GRPC_MEMORY_SEARCH_PORT` | `50054` | RAG-MEMORY 端口 |
| `REDIS_ENABLED` | `false` | 启用 Redis 缓存 |
| `KAFKA_ENABLED` | `false` | 启用 Kafka 消费 |
| `OTEL_ENABLED` | `false` | 启用 OpenTelemetry |
| `TENANTS_ENABLED` | `false` | 启用多租户 |

---

## 8. 可观测性

### Prometheus Metrics

访问 `http://localhost:8080/metrics`：

```
# HELP que_requests_total Total QUE pipeline requests
# TYPE que_requests_total counter
que_requests_total 42

# HELP que_latency_ms QUE pipeline latency in ms
# TYPE que_latency_ms summary
que_latency_ms{quantile="0.5"} 450
que_latency_ms{quantile="0.95"} 2100
que_latency_ms{quantile="0.99"} 4800
que_latency_ms_count 42

# HELP que_errors_total Total QUE pipeline errors
# TYPE que_errors_total counter
que_errors_total 2
```

### OpenTelemetry Tracing

```bash
# 启用 OTel（指向 Jaeger）
export OTEL_ENABLED=true
export OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4317
export OTEL_SERVICE_NAME=que-engine
```

每个请求生成以下 Span：
- `QUE.Execute` — 根 Span
  - `QUE.intent` — 意图识别
  - `QUE.rewrite` — 查询重写
  - `QUE.plan` — 查询规划
  - `QUE.execute` — 并行执行
  - `QUE.synthesize` — 结果综合

### gRPC ExecutionTrace

每次请求的 `QueResponse.trace` 包含完整执行追踪：

```json
{
  "entries": [
    {"stage": "intent", "description": "primary=comparison, confidence=0.85", "latency_ms": 3.2},
    {"stage": "rewrite", "description": "coref=True, expanded=4, hyde=True", "latency_ms": 1240.0},
    {"stage": "plan", "description": "sub_queries=2, waves=1", "latency_ms": 45.0},
    {"stage": "execute", "description": "ok=2/2", "latency_ms": 320.0},
    {"stage": "synthesize", "description": "context_length=2847", "latency_ms": 2.1}
  ]
}
```

---

## 9. 自定义后端开发

实现 `SearchBackend` 接口即可接入 QUE Engine 的路由系统：

```python
from engine.backend_registry import (
    SearchBackend, SearchResult, BackendCapability, get_registry
)

class MyElasticsearchBackend(SearchBackend):
    """自定义 Elasticsearch 后端"""

    @property
    def name(self) -> str:
        return "elasticsearch"

    @property
    def capabilities(self) -> list[BackendCapability]:
        return [BackendCapability.SEARCH]

    @property
    def route_patterns(self) -> list[str]:
        # 匹配包含特定关键词的查询
        return [r"(?:log|日志|监控|指标)"]

    def search(self, query, context, top_k=10, timeout=30.0):
        # ES 查询逻辑
        results = es_client.search(index="docs", body={
            "query": {"match": {"content": query}},
            "size": top_k,
        })

        return [
            SearchResult(
                id=hit["_id"],
                content=hit["_source"]["content"],
                metadata={"index": hit["_index"], "score": str(hit["_score"])},
                score=hit["_score"],
                source_backend=self.name,
            )
            for hit in results["hits"]["hits"]
        ]

    def health_check(self) -> bool:
        try:
            return es_client.ping()
        except:
            return False


# 注册
registry = get_registry()
registry.register(MyElasticsearchBackend())

# 验证
print(registry.list_all())  # 4 backends total (3 built-in + 1 custom)
```

---

## 10. 多租户配置

### 静态配置

```yaml
# config/settings.yaml
tenants:
  enabled: true
  default:
    max_qps: 100
    max_sub_queries: 8
    timeout_ms: 30000
```

### 动态注册

```python
from engine.tenant import get_tenant_manager, TenantConfig

mgr = get_tenant_manager()

# 注册高优先级租户
mgr.register(TenantConfig(
    tenant_id="vip-customer",
    quota_max_qps=500,
    quota_max_sub_queries=16,
    quota_timeout_ms=60000,
    pipeline_name="icar",
    allowed_backends=["rag_retrieval", "direct_llm"],  # 不允许 memory
))

# 注册受限租户
mgr.register(TenantConfig(
    tenant_id="trial-user",
    quota_max_qps=10,
    quota_max_sub_queries=4,
    quota_timeout_ms=15000,
    pipeline_name="retrieval_only",  # 跳过 rewrite 阶段
))

# 在请求中使用
tenant = mgr.get("vip-customer")
if mgr.check_rate_limit("vip-customer"):
    # 执行管线...
    pass
```

### REST API 中的租户

```bash
# 查看租户配额
curl http://localhost:8080/api/v1/tenants

# 以特定租户执行
curl -X POST http://localhost:8080/api/v1/execute \
  -H "Content-Type: application/json" \
  -d '{"query": "...", "tenant_id": "vip-customer"}'
```

---

## 许可证

MIT License

## 相关仓库

| 仓库 | 说明 |
|------|------|
| [LJ-Agent/rag_backend](https://github.com/LJ-Agent/rag_backend) | Java 后端核心 |
| [LJ-Agent/rag_python](https://github.com/LJ-Agent/rag_python) | Python AI 核心服务 |
| [LJ-Agent/rag_memory](https://github.com/LJ-Agent/rag_memory) | 记忆引擎 |
| [LJ-Agent/rag_cleaning](https://github.com/LJ-Agent/rag_cleaning) | 文档清洗服务 |
| [LJ-Agent/rag_front](https://github.com/LJ-Agent/rag_front) | Vue 前端 |
| [LJ-Agent/rag_gateway](https://github.com/LJ-Agent/rag_gateway) | API 网关 |
