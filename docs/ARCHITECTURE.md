# QUE Engine — 查询理解与执行引擎 架构设计文档

## 一、概述

QUE Engine（Query Understanding & Execution Engine）是 RAG 知识库系统的智能调度中枢，采用 **Agentic RAG** 与 **Plan-then-Execute** 范式，将用户原始问题转化为结构化检索计划并执行，最终汇总为可用于 LLM 生成的上下文字符串。

**核心定位**：从"被动检索"到"主动规划"——不直接访问向量库、不存储数据、不做最终回答，纯中间件。

## 二、总体架构：ICAR 五段式流水线

```
用户原始输入
    │
    ▼
┌──────────────────────────────────────────────────────────┐
│  Stage 1: Intent    │  意图识别（规则+LLM 混合）          │
│  Stage 2: Clarify   │  指代消解 + 查询补全               │
│  Stage 3: Augment   │  多查询扩展 + HyDE                 │
│  Stage 4: Retrieve  │  DAG 规划 + 并行执行               │
│  Stage 5: Synthesize│  按逻辑关系汇总                    │
└──────────────────────────────────────────────────────────┘
    │
    ▼
结构化上下文字符串 → 传给 LLM 生成最终回答
```

## 三、6 大核心子系统

### 3.1 Intent Recognizer（意图识别器）

- **文件**: `engine/intent_recognizer.py`
- **策略**: 规则引擎优先（<10ms），低置信度时 LLM 兜底
- **意图类型**:
  - `fact_lookup` — 事实查询（如"什么是 RAG"）
  - `comparison` — 对比分析（如"RAG 与微调的区别"）
  - `multi_hop` — 多跳推理（如"谁开发了 X，这家公司后来被谁收购"）
  - `procedural` — 流程指南（如"如何部署 Milvus"）
  - `open_discussion` — 开放讨论（如"你觉得 AI 的未来如何"）
- **复杂度**: `simple` / `multi-hop` / `compound`
- **配置**: `rule_confidence_threshold` 控制 LLM fallback 触发点（默认 0.7）

### 3.2 Query Rewriter（查询重写器）

- **文件**: `engine/query_rewriter.py`
- **管线**:
  1. **指代消解** — 结合 RAG-MEMORY 工作记忆，将"这个/它/刚才那个"替换为明确实体
  2. **查询补全** — 短/模糊 query 自动补全为完整问句
  3. **多查询扩展** — 生成 3-5 个语义等价变体提升召回率
  4. **HyDE** — 生成假设性文档片段用于向量检索
- **环境变量**: `enable_hyde`、`enable_multi_query` 控制开关

### 3.3 Query Planner（查询规划器）

- **文件**: `engine/query_planner.py`
- **按意图生成 DAG 计划**:
  - Simple → 单节点检索
  - Comparison → LLM 拆分为 A/B 子查询（并行 wave 0）
  - Multi-hop → LLM 分解为依赖链（串行 waves）
  - Compound → 多独立子查询 + 汇总
- **DAG 结构**: `DAGPlan { sub_queries[], total_queries, parallel_waves, max_depth }`
- **LLM 辅助拆分**: comparison/multi-hop 使用 `chat_structured` 进行结构化分解

### 3.4 Execution Router（执行路由器）

- **文件**: `engine/execution_router.py`
- **三通道路由**:
  | 路由类型 | 目标 | 触发条件 |
  |----------|------|----------|
  | `rag_retrieval` | RAG-PYTHON:50051 | 默认，常规知识检索 |
  | `direct_llm` | LLM 直接推理 | 定义/观点/翻译/计算类问题 |
  | `memory_lookup` | RAG-MEMORY:50054 | 包含"我/我的/上次/之前"等用户相关词 |
- **路由优先级**: 预分配路由 > 记忆查询 > 直接 LLM > 默认 RAG 检索

### 3.5 DAG Executor（DAG 执行器）

- **文件**: `engine/dag_executor.py`
- **执行模型**: Wave 并行 — 同 wave 内的无依赖节点并发执行
- **并发**: `ThreadPoolExecutor`（默认 max_workers=10）
- **缓存**: query_hash → SubQueryResult，TTL 可配置（默认 300s）
- **依赖注入**: 前序 wave 的结果自动拼入后续子查询的 query_text
- **容错**:
  - 每个子查询独立超时
  - 失败节点标记 `success=false`，不阻塞同 wave 其他节点
  - 超时节点自动标记为失败

### 3.6 Result Synthesizer（结果综合器）

- **文件**: `engine/result_synthesizer.py`
- **4 种综合模式**:
  | 模式 | 触发条件 | 输出格式 |
  |------|----------|----------|
  | COMPARE | comparison 意图 | A/B 并排注入 |
  | CHAIN | multi_hop 意图 | 步骤 n 因果串联 |
  | AGGREGATE | compound 复杂度 | 去重合并 + 分数排序 |
  | default | simple 意图 | 直接拼接 |
- **去重**: 按 content 前 100 字符 hash 去重

## 四、模块层次

```
src/
├── main.py                          # gRPC 服务入口 :50055
├── common/                          # 共享层
│   ├── enums.py                     # IntentCategory, ComplexityLevel, RouteType, SynthesizerMode
│   ├── config_loader.py             # YAML + ${ENV:default} 配置加载
│   ├── logger.py                    # loguru 日志配置
│   ├── exceptions.py                # QueException 异常体系
│   └── result.py                    # QueResult 通用结果封装
├── engine/                          # 核心引擎（6 子系统，无 IO 依赖）
│   ├── models.py                    # 7 个 dataclass（IntentResult, DAGPlan, SubQuery...)
│   ├── intent_recognizer.py         # 意图识别
│   ├── query_rewriter.py            # 查询重写
│   ├── query_planner.py             # 查询规划
│   ├── execution_router.py          # 执行路由
│   ├── dag_executor.py              # DAG 执行
│   └── result_synthesizer.py        # 结果综合
├── infrastructure/                  # 基础设施适配器
│   ├── llm/
│   │   ├── adapter.py               # OpenAI 兼容接口（chat, chat_structured, health_check）
│   │   └── prompts.py               # 6 个 Prompt 模板
│   ├── rag_client/
│   │   └── retrieval_client.py      # gRPC → RAG-PYTHON:50051
│   └── memory_client/
│       └── search_client.py         # gRPC → RAG-MEMORY:50054
└── communication/                   # 对外通信
    └── grpc_server/
        ├── que_service.py           # QueEngineServiceServicer（ICAR 管线编排）
        └── generated/               # Proto 编译桩代码
```

## 五、gRPC 接口

```protobuf
service QueEngineService {
  rpc Execute(QueRequest) returns (QueResponse);
  rpc HealthCheck(HealthCheckRequest) returns (HealthCheckResponse);
}
```

**QueRequest**: `query`, `user_id`, `session_id`, `kb_ids`, `max_sub_queries`, `timeout_ms`, `enable_hyde`, `enable_multi_query`

**QueResponse**: `original_query`, `rewritten_queries[]`, `plan (DAGPlan)`, `sub_results[]`, `synthesized_context`, `execution_trace`, `total_latency_ms`

## 六、与上下游的集成

```
RAG-BACKEND:8080 (Java)
    │ gRPC
    ▼
QUE Engine:50055
    │ gRPC                    │ gRPC
    ▼                         ▼
RAG-PYTHON:50051          RAG-MEMORY:50054
(向量检索)                 (记忆检索)
```

QUE Engine 不直接访问 Milvus/MySQL/Redis/Kafka，所有数据操作通过下游 gRPC 服务完成。

## 七、关键架构决策

| 决策 | 选择 | 理由 |
|------|------|------|
| 规划模式 | Plan-then-Execute | 可解释性强，支持并行优化 |
| 并行策略 | ThreadPoolExecutor | I/O 密集型，比 asyncio 更简单，与 gRPC 线程模型一致 |
| 意图识别 | 规则路由 + LLM 兜底 | 高频低延迟（<10ms），长尾高泛化 |
| 查询重写 | 多查询扩展 + HyDE 混合 | 多查询提升召回，HyDE 提升精度 |
| 服务状态 | 无状态（无 DB） | 纯中间件，降低运维复杂度 |
| 拆分粒度 | 子问题级 | 保留完整语义，便于独立检索 |
| 故障隔离 | 单节点失败不传播 | 部分失败仍可生成可用上下文 |

## 八、数据流图

```
用户原始 Query
    │
    ├─[Intent]──→ IntentResult { primary_intent, complexity_level, confidence }
    │
    ├─[Clarify]─→ 结合 RAG-MEMORY 工作记忆做指代消解
    │
    ├─[Augment]─→ 多查询扩展 + HyDE 生成
    │
    ├─[Plan]────→ DAGPlan { sub_queries[], parallel_waves }
    │               │
    │               ├── Wave 0: [SQ_A, SQ_B] (并行)
    │               ├── Wave 1: [SQ_C] (依赖 Wave 0)
    │               └── Wave 2: [SQ_D] (依赖 Wave 1)
    │
    ├─[Route]───→ 每个子查询 → rag_retrieval / direct_llm / memory_lookup
    │
    ├─[Execute]─→ ThreadPoolExecutor 按 wave 执行 → SubQueryResult[]
    │
    └─[Synthesize] → 按模式汇总 → synthesized_context (string)
```

## 九、ExecutionTrace（可观测性）

每个请求生成完整执行轨迹，记录每个阶段的耗时和元数据：

```
TraceEntry { stage, description, latency_ms, metadata }
```

阶段包括：`intent` → `rewrite` → `plan` → `route` → `execute` → `synthesize`

## 十、配置要点

```yaml
grpc.port: 50055                          # QUE Engine 监听端口
engine.max_sub_queries: 8                 # 最大子查询数
engine.rule_confidence_threshold: 0.7     # LLM fallback 触发点
engine.hyde_enabled: true                 # HyDE 开关
engine.multi_query_enabled: true          # 多查询扩展开关
engine.cache_ttl: 300                     # 查询缓存 TTL（秒）
clients.rag_python: localhost:50051       # RAG-PYTHON 地址
clients.rag_memory: localhost:50054       # RAG-MEMORY 地址
```
