# AegisExchange Distributed Microservices Platform

AegisExchange is an enterprise-grade distributed stock exchange and autonomous financial security platform built in Java 21 and Spring Boot 3.4.3. It coordinates distributed order ingestion, ledger SSOT validation, concurrent price-time priority matching, real-time market data dissemination, statistical abuse detection, and autonomous AI-assisted governance.

---

## 🏛️ System Topology & Node Allocation

| Node | Service | Port | Persistence / Storage | Responsibility |
|---|---|---|---|---|
| **Node-0** | Nginx Reverse Proxy Gateway | `:8080` | Stateless (Docker) | Route reverse-proxying, upstream load-balancing & failover |
| **Node-1A / 1B** | `order-ingestion-service` | `:8081` / `:8082` | In-Memory Local Cache | Edge validation, signature checking, idempotency, Redpanda publishing |
| **Node-2** | `account-wallet-service` | `:8083` | PostgreSQL 16 (`:5432`) | Single Source of Truth (SSOT) ledger, balance holds, trade settlement |
| **Node-3A / 3B / 3C** | `matching-engine-service` | `:8084` / `:8085` / `:8086` | In-Memory Priority Queues | Parallel ticker partition matching, fine-grained ReentrantLock, deterministic tradeId |
| **Node-4** | `market-data-service` | `:8087` | Local H2 In-Memory DB | L2 OrderBook reconstruction, OHLCV candlestick aggregation, REST quotes |
| **Node-5** | `surveillance-risk-service` | `:8088` | Local H2 In-Memory DB | Spoofing, Wash-trading rings, and Order Flooding velocity detection |
| **Node-6** | `enforcement-governance-service` | `:8089` | Local H2 In-Memory DB | AI proposal management (`PENDING`/`APPROVED`), human-in-the-loop audit |
| **Node-7** | `aegis-mcp-server` | `:8090` | HTTP / SSE | Model Context Protocol endpoints exposing exchange telemetry & action tools |
| **Node-8** | `autonomous-secops-agent` | `:8091` | Stateless AI Agent Loop | 60s autonomous threat inspection loop using Ollama / NVIDIA NIM / Groq |
| **Node-9** | `aegis-client-suite` | `:8092` | Web Dashboard / Bots | Multi-bot simulation (SpooferBot, WashTraderBot, FloodBot) & monitoring UI |

---

## 📡 Kafka / Redpanda Topics Specification

1. **`order-requests`** (Partitions: 3) — Key: `userId`. Raw inbound order intents from client ingestion.
2. **`order-accepted`** (Partitions: 3) — Key: `ticker`. Orders validated and balance-reserved by Node-2.
3. **`order-rejected`** (Partitions: 3) — Key: `userId`. Orders failing ledger pre-validation or edge security checks.
4. **`execution-events`** (Partitions: 3) — Key: `ticker`. Matches (`TradeExecuted`) and cancellations (`OrderCancelled`). Deterministic tradeId format: `partition-offset-seq`.
5. **`enforcement-events`** (Partitions: 1) — Key: `targetId`. Sanctions approved by governance (`FREEZE_ACCOUNT`, `THROTTLE_TRADER`, `HALT_TICKER`).

---

## 🛠️ Master Build Roadmap (6 Core Architectural Concepts)

- **Concept 1: Infrastructure + Root Multi-Module Maven + `common-events` (Foundation)**
- **Concept 2: Edge Ingestion (`order-ingestion-service`) + Wallet SSOT (`account-wallet-service`)**
- **Concept 3: Parallel Matching Engine Cluster (`matching-engine-service`)**
- **Concept 4: Event-Driven H2 Read Replicas (`market-data-service` & `surveillance-risk-service`)**
- **Concept 5: Governance, Remote MCP Server & Autonomous AI Agent (`Node-6`, `Node-7`, `Node-8`)**
- **Concept 6: Client Dashboard & Multi-Bot Simulator (`aegis-client-suite`)**
