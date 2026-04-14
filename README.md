# distributed-kv

> A fault-tolerant, distributed key-value store built from scratch — routing, storage, and node communication included.

---

## Overview

`distributed-kv` is a distributed key-value store that runs across multiple nodes. Inspired by systems like **DynamoDB** and **Cassandra**, it is designed to be scalable, fault-tolerant, and easy to reason about.

Each node in the cluster can handle reads and writes. Requests are routed to the right node using **consistent hashing**, data is persisted durably using a **write-ahead log and bbolt**, and nodes talk to each other over **gRPC**.

This is an active work-in-progress — built feature by feature, with production-grade concepts at its core.

---

## Key Features

- **Consistent Hashing** — Keys are distributed evenly across nodes. Adding or removing a node only affects a minimal subset of keys.
- **Durable Storage** — Writes go through a write-ahead log (WAL) before being committed to a bbolt embedded database. No data is lost on a crash.
- **gRPC Communication** — Nodes communicate via strongly-typed gRPC, making inter-node calls fast and reliable.

---

## Architecture

```
Client Request
      │
      ▼
 Router Layer  ──── Consistent Hash Ring ────▶  Target Node
                                                     │
                                              ┌──────▼──────┐
                                              │  WAL (Log)  │
                                              │  bbolt (DB) │
                                              └─────────────┘
```

- The **router** uses a consistent hash ring to determine which node owns a given key.
- Each node runs an embedded **storage engine** backed by a write-ahead log for durability and bbolt for persistence.
- Nodes expose a **gRPC server** to accept forwarded requests from other nodes in the cluster.

---

## Tech Stack

| Layer         | Technology                  |
|---------------|-----------------------------|
| Language      | Go                          |
| Storage       | bbolt (embedded B-tree DB)  |
| Durability    | Write-Ahead Log (WAL)       |
| Routing       | Consistent Hashing          |
| Communication | gRPC + Protocol Buffers     |

---

## Getting Started

### Prerequisites

- Go 1.21+
- `protoc` with the Go gRPC plugin (for regenerating protobufs)

### Run a Node

```bash
# Clone the repo
git clone https://github.com/your-username/distributed-kv.git
cd distributed-kv

# Run a node on port 8080
go run ./cmd/node --port=8080

# Run a second node on port 8081
go run ./cmd/node --port=8081
```

### Basic Operations

```bash
# Put a key
curl -X POST http://localhost:8080/put -d '{"key": "hello", "value": "world"}'

# Get a key
curl http://localhost:8080/get?key=hello
```

> More detailed usage instructions will be added as the API stabilises.

---

## Progress Tracker

### ✅ Completed

- [x] **Day 1** — Consistent hashing for key distribution across nodes
- [x] **Day 2** — Storage engine: in-memory map + write-ahead log + bbolt persistence
- [x] **Day 3** — gRPC communication between nodes

### 🔄 In Progress / Upcoming

- [ ] **Gossip Protocol (SWIM)** — Decentralised node discovery and failure detection
- [ ] **Replication** — Replicate keys across multiple nodes for fault tolerance
- [ ] **Hinted Handoff** — Buffer writes for unavailable nodes and deliver them on recovery
- [ ] **Metrics** — Prometheus instrumentation + Grafana dashboards
- [ ] **Chaos Testing** — Simulate node failures, network partitions, and recovery scenarios

---

## Future Improvements

- **Quorum Reads/Writes** — Configurable consistency levels (e.g. read from N of M replicas)
- **Compaction** — Periodic WAL compaction to reclaim disk space
- **CLI Tool** — A simple command-line client for cluster inspection and operations
- **Docker Compose Setup** — Spin up a multi-node cluster with a single command

---

## Contributing

Contributions, ideas, and feedback are welcome.

1. Fork the repo
2. Create a feature branch: `git checkout -b feature/your-idea`
3. Commit your changes and open a pull request

Please keep PRs focused and include a short description of what changed and why.

---

## License

MIT
