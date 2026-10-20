# go-logger

An ultra-high-performance, structured JSON logging package for Go. Architected specifically for high-concurrency systems requiring deterministic **zero-allocation** memory footprints and non-blocking safety under extreme load saturation.

[![Go Report Card](https://goreportcard.com)](https://goreportcard.com)
[![License](https://shields.io)](LICENSE)

## ⚡ Performance Matrix

Validated under peak stress profiles over **3 billion continuous operations** across 22 concurrent hardware execution threads on an *Intel(R) Core(TM) Ultra 7 155H* architecture:

| Operational Path | Throughput (per core) | Memory Allocation | Heap Object Overhead |
| :--- | :--- | :--- | :--- |
| **`LogInfo` (String Path)** | ~18.0M ops/sec (`55.57 ns/op`) | **`0 B/op`** | **`0 allocs/op`** |
| **`LogInfoBytes` (Pooled Byte Path)** | ~15.3M ops/sec (`65.25 ns/op`) | **`0 B/op`** | **`0 allocs/op`** |
| **`LogErrorObj` (Error Wrap Path)** | ~16.2M ops/sec (`61.55 ns/op`) | **`0 B/op`** | **`0 allocs/op`** |

The tiny 10-nanosecond difference between paths represents the cost of executing the conditional block evaluation logic and processing string validation delimiters inside the worker.

### Key Architectural Takeaways
* **GC-Immune Runtime:** Emits fully-structured JSON to `os.Stdout` with absolute zero memory contamination, eliminating Garbage Collection spikes in low-latency production applications.
* **Lock-Free Concurrency Execution:** Utilizes a highly optimized internal `sync.Pool` pairing logic coupled with direct-to-buffer trailing-capacity mutations (`freeSpace` slicing).
* **Zero Runtime Interface Boxing:** Bypasses dynamic type reflections and runtime interface wrappers via explicit, type-safe internal processing channels.

---

## 🛠️ Core Features

* **Strict Asynchronous Design:** Decouples application worker logic from file/terminal write blocking I/O using a single dedicated background processing engine.
* **Intelligent JSON Structural Injection:** Dynamically detects pre-formatted JSON objects or arrays inside inputs, seamlessly nesting them inside the core payload message instead of escaping them as a plain string text blob.
* **Non-Blocking Overload Safeguard:** When the asynchronous logging queue hits full saturation, the logger drops excess messages via a non-blocking `select` circuit rather than locking up application workers.
* **Integrated Telemetry Monitor:** Automatically wakes up every 10 seconds to push localized telemetry summaries to `os.Stderr` when congestion forces log dropping.

---

## 📦 Installation

Initialize your project module directory and download the local repository package:

```bash
go get github.com/juan-carlos-trimino/go-logger
```

---

## 🚀 Quick Start

```go
package main

import (
	"errors"
	"github.com/juan-carlos-trimino/go-logger"
)

func main() {
	// 1. Initialize background log workers and metrics channels
	logger.InitLogger()

	// 2. Ensure graceful shutdown and flush pending queue records upon termination
	defer logger.CloseLogger()

	cid := "req-cb48-a901"

	// Standard text logging string emission
	logger.LogInfo("User login verified successfully from remote cluster", cid)

	// Automated JSON object detection and structured injection
	mockJSON := []byte(`{"user_id":4561,"permissions":["read","write"],"tier":"enterprise"}`)
	logger.LogInfoBytes(mockJSON, cid)

	// Safe error instance formatting wrapper
	if err := simulateDatabaseTimeout(); err != nil {
		logger.LogErrorObj(err, cid)
	}
}

func simulateDatabaseTimeout() error {
	return errors.New("relational db connection pool exhausted")
}
```

### JSON Output Profiles Emitted to `os.Stdout`:

```json
{"date_time":"2026-10-17T16:05:00.123456789Z","level":"INFO","correlation_id":"req-cb48-a901","msg":"User login verified successfully from remote cluster"}
{"date_time":"2026-10-17T16:05:00.123558122Z","level":"INFO","correlation_id":"req-cb48-a901","msg":{"user_id":4561,"permissions":["read","write"],"tier":"enterprise"}}
{"date_time":"2026-10-17T16:05:00.123991201Z","level":"ERROR","correlation_id":"req-cb48-a901","msg":"relational db connection pool exhausted"}
```

---

## 🛡️ Overload Resiliency & Telemetry

When systems go out of bounds and traffic spikes saturate the asynchronous pipeline capacity, application workers stay operational. In the event of drop mitigations, automated telemetry aggregates log drops safely every 10 seconds to `os.Stderr`:

```text
ALERT: Logging queue capacity saturated! Dropped 192319352 log(s) in the last 10s. Total dropped so far: 192319352
LOGGER SHUTDOWN: Application terminated with a total of 993583793 dropped log(s) due to channel saturation.
```

---

## 🧪 Testing Suite Execution

The repository contains functional safety test routines alongside performance benchmarking infrastructure grouped cleanly inside unified files.

By default, Go runs each benchmark for exactly 1 second. To customize the benchmark sampling time, you use the -benchtime flag.
Alternatively, if you would prefer to benchmark an exact number of execution loops instead of a time window, you can append an x
to the flag value. For example, to force each benchmark block to process exactly 10 million operations, you can execute:<br>
```
go test ../../../gp-meta-repo/go-logger -v -bench=. -benchmem -benchtime=10000000x
```

### Execute Full Suite (Validation + Benchmarks)
It scans the package, fires off your functional safety validation, captures the output, resets the counters, and immediately rolls
straight into your parallel multi-core performance tests.
```bash
go test ../../../gp-meta-repo/go-logger -v -bench=. -benchmem -benchtime=2s
```

### Execute Long-Form Precision Benchmarking (e.g., 5-Second Iterations)
If you want to isolate your performance metrics (34.27 ns/op, 0 B/op) without letting the functional test disrupt your timing,
use this flag combination (the -run=^$ regex tells Go to match zero standard tests).
```bash
go test ../../../gp-meta-repo/go-logger -bench=. -benchmem -run=^$ -benchtime=5s
```

### Isolated Fallback Boundary Verification
If you are modifying code and want a fast check to ensure your safety fallback logic isn't broken, run this. It executes in
milliseconds because it skips the millions of benchmark loops.
```bash
go test ../../../gp-meta-repo/go-logger -v -run=TestLogger_PayloadExceedsPoolSize -benchtime=1s
```

## 📄 License

Distributed under the Apache License 2.0. See `LICENSE` for more information.
