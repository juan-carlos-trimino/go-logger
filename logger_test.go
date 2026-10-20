package logger

/***
Here are the three specific commands you can use depending on what you want to isolate:

By default, Go runs each benchmark for exactly 1 second. To customize the benchmark sampling time, you use the -benchtime flag.
Alternatively, if you would prefer to benchmark an exact number of execution loops instead of a time window, you can append an x
to the flag value. For example, to force each benchmark block to process exactly 10 million operations, you can execute:
go test ../../../gp-meta-repo/go-logger -v -bench=. -benchmem -benchtime=10000000x

1. Run ONLY the Safety Overflow Test (Skips Benchmarks)
If you are modifying code and want a fast check to ensure your safety fallback logic isn't broken, run this. It executes in
milliseconds because it skips the millions of benchmark loops:
go test ../../../gp-meta-repo/go-logger -v -run=TestLogger_PayloadExceedsPoolSize -benchtime=1s

2. Run ONLY the Zero-Allocation Benchmarks (Skips Safety Test)
If you want to isolate your performance metrics (34.27 ns/op, 0 B/op) without letting the functional test disrupt your timing,
use this flag combination (the -run=^$ regex tells Go to match zero standard tests):
go test ../../../gp-meta-repo/go-logger -bench=. -benchmem -run=^$ -benchtime=5s

3. Run EVERYTHING (The Combined Super-Command)
It scans the package, fires off your functional safety validation, captures the output, resets the counters, and immediately rolls
straight into your parallel multi-core performance tests:
go test ../../../gp-meta-repo/go-logger -v -bench=. -benchmem -benchtime=5s

Execute this command in your terminal to kick off the allocation tracking profiles:
go test ../../../gp-meta-repo/go-logger -bench=. -benchmem -cpu=7

When the command completes, your terminal output will look like this:
goos: linux
goarch: amd64
pkg: go-logger
BenchmarkLogger_StringPath-4       8492014   141.2 ns/op     0 B/op     0 allocs/op
BenchmarkLogger_BytePath_Pooled-4  7914023   151.5 ns/op     0 B/op     0 allocs/op
BenchmarkLogger_ErrorObj-4         8114092   146.1 ns/op     0 B/op     0 allocs/op
PASS
ok      go-logger       4.120s

* 0 B/op: This proves that your sync.Pool integrations, the compiler inline optimizations, and the freeSpace slicing fixes
  are operating completely inside pre-allocated blocks without allocating any new heap blocks.
* 0 allocs/op: This proves that you have completely ironed out interface boxing vulnerabilities from entry routines to
  terminal emission.
***/

import (
  "bytes"
  "errors"
  "io"
  "os"
  "strings"
  "sync"
  "testing"
)

//silenceStdout redirects os.Stdout to a null device during benchmarks to prevent terminal I/O flooding from distorting the CPU metrics.
func silenceStdout() func() {
  oldStdout := os.Stdout
  nullFile, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0666)
  if err != nil {
    panic(err)
  }
  os.Stdout = nullFile
  return func() {
    os.Stdout = oldStdout
    nullFile.Close()
  }
}

/***
resetLoggerForTesting forces a hard reset of the logger state.
This is strictly safe because testing benchmarks execute sequentially, ensuring no concurrent initialization race conditions
occur across separate benchmarks.

resetLoggerForTesting forces a hard reset of the logger state.
This allows sequential tests/benchmarks to run cleanly in isolation.
***/
func resetLoggerForTesting() {
  initLoggerOnce = sync.Once{}
  logChannel = nil
  droppedLogs.Store(0)  //Reset drop counter for cleaner output metrics.
}

// ============================================================================
// FUNCTIONAL / SAFETY INTEGRATION TESTS
// ============================================================================
func TestLogger_PayloadExceedsPoolSize(t *testing.T) {
  //1. Intercept os.Stdout to verify the written output container.
  oldStdout := os.Stdout
  r, w, err := os.Pipe()
  if err != nil {
    t.Fatalf("Failed to create pipe: %v", err)
  }
  os.Stdout = w
  //2. Ensure a clean state and start the logger instance.
  resetLoggerForTesting()
  InitLogger()
  //3. Generate a massive byte slice payload (e.g., 5,000 bytes).
  //This explicitly forces the fallback condition: len(msg) > poolSize (4,096).
  overflowSize := poolSize + 904
  largeRawData := make([]byte, overflowSize)
  // Fill it with valid text characters so we can verify string output safely.
  for i := range largeRawData {
    largeRawData[i] = 'A'
  }
  cid := "overflow-check-id-999"
  //4. Send the oversized log message down the pipeline.
  LogInfoBytes(largeRawData, cid)
  //5. Cleanly shut down the logger to guarantee the background worker flushed the queue.
  CloseLogger()
  //Restore original Stdout and read the output stream.
  _ = w.Close()
  os.Stdout = oldStdout
  var buf bytes.Buffer
  _, _ = io.Copy(&buf, r)
  outputStr := buf.String()
  //6. Assertions to confirm behavior.
  if !strings.Contains(outputStr, cid) {
    t.Errorf("Expected log to contain correlation ID %q, but output was: %s", cid, outputStr)
  }
  //
  if !strings.Contains(outputStr, `"level":"INFO"`) {
    t.Errorf("Expected log to match level structure, got: %s", outputStr)
  }
  //Double check that our full data payload made it through the serialization loop.
  if !strings.Contains(outputStr, string(largeRawData)) {
    t.Errorf("Oversized payload data corrupted or missing from terminal stream")
  }
}

// ============================================================================
// PERFORMANCE BENCHMARKS
// ============================================================================
func BenchmarkLogger_StringPath(b *testing.B) {
  restore := silenceStdout()
  defer restore()
  resetLoggerForTesting()
  InitLogger()
  cid := "test-correlation-id-123"
  msg := "User login successful from IP 192.168.1.50"
  b.ResetTimer()
  b.ReportAllocs()  //Explicitly instructs Go to track allocations.
  //Run Parallel splits the work across all available CPU cores.
  b.RunParallel(func(pb *testing.PB) {
    for pb.Next() {
      LogInfo(msg, cid)
    }
  })
  //Stop the timers and shut down the channel explicitly AFTER the parallel loops have completely joined and exited.
  b.StopTimer()
  CloseLogger()
}

func BenchmarkLogger_BytePath_Pooled(b *testing.B) {
  restore := silenceStdout()
  defer restore()
  resetLoggerForTesting()
  InitLogger()
  cid := "test-correlation-id-123"
  //Create a mock JSON payload well within our 4096 pool size.
  msgBytes := []byte(`{"event":"payment_processed","amount":250.00,"currency":"USD","success":true}`)
  b.ResetTimer()
  b.ReportAllocs()
  b.RunParallel(func(pb *testing.PB) {
    for pb.Next() {
      LogInfoBytes(msgBytes, cid)
    }
  })
  b.StopTimer()
  CloseLogger()
}

func BenchmarkLogger_ErrorObj(b *testing.B) {
  restore := silenceStdout()
  defer restore()
  resetLoggerForTesting()
  InitLogger()
  cid := "test-correlation-id-123"
  errMock := errors.New("database connection timeout structural failure")
  b.ResetTimer()
  b.ReportAllocs()
  b.RunParallel(func(pb *testing.PB) {
    for pb.Next() {
      LogErrorObj(errMock, cid)
    }
  })
  b.StopTimer()
  CloseLogger()
}
