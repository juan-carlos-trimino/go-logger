//To fold all block comments:
//  Ctrl+K and Ctrl+/
//To unfold all block comments:
//  Ctrl+K and Ctrl+J
//
//Build constraint: Only include this file in the build if the target OS is Linux and NOT Windows.
//Note: There must be a blank line between the build constraints and the 'package' keyword.
//go:build linux && !windows

package logger

import (
  "bytes"
  "encoding/json"
  "os"
  "strconv"
  "strings"
  "sync"
  "sync/atomic"
  "time"
)

/***
Implementation of a high-throughput logging library: It integrates Go 1.18+ Generics, sync.Pool memory recycling, an Asynchronous
Channel Pipeline, and the Non-Blocking Emergency Fallback Engine.

To handle heavy customer traffic without slowing down your HTTP web requests, you must remove synchronous terminal/disk I/O from
the request lifecycle. Writing directly to os.Stdout inside a request thread acts like a brake pedal. If your terminal is slow or
your disk utilization spikes, your web request gets blocked waiting for the log line to print.

By routing logs through an Asynchronous Channel, your web request goroutines simply drop the log payload into an in-memory queue
and instantly continue serving the customer. A dedicated background worker routine processes the queue and handles the actual writing.

Why this architecture survives critical enterprise scales:
* True Non-Blocking Execution: Your HTTP request routines execute a swift memory-copy operation via queueMessage and instantly free
  up the thread to accept the next concurrent customer request.
* Serialized I/O Streaming: By dedicating a single background worker thread to manage all writes to os.Stdout, you completely
  eliminate terminal resource contention locks across simultaneous request scopes.
* Zero System Waste: Combining carrier pools (payloadPool) with encoding buffer recycles (bufferPool) allows you to loop through
  millions of rows without triggering memory spikes or GC pauses.
***/

const (
  //The async queue capacity (up to 50,000 log bursts in memory before blocking).
  logQueueCapacity = 50_000
)

var (
  logChannel chan asyncLogPayload
  workerWg sync.WaitGroup  //Declared and ready to use immediately.
  /***
  In Go, a sync.Pool is a concurrent-safe cache of temporary objects used to reduce memory allocations and pressure on the
  garbage collector (GC). Objects stored in a sync.Pool can be cleared at any time by the garbage collector without notice. It
  is strictly for temporary items.
  ***/
  bufferPool = sync.Pool{
    //New defines how to generate a fresh object if the pool is empty.
    New: func() any {
      return new(bytes.Buffer)
    },
  }
  //A dedicated pool for raw byte slice snapshots; it allocates fixed 4KB blocks to prevent frequent heap resizing.
  snapshotPool = sync.Pool{
    New: func() any {
      b := make([]byte, 4096)
      return &b  //Pass a pointer to avoid interface boxing overhead.
    },
  }
  droppedLogs uint64 = 0  //Atomic counter for dropped logs.
  /***
  Signal the monitoring routine to stop on shutdown. In Go, an empty struct (struct{}) occupies exactly 0 bytes of memory.
  Developers use it to create pure signaling channels.
  ***/
  monitorStopChan chan struct{}
)

//Generic constraint that limits parameters strictly to string or byte slices.
type Loggable interface{
  string | []byte
}

//Internal payload carrier used to pass messages safely across channels.
type asyncLogPayload struct{
  msgStr   string  //Separate fields prevent string-to-byte mutations.
  msgBytes []byte
  cid      string
  level    string
  isPooled bool
}

// =========================================================================
// LIFECYCLE MANAGEMENT (With Background Ticker & Graceful Flush)
// =========================================================================
/***
Initialize the logging engine pipeline. Call InitLogger() ONCE from the main.go init or main function;
ensure it is called before any log statements.
***/
func InitLogger() {
  //A channel that can hold logQueueCapacity asyncLogPayload before blocking the sender.
  logChannel = make(chan asyncLogPayload, logQueueCapacity)
  /***
  Initialize an unbuffered channel.
  * The sending goroutine blocks until another goroutine receives from the channel.
  * The receiving goroutine blocks until another goroutine sends data into the channel.
  ***/
  monitorStopChan = make(chan struct{})
  //Spin up a single dedicated background worker thread to process all terminal write I/O.
  workerWg.Add(1)
  go logWorker()
  //Kick off the background telemetry routine
  startDroppedLogsMonitor()
}

//Call CloseLogger() when the main application shuts down to flush remaining logs.
func CloseLogger() {
  //Stop the background telemetry ticker cleanly.
  close(monitorStopChan)
  //Shut down the log stream channel and wait for the worker to finish flushing.
  close(logChannel)
  workerWg.Wait()  //Wait for the background worker to safely print out everything left in the queue.
  //Perform a final, definitive count check of total drops upon application exit.
  if finalDrops := atomic.LoadUint64(&droppedLogs); finalDrops > 0 {
    /***
    Ignore the number of bytes written and error. Unlike os.Stderr.Write(), it does not require to convert the string
    into a []byte slice first, saving a memory allocation.
    ***/
    _, _ = os.Stderr.WriteString("LOGGER SHUTDOWN: Application terminated with a total of " +
      strconv.FormatUint(finalDrops, 10/*base*/) + " dropped logs due to channel saturation.\n")
  }
}

//startDroppedLogsMonitor tracks logging pipeline health every 10 seconds.
func startDroppedLogsMonitor() {
  go func() {
    /***
    This code sets up a time-based recurring interval that fires an event every 10 seconds and ensures its underlying OS
    resources are cleaned up when the surrounding function finishes.
    ***/
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()  //Guarantees cleanup when function exits.
    var lastLoggedDrops uint64 = 0
    for {
      //If none of the channels are ready to send or receive data, the goroutine completely pauses at the select statement.
      select {
      case <-ticker.C:
        currentDrops := atomic.LoadUint64(&droppedLogs)
        if currentDrops > lastLoggedDrops {
          newDrops := currentDrops - lastLoggedDrops
          lastLoggedDrops = currentDrops
          //Forcing an unbuffered stderr write bypasses the stuck worker channel.
          _, _ = os.Stderr.WriteString("ALERT: Logging queue capacity saturated! Dropped " + strconv.FormatUint(newDrops, 10) +
            " message(s) in the last 10s. Total dropped so far: " + strconv.FormatUint(currentDrops, 10) + "\n")
        }
      case <-monitorStopChan:
        return  //Gracefully exit the loop when CloseLogger is called.
      }
    }
  }()
}

// =========================================================================
// PUBLIC API FUNCTIONS
// =========================================================================
// STRING API (Zero Allocations)
// No Heap Allocations (Strings are immutable)
// =========================================================================
func LogInfo[T Loggable](msg T, cid string) { queueMessage(msg, cid, "INFO") }
func LogWarning[T Loggable](msg T, cid string) { queueMessage(msg, cid, "WARN") }
func LogError[T Loggable](msg T, cid string) { queueMessage(msg, cid, "ERROR") }
// =========================================================================
// EXPLICIT ERROR API (Handles native error interfaces cleanly)
// =========================================================================
func LogErrorObj(err error, cid string) {
  if err != nil {
    queueMessage(err.Error(), cid, "ERROR")
  }
}

func DatetimeFormat() string {
  //time.Now() returns the current local time; using the current time in UTC.
  return time.Now().UTC().Format(time.RFC3339Nano)
}

func queueMessage[T Loggable](msg T, cid, level string) {
  var payload asyncLogPayload
  payload.cid = cid
  payload.level = level
  payload.isPooled = false
  switch v := any(msg).(type) {
  case string:
    payload.msgStr = v
  case []byte:
    //Only use the pool if the log fits within the standard 4KB block size.
    if len(v) <= 4096 {
      poolPtr := snapshotPool.Get().(*[]byte)
      snapshot := (*poolPtr)[:len(v)]  //Slice it to exact match length.
      copy(snapshot, v)
      payload.msgBytes = snapshot
      payload.isPooled = true  //Flag so the worker knows to return it!
    } else {  //Fallback to a standard allocation for exceptionally large payloads.
      //Only allocate and snapshot here because []byte is mutable and could change before the async worker processes the channel.
      snapshot := make([]byte, len(v))
      /***
      Isolate the memory immediately on the calling thread. The payload carries a unique pointer (snapshot) that no other goroutine
      in the application has access to, except for the background logWorker. This step does cost one memory allocation per byte slice
      log, but it is 100% required to ensure memory isolation and data thread safety when dealing with mutable slices across channels.
      ***/
      copy(snapshot, v)
      payload.msgBytes = snapshot
    }
  }
  /***
  Implement the non-blocking, lossy emergency fallback mechanism.
  * Queue has room: When the logging volume is normal, the 'case logChannel <- payload:' statement succeeds immediately.
  * Queue is full: If the logging channel fills up entirely, a standard channel send statement like 'logChannel <- payload' would block
    the thread and freeze the application. This means user HTTP requests would stall, cascading into a major outage. By adding the
    'default:' branch, the Go runtime instantly skip the channel and run the code inside the default block instead.
  ***/
  select {
  case logChannel <- payload:
  default:
    atomic.AddUint64(&droppedLogs, 1)
    //If the queue is full and we drop the log, return the memory to the pool immediately!
    if payload.isPooled {
      /***
      When you execute (*poolPtr)[:len(v)] to fit a message exactly, you alter the slice header's length. Since Go's
      sync.Pool stores the actual slice descriptor, you must reset the length of the slice back to its maximum capacity
      (:cap(...)) right before calling .Put(). This guarantees that whoever rents that block next receives the full 4KB
      array capacity intact.
      ***/
      fullSlice := payload.msgBytes[:cap(payload.msgBytes)]
      snapshotPool.Put(&fullSlice)
    }
  }
}



// ==========================================
// BACKGROUND WORKER
// Zero-Allocation (Reuses pool buffers directly)
// ==========================================
func logWorker() {
  defer workerWg.Done()
  for item := range logChannel {
    //Borrow an object from the pool (returns an 'any' type, so assert it).
    buf := bufferPool.Get().(*bytes.Buffer)
    //Always reset the object's internal state before or after reuse.
    buf.Reset()
    //Manually building JSON layout avoids encoding/json reflection overhead
    buf.WriteString(`{"date_time":"`)
    buf.WriteString(time.Now().UTC().Format(time.RFC3339Nano))
    buf.WriteString(`","level":"`)
    buf.WriteString(item.level)
    buf.WriteString(`","correlation_id":"`)
    buf.WriteString(item.cid)
    buf.WriteString(`","msg":`)
    //Process the message content based on what was provided.
    if item.msgBytes != nil {
      trimmed := bytes.TrimSpace(item.msgBytes)
      //Check if it's already a structural JSON block (like an object or array)
//checking for a length of at least 2 bytes (> 1) is the correct mathematical representation of a JSON block. A valid JSON object or array requires both an opening and a closing delimiter (like {} or []), meaning it must be at least 2 bytes long.
      if len(trimmed) > 1 && ((trimmed[0] == '{' && trimmed[len(trimmed) - 1] == '}') ||
                              (trimmed[0] == '[' && trimmed[len(trimmed) - 1] == ']')) {
        buf.Write(trimmed)
      } else {
        //Otherwise, safely escape it as a string
        b, _ := json.Marshal(string(item.msgBytes))
        buf.Write(b)
      }
    } else {
      msgStr := item.msgStr
      length := len(msgStr)
      isJSON := false

      // Fast-path: Check if the string naturally starts and ends with brackets (no spaces)
      if length > 1 && ((msgStr[0] == '{' && msgStr[length - 1] == '}') ||
                        (msgStr[0] == '[' && msgStr[length - 1] == ']')) {
        isJSON = true
      } else {
        // Slow-path: Only run standard trimming if it didn't pass the fast-path.
        // strings.TrimSpace cleanly handles both ASCII and Unicode if an emoji ever slips in.
        trimmedStr := strings.TrimSpace(msgStr)
        tLen := len(trimmedStr)
        if tLen >= 2 && ((trimmedStr[0] == '{' && trimmedStr[tLen-1] == '}') ||
                        (trimmedStr[0] == '[' && trimmedStr[tLen-1] == ']')) {
          msgStr = trimmedStr
          isJSON = true
        }
      }

      if isJSON {
        buf.WriteString(msgStr)
      } else {
        // Safely escape normal text strings or logs with messages
        b, _ := json.Marshal(item.msgStr)
        buf.Write(b)
      }
      //It was a string or error type payload. Check if it happens to contain a raw JSON string
      // while handling string escaping efficiently.
      // ACTUALLY trim the space so the JSON structural check is accurate
      // trimmed := strings.TrimSpace(item.msgStr)

      // if len(trimmed) > 1 && ((trimmed[0] == '{' && trimmed[len(trimmed) - 1] == '}') ||
      //                            (trimmed[0] == '[' && trimmed[len(trimmed) - 1] == ']')) {
      //   buf.WriteString(trimmed)
      // } else {
      //   b, _ := json.Marshal(item.msgStr)
      //   buf.Write(b)
      // }
    }
    //Adding a newline character (\n) right after the closing brace acts as a delimiter.
    //If you omit the newline, multiple distinct log logs will clump together into a single, massive string line (e.g., {"date_time":...}{"date_time":...}).
    //Almost all enterprise log management utilities rely strictly on the JSON Lines (JSONL) format, which mandates that every single independent JSON object must sit cleanly on its own individual text line.
    buf.WriteString("}\n")
    //Perform a single write call per log line
    _, _ = os.Stdout.Write(buf.Bytes())
    //Return the object back to the pool when done.
    bufferPool.Put(buf)
    //Return the snapshot memory to the pool.
    if item.isPooled {
      //Re-slice to its full underlying array capacity before returning.
      fullSlice := item.msgBytes[:cap(item.msgBytes)]
      snapshotPool.Put(&fullSlice)
    }
  }
}












//queueMessage isolates memory snapshots and shifts routing to the async loop or fallback valves.
// func queueMessage(msg []byte, cid, level string) {
//   /***
//   Allocate a fresh slice exactly matching the size of this specific message payload.
//   This isolates the memory so the HTTP request thread can never overwrite it.
//   ***/
//   snapshot := make([]byte, len(msg))
//   copy(snapshot, msg)
//   payload := asyncLogPayload{
//     msg:   snapshot,
//     cid:   cid,
//     level: level,
//   }
//   //Use a 'select' statement with a 'default' branch to avoid blocking.
//   select {
//   case logChannel <- payload:
//     //Queue has room, message safely dropped into the channel loop!
//   default:
//     /***
//     An emergency fallback mechanism is a safety valve for your logging pipeline. It defines what your application should do when
//     your asynchronous queue fills up completely (reaches its 50,000 capacity limit) during a sudden traffic spike or a downstream
//     disk slow-down.

//     In a standard channel implementation, when a channel reaches capacity, it blocks the calling thread. If your logger blocks,
//     all of your incoming user HTTP requests will freeze, cascading into a major application outage.

//     Under a heavy load, you must choose how your application responds if the queue fills up. The strategy implemented here is the
//     "Drop Lower-Priority Logs"; i.e., if the queue fills up, you drop minor logs (like INFO or WARN) to make room for critical logs
//     and keep user HTTP requests moving at full speed.
//     ***/
//     //EMERGENCY FALLBACK: Queue is full!
//     if level == "ERROR" {
//       //Sink critical errors into valid, structural JSON directly to Stderr.
//       var rawMsg json.RawMessage
//       trimmed := bytes.TrimSpace(snapshot)
//       if len(trimmed) > 0 && ((trimmed[0] == '{' && trimmed[len(trimmed) - 1] == '}') ||
//                               (trimmed[0] == '[' && trimmed[len(trimmed) - 1] == ']')) {
//         rawMsg = trimmed
//       } else {
//         safeBytes, _ := json.Marshal(string(snapshot))
//         rawMsg = safeBytes
//       }
//       //Ensure it still outputs as a valid JSON structural block for the central log collector.
//       le := logEntry{
//         DateTime:      time.Now().UTC().Format(time.RFC3339Nano),
//         CorrelationId: cid,
//         Level:         "ERROR_OVERFLOW",  //Clearly identifies dropped traffic failovers.
//         Message:       rawMsg,
//       }
//       //Marshal it safely.
//       if jsonData, err := json.Marshal(le); err == nil {
//         jsonData = append(jsonData, '\n')
//         //If it's a critical error, force-write it synchronously to stderr so it's not lost.
//         _, _ = os.Stderr.Write(jsonData)
//       }
//     }
//     //For other message logs (e.g., INFO or WARN), they get dropped silently here to protect application uptime.
//   }
// }
