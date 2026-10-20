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
  "os"
  "strconv"
  "strings"
  "sync"
  "sync/atomic"
  "time"
)

/***
Implementation of a high-throughput logging library: It integrates sync.Pool memory recycling, an Asynchronous Channel Pipeline,
and the Non-Blocking Emergency Fallback Engine.

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
  //Pool allocates fixed-size byte chunks, which easily accommodate most log lines.
  poolSize = 4_096
)

var (
  logChannel chan logPayload
  workerWg sync.WaitGroup  //Declared and ready to use immediately.
  /***
  In Go, a sync.Pool is a concurrent-safe cache of temporary objects used to reduce memory allocations and pressure on the
  garbage collector (GC). Objects stored in a sync.Pool can be cleared at any time by the garbage collector without notice. It
  is strictly for temporary items.
  ***/
  bufferPool = sync.Pool{
    //New defines how to generate a fresh object if the pool is empty.
    New: func() any {
      return &bytes.Buffer{}
    },
  }
  //A dedicated pool for raw byte slice snapshots; it allocates fixed 4KB blocks to prevent frequent heap resizing.
  snapshotPool = sync.Pool{
    New: func() any {
      b := make([]byte, poolSize)  //The capacity defaults to the specified length.
      return &b  //Pass a pointer to avoid interface boxing overhead.
    },
  }
  droppedLogs atomic.Uint64  //Atomic counter for dropped logs.
  /***
  Signal the monitoring routine to stop on shutdown. In Go, an empty struct (struct{}) occupies exactly 0 bytes of memory.
  Developers use it to create pure signaling channels.
  ***/
  monitorStopChan chan struct{}
  initLoggerOnce sync.Once  //Zero value is ready to use; do not copy after use.
)

//Internal payload carrier used to pass messages safely across channels.
type logPayload struct{
  msgStr   string  //Separate fields prevent string-to-byte mutations.
  msgBytes []byte
  poolSlice *[]byte  //Hold the exact pointer returned by Get().
  cid      string
  level    string
  isPooled bool  //Zero value is 'false'.
}

// =========================================================================
// LIFECYCLE MANAGEMENT (With Background Ticker & Graceful Flush)
// =========================================================================
/***
Initialize the logging engine pipeline. Call InitLogger() ONCE from the main.go function;
ensure it is called before any log statements.
***/
func InitLogger() {
  initLoggerOnce.Do(func() {
    droppedLogs.Store(0)
    //A channel that can hold logQueueCapacity of logPayload before blocking the sender.
    logChannel = make(chan logPayload, logQueueCapacity)
    /***
    Initialize an unbuffered channel.
    * The sending goroutine blocks until another goroutine receives from the channel.
    * The receiving goroutine blocks until another goroutine sends data into the channel.
    ***/
    monitorStopChan = make(chan struct{})
    //Spin up a single dedicated background worker thread to process all terminal write I/O.
    workerWg.Add(1)  //Increase the WaitGroup counter by exactly 1.
    go logWorker()
    //Kick off the background telemetry routine
    go startDroppedLogsMonitor()
  })
}

//Call CloseLogger() when the main application shuts down to flush remaining logs.
func CloseLogger() {
  if logChannel == nil { return }
  //Stop the background telemetry ticker cleanly.
  close(monitorStopChan)
  //Shut down the log stream channel and wait for the worker to finish flushing.
  close(logChannel)  //TO AVOID A DEADLOCK, DO NOT PLACE THIS LINE AFTER THE WAIT(); SEE THE logWorker.
  workerWg.Wait()  //Wait for the background worker to safely print out everything left in the queue.
  //Perform a final, definitive count check of total drops upon application exit.
  if finalDrops := droppedLogs.Load(); finalDrops > 0 {
    /***
    Ignore the number of bytes written and any error.
    Unlike os.Stderr.Write(), it does not require to convert the string into a []byte slice first, saving a
    memory allocation.
    ***/
    _, _ = os.Stderr.WriteString("LOGGER SHUTDOWN: Application terminated with a total of " +
      strconv.FormatUint(finalDrops, 10/*base*/) + " dropped log(s) due to channel saturation.\n")
  }
}

/***
startDroppedLogsMonitor tracks logging pipeline health every 10 seconds.
This function runs permanently in the background as a dedicated goroutine.
***/
func startDroppedLogsMonitor() {
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
      if currentDrops := droppedLogs.Load(); currentDrops > lastLoggedDrops {
        newDrops := currentDrops - lastLoggedDrops
        lastLoggedDrops = currentDrops
        //Forcing an unbuffered stderr write bypasses the stuck worker channel.
        _, _ = os.Stderr.WriteString("ALERT: Logging queue capacity saturated! Dropped " + strconv.FormatUint(newDrops, 10) +
          " log(s) in the last 10s. Total dropped so far: " + strconv.FormatUint(currentDrops, 10) + "\n")
      }
    case <-monitorStopChan:
      return  //Gracefully exit the loop when CloseLogger is called.
    }
  }
}

// =========================================================================
// PUBLIC API FUNCTIONS
// =========================================================================
// STRING API (Zero Allocations)
// No Heap Allocations (Strings are immutable)
// =========================================================================
func LogInfo(msg, cid string) { queueStringMessage(msg, cid, "INFO") }
func LogInfoBytes(msg []byte, cid string) { queueBytesMessage(msg, cid, "INFO") }
func LogWarning(msg, cid string) { queueStringMessage(msg, cid, "WARN") }
func LogWarningBytes(msg []byte, cid string) { queueBytesMessage(msg, cid, "WARN") }
func LogError(msg, cid string) { queueStringMessage(msg, cid, "ERROR") }
func LogErrorBytes(msg []byte, cid string) { queueBytesMessage(msg, cid, "ERROR") }
// =========================================================================
// EXPLICIT ERROR API (Handles native error interfaces cleanly)
// =========================================================================
func LogErrorObj(err error, cid string) {
  if err != nil {
    queueStringMessage(err.Error(), cid, "ERROR")
  }
}

func DatetimeFormat() string {
  //time.Now() returns the current local time; using the current time in UTC.
  return time.Now().UTC().Format(time.RFC3339Nano)
}

func queueStringMessage(msg, cid, level string) {
  /***
  Explicit Enforcer: If the check is true, it forces a fast-fail crash, completely preventing unmanaged goroutines and memory
  leaks from sliding into the production environment undetected.
  ***/
  if logChannel == nil {
    panic("CRITICAL: Logger used before initialization! Ensure InitLogger() AND defer CloseLogger run at application startup.")
  }
  var payload logPayload
  payload.msgStr = msg
  payload.cid = cid
  payload.level = level
  /***
  Implement the non-blocking emergency fallback mechanism.
  * Queue has room: When the logging volume is normal, the 'case logChannel <- payload:' statement succeeds immediately.
  * Queue is full: If the logging channel fills up entirely, a standard channel send statement like 'logChannel <- payload'
    would block the thread and freeze the application. This means user HTTP requests would stall, cascading into a major outage.
    By adding the 'default:' branch, the Go runtime instantly skip the channel and run the code inside the default block instead.
  ***/
  select {
  case logChannel <- payload:
  default:
    droppedLogs.Add(1)
  }
}

func queueBytesMessage(msg []byte, cid, level string) {
  if logChannel == nil {
    panic("CRITICAL: Logger used before initialization! Ensure InitLogger() AND defer CloseLogger run at application startup.")
  }
  var payload logPayload
  payload.cid = cid
  payload.level = level
  /***
  Isolate the memory immediately on the calling thread.
  The payload carries a unique pointer (snapshot) that no other goroutine in the application has access to,
  except for the background logWorker.
  ***/
  if len(msg) <= poolSize {
    //Only use the pool if the log fits within the standard 4KB block size.
    poolPtr := snapshotPool.Get().(*[]byte)
    payload.poolSlice = poolPtr  //Save the exact pointer container.
    snapshot := (*poolPtr)[:len(msg)]  //Slice it to exact match length.
    copy(snapshot, msg)
    payload.msgBytes = snapshot
    payload.isPooled = true  //Flag so the worker knows to return it!
  } else {
    /***
    Fallback to a standard allocation for exceptionally large payloads. This step does cost one memory allocation per byte
    slice log, but it is 100% required to ensure memory isolation and data thread safety when dealing with mutable slices
    across channels. That is, only allocate and snapshot here because []byte is mutable and could change before the async
    worker processes the channel.
    ***/
    snapshot := make([]byte, len(msg))
    copy(snapshot, msg)
    payload.msgBytes = snapshot
  }
  //
  select {
  case logChannel <- payload:
  default:
    droppedLogs.Add(1)
    //If the queue is full and we drop the log, return the memory to the pool immediately!
    if payload.isPooled {
      /***
      When you execute (*poolPtr)[:len(msg)] to fit a message exactly, you alter the slice header's length. Since Go's
      sync.Pool stores the actual slice descriptor, you must reset the length of the slice back to its maximum capacity
      (:cap(...)) right before calling .Put(). This guarantees that whoever rents that block next receives the full 4KB
      array capacity intact.

      Re-slice the underlying array back to full capacity inside the original container.
      ***/
      *payload.poolSlice = (*payload.poolSlice)[:cap(*payload.poolSlice)]
      snapshotPool.Put(payload.poolSlice)  //Pass the original valid heap pointer.
    }
  }
}

// =========================================================================
// BACKGROUND WORKER
// Zero-Allocation (Reuses pool buffers directly)
// =========================================================================
func logWorker() {
  defer workerWg.Done()  //Decrease the WaitGroup counter by exactly 1.
  /***
  The loop blocks and processes logs as they arrive.
  When logChannel is closed, it cleanly drains all remaining logs. SEE CloseLogger TO AVOID A DEADLOCK.
  ***/
  for payload := range logChannel {
    /***
    bufferPool.Get(): This asks the sync.Pool for an object. If the pool has a previously used bytes.Buffer sitting inside it,
    it hands it back to you. If the pool is empty, it runs your 'New:' function to allocate a fresh one.
    .(*bytes.Buffer): Because Get() returns Go's empty interface type (any), you must use this syntax -- called a type assertion --
    to tell the compiler, "I know this object is a pointer to a bytes.Buffer, treat it as one."
    ***/
    buf := bufferPool.Get().(*bytes.Buffer)
    /***
    This empties the buffer's internal slice, making it completely blank and ready for a new log message. But while .Reset() sets
    the string length back to zero, it does not throw away the underlying byte slice allocation. The capacity remains intact.

    Without a sync.Pool and .Reset(), writing 1,000,000 log messages requires you to create 1,000,000 separate buffer objects. This
    floods your computer's memory and forces Go's Garbage Collector (GC) to constantly run to clean them up, which slows down your
    application.

    By combining Get() and Reset(), your application might only ever allocate a handful of buffers. Goroutines constantly cycle
    through them -- borrowing, clearing, using, and returning them -- resulting in near-zero memory allocations and a much faster
    application.
    ***/
    buf.Reset()
    //Manually building JSON layout avoids encoding/json reflection overhead.
    buf.WriteString(`{"date_time":"`)
    buf.WriteString(time.Now().UTC().Format(time.RFC3339Nano))
    buf.WriteString(`","level":"`)
    buf.WriteString(payload.level)
    buf.WriteString(`","correlation_id":"`)
    buf.WriteString(payload.cid)
    buf.WriteString(`","msg":`)
    //Process the message content based on what was provided.
    if payload.msgBytes != nil {
      payloadTrimmed := bytes.TrimSpace(payload.msgBytes)
      payloadTrimmedLen := len(payloadTrimmed)
      /***
      Check if it's already a structural JSON block (like an object or array).
      Checking for a length of at least 2 bytes (> 1) is the correct mathematical representation of a JSON block. A valid JSON
      object or array requires both an opening and a closing delimiter (like {} or []), meaning it must be at least 2 bytes long.
      ***/
      if payloadTrimmedLen > 1 && ((payloadTrimmed[0] == '{' && payloadTrimmed[payloadTrimmedLen - 1] == '}') ||
                                   (payloadTrimmed[0] == '[' && payloadTrimmed[payloadTrimmedLen - 1] == ']')) {
        /***
        If the app passes a pre-formatted JSON string or raw JSON byte slice into the logger, you want to embed it cleanly as a
        nested structural JSON object or array inside your main log payload, rather than treating it as a plain string.
        Nested JSON object:
        {"time":"2026-10-16T23:25:00Z","level":"INFO","msg":{"user_id": 123, "status": "active"}}
        As opposed to a plain string literal (everything would be passed to AppendQuote):
        {"time":"2026-10-16T23:25:00Z","level":"INFO","msg":"{\"user_id\": 123, \"status\": \"active\"}"}
        ***/
        buf.Write(payloadTrimmed)
      } else {
        //Get a slice header representing the empty capacity right after your current data.
        currentLen := buf.Len()
        freeSpace := buf.Bytes()[currentLen:currentLen]  //Zero-length slice sitting at the end of written data.
        /***
        By writing string(payloadTrimmed) directly inside the argument list of the append context, you trigger the compiler's
        specialized fast-path, achieving the speed of a raw pointer copy with zero garbage collection overhead.
        ***/
        //AppendQuote appends to that empty slice using the buffer's backing memory pool.
        quotedBytes := strconv.AppendQuote(freeSpace, string(payloadTrimmed))
        /***
        Write those newly generated bytes directly to the buffer.
        This safely updates the buffer's length and trackers automatically without Truncate.
        ***/
        buf.Write(quotedBytes)
      }
    } else {
      msgStr := payload.msgStr
      msgStrLen := len(msgStr)
      isJSON := false
      //Fast-path: Check if the string naturally starts and ends with brackets (no spaces).
      if msgStrLen > 1 && ((msgStr[0] == '{' && msgStr[msgStrLen - 1] == '}') ||
                           (msgStr[0] == '[' && msgStr[msgStrLen - 1] == ']')) {
        isJSON = true
      } else {
        /***
        Slow-path: Only run standard trimming if it didn't pass the fast-path.
        strings.TrimSpace cleanly handles both ASCII and Unicode if an emoji ever slips in.
        strings.TrimSpace just manipulates the indexes -- it does not allocate a new string structure or duplicate
        the text in memory.
        ***/
        msgStrTrimmed := strings.TrimSpace(msgStr)
        msgStrTrimmedLen := len(msgStrTrimmed)
        if msgStrTrimmedLen > 1 && ((msgStrTrimmed[0] == '{' && msgStrTrimmed[msgStrTrimmedLen - 1] == '}') ||
                                    (msgStrTrimmed[0] == '[' && msgStrTrimmed[msgStrTrimmedLen - 1] == ']')) {
          msgStr = msgStrTrimmed
          isJSON = true
        }
      }
      //
      if isJSON {
        buf.WriteString(msgStr)
      } else {  //Safely escape normal text strings or logs with messages.
        //Get a slice header representing the empty capacity right after your current data.
        currentLen := buf.Len()
        freeSpace := buf.Bytes()[currentLen:currentLen]  //Zero-length slice sitting at the end of written data.
        /***
        Direct, zero-allocation appending of the string message.
        Under the hood, strconv.AppendQuote always takes your input, wraps it in quotes ("), escapes any special
        characters, and appends those characters onto a backing byte slice.
        ***/
        //AppendQuote appends to that empty slice using the buffer's backing memory pool.
        quotedBytes := strconv.AppendQuote(freeSpace, payload.msgStr)
        buf.Write(quotedBytes)
      }
    }
    /***
    Adding a newline character (\n) right after the closing brace acts as a delimiter. If you omit the newline, multiple
    distinct logs will clump together into a single, massive string line (e.g., {"date_time":...}{"date_time":...}).

    Almost all enterprise log management utilities rely strictly on the JSON Lines (JSONL) format, which mandates that
    every single independent JSON object must sit cleanly on its own individual text line.
    ***/
    buf.WriteString("}\n")
    //Perform a single write call per log line.
    _, _ = os.Stdout.Write(buf.Bytes())
    //Return the object back to the pool when done.
    bufferPool.Put(buf)
    //Return the snapshot memory to the pool.
    if payload.isPooled {
      //Re-slice the underlying array back to full capacity inside the original container.
      *payload.poolSlice = (*payload.poolSlice)[:cap(*payload.poolSlice)]
      snapshotPool.Put(payload.poolSlice)  //Pass the original valid heap pointer.
    }
  }
}
