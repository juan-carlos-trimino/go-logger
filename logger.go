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
  "encoding/json"
  "fmt"
  "time"
)

type logEntry struct {
  DateTime      string "json:\"date_time\""
  Level         string `json:"level"`
  CorrelationId string `json:"correlation_id"`
  Message       string `json:"msg"`
}

func LogInfo(msg, cid string) {
  printMessage(msg, cid, "INFO")
}

func LogWarning(msg, cid string) {
  printMessage(msg, cid, "WARN")
}

func LogError(msg, cid string) {
  printMessage(msg, cid, "ERROR")
}

func DatetimeFormat() string {
  //time.Now() returns the current local time; using the current time in UTC.
  return time.Now().UTC().Format(time.RFC3339Nano)
}

func printMessage(msg, cid, level string) {
  le := logEntry{
    DateTime:      DatetimeFormat(),
    CorrelationId: cid,
    Level:         level,
    Message:       msg,
  }
  json, err := json.Marshal(le)
  if err != nil {
    panic(err)
  }
  fmt.Println(string(json))
}
