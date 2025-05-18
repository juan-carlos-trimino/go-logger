package logger

/***
Create repo and clone it.
$ git clone https://github.com/juan-carlos-trimino/gplogger.git

Initialize Go.
$ cd gplogger
Execute "go mod init github.com/{GitHub-Username}/{Repo-Name}
$ go mod init github.com/juan-carlos-trimino/gplogger

Create the file "main.go" and add the code to it.

Commit and push the code.
$ git add .
$ git commit -m "initial commit."
$ git push origin main

Go uses "Git Tags" to manage versions of the code. Create the tag and push it.
When code is pushed to the repo, repeat these two steps; ensure the version is changed accordingly.
$ git tag "v1.0.0"
$ git push origin main --tags

To use the package, install it (go get -u {copy the repo url from GitHub}).
$ go get -u github.com/juan-carlos-trimino/gplogger

Next, open the file that will use the package and add this line
("github.com/{GitHub-Username}/{Repo-Name}").

import "github.com/juan-carlos-trimino/gplogger"

To upgrade/downgrade the version of the package, move to the root of the module's directory
structure (where the go.mod file is located) and execute
(go get -u "{package-name}@{git-commit-hash}").
$ go get -u "github.com/juan-carlos-trimino/gplogger@f218b1c"
***/

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
