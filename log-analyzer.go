package main

import (
	"fmt"
	"os"
	"bufio"
	"time"
	"regexp"
	"strconv"
	"strings"
	"flag"
)

var (
	logFile = flag.String("log-file", "kube-apiserver.log", "Name of the apiserver log file to analyze.")
	outputFile = flag.String("output-file", "output.csv", "Name of the output file")
)

func main() {
	fmt.Println("Hello World!")

	file, err := os.Open(*logFile)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	entries := []*Entry{}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		re := regexp.MustCompile("^I\\d+\\s+([0-9:\\.]+)\\s+[^\\]]+]\\s+([A-Z]+)\\s+([^:]+):\\s+\\(([^\\)]+)\\)\\s+(\\d+)\\s+\\[([^\\s]+)\\s+")

		groups := re.FindStringSubmatch(line)
		if len(groups) > 0 {
			t, _ := time.Parse("15:04:05.000000", groups[1])
			t = t.AddDate(2019, 00, 00)
			latency, _ := time.ParseDuration(groups[4])
			responseCode, _ := strconv.Atoi(groups[5])

			entry := Entry{
				time: t,
				method: groups[2],
				path: groups[3],
				latency: latency,
				responseCode: responseCode,
				caller: groups[6],
			}

			entries = append(entries, &entry)
		}
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}

	fmt.Println(len(entries))
	f, err := os.Create(*outputFile)
	if err != nil { panic(err) }
	defer f.Close()

	w := bufio.NewWriter(f)
	fmt.Fprintln(w, Header)
	for _, e := range entries {
		fmt.Fprintln(w, e.toString())
	}
	w.Flush()
}

type Entry struct {
	time time.Time
	method string
	path string
	latency time.Duration
	responseCode int
	caller string
}

func (e *Entry) isWatch() bool {
	return strings.Contains(e.path, "watch=true")
}

const Header = "Time,Method,Path,Latency,ResponseCode,Caller"

func (e *Entry) toString() string {
	return fmt.Sprintf("%d,%s,%s,%d,%d,%s", e.time.UnixNano(), e.method, e.path, e.latency, e.responseCode, e.caller)
}



