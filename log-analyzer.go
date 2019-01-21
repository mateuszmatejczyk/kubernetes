package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	logFile = flag.String("log-file", "kube-apiserver.log", "Name of the apiserver log file to analyze.")
	outputFile = flag.String("output-file", "output.csv", "Name of the output file")
	nWorkers = flag.Int("n-workers", 1000, "Number of routines processing file")

	re = regexp.MustCompile("^I\\d+\\s+([0-9:\\.]+)\\s+[^\\]]+]\\s+([A-Z]+)\\s+([^:]+):\\s+\\(([^\\)]+)\\)\\s+(\\d+)\\s+\\[([^\\s]+)\\s+")
)

const (
	stop = ""
)

// TODO(mmat): Create util, refactor code, add options for nParsers and nWrtiers?

func main() {
	flag.Parse()

	fmt.Println("Processing api-server logs...")

	file, err := os.Open(*logFile)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	f, err := os.Create(*outputFile)
	if err != nil { panic(err) }
	defer f.Close()
	w := bufio.NewWriter(f)
	fmt.Fprintln(w, Header)


	workerChan := make(chan string, *nWorkers*20)
	writerChan := make(chan string, *nWorkers*20)
	writerStopChan := make(chan int, *nWorkers)
	mainStopChan := make(chan int)

	worker := func () {
		for {
			line := <-workerChan
			if line == stop {
				writerStopChan <- 1
				return
			}

			groups := re.FindStringSubmatch(line)
			if len(groups) > 0 {
				t, _ := time.Parse("15:04:05.000000", groups[1])
				t = t.AddDate(2019, 00, 00)
				latency, _ := time.ParseDuration(groups[4])
				responseCode, _ := strconv.Atoi(groups[5])

				entry := Entry{
					time:         t,
					method:       groups[2],
					path:         groups[3],
					latency:      latency,
					responseCode: responseCode,
					caller:       groups[6],
				}

				writerChan <- entry.toString()
			}
		}
	}

	for i := 0; i < *nWorkers; i++ {
		go worker()
	}

	go func() {
		c := 0
		activeWorkers := *nWorkers
		for {
			select {
				case line := <-writerChan:
					fmt.Fprintln(w, line)

					c++
					if c%100000 == 0 {
						fmt.Printf("Wrote %d lines\n", c)
						w.Flush()
					}
				case <-writerStopChan:
					activeWorkers--
					if activeWorkers == 0 {
						w.Flush()
						mainStopChan <- 0
						return
					}
			}
		}
	}()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		workerChan <- line
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}

	for i := 0; i < *nWorkers; i++ {
		workerChan <- stop
	}


	<-mainStopChan
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



