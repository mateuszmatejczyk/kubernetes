/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/


package endpoint

import (
	"testing"
	"time"
)

var (
	t0 = time.Date(2018, 01, 01, 0, 0, 0, 0, time.UTC)
	t1 = t0.Add(time.Second)
	t2 = t1.Add(time.Second)
	t3 = t2.Add(time.Second)

	key = "my_endpoint"
)

func TestSingleEvent(t *testing.T) {
	tracker := newTriggerTimeTracker()

	tracker.Observe(key, t0)
	tracker.StartListing(key)
	if got := tracker.StopListeningAndReset(key, t0); got != t0 {
		t.Errorf("Wrong trigger time, expected %s, got %s", t0, got)
	}
}

func TestSingleBatchTwoEvents(t *testing.T) {
	tracker := newTriggerTimeTracker()

	tracker.Observe(key, t0)
	tracker.Observe(key, t1)
	tracker.StartListing(key)
	if got := tracker.StopListeningAndReset(key, t1); got != t0 {
		t.Errorf("Wrong trigger time, expected %s, got %s", t0, got)
	}
}

func TestSingleBatchMissingEvent(t *testing.T) {
	tracker := newTriggerTimeTracker()

	tracker.Observe(key, t0)
	tracker.Observe(key, t1)
	tracker.StartListing(key)
	if got := tracker.StopListeningAndReset(key, t2); got != t0 {
		t.Errorf("Wrong trigger time, expected %s, got %s", t0, got)
	}
}

func TestSingleBatchEventObservedAfterStartListing(t *testing.T) {
	tracker := newTriggerTimeTracker()

	tracker.Observe(key, t0)
	tracker.StartListing(key)
	tracker.Observe(key, t1)
	if got := tracker.StopListeningAndReset(key, t1); got != t0 {
		t.Errorf("Wrong trigger time, expected %s, got %s", t0, got)
	}
}

func TestSingleEventObservedAfterStartListing(t *testing.T) {
	tracker := newTriggerTimeTracker()

	tracker.StartListing(key)
	tracker.Observe(key, t0)
	if got := tracker.StopListeningAndReset(key, t0); got != t0 {
		t.Errorf("Wrong trigger time, expected %s, got %s", t0, got)
	}
}

func TestTwoEventsFirstObservedAfterStartListing(t *testing.T) {
	tracker := newTriggerTimeTracker()

	tracker.StartListing(key)
	tracker.Observe(key, t0)
	if got := tracker.StopListeningAndReset(key, t1); got != t0 {
		t.Errorf("Wrong trigger time, expected %s, got %s", t0, got)
	}
}

func TestNoEventObserved(t *testing.T) {
	tester := newTester(t)

	tester.StartListing(key)
	tester.whenListeningReturned(key, t1).expect(t1)
}

// ------- Test Utils -------

type tester struct {
	*triggerTimeTracker
	t *testing.T
}

func newTester(t *testing.T) *tester {
	return &tester { newTriggerTimeTracker(), t}
}

func (this *tester) whenListeningReturned(key string, val time.Time) subject {
	return subject { this.StopListeningAndReset(key, val), this.t }
}

type subject struct {
	got time.Time
	t *testing.T
}

func (s subject) expect(val time.Time) {
	if s.got != val {
		s.t.Errorf("Wrong trigger time, expected %s, got %s", val, s.got)
	}
}
