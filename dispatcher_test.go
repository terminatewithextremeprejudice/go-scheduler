package scheduler

import (
	"testing"
)

func testFn(arg1 string, arg2 string) (*testResult, error) {
	return &testResult{arg1, arg2}, nil
}

func TestShouldCreateWorkerPoolOfSizeNWorkers(t *testing.T) {
	dispatcher := NewDispatcher(10)
	if len(dispatcher.threads) != 10 {
		t.Fatalf("invalid number of created threads")
	}
	dispatcher.Stop()
}

func TestProcessJobInsideWorker(t *testing.T) {
	var result chan *testResult = make(chan *testResult)

	cb := func(t *testResult, err error) {
		result <- t
	}

	job := NewJob(true, nil)
	job.SetExecutionFn(testFn, "kauhee", "doapskdasopidksao")
	job.AddEventListeners(nil, cb)
	dispatcher := NewDispatcher(1)
	dispatcher.Run()
	dispatcher.Queue <- job
	res := <-result
	if res.arg1 != "kauhee" {
		t.Fatalf("invalid response")
	}
	dispatcher.Stop()
}
