package scheduler

import (
	// "errors"
	"context"
	"testing"
	"time"
)

type testResult struct {
	arg1 string
	arg2 string
}

func test(arg1 string, arg2 string) (string, error) {
	return "aa", nil
}

func test2(arg1 string, arg2 string) (*testResult, error) {
	return &testResult{arg1, arg2}, nil
}

func testBeforeCallBack() {
}

func testAfterCallBack(t *testResult, err error) {
	t.arg1 = "kauhee muutettu"
}

func TestExecution(t *testing.T) {
	_, err := runExecutorWithParamsFn(test, "a", "b")

	if err != nil {
		return
	}
}

func TestJobCallback(t *testing.T) {
	job := NewJob(true, nil)
	job.AddEventListeners(testBeforeCallBack, testAfterCallBack)
	job.SetExecutionFn(test2, "kauhee", "doapskdasopidksao")
	vals, err := job.Run()
	if err != nil {
		t.Fatalf(err.Error())
	}
	v := vals.(*testResult)
	if v.arg1 != "kauhee muutettu" || v.arg2 != "doapskdasopidksao" {
		t.Fatalf("unexpected return value")
	}
	return
}

func TestJobRunShouldAddRunTimeWhenAdded(t *testing.T) {
	var now RunTime
	now.Time = time.Now()
	now.AddDate(1, 1, 1)

	job := NewJob(true, nil)
	job.AddEventListeners(testBeforeCallBack, testAfterCallBack)
	job.SetExecutionFn(test2, "kauhee", "doapskdasopidksao")
	job.AddAtTime(&now)

	if job.runTimes.Len() != 2 {
		t.Fatalf("wrong number of run times")
	}

	vals, err := job.Run()
	if err != nil {
		t.Fatalf(err.Error())
	}
	v := vals.(*testResult)
	if v.arg1 != "kauhee muutettu" || v.arg2 != "doapskdasopidksao" {
		t.Fatalf("unexpected return value")
	}
	return
}

func TestJobCopy(t *testing.T) {
	job := NewJob(true, nil)
	job.AddEventListeners(testBeforeCallBack, testAfterCallBack)
	job.SetExecutionFn(test2, "kauhee", "doapskdasopidksao")

	c := job.Copy()
	if c.runTimes.Len() == 0 {
		t.Fatalf("no runtimes")
	}

	vals, err := c.Run()
	if err != nil {
		t.Fatalf(err.Error())
	}
	v := vals.(*testResult)
	if v.arg1 != "kauhee muutettu" || v.arg2 != "doapskdasopidksao" {
		t.Fatalf("unexpected return value")
	}
	return
}

func TestJobRunShouldNotRunIfTheresNoRuntimes(t *testing.T) {
	job := NewJob(false, nil)
	job.AddEventListeners(testBeforeCallBack, testAfterCallBack)
	job.SetExecutionFn(test2, "kauhee", "doapskdasopidksao")

	_, err := job.Run()
	if err == nil {
		t.Fatalf("should have thrown error if no run time")
	}
}

func TestJobRunShouldRemoveRunTimeWhenRan(t *testing.T) {
	var now RunTime
	now.Time = time.Now()
	now.AddDate(1, 1, 1)

	job := NewJob(false, nil)
	job.AddEventListeners(testBeforeCallBack, testAfterCallBack)
	job.SetExecutionFn(test2, "kauhee", "doapskdasopidksao")
	job.AddAtTime(&now)

	vals, err := job.Run()
	if err != nil {
		t.Fatalf(err.Error())
	}
	v := vals.(*testResult)
	if v.arg1 != "kauhee muutettu" || v.arg2 != "doapskdasopidksao" {
		t.Fatalf("unexpected return value")
	}

	if job.runTimes.Len() > 0 {
		t.Fatalf("should have removed runtime")
	}

	return
}

func TestJobRunOrderShouldExecutAccordingToTimestamp(t *testing.T) {
	var now, next RunTime
	now.Time = time.Now()
	next.Time = now.AddDate(1, 1, 1)

	job := NewJob(false, nil)
	job.SetExecutionFn(test2, "kauhee", "doapskdasopidksao")
	job.AddAtTime(&now)
	job.AddAtTime(&next)

	if n := job.NextRun(); n != &now {
		t.Fatalf("invalid next run time")
	}

	_, err := job.Run()
	if err != nil {
		t.Fatalf(err.Error())
	}
	if job.runTimes.Len() != 1 {
		t.Fatalf("should have removed runtime")
	}

	return
}

func TestJobShouldCancelTheExecutionOfTheFunctionWithContextIfGetsCancelled(t *testing.T) {
	cancelTestFn2 := func(ctx *context.Context, arg1 string, arg2 string) (*testResult, error) {
		for i := 0; i < 100000; i++ {
			time.Sleep(time.Millisecond * 10)
			if (*ctx).Err() != nil {
				return nil, (*ctx).Err()
			}
		}

		return &testResult{"1", "2"}, nil
	}

	var now RunTime
	now.Time = time.Now()

	ctx, cancelFn := context.WithCancel(context.Background())

	job := NewJobWithContext(false, nil, &ctx, &cancelFn)
	job.SetExecutionFn(cancelTestFn2, job.executor.ctx, "kauhee", "doapskdasopidksao")
	job.AddAtTime(&now)

	go func() {
		time.Sleep(500 * time.Millisecond)
		job.Cancel()
	}()

	_, err := job.Run()
	if err != context.Canceled {
		t.Fatalf(err.Error())
	}

	return

}
