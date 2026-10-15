package scheduler

import (
	"context"
	"testing"
	"time"
)

type scheduler_test_result struct {
	arg1 int
	arg2 string
}

func TestSchedulerAddJob(t *testing.T) {
	scheduler := NewScheduler(1, 10*1000) // 1 thread that polls every 10ms
	scheduler.Run()
	go func() {
		for i := 0; i < 10; i++ {
			executioner := func(arg1 int, arg2 string) (*scheduler_test_result, error) {
				return &scheduler_test_result{arg1, arg2}, nil
			}

			var now RunTime
			times := make([]*RunTime, 0)
			now.Time = time.UnixMilli(int64(i))
			times = append(times, &now)
			job := NewJob(false, times)
			job.SetExecutionFn(executioner, i, "doapskdasopidksao")
			scheduler.AddJob(job)
		}

		if scheduler.jobs.Len() == 0 {
			t.Fatalf("no jobs were added")
		}

		scheduler.QuitCh <- true
	}()

	<-scheduler.DoneCh
}

func TestSchedulerShouldRunMultipleTimesIfMultipleTimesSet(t *testing.T) {
	exec2 := func(arg1 int, arg2 string) (*scheduler_test_result, error) {
		return &scheduler_test_result{arg1, arg2}, nil
	}

	scheduler := NewScheduler(1, 10*100)
	scheduler.Run()

	for i := 0; i < 1; i++ {
		var now, next RunTime
		times := make([]*RunTime, 0)
		now.Time = time.UnixMilli(int64(i))
		next.Time = time.Now().Add(1 * time.Hour)
		times = append(times, &now)
		times = append(times, &next)
		job := NewJob(false, times)
		job.SetExecutionFn(exec2, i, "doapskdasopidksao")
		scheduler.AddJob(job)
	}

	if scheduler.jobs.Len() == 0 {
		t.Fatalf("no jobs were rescheduled")
	}

	scheduler.QuitCh <- true
	<-scheduler.DoneCh
}

func TestSchedulerShouldRescheduleJobIfRepeating(t *testing.T) {
	exec2 := func(arg1 int, arg2 string) (*scheduler_test_result, error) {
		return &scheduler_test_result{arg1, arg2}, nil
	}

	scheduler := NewScheduler(1, 10*100)
	scheduler.Run()

	for i := 0; i < 1; i++ {
		var now RunTime
		times := make([]*RunTime, 0)
		next := time.Duration(24 * time.Hour)
		now.Time = time.UnixMilli(int64(i))
		now.Repeating = true
		now.Interval = &next
		times = append(times, &now)
		job := NewJob(false, times)
		job.SetExecutionFn(exec2, i, "doapskdasopidksao")
		scheduler.AddJob(job)
	}

	if scheduler.jobs.Len() == 0 {
		t.Fatalf("no jobs were rescheduled")
	}

	scheduler.QuitCh <- true
	<-scheduler.DoneCh
}

func TestSchedulerRemoveJob(t *testing.T) {
	exec2 := func(arg1 int, arg2 string) (*scheduler_test_result, error) {
		return &scheduler_test_result{arg1, arg2}, nil
	}

	scheduler := NewScheduler(1, 10*100)
	scheduler.Run()

	var now, next RunTime
	times1 := make([]*RunTime, 0)
	times2 := make([]*RunTime, 0)
	now.Time = time.Now().Add(1 * time.Hour)
	next.Time = time.Now().Add(2 * time.Hour)
	times1 = append(times1, &next)
	times2 = append(times2, &now)

	job1 := NewJob(false, times1)
	job1.SetExecutionFn(exec2, 0, "doapskdasopidksao")
	job2 := NewJob(false, times2)
	job2.SetExecutionFn(exec2, 1, "sadsadsadsad")

	scheduler.AddJob(job1)
	scheduler.AddJob(job2)

	if scheduler.jobs.Len() != 2 {
		t.Fatalf("no jobs were scheduled")
	}

	scheduler.RemoveJob(job1)

	if scheduler.jobs.Len() != 1 {
		t.Fatalf("no jobs were scheduled")
	}

	if scheduler.Pop() != job2 {
		t.Fatalf("wrong job got removed")
	}

	scheduler.QuitCh <- true
	<-scheduler.DoneCh
}

func TestSchedulerShouldRescheduleJob(t *testing.T) {
	exec2 := func(arg1 int, arg2 string) (*scheduler_test_result, error) {
		return &scheduler_test_result{arg1, arg2}, nil
	}

	scheduler := NewScheduler(1, 10*100)
	scheduler.Run()

	var now, next RunTime
	times := make([]*RunTime, 0)
	now.Time = time.Now().Add(1 * time.Hour)
	times = append(times, &now)
	rescheduledTimes := make([]*RunTime, 0)
	next.Time = time.Now().Add(8 * time.Hour)
	rescheduledTimes = append(rescheduledTimes, &next)

	job := NewJob(false, times)
	job.SetExecutionFn(exec2, 0, "doapskdasopidksao")

	scheduler.AddJob(job)
	scheduler.RescheduleJob(job, rescheduledTimes)

	r := scheduler.Pop()
	if r.nextRun != &next {
		t.Fatalf("job not rescheduled for later")
	}

	scheduler.QuitCh <- true
	<-scheduler.DoneCh
}

func TestSchedulerShouldCancelJobWithContextOnQuit(t *testing.T) {
	cancelTestFn := func(ctx *context.Context, arg1 int, arg2 string) (*scheduler_test_result, error) {
		for i := 0; i < 1000000; i++ {
			time.Sleep(time.Millisecond * 100)
			if (*ctx).Err() != nil {
				return nil, (*ctx).Err()
			}
		}

		return &scheduler_test_result{1, "2"}, nil
	}

	scheduler := NewScheduler(1, 10*100)
	scheduler.Run()

	var now RunTime
	times := make([]*RunTime, 0)
	now.Time = time.Now()
	times = append(times, &now)

	ctx, cancelFn := context.WithCancel(context.Background())

	job := NewJobWithContext(false, times, &ctx, &cancelFn)
	job.SetExecutionFn(cancelTestFn, &ctx, 0, "doapskdasopidksao")

	scheduler.AddJob(job)

	if scheduler.jobs.Len() != 1 {
		t.Fatalf("job not scheduled")
	}

	time.Sleep(time.Millisecond * 5000)
	scheduler.QuitCh <- true
	<-scheduler.DoneCh

	if job.error != context.Canceled {
		t.Fatalf("job not cancelled")
	}
}

func TestSchedulerWhenNotRunningShouldNotExecuteJobs(t *testing.T) {
	cancelTestFn := func(ctx *context.Context, arg1 int, arg2 string) (*scheduler_test_result, error) {
		for i := 0; i < 100000; i++ {
			time.Sleep(time.Millisecond * 10)
			if (*ctx).Err() != nil {
				return nil, (*ctx).Err()
			}
		}

		return &scheduler_test_result{1, "2"}, nil
	}

	scheduler := NewScheduler(1, 10)
	// scheduler.Run()

	var now RunTime
	times := make([]*RunTime, 0)
	now.Time = time.Now()
	times = append(times, &now)

	ctx, cancelFn := context.WithCancel(context.Background())

	job := NewJobWithContext(false, times, &ctx, &cancelFn)
	job.SetExecutionFn(cancelTestFn, &ctx, 0, "doapskdasopidksao")

	scheduler.AddJob(job)

	if scheduler.jobs.Len() != 1 {
		t.Fatalf("job not scheduled")
	}

	scheduler.Run()

	time.Sleep(100 * time.Millisecond)

	if scheduler.jobs.Len() != 0 {
		t.Fatalf("job not runned")
	}

	scheduler.QuitCh <- true
	<-scheduler.DoneCh
}
