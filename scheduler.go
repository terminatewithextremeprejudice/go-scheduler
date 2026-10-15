package scheduler

import (
	"container/heap"
	"context"
	"sync"
	"time"
)

type JobHeap []*Job

func (jh JobHeap) Len() int {
	return len(jh)
}

func (jh JobHeap) Less(i, j int) bool {
	return jh[i].NextRun().UnixMilli() < jh[j].NextRun().UnixMilli()
}

func (jh JobHeap) Swap(i, j int) {
	jh[i], jh[j] = jh[j], jh[i]
	jh[i].index = j
	jh[j].index = i
}

func (jh *JobHeap) Push(x interface{}) {
	n := len(*jh)
	item := x.(*Job)
	item.index = n
	*jh = append(*jh, item)
}

func (jh *JobHeap) Pop() interface{} {
	old := *jh
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	// item.index = -1
	*jh = old[0 : n-1]
	return item
}

func (jh *JobHeap) Peek() interface{} {
	old := *jh
	n := len(old)
	x := old[n-1]
	return x
}

func (jh *JobHeap) RemoveAt(index int) {
	old := *jh
	for i := 0; i < jh.Len(); i++ {
		x := old[i]
		n := len(old)
		if x.index == index {
			old[i], old[n-1] = old[n-1], old[i]
			*jh = old[:n-1]
			break
		}
	}

	heap.Init(jh)
}

type Scheduler struct {
	jobs               JobHeap                      // pointer list of jobs (array representation of the underlying binary heap)
	dispatcher         *Dispatcher                  // thread pool to allocate queued jobs to
	timer              *time.Ticker                 // ticker that polls every pollingIntervalMS
	cancellations      map[*Job]*context.CancelFunc // map of cancellation functions to be called when quit signal is received
	RunningCh          chan bool
	DoneCh             chan bool
	QuitCh             chan bool
	cancellationsMutex *sync.RWMutex
	jobsMutex          *sync.RWMutex
	runningMutex       *sync.RWMutex
	running            bool
}

func NewScheduler(nThreads int, pollingIntervalMS int) *Scheduler {
	var (
		jobHeap  JobHeap
		interval time.Duration
	)

	heap.Init(&jobHeap)
	interval = time.Duration(time.Millisecond * time.Duration(pollingIntervalMS))

	return &Scheduler{
		jobs:               jobHeap,
		dispatcher:         NewDispatcher(nThreads),
		timer:              time.NewTicker(interval),
		cancellations:      make(map[*Job]*context.CancelFunc),
		RunningCh:          make(chan bool, 1),
		QuitCh:             make(chan bool, 1),
		DoneCh:             make(chan bool, 1),
		cancellationsMutex: &sync.RWMutex{},
		jobsMutex:          &sync.RWMutex{},
		runningMutex:       &sync.RWMutex{},
		running:            false,
	}
}

func (s *Scheduler) Stop() <-chan bool {
	s.QuitCh <- true
	return s.DoneCh
}

// callback function pointer to remove the cancellation function on scheduler when the job is actually complete
func (s *Scheduler) beforeExecutionCallback(cancelFn *context.CancelFunc, j *Job) func() {
	return func() {
		s.cancellationsMutex.Lock()
		defer s.cancellationsMutex.Unlock()

		s.cancellations[j] = cancelFn
	}
}

// callback function pointer to remove the cancellation function on scheduler when the job is actually complete
func (s *Scheduler) afterExecutionCallback(j *Job) func(r interface{}, err error) {
	return func(r interface{}, err error) {
		s.cancellationsMutex.Lock()
		defer s.cancellationsMutex.Unlock()

		delete(s.cancellations, j)
	}
}

func (s *Scheduler) RescheduleJob(j *Job, t []*RunTime) {
	s.jobsMutex.Lock()
	defer s.jobsMutex.Unlock()

	if j == nil || len(t) == 0 {
		return
	}

	j.SetRunTimes(t)
	heap.Fix(&s.jobs, j.index)
}

func (s *Scheduler) DurationToNextRun(j *Job) *time.Duration {
	var duration time.Duration

	s.jobsMutex.RLock()
	defer s.jobsMutex.RUnlock()

	if j == nil {
		return nil
	}

	duration = j.TimeUntilNextRun()

	return &duration
}

func (s *Scheduler) AddJob(j *Job) {
	s.jobsMutex.Lock()
	defer s.jobsMutex.Unlock()

	// store the pointer of cancelFn into list of cancellable functions
	if j.executor.cancelFn != nil {
		// add callback function pointer to remove the cancellation function on scheduler when the job is actually complete
		j.AddEventListeners(s.beforeExecutionCallback(j.executor.cancelFn, j), s.afterExecutionCallback(j))
	}

	heap.Push(&s.jobs, j)
}

// delete job in queue
func (s *Scheduler) RemoveJob(j *Job) {
	if j == nil {
		return
	}

	s.jobsMutex.Lock()
	defer s.jobsMutex.Unlock()

	s.cancellationsMutex.Lock()
	delete(s.cancellations, j)
	s.cancellationsMutex.Unlock()

	s.jobs.RemoveAt(j.index)
}

func (s *Scheduler) Pop() *Job {
	s.jobsMutex.Lock()
	defer s.jobsMutex.Unlock()

	return heap.Pop(&s.jobs).(*Job)
}

func (s *Scheduler) Run() {
	go func() {
		s.runningMutex.Lock()
		s.dispatcher.Run()
		s.running = true
		s.runningMutex.Unlock()

		for {
			select {
			case <-s.timer.C: // schedule interval occurs
				if s.running == false {
					continue
				}

				s.jobsMutex.RLock()
				if s.jobs.Len() == 0 {
					s.jobsMutex.RUnlock()
					continue
				}

				if nextJob := s.jobs[0]; nextJob != nil && nextJob.TimeUntilNextRun().Milliseconds() > 0 {
					s.jobsMutex.RUnlock()
					continue
				}
				s.jobsMutex.RUnlock()

				// pop next job from heap
				job := s.Pop()

				// if job is repeating one, or there's multiple periods to execute at, copy the job and put it back to queue
				if job.runTimes[0].Repeating == true || job.runTimes.Len() > 1 {
					duplicate := job.Copy()
					// pop the next execution time as it's been handled to dispatcher queue anyway
					latest := duplicate.runTimes.Pop().(*RunTime) // .runTimes[0]
					// reschedule the execution time based on interval if repeating
					if latest != nil && latest.Repeating == true && latest.Interval != nil {
						var rescheduled RunTime

						rescheduled.Repeating = true
						rescheduled.Interval = latest.Interval
						rescheduled.Time = latest.Time.Add(*latest.Interval)

						duplicate.AddAtTime(&rescheduled)
					}

					s.AddJob(duplicate)
				}

				s.dispatcher.Queue <- job
			case status := <-s.RunningCh:
				s.runningMutex.Lock()
				s.running = status
				s.runningMutex.Unlock()
			case <-s.QuitCh:
				// cancel any jobs that support context

				s.runningMutex.Lock()
				s.running = false
				s.runningMutex.Unlock()

				s.cancellationsMutex.Lock()

				for _, n := range s.cancellations {
					if n != nil {
						(*n)()
					}
				}

				s.cancellationsMutex.Unlock()

				s.dispatcher.Stop()
				s.DoneCh <- true
				return
			}
		}
	}()
}
