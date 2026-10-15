package scheduler

import (
	"container/heap"
	"context"
	"errors"
	"reflect"
	"sync"
	"time"
)

type RunTime struct {
	time.Time
	Repeating bool
	Interval  *time.Duration
}

type TimeHeap []*RunTime

func (h TimeHeap) Len() int {
	return len(h)
}

func (h TimeHeap) Less(i, j int) bool {
	return h[i].Unix() < h[j].Unix()
}

func (h TimeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *TimeHeap) Push(t interface{}) {
	// Push and Pop use pointer receivers as they modify the underlaying slice
	*h = append(*h, t.(*RunTime))
}

func (h *TimeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h *TimeHeap) Peek() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	return x
}

type eventListeners struct {
	onBeforeExecution interface{} // pointer list of listeners of the execution
	onAfterExecution  interface{} // pointer list of listeners of the execution
	next              *eventListeners
}

type executorFn func([]interface{}) (interface{}, error)

type executor struct {
	eventListeners  **eventListeners
	nEventListeners int                 // number of event listeners
	function        interface{}         // function to execute
	parameters      []interface{}       // parameters for the function to execute
	nParameters     int                 // number of paramters for the function to execute
	runState        *int64              // (0=not running, 1=running, 2=done, -1=error)
	ctx             *context.Context    // context to run the task on
	cancelFn        *context.CancelFunc // cancelFn to halt execution
}

func runExecutorFn(fn interface{}) (interface{}, error) {
	if fn != nil {
		reflect.ValueOf(fn).Call([]reflect.Value{})
	}

	return nil, errors.New("invalid executor function: nil")
}

func runExecutorWithParamsFn(fn interface{}, params ...interface{}) (interface{}, error) {
	f := reflect.ValueOf(fn)
	if len(params) != f.Type().NumIn() {
		return nil, errors.New("invalid number of arguments")
	}
	in := make([]reflect.Value, 0)
	for _, param := range params {
		var val reflect.Value

		if param != nil {
			val = reflect.ValueOf(param)
		} else {
			val = reflect.Zero(reflect.TypeOf((*error)(nil)).Elem())
		}

		in = append(in, val)
	}
	vals := f.Call(in)
	length := len(vals) - 1

	if length < 1 {
		return nil, errors.New("runExecutorWithParamsFn: invalid return value length")
	}

	last := vals[length]

	if last.IsNil() == false && last.Type() == reflect.TypeOf((*error)(nil)).Elem() {
		val := last.Interface()
		err := val.(error)
		return nil, err
	}

	return vals[0].Interface(), nil
}

func (e *executor) Run() (interface{}, error) {
	var (
		listener *eventListeners = *e.eventListeners
		vals     interface{}
		err      error
	)

	for listener != nil {
		if listener.onBeforeExecution != nil {
			runExecutorFn(listener.onBeforeExecution)
		}
		listener = listener.next
	}

	if e.nParameters > 0 {
		vals, err = runExecutorWithParamsFn(e.function, e.parameters...)
		goto SetListeners
	}

	vals, err = runExecutorFn(e.function)

SetListeners:
	listener = *e.eventListeners
	cbargs := make([]interface{}, 2)
	cbargs[0] = vals
	cbargs[1] = err
	for listener != nil {
		if listener.onAfterExecution != nil {
			if len(cbargs) > 1 {
				runExecutorWithParamsFn(listener.onAfterExecution, cbargs...)
			} else {
				runExecutorFn(listener.onAfterExecution)
			}
		}
		listener = listener.next
	}

	return vals, err
}

type Job struct {
	executor         *executor
	mu               *sync.Mutex
	error            error     // error related to job
	nextRun          *RunTime  // when to trigger job the next time (priority in heap)
	lastRun          *RunTime  // when was the last time job was triggered
	runCount         int       // how many times task have been ran
	runTimes         TimeHeap  // (heap of) times at which job should be ran
	startImmediately bool      // should task be processed asap instead of scheduling it for later
	timer            time.Time // handles running tasks at specific time
	index            int       // index inside heap
}

func (j *Job) AddAtTime(t *RunTime) {
	j.mu.Lock()
	defer j.mu.Unlock()

	if t == nil {
		return
	}

	heap.Push(&j.runTimes, t)
	j.nextRun = j.runTimes[0]
}

func (j *Job) SetRunTimes(t []*RunTime) {
	j.mu.Lock()
	defer j.mu.Unlock()

	j.runTimes = t

	heap.Init(&j.runTimes)

	if j.runTimes.Len() > 0 {
		j.nextRun = j.runTimes[0]
	} else {
		j.nextRun = nil
	}
}

func (j *Job) RemoveAtTime(t *RunTime) {
}

func (j *Job) NextRun() *RunTime {
	j.mu.Lock()
	defer j.mu.Unlock()

	return j.nextRun
}

func (j *Job) LastRun() *RunTime {
	j.mu.Lock()
	defer j.mu.Lock()

	return j.lastRun
}

func (j *Job) TimeUntilNextRun() time.Duration {
	j.mu.Lock()
	defer j.mu.Unlock()

	// if no next run ran not scheduled, return time in future
	if j.nextRun == nil {
		return time.Duration(time.Hour)
	}

	return j.nextRun.Sub(time.Now())
}

func (j *Job) TimeSinceLastRun() time.Duration {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.lastRun == nil {
		return 0
	}

	return time.Now().Sub((*j.lastRun).Time)
}

func (j *Job) HasRan() bool {
	if j.lastRun == nil {
		return false
	}
	return true
}

func (j *Job) AddEventListeners(onBeforeExecution interface{}, onAfterExecution interface{}) {
	j.mu.Lock()
	defer j.mu.Unlock()

	lis := &eventListeners{}

	if onBeforeExecution != nil {
		lis.onBeforeExecution = onBeforeExecution
	}

	if onAfterExecution != nil {
		lis.onAfterExecution = onAfterExecution
	}

	j.executor.nEventListeners++
	if j.executor.eventListeners != nil {
		lis.next = *j.executor.eventListeners
	}
	j.executor.eventListeners = &lis
}

func (j *Job) SetExecutionFn(fn interface{}, params ...interface{}) {
	j.mu.Lock()
	defer j.mu.Unlock()

	j.executor.function = fn
	j.executor.parameters = params
	j.executor.nParameters = len(params)
}

func (j *Job) Pop() *RunTime {
	return heap.Pop(&j.runTimes).(*RunTime)
}

func (j *Job) Run() (interface{}, error) {
	var now RunTime
	now.Time = time.Now()

	j.mu.Lock()

	if j.runTimes.Len() == 0 {
		j.error = errors.New("no runtimes to run at")
		j.mu.Unlock()
		return nil, j.error
	}

	// pop the execution time that is being executed now
	j.Pop()
	j.lastRun = &now

	if j.executor.function == nil {
		j.error = errors.New("invalid executor function: nil")
		j.mu.Unlock()
		return nil, j.error
	}

	ret, err := j.executor.Run()

	if j.runTimes.Len() > 0 {
		j.nextRun = j.runTimes[0]
	} else {
		j.nextRun = nil
	}
	j.runCount++

	if err != nil {
		j.error = err
	}

	j.mu.Unlock()

	// cancel signal called
	if err == context.Canceled {
		j.SetRunTimes(make([]*RunTime, 0))
	}

	return ret, err
}

// should interrupt running execution of the executor and remove any future executions of the job
func (j *Job) Cancel() {
	if j.executor.cancelFn != nil {
		(*j.executor.cancelFn)()
	}
}

func (j *Job) Copy() *Job {
	var (
		timeHeap TimeHeap
		t        RunTime
		nextRun  *RunTime
	)

	t.Time = time.Now()

	j.mu.Lock()
	defer j.mu.Unlock()

	heap.Init(&timeHeap)

	for i := 0; i < j.runTimes.Len(); i++ {
		var runTime RunTime

		runTime.Time = j.runTimes[i].Time
		runTime.Repeating = j.runTimes[i].Repeating
		runTime.Interval = j.runTimes[i].Interval

		heap.Push(&timeHeap, &runTime)
	}

	if j.startImmediately == true {
		heap.Push(&timeHeap, &t)
	}

	if timeHeap.Len() > 0 {
		nextRun = timeHeap[0]
	}

	return &Job{
		mu:               &sync.Mutex{},
		executor:         j.executor,
		startImmediately: j.startImmediately,
		runTimes:         timeHeap,
		nextRun:          nextRun,
		timer:            j.timer,
		lastRun:          j.lastRun,
		runCount:         j.runCount,
	}
}

func NewJob(startImmediately bool, runTimes []*RunTime) *Job {
	var (
		t        RunTime
		zero     int64
		timeHeap TimeHeap
	)

	t.Time = time.Now()
	heap.Init(&timeHeap)

	if startImmediately == true {
		runTimes = append(runTimes, &t)
	}

	j := &Job{
		mu: &sync.Mutex{},
		executor: &executor{
			runState:       &zero,
			eventListeners: new(*eventListeners),
		},
		startImmediately: startImmediately,
		runTimes:         timeHeap,
	}

	j.SetRunTimes(runTimes)

	return j
}

func NewJobWithContext(startImmediately bool, runTimes []*RunTime, ctx *context.Context, cancelFn *context.CancelFunc) *Job {
	var (
		t        RunTime
		zero     int64
		timeHeap TimeHeap
	)

	t.Time = time.Now()
	heap.Init(&timeHeap)

	if startImmediately == true {
		runTimes = append(runTimes, &t)
	}

	j := &Job{
		mu: &sync.Mutex{},
		executor: &executor{
			runState:       &zero,
			ctx:            ctx,
			cancelFn:       cancelFn,
			eventListeners: new(*eventListeners),
		},
		startImmediately: startImmediately,
		runTimes:         timeHeap,
	}

	j.SetRunTimes(runTimes)

	return j
}
