package scheduler

// A buffered channel that we can send work requests on
type JobQueue chan *Job

// Worker is a routine that is responsible of executing the job
type Worker struct {
	WorkerPool chan chan *Job
	JobChannel chan *Job
	quit       chan bool
}

func NewWorker(workerPool chan chan *Job) *Worker {
	return &Worker{
		WorkerPool: workerPool,
		JobChannel: make(chan *Job),
		quit:       make(chan bool),
	}
}

// Run worker until quit signal is passed
func (w *Worker) Start() {
	go func() {
		for {
			// register the current worker into worker queue.
			w.WorkerPool <- w.JobChannel

			select {
			case job := <-w.JobChannel:
				// we have received a work request
				if _, err := job.Run(); err != nil {
				}
			case <-w.quit:
				// quit signal received so stop the thread
				return
			}
		}
	}()
}

func (w *Worker) Stop() {
	w.quit <- true
}

type Dispatcher struct {
	Queue      JobQueue       // queue for processing jobs
	workerPool chan chan *Job // pool of worker threads
	threads    []*Worker      // array of ptr's to workers
	nThreads   int            // number of concurrent workers
}

func NewDispatcher(nThreads int) *Dispatcher {
	pool := make(chan chan *Job, nThreads)
	return &Dispatcher{workerPool: pool, Queue: make(JobQueue), threads: make([]*Worker, nThreads), nThreads: nThreads}
}

func (d *Dispatcher) Run() {
	// start n number of workers
	for i := 0; i < d.nThreads; i++ {
		worker := NewWorker(d.workerPool)
		worker.Start()
		d.threads[i] = worker
	}

	go d.dispatch()
}

func (d *Dispatcher) Stop() {
	for i := 0; i < d.nThreads; i++ {
		worker := d.threads[i]
		if worker != nil {
			worker.Stop()
			worker = nil
		}
	}
}

func (d *Dispatcher) dispatch() {
	for {
		select {
		case job := <-d.Queue:
			go func(job *Job) {
				// obtain available worker job channel
				// block the channel util worker is idle
				jobChannel := <-d.workerPool

				// dispatch the job to the worker job channel
				jobChannel <- job
			}(job)
		}
	}
}
