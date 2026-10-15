# go-scheduler
[![License: MIT](https://img.shields.io/badge/Licence-MIT-brightgreen)](https://opensource.org/license/mit)

go-scheduler is yet another scheduling package that allows you to schedule jobs at given times or intervals.


## Installation Instructions

Add [terminatewithextremeprejudice/go-scheduler](https://github.com/terminatewithextremeprejudice/go-scheduler) as a dependency to your project:

```cli
go get github.com/terminatewithextremeprejudice/go-scheduler
```

## Usage Examples

```
// function to execute when scheduled
doSomething := func (s string) (retval int, err error) {
    ...
}

scheduler := NewScheduler(1, 10*1000) // only 1 worker thread that polls every 10ms
scheduler.Run()

// schedule job that triggers now and every 24 hours from here on 
times := make([]*RunTime, 0)
next := time.Duration(24 * time.Hour)
now.Time = time.UnixMilli(int64(i))
now.Repeating = true
now.Interval = &next
times = append(times, &now)
job := NewJob(false, times)
job.SetExecutionFn(doSomething, "do something with this")
scheduler.AddJob(job)
```

Take a look into scheduler_test.go for more examples.
