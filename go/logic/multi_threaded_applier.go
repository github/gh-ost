package logic

import (
	"fmt"
	"sync"
	"time"
)

type Job struct {
	// The sequence number of the job
	SequenceNumber int64

	// The sequence number of the job this job depends on
	LastCommitted int64

	// channel that's closed once the job's dependencies are met
	// `nil` if the job's dependencies are already met when the job is submitted
	waitChannel chan struct{}

	// Do the job
	Do func() error

	// Data for the job
	//Changes chan *replication.RowsEvent
}

type Coordinator struct {
	// The queue of jobs to be executed
	queue chan *Job

	wg sync.WaitGroup

	// List of workers
	workers []*Worker

	// Mutex to protect the fields below
	mu sync.Mutex

	// The low water mark. This is the sequence number of the last job that has been committed.
	lowWaterMark int64

	// This is a map of completed jobs by their sequence numbers.
	// This is used when updating the low water mark.
	completedJobs map[int64]bool

	// These are the jobs that are waiting for a previous job to complete.
	// They are indexed by the sequence number of the job they are waiting for.
	waitingJobs map[int64][]*Job

	// Is this the first job we're processing? If yes, we can schedule it even if the last committed
	// sequence number is greater than the low water mark.
	firstJob bool
}

type Worker struct {
	executedJobs int
}

func NewCoordinator() *Coordinator {
	return &Coordinator{
		lowWaterMark:  0,
		firstJob:      true,
		completedJobs: make(map[int64]bool),
		waitingJobs:   make(map[int64][]*Job),
		queue:         make(chan *Job),
	}
}

func (c *Coordinator) StartWorkers(count int) {
	jobTimeout := 10 * time.Second
	for i := 0; i < count; i++ {
		go func() {
			for job := range c.queue {
				if job.waitChannel != nil {
					fmt.Printf("Coordinator: Job %d is waiting for job %d to complete\n", job.SequenceNumber, job.LastCommitted)
					select {
					case <-job.waitChannel:
						break
					case <-time.After(jobTimeout):
						// TODO: something problably went wrong here
						fmt.Printf("Coordinator: Job %d timed out waiting for job %d\n", job.SequenceNumber, job.LastCommitted)
						panic("worker timeout")
					}
					// fmt.Printf("Worker received signal for job: %d\n", job.sequenceNumber)
				}

				if err := job.Do(); err != nil {
					// TODO(meiji163) handle error
					panic(err)
				}
				c.markJobCompleted(job)
			}
		}()
	}
	c.wg.Wait()
}

func (c *Coordinator) SubmitJob(job *Job) {
	c.mu.Lock()

	c.wg.Add(1)

	// Jobs might need for their dependencies to be met before they can be executed. We use the
	// `waitChannel` to signal that the job's dependencies are met.
	//
	// We can short-circuit this if:
	// * this is the first job we're processing
	// * the job's last committed sequence number is less than or equal to the low water mark,
	//   thus we know that the job's dependencies are already met.
	// * the job's dependency has completed, but the lowWaterMark has not been updated yet because
	//   a job with a lower sequence number is still being processed.
	//
	// When short-circuiting, we don't assign a `waitChannel` to the job, thus signaling that the
	// job's dependencies are already met.
	if c.firstJob {
		fmt.Printf("Coordinator: Scheduling first job: %d\n", job.SequenceNumber)
		// assume everything before the first job has been processed
		c.lowWaterMark = job.SequenceNumber
		c.firstJob = false
	} else if job.LastCommitted <= c.lowWaterMark || c.completedJobs[job.LastCommitted] {
		fmt.Printf("Coordinator: Scheduling job: %d\n", job.SequenceNumber)
	} else {
		job.waitChannel = make(chan struct{})
		c.waitingJobs[job.LastCommitted] = append(c.waitingJobs[job.LastCommitted], job)
	}

	c.mu.Unlock()
	// Add the job to the queue. This will block until a worker picks up this job.
	c.queue <- job
}

func (c *Coordinator) markJobCompleted(job *Job) {
	c.mu.Lock()
	defer c.wg.Done()

	fmt.Printf("Coordinator: Marking job as completed: %d\n", job.SequenceNumber)

	// Mark the job as completed
	c.completedJobs[job.SequenceNumber] = true

	// Then, update the low water mark if possible

	// TODO: this won't work because the intermediate sequence numbers
	// can be for trxs on tables other than the one we're migrating
	for {
		if c.completedJobs[c.lowWaterMark+1] {
			c.lowWaterMark++
			delete(c.completedJobs, c.lowWaterMark)
		} else {
			break
		}
	}

	// Schedule any jobs that were waiting for this job to complete
	jobsToNotify := c.waitingJobs[job.SequenceNumber]
	delete(c.waitingJobs, job.SequenceNumber)

	c.mu.Unlock()

	for _, waitingJob := range jobsToNotify {
		fmt.Printf("Scheduling previously waiting job: %d - %d\n", waitingJob.SequenceNumber, waitingJob.LastCommitted)
		waitingJob.waitChannel <- struct{}{}
	}
}
