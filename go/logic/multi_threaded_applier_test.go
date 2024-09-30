package logic

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

type Job struct {
	// The sequence number of the job
	sequenceNumber int64

	// The sequence number of the job this job depends on
	lastCommitted int64

	// channel that's closed once the job's dependencies are met
	// `nil` if the job's dependencies are already met when the job is submitted
	waitChannel chan struct{}

	// Data for the job
	changes chan struct{}
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
	for i := 0; i < count; i++ {
		w := Worker{}
		c.workers = append(c.workers, &w)

		go func() {
			for job := range c.queue {
				// fmt.Printf("Worker processing job: %d\n", job.sequenceNumber)

				// If `waitChannel` is set, we need to wait for a signal so that we know the job
				// can be processed.
				if job.waitChannel != nil {
					// fmt.Printf("Worker waiting for job: %d\n", job.sequenceNumber)
					<-job.waitChannel
					// fmt.Printf("Worker received signal for job: %d\n", job.sequenceNumber)
				}

				w.processJob(job)
				// fmt.Printf("Marking job as completed: %d\n", job.sequenceNumber)
				c.markJobCompleted(job)
			}
		}()
	}
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
		// fmt.Printf("Scheduling first job: %d\n", job.sequenceNumber)
		c.firstJob = false
	} else if job.lastCommitted <= c.lowWaterMark || c.completedJobs[job.lastCommitted] {
		// fmt.Printf("Scheduling job: %d\n", job.sequenceNumber)
	} else {
		// fmt.Printf("Job %d is waiting for job %d to complete\n", job.sequenceNumber, job.lastCommitted)

		job.waitChannel = make(chan struct{})
		c.waitingJobs[job.lastCommitted] = append(c.waitingJobs[job.lastCommitted], job)
	}

	c.mu.Unlock()

	// Add the job to the queue. This will block until a worker picks up this job.
	c.queue <- job
}

func (c *Coordinator) markJobCompleted(job *Job) {
	c.mu.Lock()
	defer c.wg.Done()

	// fmt.Printf("Marking job as completed: %d\n", job.sequenceNumber)

	// Mark the job as completed
	c.completedJobs[job.sequenceNumber] = true

	// Then, update the low water mark if possible
	// fmt.Printf("Low water mark before update: %d\n", c.lowWaterMark)
	for {
		if c.completedJobs[c.lowWaterMark+1] {
			c.lowWaterMark++
			delete(c.completedJobs, c.lowWaterMark)
		} else {
			break
		}
	}
	// fmt.Printf("Low water mark after update: %d\n", c.lowWaterMark)

	jobsToNotify := make([]*Job, 0)

	// Schedule any jobs that were waiting for this job to complete
	for lastCommitted, jobs := range c.waitingJobs {
		if lastCommitted <= c.lowWaterMark {
			jobsToNotify = append(jobsToNotify, jobs...)
			delete(c.waitingJobs, lastCommitted)
		}
	}

	c.mu.Unlock()

	for _, waitingJob := range jobsToNotify {
		// fmt.Printf("Scheduling previously waiting job: %d - %d\n", waitingJob.sequenceNumber, waitingJob.lastCommitted)
		waitingJob.waitChannel <- struct{}{}
	}
}

func (w *Worker) processJob(job *Job) error {
	// Simulate `BEGIN`
	time.Sleep(100 * time.Microsecond)

	for range job.changes {
		// Simulate processing a change
		time.Sleep(100 * time.Microsecond)
	}

	// Simulate `COMMIT`
	time.Sleep(100 * time.Microsecond)

	w.executedJobs++

	return nil
}

func TestMultiThreadedApplier(t *testing.T) {
	coordinator := NewCoordinator()
	coordinator.StartWorkers(16)

	for i := int64(1); i < 300; i++ {
		job := &Job{sequenceNumber: i, lastCommitted: i - 1, changes: make(chan struct{}, 1000)}
		coordinator.SubmitJob(job)
		for j := 0; j < 10; j++ {
			job.changes <- struct{}{}
		}
		close(job.changes)
	}

	coordinator.wg.Wait()

	for i, w := range coordinator.workers {
		fmt.Printf("Worker %d executed %d jobs\n", i, w.executedJobs)
	}
}

func TestMultiThreadedApplierWithDependentJobs(t *testing.T) {
	coordinator := NewCoordinator()
	coordinator.StartWorkers(16)

	for i := int64(1); i < 300; i++ {
		job := &Job{sequenceNumber: i, lastCommitted: ((i - 1) / 10) * 10, changes: make(chan struct{}, 1000)}
		coordinator.SubmitJob(job)
		for j := 0; j < 10; j++ {
			job.changes <- struct{}{}
		}
		close(job.changes)
	}

	coordinator.wg.Wait()

	for i, w := range coordinator.workers {
		fmt.Printf("Worker %d executed %d jobs\n", i, w.executedJobs)
	}
}

func TestMultiThreadedApplierWithManyDependentJobs(t *testing.T) {
	coordinator := NewCoordinator()
	coordinator.StartWorkers(16)

	for i := int64(1); i < 300; i++ {
		job := &Job{sequenceNumber: i, lastCommitted: 1, changes: make(chan struct{}, 1000)}
		coordinator.SubmitJob(job)
		for j := 0; j < 10; j++ {
			job.changes <- struct{}{}
		}
		close(job.changes)
	}

	coordinator.wg.Wait()

	for i, w := range coordinator.workers {
		fmt.Printf("Worker %d executed %d jobs\n", i, w.executedJobs)
	}
}

func TestMultiThreadedApplierWithVaryingDependentJobs(t *testing.T) {
	coordinator := NewCoordinator()
	coordinator.StartWorkers(16)

	for i := int64(1); i < 300; i++ {
		job := &Job{sequenceNumber: i, lastCommitted: rand.Int63n(i), changes: make(chan struct{}, 1000)}
		coordinator.SubmitJob(job)
		for j := 0; j < 10; j++ {
			job.changes <- struct{}{}
		}
		close(job.changes)
	}

	coordinator.wg.Wait()

	for i, w := range coordinator.workers {
		fmt.Printf("Worker %d executed %d jobs\n", i, w.executedJobs)
	}
}
