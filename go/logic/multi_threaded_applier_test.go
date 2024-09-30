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
	sequenceNumber int

	// The sequence number of the job this job depends on
	lastCommitted int
}

type Coordinator struct {
	lowWaterMark int

	queue chan *Job

	completedJobs map[int]bool
	waitingJobs   map[int][]*Job
	mu            sync.Mutex

	wg sync.WaitGroup

	workers []*Worker
}

type Worker struct {
	executedJobs int
}

func NewCoordinator() *Coordinator {
	return &Coordinator{
		lowWaterMark:  0,
		completedJobs: make(map[int]bool),
		waitingJobs:   make(map[int][]*Job),
		queue:         make(chan *Job, 100),
	}
}

func (c *Coordinator) StartWorkers() {
	for i := 0; i < 10; i++ {
		go func() {
			w := Worker{}
			c.workers = append(c.workers, &w)

			for job := range c.queue {
				w.processJob(job)
				c.markJobCompleted(job)
			}
		}()
	}
}

func (c *Coordinator) SubmitJob(job *Job) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.wg.Add(1)

	if job.sequenceNumber <= c.lowWaterMark || job.lastCommitted == 0 {
		fmt.Printf("Scheduling job: %d\n", job.sequenceNumber)
		c.queue <- job
	} else {
		fmt.Printf("Job %d is waiting for job %d to complete\n", job.sequenceNumber, job.lastCommitted)
		c.waitingJobs[job.lastCommitted] = append(c.waitingJobs[job.lastCommitted], job)
	}
}

func (c *Coordinator) markJobCompleted(job *Job) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.wg.Done()

	fmt.Printf("Marking job as completed: %d\n", job.sequenceNumber)

	// Mark the job as completed
	c.completedJobs[job.sequenceNumber] = true

	// Then, update the low water mark if possible
	fmt.Printf("Low water mark before update: %d\n", c.lowWaterMark)
	for {
		if c.completedJobs[c.lowWaterMark+1] {
			c.lowWaterMark++
			delete(c.completedJobs, c.lowWaterMark)
		} else {
			break
		}
	}
	fmt.Printf("Low water mark after update: %d\n", c.lowWaterMark)

	// Schedule any jobs that were waiting for this job to complete
	for _, waitingJob := range c.waitingJobs[job.sequenceNumber] {
		fmt.Printf("Scheduling previously waiting job: %d\n", waitingJob.sequenceNumber)
		c.queue <- waitingJob
	}
	delete(c.waitingJobs, job.sequenceNumber)
}

func (w *Worker) processJob(job *Job) error {
	// sleep random time between 1 and 200 ms to simulate work
	time.Sleep(time.Duration(rand.Intn(100)+1) * time.Millisecond)

	w.executedJobs++

	return nil
}

func TestMultiThreadedApplier(t *testing.T) {
	coordinator := NewCoordinator()
	coordinator.StartWorkers()

	for i := 1; i < 101; i++ {
		coordinator.SubmitJob(&Job{sequenceNumber: i, lastCommitted: i - 1})
	}

	coordinator.wg.Wait()

	for i, w := range coordinator.workers {
		fmt.Printf("Worker %d executed %d jobs\n", i, w.executedJobs)
	}
}

func TestMultiThreadedApplierWithDependentJobs(t *testing.T) {
	coordinator := NewCoordinator()
	coordinator.StartWorkers()

	for i := 1; i < 101; i++ {
		coordinator.SubmitJob(&Job{sequenceNumber: i, lastCommitted: ((i - 1) / 10) * 10})
	}

	coordinator.wg.Wait()

	for i, w := range coordinator.workers {
		fmt.Printf("Worker %d executed %d jobs\n", i, w.executedJobs)
	}
}
