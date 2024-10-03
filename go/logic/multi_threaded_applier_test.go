package logic

// import (
// 	"fmt"
// 	"math/rand"
// 	"testing"
// 	"time"
// )

// func (c *Coordinator) startWorkers(count int) {
// 	for i := 0; i < count; i++ {
// 		w := Worker{}
// 		c.workers = append(c.workers, &w)

// 		go func() {
// 			for job := range c.queue {
// 				// fmt.Printf("Worker processing job: %d\n", job.sequenceNumber)

// 				// If `waitChannel` is set, we need to wait for a signal so that we know the job
// 				// can be processed.
// 				if job.waitChannel != nil {
// 					// fmt.Printf("Worker waiting for job: %d\n", job.sequenceNumber)
// 					<-job.waitChannel
// 					// fmt.Printf("Worker received signal for job: %d\n", job.sequenceNumber)
// 				}

// 				w.processJob(job)
// 				// fmt.Printf("Marking job as completed: %d\n", job.sequenceNumber)
// 				c.markJobCompleted(job)
// 			}
// 		}()
// 	}
// }

// func (w *Worker) processJob(job *Job) error {
// 	// Simulate `BEGIN`
// 	time.Sleep(100 * time.Microsecond)

// 	for range job.changes {
// 		// Simulate processing a change
// 		time.Sleep(100 * time.Microsecond)
// 	}

// 	// Simulate `COMMIT`
// 	time.Sleep(100 * time.Microsecond)

// 	w.executedJobs++

// 	return nil
// }

// func TestMultiThreadedApplier(t *testing.T) {
// 	coordinator := NewCoordinator()
// 	coordinator.startWorkers(16)

// 	for i := int64(1); i < 300; i++ {
// 		job := &Job{sequenceNumber: i, lastCommitted: i - 1, changes: make(chan struct{}, 1000)}
// 		coordinator.SubmitJob(job)
// 		for j := 0; j < 10; j++ {
// 			job.changes <- struct{}{}
// 		}
// 		close(job.changes)
// 	}

// 	coordinator.wg.Wait()

// 	for i, w := range coordinator.workers {
// 		fmt.Printf("Worker %d executed %d jobs\n", i, w.executedJobs)
// 	}
// }

// func TestMultiThreadedApplierWithDependentJobs(t *testing.T) {
// 	coordinator := NewCoordinator()
// 	coordinator.startWorkers(16)

// 	for i := int64(1); i < 300; i++ {
// 		job := &Job{sequenceNumber: i, lastCommitted: ((i - 1) / 10) * 10, changes: make(chan struct{}, 1000)}
// 		coordinator.SubmitJob(job)
// 		for j := 0; j < 10; j++ {
// 			job.changes <- struct{}{}
// 		}
// 		close(job.changes)
// 	}

// 	coordinator.wg.Wait()

// 	for i, w := range coordinator.workers {
// 		fmt.Printf("Worker %d executed %d jobs\n", i, w.executedJobs)
// 	}
// }

// func TestMultiThreadedApplierWithManyDependentJobs(t *testing.T) {
// 	coordinator := NewCoordinator()
// 	coordinator.startWorkers(16)

// 	for i := int64(1); i < 300; i++ {
// 		job := &Job{sequenceNumber: i, lastCommitted: 1, changes: make(chan struct{}, 1000)}
// 		coordinator.SubmitJob(job)
// 		for j := 0; j < 10; j++ {
// 			job.changes <- struct{}{}
// 		}
// 		close(job.changes)
// 	}

// 	coordinator.wg.Wait()

// 	for i, w := range coordinator.workers {
// 		fmt.Printf("Worker %d executed %d jobs\n", i, w.executedJobs)
// 	}
// }

// func TestMultiThreadedApplierWithVaryingDependentJobs(t *testing.T) {
// 	coordinator := NewCoordinator()
// 	coordinator.startWorkers(16)

// 	for i := int64(1); i < 300; i++ {
// 		job := &Job{sequenceNumber: i, lastCommitted: rand.Int63n(i), changes: make(chan struct{}, 1000)}
// 		coordinator.SubmitJob(job)
// 		for j := 0; j < 10; j++ {
// 			job.changes <- struct{}{}
// 		}
// 		close(job.changes)
// 	}

// 	coordinator.wg.Wait()

// 	for i, w := range coordinator.workers {
// 		fmt.Printf("Worker %d executed %d jobs\n", i, w.executedJobs)
// 	}
// }
