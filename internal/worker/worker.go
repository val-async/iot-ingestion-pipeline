package worker

import (
	"context"
	"sync"
	"time"

	// "github.com/val-async/iot-ingestion-pipeline/proto"
	"go.uber.org/zap"
	// "google.golang.org/grpc/encoding/proto"
)

type TelemetryData struct{
	DeviceID string
	Voltage float64
	Current float64
	Timestamp time.Time
}

//workerPool
type WorkerPool struct{
	numWorkers int
	taskChan chan TelemetryData
	wg sync.WaitGroup
	logger *zap.Logger
	pool sync.Pool //Reuse TelemetyData structs
}

//NewWorkerPool createa a new pool with a fixed number of workers
func NewWorkerPool(numWorkers int, logger *zap.Logger)*WorkerPool{
	wp := &WorkerPool{
		numWorkers: numWorkers,
		taskChan: make(chan TelemetryData,1000),
		logger: logger,
	}

	// init syncpool
	wp.pool = sync.Pool{
		New: func() interface{}{
			return &TelemetryData{}
		},
	}

	//start the Workers 
	for i := range numWorkers{
		wp.wg.Add(1)
		go wp.worker(i);
	} 
	
	return wp 
}

//the goroutine method
func (wp* WorkerPool) worker(id int){
	defer wp.wg.Done()

	for task := range wp.taskChan{
		//task processing simulator
		wp.processTask(task)

		//return struct to pool to reuse memory
		//resetting fields to avoid stale data
		wp.pool.Put(&TelemetryData{
			DeviceID: "",
			Voltage: 0,
			Current: 0,
			Timestamp: time.Time{},
		})
	}
}

//task simulator (DB Write , validation, e.t.c)
func (wp *WorkerPool) processTask(task TelemetryData){
	wp.logger.Info("Processing telemety",
	zap.String("device",task.DeviceID),
	zap.Float64("voltage",task.Voltage),
	)
	
	time.Sleep(1*time.Millisecond)
}

//submit tasks
// todo: study how to handle backpressure in golang
func (wp *WorkerPool) Submit(task TelemetryData){
	wp.taskChan <-task
}

//graceful shudown to stop the poop
func (wp *WorkerPool) Shutdown(ctx context.Context){
	close(wp.taskChan)

	//wait for workers or time out
	done := make(chan struct{})
	go func(){
		wp.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		wp.logger.Info("Worket pool shutdown complete")
	case <-ctx.Done():
		wp.logger.Warn("Shutdown timed out, forcing exit")
	}
}

//proto to struct convert helper

// func(wp *WorkerPool) ConvertProtoTask(proto *) *TelemetryData{
// }
