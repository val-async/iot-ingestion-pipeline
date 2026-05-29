package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
	"github.com/val-async/iot-ingestion-pipeline/internal/worker"
)

func main(){
	logger, err := zap.NewProduction()
	if err != nil{
		log.Fatalf("Failed to init logger: %v", err);
	}

	defer logger.Sync()

	logger.Info("Starting IoT Ingestion Pipeline")

	pool := worker.NewWorkerPool(4,logger)

	go simulateIncomingData(pool, logger)

	quit := make(chan os.Signal,1)

	signal.Notify(quit, syscall.SIGINT,syscall.SIGTERM)
	<- quit

	logger.Info("Shutting down pipeline...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	defer cancel()

	pool.Shutdown(ctx)

	logger.Info("Pipeline stopped")

	
}
	
func simulateIncomingData(pool *worker.WorkerPool,logger *zap.Logger){
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	count := 0
	for range ticker.C{
		count++
		task := worker.TelemetryData{
			DeviceID: "meter-001",
			Voltage: 220.5 + float64(count%10),
			Current: 5.0 + float64(count%5),
			Timestamp: time.Now(),
		}

		pool.Submit(task)

		if count %100 == 0{
			logger.Info("Process packets", zap.Int("count",count))
		}

	}
}
