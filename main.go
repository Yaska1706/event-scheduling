package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/yaska1706/event-scheduler/dbconfig"
	"github.com/yaska1706/event-scheduler/events"
	"github.com/yaska1706/event-scheduler/scheduler"
)

var eventlistener = scheduler.Listeners{
	"SendEmail": events.SendEmail,
	"PayBills":  events.PayBills,
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	database := dbconfig.DBConnection()
	dbconfig.SeedDB(database)

	schedule := scheduler.NewScheduler(database, eventlistener)

	stopCron := schedule.StartCron()
	defer stopCron()
	schedule.CheckEventsInInterval(ctx, time.Minute)

	schedule.Schedule(
		scheduler.Event{
			Name:    "SendEmail",
			Payload: "mail: nilkantha.dipesh@gmail.com",
			RunAt:   time.Now().Add(1 * time.Minute),
		})
	schedule.Schedule(
		scheduler.Event{
			Name:    "PayBills",
			Payload: "paybills: $4,000 bill",
			RunAt:   time.Now().Add(2 * time.Minute),
		})

	schedule.ScheduleCron(
		scheduler.Event{
			Name:    "SendEmail",
			Payload: "paybills: $4,000 bill",
			Cron:    "1 * * * *",
		},
	)
	go func() {
		for range interrupt {
			log.Println("\n❌ Interrupt received closing...")
			cancel()
		}
	}()

	<-ctx.Done()
}
