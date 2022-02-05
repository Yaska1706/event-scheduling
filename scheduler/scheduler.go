package scheduler

import (
	"context"
	"database/sql"
	"log"
	"time"
)

type Scheduler struct {
	db        *sql.DB
	listeners Listeners
}

func NewScheduler(db *sql.DB, listeners Listeners) Scheduler {
	return Scheduler{
		db:        db,
		listeners: listeners,
	}
}

func (s Scheduler) Schedule(event Event) {
	log.Print("🚀 Scheduling event ", event.Name, " to run at ", event.RunAt)
	_, err := s.db.Exec(`INSERT INTO "public"."jobs" ("name", "payload", "runAt") VALUES ($1, $2, $3)`, event.Name, event.Payload, event.RunAt)
	if err != nil {
		log.Print("schedule insert error: ", err)
	}
}

func (s Scheduler) AddListener(event string, listenfunc ListenFunc) {
	s.listeners[event] = listenfunc
}

func (s Scheduler) CheckDueEvents() []Event {
	events := []Event{}
	rows, err := s.db.Query(`SELECT "id", "name", "payload" FROM "public"."jobs" WHERE "runAt" < $1`, time.Now())
	if err != nil {
		log.Print("💀 error: ", err)
		return nil
	}
	for rows.Next() {
		event := Event{}
		rows.Scan(&event.ID, &event.Name, &event.Payload)
		events = append(events, event)
	}
	return events
}

func (s Scheduler) CallListeners(event Event) {
	eventfn, ok := s.listeners[event.Name]
	if ok {
		go eventfn(event.Payload)
		_, err := s.db.Exec(`DELETE FROM "public"."jobs" WHERE "id" = $1`, event.ID)
		if err != nil {
			log.Print("💀 error: ", err)
		}
	} else {
		log.Print("💀 error: couldn't find event listeners attached to ", event.Name)
	}

}

func (s Scheduler) CheckEventsInInterval(ctx context.Context, duration time.Duration) {
	ticker := time.NewTicker(duration)
	go func() {
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				log.Println("⏰ Ticks Received...")
				events := s.CheckDueEvents()
				for _, event := range events {
					s.CallListeners(event)
				}
			}
		}
	}()
}
