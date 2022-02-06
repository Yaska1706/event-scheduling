package scheduler

import (
	"context"
	"database/sql"
	"log"
	"time"

	"github.com/robfig/cron/v3"
)

type Scheduler struct {
	db          *sql.DB
	listeners   Listeners
	cron        *cron.Cron
	cronEntries map[string]cron.EntryID
}

func NewScheduler(db *sql.DB, listeners Listeners) Scheduler {
	return Scheduler{
		db:          db,
		listeners:   listeners,
		cron:        cron.New(),
		cronEntries: map[string]cron.EntryID{},
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

func (s Scheduler) ScheduleCron(event Event) {
	log.Print("🚀 Scheduling event ", event.Name, " with cron string ", event.Cron)
	entryID, ok := s.cronEntries[event.Name]
	if ok {
		s.cron.Remove(entryID)
		_, err := s.db.Exec(`UPDATE "public"."jobs" SET "cron" = $1 , "payload" = $2 WHERE "name" = $3 AND "cron" != '-'`, event.Cron, event.Payload, event.Name)
		if err != nil {
			log.Print("schedule cron update error: ", err)
		}
	} else {
		_, err := s.db.Exec(`INSERT INTO "public"."jobs" ("name", "payload", "runAt", "cron") VALUES ($1, $2, $3, $4)`, event.Name, event.Payload, time.Now(), event.Cron)
		if err != nil {
			log.Print("schedule cron insert error: ", err)
		}
	}

	eventFn, ok := s.listeners[event.Name]
	if ok {
		entryID, err := s.cron.AddFunc(event.Cron, func() { eventFn(event.Payload) })
		s.cronEntries[event.Name] = entryID
		if err != nil {
			log.Print("💀 error: ", err)
		}
	}
}

func (s Scheduler) attachCronJobs() {
	log.Printf("Attaching cron jobs")
	rows, err := s.db.Query(`SELECT "id", "name", "payload", "cron" FROM "public"."jobs" WHERE "cron"!='-'`)
	if err != nil {
		log.Print("💀 error: ", err)
	}
	for rows.Next() {
		evt := Event{}
		rows.Scan(&evt.ID, &evt.Name, &evt.Payload, &evt.Cron)
		eventFn, ok := s.listeners[evt.Name]
		if ok {
			entryID, err := s.cron.AddFunc(evt.Cron, func() { eventFn(evt.Payload) })
			s.cronEntries[evt.Name] = entryID

			if err != nil {
				log.Print("💀 error: ", err)
			}
		}
	}
}

// StartCron starts cron job
func (s Scheduler) StartCron() func() {
	s.attachCronJobs()
	s.cron.Start()

	return func() {
		s.cron.Stop()
	}
}
