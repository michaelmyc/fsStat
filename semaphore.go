package main

type Semaphore struct {
	semC           chan bool
	maxConcurrency int
}

func CreateSemaphore(count int) *Semaphore {
	return &Semaphore{semC: make(chan bool, count), maxConcurrency: count}
}

func (s *Semaphore) Acquire() {
	s.semC <- true
}

func (s *Semaphore) Release() {
	<-s.semC
}
