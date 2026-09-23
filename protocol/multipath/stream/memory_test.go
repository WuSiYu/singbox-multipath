package stream

import (
	"testing"
	"time"
)

func TestIdleSenderReleasesLargeIndexes(t *testing.T) {
	s := NewSender(1 << 30)
	for range 4096 {
		if err := s.Append(NewBuffer([]byte{1}, nil)); err != nil {
			t.Fatal(err)
		}
		segment, _ := s.NextRange(1)
		if err := s.Sent(segment); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Acknowledge(s.Next, s.WindowEnd); err != nil {
		t.Fatal(err)
	}
	if cap(s.segments) > 1024 {
		t.Fatal("idle sender retained high-water index")
	}
	if err := s.Append(NewBuffer([]byte{2}, nil)); err != nil {
		t.Fatal(err)
	}
	segment, ok := s.NextRange(1)
	if !ok || segment.Data()[0] != 2 {
		t.Fatal("reuse after index release failed")
	}
	s.Close()
}

func TestIdlePathReleasesLargeIndexes(t *testing.T) {
	p := Path{Generation: 1}
	now := time.Now()
	for range 4096 {
		if _, err := p.Submitted(1, now); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Feedback(Receipt{Generation: 1, Next: p.Sent}, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if cap(p.flights) > 128 {
		t.Fatal("idle path retained high-water index")
	}
	if _, err := p.Submitted(1, now); err != nil {
		t.Fatal(err)
	}
	if p.Outstanding() != 1 {
		t.Fatal("reuse after index release failed")
	}
	p.Close()
}
