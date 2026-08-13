package simulator

import (
	"math"
	"testing"
	"time"
)

func TestTargetRPSHoldsBaselineBeforeAndAfter(t *testing.T) {
	s := Scenario{BaselineRPS: 10, PeakRPS: 100, RampUp: 5 * time.Second, PeakDuration: 10 * time.Second, RampDown: 5 * time.Second}

	if got := s.TargetRPS(-time.Second); got != 10 {
		t.Errorf("TargetRPS before start = %v, want 10", got)
	}
	if got := s.TargetRPS(s.TotalDuration() + time.Second); got != 10 {
		t.Errorf("TargetRPS after end = %v, want 10", got)
	}
}

func TestTargetRPSRampsLinearlyUp(t *testing.T) {
	s := Scenario{BaselineRPS: 0, PeakRPS: 100, RampUp: 10 * time.Second, PeakDuration: 5 * time.Second}

	if got := s.TargetRPS(0); got != 0 {
		t.Errorf("TargetRPS(0) = %v, want 0", got)
	}
	if got := s.TargetRPS(5 * time.Second); math.Abs(got-50) > 0.001 {
		t.Errorf("TargetRPS(halfway) = %v, want 50", got)
	}
	if got := s.TargetRPS(10 * time.Second); math.Abs(got-100) > 0.001 {
		t.Errorf("TargetRPS(end of ramp) = %v, want 100", got)
	}
}

func TestTargetRPSPlateausAtPeak(t *testing.T) {
	s := Scenario{BaselineRPS: 10, PeakRPS: 100, RampUp: 2 * time.Second, PeakDuration: 10 * time.Second, RampDown: 2 * time.Second}

	for _, elapsed := range []time.Duration{2 * time.Second, 5 * time.Second, 11 * time.Second, 12 * time.Second} {
		if got := s.TargetRPS(elapsed); got != 100 {
			t.Errorf("TargetRPS(%v) = %v, want 100 (within plateau)", elapsed, got)
		}
	}
}

func TestTargetRPSRampsLinearlyDown(t *testing.T) {
	s := Scenario{BaselineRPS: 0, PeakRPS: 100, PeakDuration: 5 * time.Second, RampDown: 10 * time.Second}

	if got := s.TargetRPS(5 * time.Second); got != 100 {
		t.Errorf("TargetRPS(start of rampdown) = %v, want 100", got)
	}
	if got := s.TargetRPS(10 * time.Second); math.Abs(got-50) > 0.001 {
		t.Errorf("TargetRPS(halfway down) = %v, want 50", got)
	}
	if got := s.TargetRPS(15 * time.Second); math.Abs(got-0) > 0.001 {
		t.Errorf("TargetRPS(end of rampdown) = %v, want 0", got)
	}
}

func TestAllScenariosHaveValidShape(t *testing.T) {
	for _, s := range All() {
		if s.Name == "" {
			t.Error("scenario with empty name")
		}
		if s.PeakRPS <= 0 {
			t.Errorf("%s: PeakRPS = %v, want > 0", s.Name, s.PeakRPS)
		}
		if s.WriteRatio < 0 || s.WriteRatio > 1 {
			t.Errorf("%s: WriteRatio = %v, want [0,1]", s.Name, s.WriteRatio)
		}
		if s.HotKeyFraction < 0 || s.HotKeyFraction > 1 {
			t.Errorf("%s: HotKeyFraction = %v, want [0,1]", s.Name, s.HotKeyFraction)
		}
		if s.TotalDuration() <= 0 {
			t.Errorf("%s: TotalDuration = %v, want > 0", s.Name, s.TotalDuration())
		}
	}
}

func TestByNameFindsEveryScenario(t *testing.T) {
	for _, s := range All() {
		got, ok := ByName(s.Name)
		if !ok {
			t.Errorf("ByName(%q) not found", s.Name)
		}
		if got.Name != s.Name {
			t.Errorf("ByName(%q).Name = %q", s.Name, got.Name)
		}
	}
	if _, ok := ByName("nonexistent"); ok {
		t.Error("ByName(nonexistent) = ok, want not found")
	}
}
