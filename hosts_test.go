package main

import (
	"reflect"
	"testing"
	"time"
)

// GPU allocation: pickAndReserve hands out concrete device indices per
// job — never "all GPUs" — with deterministic placement (most free
// devices first, then name order), exclusivity for the job's lifetime,
// and release on settlement.

func mkGpus(idxs ...int) []GpuInfo {
	var g []GpuInfo
	for _, i := range idxs {
		g = append(g, GpuInfo{Index: i, Name: "test-gpu", MemoryMiB: 8192})
	}
	return g
}

func TestRegistryGPUAllocationExclusive(t *testing.T) {
	r := newHostRegistry(5 * time.Second)
	r.register("hostA", "10.0.0.1:4343", nil, mkGpus(0, 1))

	h, g, ok := r.pickAndReserve("", 1)
	if !ok || h.Name != "hostA" || !reflect.DeepEqual(g, []int{0}) {
		t.Fatalf("first pick = %v %v %v, want hostA [0]", h.Name, g, ok)
	}
	h2, g2, ok2 := r.pickAndReserve("", 1)
	if !ok2 || h2.Name != "hostA" || !reflect.DeepEqual(g2, []int{1}) {
		t.Fatalf("second pick = %v %v %v, want hostA [1] — devices must be exclusive", h2.Name, g2, ok2)
	}
	if _, _, ok3 := r.pickAndReserve("", 1); ok3 {
		t.Fatalf("third pick succeeded with both devices busy — a GPU job must never oversubscribe a device")
	}
	// the wire list reflects the allocation
	l := r.list()
	if len(l) != 1 || l[0].Gpus[0].Busy != true || l[0].Gpus[1].Busy != true {
		t.Fatalf("list busy flags = %+v, want both busy", l)
	}

	r.release("hostA", g)
	h3, g3, ok3 := r.pickAndReserve("", 1)
	if !ok3 || h3.Name != "hostA" || !reflect.DeepEqual(g3, []int{0}) {
		t.Fatalf("post-release pick = %v %v %v, want hostA [0]", h3.Name, g3, ok3)
	}
}

func TestRegistryGPUSpreadsAcrossHosts(t *testing.T) {
	r := newHostRegistry(5 * time.Second)
	r.register("hostA", "10.0.0.1:4343", nil, mkGpus(0, 1))
	r.register("hostB", "10.0.0.2:4343", nil, mkGpus(0, 1))

	// equal free counts: name order first
	h, g, ok := r.pickAndReserve("", 1)
	if !ok || h.Name != "hostA" || !reflect.DeepEqual(g, []int{0}) {
		t.Fatalf("pick 1 = %v %v %v, want hostA [0]", h.Name, g, ok)
	}
	// hostB now has more free devices: GPU load must spread, not pile
	h2, g2, ok2 := r.pickAndReserve("", 1)
	if !ok2 || h2.Name != "hostB" || !reflect.DeepEqual(g2, []int{0}) {
		t.Fatalf("pick 2 = %v %v %v, want hostB [0] (most-free-first)", h2.Name, g2, ok2)
	}
	h3, g3, ok3 := r.pickAndReserve("", 1)
	if !ok3 || h3.Name != "hostA" || !reflect.DeepEqual(g3, []int{1}) {
		t.Fatalf("pick 3 = %v %v %v, want hostA [1]", h3.Name, g3, ok3)
	}
	h4, g4, ok4 := r.pickAndReserve("", 1)
	if !ok4 || h4.Name != "hostB" || !reflect.DeepEqual(g4, []int{1}) {
		t.Fatalf("pick 4 = %v %v %v, want hostB [1]", h4.Name, g4, ok4)
	}
	if _, _, ok5 := r.pickAndReserve("", 1); ok5 {
		t.Fatalf("pick 5 succeeded with every device busy")
	}
}

func TestRegistryGPUWithLabel(t *testing.T) {
	r := newHostRegistry(5 * time.Second)
	r.register("hostA", "10.0.0.1:4343", nil, mkGpus(0))
	r.register("hostB", "10.0.0.2:4343", []string{"big"}, mkGpus(0, 1))

	// label filter excludes hostA even though it has a free device
	h, g, ok := r.pickAndReserve("big", 2)
	if !ok || h.Name != "hostB" || !reflect.DeepEqual(g, []int{0, 1}) {
		t.Fatalf("labeled pick = %v %v %v, want hostB [0 1]", h.Name, g, ok)
	}
	if _, _, ok2 := r.pickAndReserve("big", 1); ok2 {
		t.Fatalf("labeled pick oversubscribed hostB")
	}
	// an unlabeled GPU request still sees hostA
	h3, g3, ok3 := r.pickAndReserve("", 1)
	if !ok3 || h3.Name != "hostA" || !reflect.DeepEqual(g3, []int{0}) {
		t.Fatalf("unlabeled pick = %v %v %v, want hostA [0]", h3.Name, g3, ok3)
	}
}

func TestRegistryGPUHeartbeatKeepsAllocation(t *testing.T) {
	r := newHostRegistry(5 * time.Second)
	r.register("hostA", "10.0.0.1:4343", nil, mkGpus(0, 1))
	if _, _, ok := r.pickAndReserve("", 1); !ok {
		t.Fatalf("initial pick failed")
	}

	// a heartbeat refresh re-asserts capacity; the in-flight allocation
	// must survive it (the job still holds the device)
	r.register("hostA", "10.0.0.1:4343", nil, mkGpus(0, 1))
	h2, g2, ok2 := r.pickAndReserve("", 1)
	if !ok2 || h2.Name != "hostA" || !reflect.DeepEqual(g2, []int{1}) {
		t.Fatalf("post-heartbeat pick = %v %v %v, want hostA [1]", h2.Name, g2, ok2)
	}

	// a capacity shrink drops allocations for devices no longer reported
	r.register("hostA", "10.0.0.1:4343", nil, mkGpus(1))
	if _, _, ok3 := r.pickAndReserve("", 1); ok3 {
		t.Fatalf("pick succeeded after capacity shrank to a busy device")
	}
	r.register("hostA", "10.0.0.1:4343", nil, mkGpus(0, 1))
	h4, g4, ok4 := r.pickAndReserve("", 1)
	if !ok4 || h4.Name != "hostA" || !reflect.DeepEqual(g4, []int{0}) {
		t.Fatalf("post-shrink pick = %v %v %v, want hostA [0]", h4.Name, g4, ok4)
	}
}

func TestRegistryGPUZeroWantKeepsLabelPlacement(t *testing.T) {
	r := newHostRegistry(5 * time.Second)
	r.register("hostB", "10.0.0.2:4343", []string{"gpu"}, mkGpus(0))
	r.register("hostA", "10.0.0.1:4343", nil, nil)

	// a non-GPU request ignores GPU capacity entirely: name order, like
	// the pre-GPU placement
	h, g, ok := r.pickAndReserve("", 0)
	if !ok || h.Name != "hostA" || len(g) != 0 {
		t.Fatalf("non-GPU pick = %v %v %v, want hostA with no devices", h.Name, g, ok)
	}
	// a GPU request goes to the GPU host — a host advertising no devices
	// is never chosen for GPU work
	h2, g2, ok2 := r.pickAndReserve("", 1)
	if !ok2 || h2.Name != "hostB" || !reflect.DeepEqual(g2, []int{0}) {
		t.Fatalf("GPU pick = %v %v %v, want hostB [0]", h2.Name, g2, ok2)
	}
	// with the only GPU device busy, a further GPU request is unplaceable
	if _, _, ok3 := r.pickAndReserve("", 1); ok3 {
		t.Fatalf("GPU pick succeeded with no free devices")
	}
}
