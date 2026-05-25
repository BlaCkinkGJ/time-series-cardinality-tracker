package bench_test

import (
	"fmt"
	"testing"

	"github.com/yourorg/cardinality-tracker/internal/hll"
)

func BenchmarkHLL_Add(b *testing.B) {
	h := hll.New()
	val := []byte("benchmark-value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Add(val)
	}
}

func BenchmarkHLL_Estimate(b *testing.B) {
	h := hll.New()
	for i := 0; i < 10000; i++ {
		h.Add([]byte(fmt.Sprintf("v%d", i)))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Estimate()
	}
}

func BenchmarkEngine_Add_Parallel(b *testing.B) {
	eng := hll.NewEngine()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			eng.Add("ts-bench", []byte(fmt.Sprintf("val-%d", i)))
			i++
		}
	})
}

func BenchmarkHLL_Marshal(b *testing.B) {
	h := hll.New()
	for i := 0; i < 100000; i++ {
		h.Add([]byte(fmt.Sprintf("v%d", i)))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := h.Marshal()
		if err != nil {
			b.Fatal(err)
		}
	}
}
