package helpers

import "runtime"

func GetAvailableMemory() uint64 {
	var memStats runtime.MemStats

	runtime.ReadMemStats(&memStats)

	availableMemory := memStats.Sys - memStats.HeapInuse

	return availableMemory
}
