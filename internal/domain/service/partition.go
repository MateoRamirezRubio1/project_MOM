package service

import "hash/fnv"

func HashPartition(key string, parts int) int {
	if parts == 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32()) % parts
}
