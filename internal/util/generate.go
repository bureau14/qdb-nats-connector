// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package util: testing & development utilities
// Types: none
// Ex: util.RandomAlias() → "a8Bc3dEf9GhI2jKl"
package util

import (
	"bytes"
	"math/rand"
)

// latin: safe alphanumeric chars for IDs, avoids ambiguous chars
const latin = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

// RandomAlias generates 16-char alphanumeric ID.
// Out: string - [A-Za-z0-9]{16}
// Ex: RandomAlias() → "a8Bc3dEf9GhI2jKl"
func RandomAlias() string {
	const size = 16
	var buffer bytes.Buffer
	for i := 0; i < size; i++ {
		buffer.WriteString(string(latin[rand.Intn(len(latin))]))
	}

	return buffer.String()
}

// RandomTopicName creates test topic name.
// Out: string - 16-char topic
// Ex: RandomTopicName() → "topic1a2b3c4d5e6"
func RandomTopicName() string {
	return RandomAlias()
}

// GenerateTestData creates test messages (TODO).
// Out: ∅ - placeholder
// Ex: GenerateTestData() → TODO
func GenerateTestData() {
}
