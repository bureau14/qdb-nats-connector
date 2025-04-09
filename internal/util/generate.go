package util

import (
	"bytes"
	"math/rand"
)


// Latin / ASCII characters that are "safe" to use everywhere
const latin = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz01233456789"

// Generates a random alias, which consists only of "normal" ASCII characters
func RandomAlias() string {
	const size = 16
	var buffer bytes.Buffer
	for i := 0; i < size; i++ {
		buffer.WriteString(string(latin[rand.Intn(len(latin))]))
	}

	return buffer.String()
}

func RandomTopicName() string {
	return RandomAlias()
}
