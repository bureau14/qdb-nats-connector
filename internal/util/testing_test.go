package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUtilRandomAlias(t *testing.T) {
	alias1 := RandomAlias()
	alias2 := RandomAlias()

	assert.Len(t, alias1, 16, "alias should be 16 characters")
	assert.Len(t, alias2, 16, "alias should be 16 characters")
	assert.NotEqual(t, alias1, alias2, "aliases should be unique")
	assert.Regexp(t, `^[A-Za-z0-9]+$`, alias1, "alias should contain only alphanumeric characters")
}

func TestUtilRandomTopicName(t *testing.T) {
	topic1 := RandomTopicName()
	topic2 := RandomTopicName()

	assert.Len(t, topic1, 16, "topic name should be 16 characters")
	assert.Len(t, topic2, 16, "topic name should be 16 characters")
	assert.NotEqual(t, topic1, topic2, "topic names should be unique")
	assert.Regexp(t, `^[A-Za-z0-9]+$`, topic1, "topic name should contain only alphanumeric characters")
}

func TestUtilRandomNumber(t *testing.T) {
	num := RandomNumber()
	assert.GreaterOrEqual(t, num, 0, "random number should be non-negative")
	assert.Less(t, num, 1000000, "random number should be less than 1000000")
}

func TestUtilEmail(t *testing.T) {
	email := Email()
	assert.Contains(t, email, "@example.com", "email should contain domain")
	assert.Contains(t, email, "test", "email should contain test prefix")
	assert.Regexp(t, `^test\d+@example\.com$`, email, "email should match expected pattern")
}
