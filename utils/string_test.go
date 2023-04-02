package utils

import (
	testifyAssert "github.com/stretchr/testify/assert"
	"testing"
)

func Test_suffix_compare_fails_when_main_string_is_short(t *testing.T) {
	// Given
	main := "TheMainString"

	// When
	response := HasSuffixCaseInsensitive(main, main)

	// Then
	testifyAssert.False(t, response)
}

func Test_suffix_compare_fails_when_compared_sentence_does_not_placed_as_suffix_of_main(t *testing.T) {
	// Given
	main := "TheMainString"
	compared := "main"

	// When
	response := HasSuffixCaseInsensitive(main, compared)

	// Then
	testifyAssert.False(t, response)
}

func Test_suffix_compare_succeeds_when_compared_sentence_is_placed_as_suffix_of_main(t *testing.T) {
	// Given
	main := "TheMain_String"
	compared := "_string"

	// When
	response := HasSuffixCaseInsensitive(main, compared)

	// Then
	testifyAssert.True(t, response)
}
