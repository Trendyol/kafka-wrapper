package utils

import (
	"unicode"
	"unicode/utf8"
)

func HasSuffixCaseInsensitive(main, compared string) bool {
	if len(compared) >= len(main) {
		return false
	}

	suffix := main[len(main)-len(compared):]
	for i, w := 0, 0; i < len(suffix); i += w {
		mainRuneValue, width := utf8.DecodeRuneInString(suffix[i:])
		comparedValue, _ := utf8.DecodeRuneInString(compared[i:])

		w = width

		if mainRuneValue == comparedValue {
			continue
		}

		if unicode.ToLower(mainRuneValue) != unicode.ToLower(comparedValue) {
			return false
		}

	}

	return true
}
