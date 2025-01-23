package handlers

import "strings"

func ParseURLPath(path string) []string {
	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return []string{}
	}
	return strings.Split(trimmed, "/")
}

func IsValidDir(s string) bool {
	if strings.Contains(s, ".") || strings.Contains(s, "..") || strings.Contains(s, "/") || strings.Contains(s, "\\") || strings.Contains(s, ":") || strings.Contains(s, "~") || strings.Contains(s, "*") {
		return false
	}
	return true
}
