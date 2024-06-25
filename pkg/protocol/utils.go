package protocol

import "strings"

func fmtArgs(args ...string) string {
	fargs := make([]string, len(args))
	for i := 0; i < len(args); i++ {
		fargs[i] = quote(args[i])
	}
	return strings.Join(fargs, " ")
}

func quote(s string) string {
	return "'" + s + "'"
}
