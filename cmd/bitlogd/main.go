package main

import "net/http"

func main() {
	(&http.Server{}).ListenAndServe()
}
