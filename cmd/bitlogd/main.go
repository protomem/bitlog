package main

import (
	"log"

	"github.com/protomem/bitlog/pkg/version"
)

func main() {
    log.Printf("bitlogd version '%s'", version.Get())
}

