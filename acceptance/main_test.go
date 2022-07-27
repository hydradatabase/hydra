package acceptance

import (
	"flag"
	"log"
	"os"
	"testing"
)

var (
	flagSpiloImage string
)

func init() {
	flag.StringVar(&flagSpiloImage, "spilo-image", os.Getenv("SPILO_IMAGE"), "Spilo image")
}

func TestMain(m *testing.M) {
	flag.Parse()
	if flagSpiloImage == "" {
		log.Fatal("missing -spil-image")
	}

	os.Exit(m.Run())
}
