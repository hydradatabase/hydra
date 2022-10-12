package acceptance

import (
	"flag"
	"log"
	"os"
	"testing"
)

var (
	flagHydraImage    string
	flagHydraAllImage string
)

func init() {
	flag.StringVar(&flagHydraImage, "hydra-image", os.Getenv("HYDRA_IMAGE"), "Hydra image")
}

func TestMain(m *testing.M) {
	flag.Parse()
	if flagHydraImage == "" {
		log.Fatal("missing -hydra-image")
	}

	os.Exit(m.Run())
}
