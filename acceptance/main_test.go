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
	flag.StringVar(&flagHydraAllImage, "hydra-all-image", os.Getenv("HYDRA_ALL_IMAGE"), "Hydra all image")
}

func TestMain(m *testing.M) {
	flag.Parse()
	if flagHydraImage == "" {
		log.Fatal("missing -hydra-image")
	}
	if flagHydraAllImage == "" {
		log.Fatal("missing -hydra-all-image")
	}

	os.Exit(m.Run())
}
