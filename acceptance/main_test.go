package acceptance

import (
	"flag"
	"log"
	"os"
	"testing"
)

var (
	flagHydraImage     string
	flagHydraProdImage string
)

func init() {
	flag.StringVar(&flagHydraImage, "hydra-image", os.Getenv("HYDRA_IMAGE"), "Hydra image")
	flag.StringVar(&flagHydraProdImage, "hydra-prod-image", os.Getenv("HYDRA_PROD_IMAGE"), "Hydra image running in prod")
}

func TestMain(m *testing.M) {
	flag.Parse()
	if flagHydraImage == "" {
		log.Fatal("missing -hydra-image")
	}
	if flagHydraProdImage == "" {
		log.Fatal("missing -hydra-prod-image")
	}

	os.Exit(m.Run())
}
