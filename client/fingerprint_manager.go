package client

import (
	"log"
	"time"

	"github.com/hashicorp/nomad/client/config"
	"github.com/hashicorp/nomad/client/driver"
	"github.com/hashicorp/nomad/client/fingerprint"
	cstructs "github.com/hashicorp/nomad/client/structs"
	"github.com/hashicorp/nomad/nomad/structs"
)

// FingerprintManager runs a client fingerprinters on a continuous basis, and
// updates the client when the node has changed
type FingerprintManager struct {
	getConfig func() *config.Config
	node      *structs.Node
	client    *Client
	logger    *log.Logger
}

// run is an  interfal function which runs each fingerprinter
// individually on an ongoing basis
func (fm *FingerprintManager) run(f fingerprint.Fingerprint, period time.Duration, name string) {
	fm.logger.Printf("[DEBUG] fingerprint_manager: fingerprinting %s every %v", name, period)

	for {
		select {
		case <-time.After(period):
			if err := fm.fingerprint(name, f); err != nil {
				fm.logger.Printf("[DEBUG] fingerprint_manager: periodic fingerprinting for %v failed: %+v", name, err)
				continue
			}

		case <-fm.client.shutdownCh:
			return
		}
	}
}

// setupDrivers is used to fingerprint the node to see if these drivers are
// supported
func (fm *FingerprintManager) SetupDrivers(drivers []string) error {
	driverCtx := driver.NewDriverContext("", "", fm.getConfig(), fm.node, fm.logger, nil)
	for _, name := range drivers {

		d, err := driver.NewDriver(name, driverCtx)
		if err != nil {
			return err
		}

		if err := fm.fingerprint(name, d); err != nil {
			fm.logger.Printf("[DEBUG] fingerprint_manager: fingerprinting for %v failed: %+v", name, err)
			return err
		}

		p, period := d.Periodic()
		if p {
			go fm.run(d, period, name)
		}
	}

	return nil
}

// fingerprint does an initial fingerprint of the client. If the fingerprinter
// is meant to be run continuously, a process is launched ro perform this
// fingerprint on an ongoing basis in the background.
func (fm *FingerprintManager) fingerprint(name string, f fingerprint.Fingerprint) error {
	request := &cstructs.FingerprintRequest{Config: fm.getConfig(), Node: fm.node}
	var response cstructs.FingerprintResponse
	if err := f.Fingerprint(request, &response); err != nil {
		return err
	}

	fm.client.updateNodeFromFingerprint(&response)
	return nil
}

// setupDrivers is used to fingerprint the node to see if these attributes are
// supported
func (fm *FingerprintManager) SetupFingerprints(fingerprints []string) error {
	for _, name := range fingerprints {
		f, err := fingerprint.NewFingerprint(name, fm.logger)

		if err != nil {
			fm.logger.Printf("[DEBUG] fingerprint_manager: fingerprinting for %v failed: %+v", name, err)
			return err
		}

		if err := fm.fingerprint(name, f); err != nil {
			return err
		}

		p, period := f.Periodic()
		if p {
			go fm.run(f, period, name)
		}
	}
	return nil
}
