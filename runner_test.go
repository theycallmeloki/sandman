package main

import "testing"

// TestIsProvisioningError pins the classifier: a pull that FAILED is a
// provisioning failure (crashes the pipeline), while a first-run pull
// that SUCCEEDED — docker prints "Unable to find image" as an
// informational notice even on success — followed by a user-code error
// must NOT be classified as provisioning (the pipeline would wrongly
// stop scheduling on a fixable transform).
func TestIsProvisioningError(t *testing.T) {
	provisioning := []string{
		"docker: invalid reference format",
		"Unable to find image 'nope/nope:latest' locally\ndocker: pull access denied for nope/nope",
		"failed to resolve reference \"nope/nope:latest\": not found",
		"manifest unknown: manifest unknown",
		"No such image: nope:latest",
	}
	for _, s := range provisioning {
		if !isProvisioningError(s) {
			t.Errorf("isProvisioningError(%q) = false, want true (pull failed)", s)
		}
	}
	userCode := []string{
		// a slow first pull that succeeded, then the real user-code failure
		"Unable to find image 'alpine:latest' locally\nPulling from library/alpine\n" +
			"55afa1ecc21d: Pull complete\nStatus: Downloaded newer image for alpine:latest\n" +
			"cat: read error: Is a directory",
		"Pulling from library/alpine\nPull complete\nsh: boom: not found",
		"Status: Downloaded newer image for alpine:latest\nexit status 1",
		"plain user-code failure with no image chatter",
	}
	for _, s := range userCode {
		if isProvisioningError(s) {
			t.Errorf("isProvisioningError(%q) = true, want false (pull succeeded / user code)", s)
		}
	}
}
