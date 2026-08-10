// Secrets: named typed metadata blobs with create/inspect/list/delete,
// and a credential requirement on every secret-management operation
// (SB-153, SB-154).
package conformance

import (
	"fmt"
	"strings"
	"testing"

	"sandman/client"
)

func TestSB153_SecretsCRUD(t *testing.T) {
	// baseline listing
	base, err := c.ListSecrets()
	if err != nil {
		t.Fatalf("list secrets: %v", err)
	}
	if len(base) != 0 {
		t.Fatalf("baseline has %d secrets, want 0", len(base))
	}

	if err := c.CreateSecret("test-secret", map[string]string{"mykey": "bXktdmFsdWU="}); err != nil {
		t.Fatalf("create secret: %v", err)
	}

	info, err := c.InspectSecret("test-secret")
	if err != nil {
		t.Fatalf("inspect secret: %v", err)
	}
	if info.Name != "test-secret" {
		t.Fatalf("secret name = %q", info.Name)
	}
	if info.Type != "Opaque" {
		t.Fatalf("secret type = %q, want Opaque", info.Type)
	}
	if info.Created == "" {
		t.Fatalf("secret has no creation timestamp")
	}

	listed, err := c.ListSecrets()
	if err != nil {
		t.Fatalf("list secrets: %v", err)
	}
	if len(listed) != len(base)+1 {
		t.Fatalf("listed %d secrets, want %d", len(listed), len(base)+1)
	}

	if err := c.DeleteSecret("test-secret"); err != nil {
		t.Fatalf("delete secret: %v", err)
	}
	listed, err = c.ListSecrets()
	if err != nil {
		t.Fatalf("list after delete: %v", err)
	}
	if len(listed) != len(base) {
		t.Fatalf("listed %d secrets after delete, want back to baseline %d", len(listed), len(base))
	}
	// a second delete is a no-op and never resurrects the secret
	if err := c.DeleteSecret("test-secret"); err != nil {
		t.Fatalf("second delete: %v", err)
	}
	if _, err := c.InspectSecret("test-secret"); err == nil {
		t.Fatalf("inspect after delete: expected an error")
	} else if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("inspect-after-delete error = %q", err.Error())
	}
}

func TestSB154_SecretsRequireCredential(t *testing.T) {
	anon := client.New(fmt.Sprintf("127.0.0.1:%d", daemonPort))
	// no token: every secret-management operation is rejected uniformly
	if err := anon.CreateSecret("s", map[string]string{"k": "v"}); err == nil {
		t.Fatalf("create without token: expected error")
	} else if !strings.Contains(err.Error(), "no authentication token") {
		t.Fatalf("create error = %q, want it to name the missing token", err.Error())
	}
	if _, err := anon.InspectSecret("s"); err == nil {
		t.Fatalf("inspect without token: expected error")
	} else if !strings.Contains(err.Error(), "no authentication token") {
		t.Fatalf("inspect error = %q", err.Error())
	}
	if _, err := anon.ListSecrets(); err == nil {
		t.Fatalf("list without token: expected error")
	} else if !strings.Contains(err.Error(), "no authentication token") {
		t.Fatalf("list error = %q", err.Error())
	}
	if err := anon.DeleteSecret("s"); err == nil {
		t.Fatalf("delete without token: expected error")
	} else if !strings.Contains(err.Error(), "no authentication token") {
		t.Fatalf("delete error = %q", err.Error())
	}
	// a wrong token is also rejected
	wrong := client.New(fmt.Sprintf("127.0.0.1:%d", daemonPort))
	wrong.SetToken("wrong-token")
	if err := wrong.CreateSecret("s", map[string]string{"k": "v"}); err == nil {
		t.Fatalf("create with a wrong token: expected error")
	}
	// the credentialed client still works
	if err := c.CreateSecret("s", map[string]string{"k": "v"}); err != nil {
		t.Fatalf("create with credential: %v", err)
	}
	if err := c.DeleteSecret("s"); err != nil {
		t.Fatalf("delete with credential: %v", err)
	}
}
