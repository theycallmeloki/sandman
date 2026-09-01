package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sandman/client"
)

// TestServiceSecretEnv pins the services-get-no-secrets fix: a service
// pipeline's transform secret bindings must reach the job env and mounts
// exactly like batch/spout jobs (execute.go contract). Regression for the
// v0.2.42 gap where runServiceJob built env from transform.env alone and
// service specs with secrets applied cleanly but ran without the values.
func TestServiceSecretEnv(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "secrets"), 0o700); err != nil {
		t.Fatal(err)
	}
	d := &daemon{state: dir}
	writeSecret := func(name string, data map[string]string) {
		b, _ := json.Marshal(map[string]any{"name": name, "type": "Opaque", "data": data})
		if err := os.WriteFile(filepath.Join(dir, "secrets", name+".json"), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeSecret("webhook", map[string]string{"secret": "s3cr3t"})
	writeSecret("multi", map[string]string{"a": "1", "b": "2"})

	pl := pipelineRec{Pipeline: client.Pipeline{Transform: &client.Transform{Secrets: []client.SecretMount{
		{Name: "webhook", Key: "secret", EnvVar: "WEBHOOK_SECRET"},
		{Name: "multi", MountPath: "/run/secrets"},                  // no key -> all keys
		{Name: "webhook", Key: "secret", MountPath: "/run/secrets"}, // merge into same mount dir
	}}}}
	env, mounts, err := d.serviceSecretEnv("job1", pl, nil, nil)
	if err != nil {
		t.Fatalf("serviceSecretEnv: %v", err)
	}

	// env injection
	envOk := false
	for _, e := range env {
		if e == "WEBHOOK_SECRET=s3cr3t" {
			envOk = true
		}
	}
	if !envOk {
		t.Fatalf("env missing WEBHOOK_SECRET=s3cr3t, got %v", env)
	}

	// one shared mount dir for /run/secrets; files a, b, secret written
	vol := ""
	for _, m := range mounts {
		if strings.HasSuffix(m, "/run/secrets") && strings.Contains(m, ":") {
			vol = strings.SplitN(m, ":", 2)[0]
		}
	}
	if vol == "" {
		t.Fatalf("no /run/secrets mount in %v", mounts)
	}
	want := map[string]string{"a": "1", "b": "2", "secret": "s3cr3t"}
	for k, v := range want {
		b, err := os.ReadFile(filepath.Join(vol, k))
		if err != nil {
			t.Fatalf("mount file %s: %v", k, err)
		}
		if string(b) != v {
			t.Fatalf("mount file %s = %q, want %q", k, b, v)
		}
	}

	// distinct keys on one path merge fine
	pl2 := pipelineRec{Pipeline: client.Pipeline{Transform: &client.Transform{Secrets: []client.SecretMount{
		{Name: "multi", MountPath: "/run/secrets"},
		{Name: "webhook", Key: "secret", MountPath: "/run/secrets"},
	}}}}
	if _, _, err := d.serviceSecretEnv("job2", pl2, nil, nil); err != nil {
		t.Fatalf("distinct keys on one path should merge: %v", err)
	}

	// missing secret fails the spawn
	pl3 := pipelineRec{Pipeline: client.Pipeline{Transform: &client.Transform{Secrets: []client.SecretMount{
		{Name: "nope", Key: "x", EnvVar: "X"},
	}}}}
	if _, _, err := d.serviceSecretEnv("job3", pl3, nil, nil); err == nil {
		t.Fatal("missing secret should fail")
	}

	// missing key on an existing secret fails
	pl4 := pipelineRec{Pipeline: client.Pipeline{Transform: &client.Transform{Secrets: []client.SecretMount{
		{Name: "webhook", Key: "missing", EnvVar: "X"},
	}}}}
	if _, _, err := d.serviceSecretEnv("job4", pl4, nil, nil); err == nil {
		t.Fatal("missing key should fail")
	}
}
