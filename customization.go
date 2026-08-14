// Execution-environment customization: a pipeline's
// transform may carry a full customization document (PodSpec) and/or a
// JSON modification list (PodPatch) that are validated as JSON at
// creation and applied to every execution participant at provisioning.
//
// The sandman vocabulary (backend-specific per the records):
//
//	{
//	  "env":     {"NAME": "value", ...},            // job environment variables
//	  "volumes": {"<name>": {                        // execution-environment mounts
//	                "hostPath": "/host/path",        //   exactly one source kind
//	                "emptyDir":  true,               //   per volume
//	              }, ...},
//	  "workdir": "/sandman/out"                       // execution working directory
//	}
//
// PodSpec is the base document; PodPatch applies RFC 6902 operations
// (add/replace/remove) to it in order. Volumes mount at
// /sandman/volumes/<name> inside the environment, so user code reaches
// them at a stable path.
package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"sandman/client"
)

type envCustomization struct {
	Env     map[string]string       `json:"env,omitempty"`
	Volumes map[string]volumeCustom `json:"volumes,omitempty"`
	Workdir string                  `json:"workdir,omitempty"`
}

// volumeCustom is one execution-environment volume: exactly one source
// kind — a host path or an ephemeral directory — must be set.
type volumeCustom struct {
	HostPath string `json:"hostPath,omitempty"`
	EmptyDir bool   `json:"emptyDir,omitempty"`
}

// parseCustomization validates a transform's PodSpec/PodPatch and
// resolves them into the applied customization. A pipeline's
// execution-environment customization — a full PodSpec document and/or
// a PodPatch JSON modification list (RFC 6902) — must be validated as
// JSON at creation: malformed spec/patch JSON, unknown top-level keys,
// and volumes specifying other than exactly one source kind fail
// pipeline creation before any execution. A well-formed spec and a
// well-formed patch are both applied to every execution participant at
// provisioning (a patch adding a volume reaches the participant mounted
// at /sandman/volumes/<name> without disturbing data processing), and a
// declared scheduling constraint is honored alongside the customization
// without being overwritten by it. It runs at creation (malformed
// customization fails pipeline creation) and again at provisioning
// (application to execution participants).
func parseCustomization(tr *client.Transform) (*envCustomization, error) {
	if tr == nil {
		return &envCustomization{}, nil
	}
	base := map[string]any{}
	if tr.PodSpec != "" {
		if !json.Valid([]byte(tr.PodSpec)) {
			return nil, fmt.Errorf("pod spec is not valid JSON")
		}
		if err := json.Unmarshal([]byte(tr.PodSpec), &base); err != nil {
			return nil, fmt.Errorf("pod spec: %v", err)
		}
	}
	if tr.PodPatch != "" {
		if !json.Valid([]byte(tr.PodPatch)) {
			return nil, fmt.Errorf("pod patch is not valid JSON")
		}
		var ops []json.RawMessage
		if err := json.Unmarshal([]byte(tr.PodPatch), &ops); err != nil {
			return nil, fmt.Errorf("pod patch: %v", err)
		}
		for _, raw := range ops {
			if err := applyPatchOp(base, raw); err != nil {
				return nil, fmt.Errorf("pod patch: %v", err)
			}
		}
	}
	var out envCustomization
	if err := remarshal(base, &out); err != nil {
		return nil, err
	}
	for name, v := range out.Volumes {
		if (v.HostPath != "") == v.EmptyDir {
			return nil, fmt.Errorf("volume %q must set exactly one source kind (hostPath or emptyDir)", name)
		}
		if v.EmptyDir {
			if v.HostPath != "" {
				return nil, fmt.Errorf("volume %q sets both hostPath and emptyDir", name)
			}
		}
	}
	return &out, nil
}

// applyPatchOp applies one RFC 6902 operation (add/replace/remove) to the
// document; the sandman vocabulary covers object keys under /env, /volumes
// and /workdir.
func applyPatchOp(doc map[string]any, raw json.RawMessage) error {
	var op struct {
		Op    string          `json:"op"`
		Path  string          `json:"path"`
		Value json.RawMessage `json:"value,omitempty"`
	}
	if err := json.Unmarshal(raw, &op); err != nil {
		return fmt.Errorf("invalid patch operation: %v", err)
	}
	if !strings.HasPrefix(op.Path, "/") || op.Path == "/" {
		return fmt.Errorf("patch path %q is not supported", op.Path)
	}
	head, rest, _ := strings.Cut(strings.TrimPrefix(op.Path, "/"), "/")
	switch op.Op {
	case "add", "replace":
		var value any
		if len(op.Value) == 0 {
			return fmt.Errorf("patch %s %q needs a value", op.Op, op.Path)
		}
		if err := json.Unmarshal(op.Value, &value); err != nil {
			return fmt.Errorf("patch %s %q: invalid value: %v", op.Op, op.Path, err)
		}
		section, ok := doc[head]
		if !ok {
			section = map[string]any{}
			doc[head] = section
		}
		m, ok := section.(map[string]any)
		if !ok {
			return fmt.Errorf("patch path %q is not an object", head)
		}
		m[rest] = value
		return nil
	case "remove":
		section, ok := doc[head]
		if !ok {
			return nil // removing an absent key is a no-op
		}
		if m, ok := section.(map[string]any); ok {
			delete(m, rest)
			return nil
		}
		return fmt.Errorf("patch path %q is not an object", head)
	default:
		return fmt.Errorf("unsupported patch operation %q", op.Op)
	}
}

// remarshal converts the document map into the typed customization,
// rejecting unknown top-level keys.
func remarshal(doc map[string]any, out *envCustomization) error {
	b, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(b, &probe); err != nil {
		return err
	}
	for k := range probe {
		switch k {
		case "env", "volumes", "workdir":
		default:
			return fmt.Errorf("unknown customization key %q", k)
		}
	}
	return json.Unmarshal(b, out)
}
