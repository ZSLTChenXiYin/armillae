---
armillae-llm: "minor:feat"
armillae-llm-rig: "minor:feat"
---

Expose `TransportConfig.max_redirects` across all seven Provider entries for complete and stream calls. Zero disables automatic redirects; positive values use reqwest's bounded redirect policy.

The default changes from reqwest's implicit ten redirects to no redirects, including for existing serialized configurations that omit the new field. Rust struct literals must supply the field or use `..Default::default()`. Positive limits can follow cross-origin redirects and do not reapply construction-time EndpointPolicy to redirect targets. HTTP failures retain status diagnostics. Redirect-limit errors retain the typed transport cause where Rig exposes it; Rig 0.42 SSE paths flatten status-less transport errors, so those streams terminate with StreamInterrupted without a redirect-specific transport kind.
