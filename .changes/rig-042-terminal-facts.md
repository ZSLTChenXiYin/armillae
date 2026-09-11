---
armillae-llm-rig: "patch:fix"
---

Upgrade all Rig Provider profiles to exactly 0.42.0 and reject EOF without a genuine Provider terminal event.

Preserve native finish reasons before Rig content aggregation, migrate ToolResult names and Provider ID replay, and retain newly exposed Anthropic events. Cover interruptions, cancellation, duplicate terminals and errors without retries. Live verification remains pending.
