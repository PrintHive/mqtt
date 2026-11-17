# pstack skills (vendored)

Three skills from [pstack](https://github.com/cursor/plugins/tree/main/pstack) 0.15.36, MIT licensed (see LICENSE),
copied (without the statechart test suite) from the copy PrintHive/hive vendors at `repos/pstack` (hive commit 696616650c). Linked into `.agents/skills`
and `.claude/skills`. Do not edit them here; update by copying a newer release over this folder.

- `statechart`: runs TLC on a TLA+ model and draws it as a Mermaid statechart with each rule's shortest bug path.
- `create-formal-verification-skill`, `maintain-formal-verification-skill`: how `verify-*` formal skills are made and kept in step.
