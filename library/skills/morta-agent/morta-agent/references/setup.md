# Set up MortaAgent — v0.5.0

This test candidate supports **Claude Desktop and Codex**. It contains instructions, practical references and a draft hub-instructions template. Each user connects their own Morta access separately. No local Morta tool suite, shared account, hosted service or paid API key for the assistant is required by this skill.

Follow only the steps still needed. An existing working connection should be reused. Account sign-in, credential entry and any organisation-controlled settings stay with the user or their authorised administrator; the agent must not ask for secrets in chat or take over the desktop without permission.

## 1. Install the skill

### Claude Desktop

1. Check that your account permits custom skills and that code execution/file creation is enabled. If the organisation controls these settings, follow its existing administrator process.
2. In **Customize > Skills**, use the add/upload-skill option and select `morta-agent-v0.5.0.zip`. Upload the ZIP, not individual reference files. Enable MortaAgent.
3. Start a fresh chat and ask: “Use MortaAgent. Read its SKILL.md and tell me its version before doing anything in Morta.” Expect **0.5.0** and **test-candidate**. This checks loading, not Morta connectivity.

The ZIP has one root folder, `morta-agent/`, containing `SKILL.md` and its references. If the upload control is absent, check the current [Claude skill guide](https://support.claude.com/en/articles/12512198-how-to-create-custom-skills) and account policy rather than opening a new paid service or assuming the file is broken.

### Codex

1. Extract the ZIP once. For personal use, place the resulting `morta-agent` folder under your home directory's `.agents/skills/`. On Windows this is `%USERPROFILE%\.agents\skills\morta-agent`; on macOS/Linux it is `~/.agents/skills/morta-agent`.
2. Alternatively, for a repository-scoped installation, place the folder in that repository's `.agents/skills/`. Choose one discoverable installation for the intended scope; do not install identical copies at several levels.
3. Check that the path ends `morta-agent/SKILL.md`, not `morta-agent/morta-agent/SKILL.md`. In a new turn, invoke `$morta-agent` and ask it to read and report its version. If it is not discovered, refresh/restart Codex and inspect existing skill enablement settings before reinstalling.

These are application discovery locations, not a requirement to store other project files in the home directory. Existing customised skill locations can be retained if the installed host supports them. See [Codex skill locations](https://learn.chatgpt.com/docs/build-skills).

## 2. Connect your own Morta access

Skip configuration if this assistant already has a working Morta connection. Inspect available tools first; a skill installation alone does not install a connector.

For a new local connection, Morta documents the `morta-mcp` Node package. You need Node.js with `npx` available and a Morta API key scoped to the intended resources and operations. Use Morta's API-key controls or the existing administrator process to create that key. A document/table-scoped key is valid; do not grant full-hub access merely to simplify setup. Use read access if your intended work only requires reading.

The configurations below pin `morta-mcp@1.4.0`, whose published package was inspected on 9 October 2026. They are documentation examples, not evidence of an installation test. A later connector version must be checked against its own published instructions before substituting it. The first connector start may download the package.

Enter the real key only in private local configuration. Do not send it to the assistant, commit it to a repository, or include it in screenshots or the shared skill package.

### Claude Desktop connection

Open Claude Desktop's settings, then **Developer > Edit Config**. Merge a `morta_api` entry into the existing `mcpServers` object; preserve all other entries. Replace the placeholder locally:

```json
{
  "mcpServers": {
    "morta_api": {
      "command": "npx",
      "args": ["-y", "morta-mcp@1.4.0", "--client=claude", "--tools=dynamic"],
      "env": {
        "MORTA_API_KEY": "REPLACE_LOCALLY_WITH_YOUR_KEY"
      }
    }
  }
}
```

Fully exit and reopen Claude Desktop, then inspect the Morta server's status. Do not upload the private configuration to chat. The connection is local; this guide does not establish browser-only Claude access.

### Codex connection

Use your personal `~/.codex/config.toml` (on Windows, `%USERPROFILE%\.codex\config.toml`). Merge this entry without replacing unrelated configuration. If Morta is already configured under another name, reuse or update that entry instead of creating another server.

```toml
[mcp_servers.morta_api]
command = "npx"
args = ["-y", "morta-mcp@1.4.0", "--tools=dynamic"]

[mcp_servers.morta_api.env]
MORTA_API_KEY = "REPLACE_LOCALLY_WITH_YOUR_KEY"
```

A user-managed environment variable is an alternative: replace the `env` subtable with `env_vars = ["MORTA_API_KEY"]` in the server table, and ensure the variable is present in the environment that launches Codex. Do not assume a desktop application inherits a terminal's variables.

Refresh/restart the connection and inspect its status. The CLI's `codex mcp list` can list configured servers; a listed server still needs a successful read to prove connectivity. No `--client=codex` flag is prescribed because that value was not in this connector's inspected client list.

If Windows reports that `npx` cannot be launched despite Node being installed, check its actual command path. A Windows command-processor fallback to verify locally is `command = "cmd"` with arguments `["/c", "npx", "-y", "morta-mcp@1.4.0", "--tools=dynamic"]`; for Claude retain its `--client=claude` argument as well. Apply a platform correction only to the affected entry and recheck startup; do not treat it as a successful Morta read.

## 3. Verify the intended target with a read

Give the assistant the exact Morta hub, document or table link you want to use. For example:

> Use MortaAgent to check my connection and read this document or table: [paste its link]. Identify the resource, find any accessible governing instructions and tell me what is verified. Do not change anything.

The agent should:

- Identify the loaded skill version and available connector operations without exposing credentials.
- Read the supplied resource directly. Verify its identity and available account context; report anything the scope does not expose.
- If no target was supplied, use accessible inventory or ask for a link. Never pick the first hub automatically.
- Follow [hub-rule discovery](hub-rules.md), using an accessible designated source. A denied hub inventory must not prevent a permitted direct-resource read.
- Inspect data only as needed for the requested first check. Do not export unrelated resources or probe writes as a connectivity test.

Keep the outcomes separate: **skill loaded**, **connector configured**, **scoped read verified**, **governing instructions established**, and **write capability/authority verified or unresolved**. A successful read does not prove write access. If applicable rules remain inaccessible, explain that connected reads work while dependent writes remain unresolved.

## 4. Establish instructions only when needed

Reuse adopted instructions. If none can be established and the user wants them created, use the [generic template](../assets/hub-ai-instructions.md) to propose purpose, audiences, authoritative resources, relevant [design patterns](design-patterns.md), defaults and actual rule-editing delegation.

Resolve template prompts from verified context. Remove inapplicable examples rather than creating registers or fields to satisfy the template. Creation/adoption and any canonical navigation link need existing authority. Follow the [amendment procedure](hub-rules.md#apply-a-rule-change), then read back the adopted content. A template never authorises its own adoption or a production migration.

## Diagnose the specific gap

| Observation | Next step |
| --- | --- |
| Skill not visible or wrong version | Check folder/ZIP structure, selected account, enablement and duplicate installations |
| Connector not configured | Follow only the relevant local configuration route above |
| Server fails to start | Inspect a redacted startup error, Node/npx availability and configuration syntax |
| Authentication rejected | Have the user check the configured key locally; do not collect it in chat |
| Named operation denied | Record the denied scope; continue permitted reads, and seek only the access actually needed |
| Operation absent from tool catalog | Check dynamic endpoint discovery/schema if exposed; otherwise explain the connector limitation |
| Error does not distinguish absence from denial | Keep the diagnosis unresolved; do not assert a product limitation or request broader access |
| Rules cannot be read | Seek an accessible authoritative source; keep dependent writes unresolved and continue independent inspection |
| Permission assignments cannot be inspected | State that effective access has not been audited; do not infer it from one successful read |
| Layout saved but cannot be rendered here | Report persisted settings separately from visual verification |

Do not create an alternative account, install another connector, change approval policy or start a hosted service merely to bypass a failure.

## Upgrade without losing customisations

Identify the installed path/account and loaded version first. Keep a copy of locally changed skill files outside discoverable skill locations; compare them with the new release. Preserve user additions, preferably moving hub-specific conventions into that hub's authorised instructions rather than the shared package. Do not copy the example credential placeholder over working connection settings.

For Codex, update the chosen installation as a complete folder without nesting it or leaving a second enabled copy. For Claude, use the available replacement/update workflow; if upload produces a separate entry, enable the intended version and disable the older one after preserving customisations. Removing the previous entry is a separate user choice. Verify the newly loaded version and repeat the scoped read.

## Sources and verification limits

Setup documentation checked on **9 October 2026**:
- [Morta's connector instructions](https://www.morta.io/help/claude-mcp-server).
- [Official connector source](https://github.com/morta-technology/morta-typescript/tree/main/packages/mcp-server) and [pinned package metadata](https://registry.npmjs.org/morta-mcp/1.4.0).
- [Claude custom skills](https://support.claude.com/en/articles/12512198-how-to-create-custom-skills).
- [Codex skills](https://learn.chatgpt.com/docs/build-skills) and [MCP configuration](https://learn.chatgpt.com/docs/extend/mcp?surface=cli).

Consult the accompanying testing guide for release-specific results. Published setup instructions, a valid package and simulated behaviour checks do not prove a real Claude/Codex installation, live connector operation or rendered Morta layout.
