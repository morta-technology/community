# Start with MortaAgent

Download [MortaAgent v0.5.0](morta-agent-v0.5.0.zip). This is a test candidate for Claude Desktop and Codex.

## 1. Install in your app

**Claude Desktop**

1. Enable code execution/file creation if your account permits it.
2. Open **Customize → Skills**, choose the upload option and select the ZIP unchanged.
3. Enable MortaAgent and start a fresh chat.

**Codex**

1. Extract the ZIP and place its `morta-agent` folder in one skill location:
   - Personal: `~/.agents/skills/` (`%USERPROFILE%\.agents\skills\` on Windows).
   - This project only: `.agents/skills/` inside the project.
2. Check that the result ends in `morta-agent/SKILL.md`, without an extra nested folder.
3. In a new turn, invoke `$morta-agent`. Refresh or restart Codex if it is not discovered.

In either app, ask:

> Use MortaAgent. Read its SKILL.md and tell me its version before doing anything in Morta.

Expect **0.5.0**, marked **test-candidate**.

## 2. Connect Morta

Already connected? Keep that connection and go to step 3.

For a new connection, you need Node.js with `npx` and a Morta API key limited to the resources and actions you need. A read-only key works for the first check. Enter the key privately in your app's local configuration, never in chat or the repository.

Use the configuration for your app:

- [Claude Desktop connection](morta-agent/references/setup.md#claude-desktop-connection)
- [Codex connection](morta-agent/references/setup.md#codex-connection)

These examples use `morta-mcp@1.4.0`. Preserve other connections when adding it. Installing the skill alone does not connect Morta.

## 3. Try one read

Paste a table or document link you can access:

> Use MortaAgent to read this resource: [link]. Tell me what it contains and which hub instructions apply. Do not change anything.

Check that it identifies the correct resource and explains any access or instruction gaps. Then try a count or total you can verify yourself. A successful read does not establish write access.

## If you need more

- [Troubleshooting](morta-agent/references/setup.md#diagnose-the-specific-gap)
- [Upgrade without losing customisations](morta-agent/references/setup.md#upgrade-without-losing-customisations)
- [Draft hub instructions](morta-agent/assets/hub-ai-instructions.md) — optional; reuse existing rules where available.
- [Test results and remaining checks](TESTING.md)
