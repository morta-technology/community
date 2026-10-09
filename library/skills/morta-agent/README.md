# MortaAgent

**Claude Desktop · Codex · v0.5.0 test candidate**

MortaAgent gives your AI assistant practical guidance for working in Morta: useful views, readable documents and careful table changes, guided by your hub's rules.

**[Download the skill](morta-agent-v0.5.0.zip)** · **[Set it up](START-HERE.md)**

## What can I ask it to do?

| Task | Example request |
| --- | --- |
| Make a table easier to use | “Create a review view with the relevant columns, filtered to open items and sorted by priority.” |
| Build a useful document | “Add this table to the project page, using a view that shows only the information readers need.” |
| Check the numbers | “Read the full register and total the open requests. Tell me which estimates are missing.” |
| Change a table carefully | “Convert this link column to text, keeping the labels and URLs and checking that nothing is lost.” |
| Work with hub rules | “Find this hub's AI instructions. Propose improvements, then apply the changes I agree to if permitted.” |

## What do I need?

Claude Desktop or Codex, access to the Morta resources you want to use, and a working Morta connection. [Setup](START-HERE.md) covers both apps and the connector. A table or document link is enough to start; full-hub access is not required. Available actions depend on your connector and permissions.

## Can I adapt it?

Yes. Defaults such as Medium spreadsheet rows and document layout adapt to your hub's needs. Existing notes and field purposes are preserved. The [hub-instructions template](morta-agent/assets/hub-ai-instructions.md) is a starting draft to adapt and adopt with the appropriate authority.

## Status

21 simulated scenarios passed; an independent ECC review found no actionable issues. Live connector, Claude installation and visual tests remain pending. See [test results and maintenance](TESTING.md).

[MIT licence](../../../LICENSE) · [Contribute](../../../CONTRIBUTING.md) · [Report an issue](https://github.com/morta-technology/community/issues)
