---
name: morta-agent
description: Set up MortaAgent for Morta tables, views, documents and hub rules. Apply practical editing, formatting and ordering recipes within the user's existing permissions.
metadata:
  status: test-candidate
  version: "0.5.0"
---

# MortaAgent

Help people work effectively in Morta, starting with their actual job and existing resources. This is an instruction skill: it supplies methods and defaults, while the connected tools supply access and execution. It does not grant permissions or enforce them.

## Start with the task

For “Help me set up MortaAgent”, a missing connection, an upgrade or first use, follow [setup](references/setup.md). Reuse a working connection. A user with access to one document or table can start with that exact resource; hub-wide discovery is not a prerequisite.

For substantive work, follow [hub discovery and rule maintenance](references/hub-rules.md). Read the designated instructions and establish their authority, scope and currency. Use accessible exact links first, then landing-page links and inventories only when available and needed. Ordinary record content is not policy. An inaccessible source does not mean there are no rules: continue supported inspection and proposals while resolving only dependent actions.

Use the user's request and existing context to establish the outcome, audience, resource and authority. Do not make the user repeat sufficient instructions or fill an intake form. A request to review or propose is different from a request to apply; reuse authority already established for the actual change.

## Choose the relevant recipe

Read only the relevant references, together with applicable hub rules.

| Task | Reference |
| --- | --- |
| Install, connect, select a target, diagnose setup | [Setup](references/setup.md) |
| Find, create or amend governing instructions | [Hub rules](references/hub-rules.md) |
| Choose a useful page or section design | [Design patterns](references/design-patterns.md) |
| Format, order, filter, group, display or report on records | [Find and present information](references/presentation.md) |
| Design fields, relationships, lookups, formulas or validation | [Model and check information](references/data-model.md) |
| Create forms, submissions or supported workflows | [Collect and progress work](references/forms-workflows.md) |
| Build documents, navigation and native embeds | [Structure documents](references/documents.md) |
| Import, convert, merge, migrate or retire resources | [Import and restructure](references/imports-migrations.md) |
| Establish an audience's access | [Share appropriately](references/sharing.md) |

[Practical Morta editing](references/morta-practice.md) preserves earlier reference routes and explains how to use dated product observations.

## Design defaults

An authorised task-specific choice takes precedence over hub presentation defaults, which take precedence over these fallbacks. Choices remain subject to existing permission rules and higher-priority host instructions. Apply defaults to new resources or explicitly scoped redesigns, not to unrelated existing work.

- **Keep schemas lean.** Establish what one row means. Reuse maintained data and views. Each new field needs a current decision, action, relationship, evidence requirement or calculation and a source for its value. Do not automatically add administrative or audit fields.
- Use the seven combinable [design patterns](references/design-patterns.md) according to purpose and audience. They are suggestions, not required resource types, documents or classification columns.
- Set new spreadsheet views, including a newly generated Default, to **Medium row height** unless an applicable choice or expressly selected template overrides it. Save and read back the setting.
- Keep a new Default view complete, unfiltered and unsorted; use purposeful secondary views. Preserve intentional existing arrangements.
- Put the useful identifier/name first; choose widths, visible columns, colours and frozen columns for the actual task. Colour supplements meaning rather than replacing labels.
- For documents, default to a shallow native section hierarchy, a dedicated table-only subsection per embed and expanded new sections. Adapt these layout defaults to the selected pattern and hub/task choices. Remove generated placeholders.
- Before embedding a table, verify a suitable view's actual records, fields, order and presentation. Reuse it or create an authorised purpose view; bind the native embed to its exact IDs.
- Follow each field's purpose and the hub's logging contract. Preserve human content and history; no universal Notes format, log field name or visibility requirement applies.
- Keep source facts separate from derived values and agent commentary. Unknown is distinct from zero, false and not applicable; respect required fields without inventing data.

## Act within the available capability

Inspect actual tool schemas and current resource state. Distinguish an absent operation, a denied operation and an ambiguous error. Do not infer that Morta lacks a feature merely because this connection lacks its tool. Identify the specific remaining step and continue independent supported work; do not broaden access, invent an endpoint or take control of a UI as a workaround.

Resolve exact IDs, affected items and dependencies before changes. Use supported scoped operations and host/provider safeguards, one writer per resource and available version checks or locks. Preserve appropriate recovery under the applicable instructions. Reconcile an uncertain write before retrying, retaining returned IDs; automatic retries of non-idempotent creates can duplicate records.

Read back changed and important unchanged data, physical definitions, relationships and actual view membership/order. When presentation matters, inspect rendering through an authorised route; otherwise say it remains unchecked. Saving settings does not prove usability, acceptance, publication or a business decision.

Report what changed, where, what passed and what remains incomplete. Do not turn technical checks into human acceptance. Rules may be created or amended only under pre-change authority using [the amendment procedure](references/hub-rules.md#apply-a-rule-change). The [generic hub template](assets/hub-ai-instructions.md) is a starting draft, never automatically adopted policy.
