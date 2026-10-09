# Practical Morta editing

Compatibility index for the practical guidance reorganised on 9 October 2026. Existing heading links remain valid. Read only the recipe relevant to the current task; shared authority and capability handling live in [MortaAgent](../SKILL.md), [setup](setup.md) and [hub rules](hub-rules.md).

| Need | Recipe reference |
| --- | --- |
| Formatting, ordering, views, filters, Calendar and reports | [Presentation](presentation.md) |
| Lean schema, relationships, lookups, formulas and validation | [Data model](data-model.md) |
| Intake, submissions, transitions and supported automation | [Forms and workflows](forms-workflows.md) |
| Sections, navigation, native responses and embeds | [Documents](documents.md) |
| Import mapping, deduplication, conversions and retirement | [Imports and migrations](imports-migrations.md) |
| Audience, permissions and sharing limits | [Sharing](sharing.md) |
| Choosing and combining document/section purposes | [Design patterns](design-patterns.md) |

## Tables and fields

Use [lean tables and fields](data-model.md#define-a-lean-table-and-its-fields), [formulas](data-model.md#formulas-and-derived-values) and [validation](data-model.md#validation-and-controlled-options). For a populated-kind change, use the [ID-keyed preservation and conversion recipe](imports-migrations.md#change-a-populated-column-kind).

## Workflow and controlled-vocabulary migrations

Use [controlled-vocabulary migration](imports-migrations.md#change-a-controlled-vocabulary) and [resource retirement](imports-migrations.md#retire-or-replace-a-resource). For workflow actions rather than a vocabulary migration, use [submissions and transitions](forms-workflows.md#submissions-and-transitions).

## Relationships and row identity

Use [relationships](data-model.md#relationships-and-row-identity) and [lookups](data-model.md#lookups). Physical reordering requires the current proven route described in [custom row ordering](presentation.md#apply-custom-row-ordering); a view sort is a different operation.

## Views and intake

Use [view formatting](presentation.md#format-a-spreadsheet-view), [filters and membership](presentation.md#select-and-filter-a-useful-view), [custom ordering](presentation.md#apply-custom-row-ordering), [specialist views](presentation.md#calendar-and-other-specialist-views) and [forms](forms-workflows.md#build-or-amend-a-form). For a read-only report, use [reporting](presentation.md#produce-a-report-from-existing-data).

## Documents and navigation

Choose appropriate [design patterns](design-patterns.md), then use [document structure](documents.md#structure-and-edit-a-document), [table embeds](documents.md#prepare-and-embed-a-table-view) or [navigation](documents.md#navigation-resource-types-and-external-embeds). Pattern combinations and defaults adapt to the task and hub; they do not justify rebuilding unrelated content.

## Writes and access

Use the shared execution/capability procedure in [MortaAgent](../SKILL.md) and [setup](setup.md), plus the recovery/verification steps of the selected recipe. Use [sharing](sharing.md) for audience and permissions and [imports and migrations](imports-migrations.md) for batch reconciliation and retirement. Provider-specific safeguards apply only when that provider is actually in use; a local write flag is not a remote connector permission control.

## Freshness and learning

The practical recipes preserve lessons assembled on 6 October 2026 and the 8 October findings reviewed on 9 October. Reconcile current target/rendered evidence, endpoint/SDK types, official documentation and dated tested guidance; no one source establishes all Morta behaviour. Recheck known-to-drift, beta and uncommon mechanics before use, especially older observations. [Official developer documentation](https://docs.morta.io/), [Tables help](https://www.morta.io/help/tables) and [Documents help](https://www.morta.io/help/documents) are reference starting points, not proof of a connector's exposed operations or a historical side effect.

Record new observations as proposed learning with date, scope and evidence. Skill-maintenance authority is needed to update shared guidance; permission to edit one hub's rules does not authorise changing everyone's skill. A saved configuration, verified behaviour, rendered usability and acceptance remain distinct findings.
