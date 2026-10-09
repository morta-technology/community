# Tables, relationships and calculations

Use these recipes for record meaning and field behaviour. Use [imports and migrations](imports-migrations.md) when changing a populated field or vocabulary, [presentation](presentation.md) for a different view of existing facts, and [setup](setup.md) for the available capabilities. Keep local rules in [hub instructions](hub-rules.md).

Historical details here were assembled on 6 October 2026 from earlier applied Morta work. Recheck current resource state and endpoint/SDK types before relying on them.

## Define a lean table and its fields

**Applicability:** Create or adapt a table to maintain a clearly understood class of records.

**Inspect:** Establish what one row represents, its stable identity and where authoritative facts already live. For each proposed field, identify the present decision, action, relationship, evidence or calculation it supports and who or what supplies it. Reuse existing tables and fields where their meaning fits; presentation alone usually needs a view.

**Execute:** Add only the needed schema. Use text for identifiers even when their current values are numeric, and numeric kinds for quantities. Choose dates, links, attachments and controlled selections according to meaning. JSON or scripts need an actual consumer. Keep source wording and metadata in their intended fields. Follow the actual purpose of Notes or other narrative fields and the hub's logging contract; preserve human content/history without imposing a universal log field, name or visibility rule.

**Recover:** If row meaning or identity remains ambiguous, resolve that modelling question before adding speculative fields while continuing independent inspection. For an uncertain create, reconcile by returned ID before retrying. Route populated-kind changes to the migration recipe rather than assuming conversion will preserve content.

**Verify:** Read back schema, sample complete records and important unchanged fields. Check that unknown, not applicable, zero and false retain distinct meanings. Ensure each new field has its intended consumer and each identifier retains its exact value; no successful write substitutes for the data model making sense.

## Relationships and row identity

**Applicability:** Refer to an existing maintained entity, relate records or maintain a relationship with its own attributes.

**Inspect:** Resolve the source table, source view, source column and actual referenced row by public IDs. Check source-view filters and available selector options. A displayed label or successful API round trip does not prove that the intended entity exists. For records at the same semantic grain, assess supported same-table selection before adding a separate table. Introduce a relationship table only when the relationship needs its own attributes, lifecycle or independently maintained record.

**Execute:** Use the supported relationship representation and preserve stable identities. Reuse canonical entities instead of copying their changing details. Keep business identifiers for human reference and public IDs for operations. Do not alter ordering fields during content-only updates; intentional reordering belongs in [presentation](presentation.md#apply-custom-row-ordering).

**Recover:** Resolve missing or ambiguous source entities rather than choosing the first matching label. If filtering a source view removes selectable options, reconcile the scoped filter/relationship design; do not invent replacement source records. Reconcile uncertain row updates against the original ID and expected reference before retrying.

**Verify:** Read complete relationship values and confirm each one resolves to the intended existing source row. Check renamed labels, blank relationships, repeated labels and orphan counts as applicable. Recheck consumers after archive, duplication, restore or physical reordering. Earlier work found that filtering a Default/source view could remove options from dependent selectors; verify the current target rather than assuming it cannot happen.

## Lookups

**Applicability:** Display maintained information from a related record without creating a competing editable copy.

**Inspect:** Verify the relationship first, then resolve the lookup's source field, value type and expected cardinality. Inspect current connector support for lookup configuration and returned values. Decide how blank links, missing source values and multiple related records should be represented.

**Execute:** Configure the supported lookup against exact IDs. Keep source editing in the authoritative table. Do not replace a live lookup with static text merely because a connector read omits a cached value, and do not add aggregation semantics that the current lookup contract does not support.

**Recover:** Distinguish an absent configuration operation from denied access or an uncertain write. Complete independent relationship/source checks and identify the precise missing capability. When a view payload exposes read-only cached lookup fields, omit those from writes according to the current schema rather than replaying an entire read response.

**Verify:** Reconcile representative displayed values with the underlying source, including blank and multiple-link cases. Check propagation after an authorised source change or report that this behaviour was not exercised. Read the physical lookup definition and affected view membership; a matching label alone does not prove the reference is correct.

## Formulas and derived values

**Applicability:** Calculate an evidenced value, validation input or rank that the task actually needs.

**Inspect:** Define inputs, output type, blank/error handling and expected examples before selecting a formula or supported script. Read the current expression contract, enabled flags and dependency graph. Reuse an existing calculation or native arrangement before adding a new field. Calculation support in a product does not establish that the current connector can configure it.

**Execute:** Apply only the supported expression/configuration to the exact field. Keep source facts separate from derived values and preserve unrelated formula/script text and enabled state. Use a new calculated field only when needed and authorised; no naming or prefix convention is universal. Keep external automation/integration work outside this recipe.

**Recover:** If evaluation fails, inspect the actual error and dependent field kinds; do not conceal failure with an invented value. Restore the preceding definition when the requested change fails within the established recovery scope, after checking for intervening edits. A view update that unexpectedly inserts formula/script defaults also needs a physical-state reconciliation; see [presentation](presentation.md#format-a-spreadsheet-view).

**Verify:** Compare calculated outputs with independently worked examples, including blanks, invalid inputs and ties where relevant. Check current values and recalculation after source changes in an authorised test or state the untested limit. Confirm the definition, enabled flags and dependencies survived read-back; returning expected values once does not prove future recalculation.

## Validation and controlled options

**Applicability:** Guide or constrain entries using an existing field's meaning and the hub's rules.

**Inspect:** Establish whether the rule is guidance, a warning or a blocked entry. Check existing records and the current supported validation behaviour. Inspect both physical selection options and affected view options, including the intended blank/unknown policy. A saved record may still display a validation error.

**Execute:** Add plain-language guidance and the smallest supported rule. Require only information the contributor must and can supply. Reconcile option definitions across affected schema/views; use [migration](imports-migrations.md#change-a-controlled-vocabulary) when renaming or replacing existing values. Do not assume a written uniqueness instruction is a technically enforced constraint.

**Recover:** If the rule rejects legitimate existing records, report and reconcile those cases within scope before enabling a blocker. Distinguish unsupported validation from a denied operation or an uncertain save. Preserve the intended rule as a proposal if the enforcing operation is unavailable; do not claim configuration succeeded.

**Verify:** Check existing records and valid, blank, duplicate and invalid examples through an authorised route. Confirm warning versus blocking behaviour, persisted option definitions and representative rendered cells. Separate rule text, saved configuration and demonstrated enforcement in the result.
