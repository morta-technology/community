# Presentation, ordering and reporting

Use the recipe for the requested change. [Setup](setup.md) covers capability discovery and [hub rules](hub-rules.md) resolve local choices; [design patterns](design-patterns.md) help choose the audience and purpose. These recipes do not require a particular connector or local tool suite.

Historical observations below were assembled on 6 October 2026 from earlier applied work, with view-update findings from 8 October 2026. Check the current endpoint/SDK contract and target state before using a workaround or numeric setting.

## Format a spreadsheet view

**Applicability:** Adjust columns, widths, row height, freezing, colours or displayed guidance without changing record meaning.

**Inspect:** Resolve the table and view by public ID. Capture its visible columns, order, dimensions, rules and available sync settings; retain relevant physical-column metadata and the state of affected sibling views. Physical schema and each view's presentation are separate state. A hidden physical field need not be recreated to show it.

**Execute:** Use the smallest proven payload. For new spreadsheet views, including a generated Default, Medium row height is the fallback unless the task, hub or expressly selected template specifies otherwise. Apply the setting to the returned view ID; do not bulk-reset existing views. Fit widths to the task and use colour for meaningful distinctions with a non-colour cue. Preserve field guidance and unrelated view settings.

**Recover:** Reconcile an uncertain creation by retained ID before retrying. If a full update ignores dimensions, read them independently and use the currently proven focused operation. If a write affects physical metadata or sibling views, resolve the unintended effect before continuing; restore only the unintended state after checking for intervening edits. Do not infer that all widths are globally shared or independent.

**Verify:** Read dimensions and actual sync settings independently. After any view payload containing column definitions, compare the physical baseline for kinds, formula/script text and enabled flags, validations and relationships. Check affected siblings and representative rendering through a permitted route. Saved settings alone do not prove legibility or colour precedence.

Dated details to recheck:

- The row-height mapping inspected on 6 October 2026 was `0` Small, `1` Medium, `2` Tall, `3` Extra Tall. Verify the current contract before sending a number, then save/read back the label where available.
- Earlier endpoints needed a focused freeze update and had a 100-pixel minimum width. Full view payloads also needed read-only cached lookup fields removed.
- On 8 October 2026, one full view-update path inserted disabled formula/script defaults into a physical column; it also left requested widths unapplied. Subsequent work changed physical widths, but did not isolate a universal cross-view propagation rule. The observed colour-sync setting was not evidence of width sync.
- Earlier linked-select colours did not inherit source colours automatically. Multiselect `contains` rules used scalar values and complete row/font flags; colours applied to cells rather than individual pills. Check the current contract and mixed-state precedence.

## Select and filter a useful view

**Applicability:** Show a distinct recurring task or audience an appropriate subset of existing data, including a view intended for a document embed.

**Inspect:** State the intended population and visible fields, including blank and unknown cases. Inspect candidate views by ID, filters, saved sorts, layout and actual rows. For a new table, keep a complete Default/source view as the fallback and use secondary views for focused arrangements. Preserve an existing intentional Default configuration. Check relationship selectors and embeds before filtering a source view.

**Execute:** Reuse a suitable view; otherwise create the smallest useful purpose-named view within the authorised task. Map fields to the target table's column IDs when adapting another design. Apply filters using the current contract and retain the complete source population. A filtered view is not an access boundary.

**Recover:** If the operation is absent, preserve the proposed settings and identify the specific supported next route using [setup](setup.md); do not substitute the wrong population. For denied operations, report the exact capability/access gap. For uncertain writes, inspect the retained ID and current state before another mutation. Opening a New View creation surface historically persisted a blank view immediately; do not treat such a surface as read-only inspection.

**Verify:** Reconcile expected and returned public row IDs across all pages, including records that must be excluded. Hidden predicate columns may be absent from view rows; join returned IDs to complete source rows before evaluating filters. Confirm column membership, layout and dependent selectors; verify rendering when permitted.

Earlier filter paths used separate `orGroup` values for a union, cumulative conditions within one group, and a nominal empty `value` for `is_not_null`. These are dated payload observations, not a present-day contract. Test the intended membership against current behaviour.

## Apply custom row ordering

**Applicability:** Display records in a business sequence, chronological order or another explicitly requested arrangement.

**Inspect:** Define the intended sequence and policies for blanks, unknown values and ties. Inspect both table and view sort settings and existing ranking/arrangement mechanisms. Select-option order is not proof of row order. Establish whether the task requires a sorted view or physical source-row reordering; they have different effects.

**Execute:** Reuse supported native arrangement or existing ranking first. Add one calculated rank only when needed and authorised, following [formulas](data-model.md#formulas-and-derived-values); no field name, prefix or vocabulary is prescribed. If the actual sorting mechanism requires its field to be visible, place it unobtrusively. For physical-row reordering, retain complete rows, public-ID-to-label mappings and dependent selections, and use the current proven provider route that preserves relationships. Omit `sortOrder` from ordinary content-only updates.

**Recover:** If the available route cannot implement the required order safely, retain the proposed sequence and complete independent presentation work. Do not guess a historical physical-reorder payload, rely on partial-row moves or change source values to force a sorting check to pass. Reconcile unintended relationship or row changes before proceeding.

**Verify:** Compare the actual ordered public IDs with the expected sequence, including ties and blanks. Change a source value only in an authorised test and check recalculation where relevant; otherwise inspect available evidence and report that recalculation remains untested. Recheck relationships after a physical reorder. An 8 October 2026 result was consistent with case/punctuation-insensitive comparison, but did not establish a universal collation rule; verify current semantics.

## Calendar and other specialist views

**Applicability:** Use Calendar for date-led work, Chart for an evidenced metric, Transpose for a few wide records, or Matrix for a suitable cross-tab. Spreadsheet is the ordinary editing fallback; use [forms and workflows](forms-workflows.md) for intake.

**Inspect:** Check that the available view type supports the actual job. Resolve genuine date fields and the relevant timezone for Calendar; define treatment of undated records and ranges. For Chart, identify aggregation, population and units. For Matrix, check uniqueness at each row-label/column-label intersection and usable width.

**Execute:** Reuse the source data and configure the supported view. Keep a suitable spreadsheet fallback where the specialist or beta behaviour cannot meet the need. Do not invent records to fill Matrix gaps or insert dates solely to make records appear in a Calendar.

**Recover:** An absent view operation, a denied operation and an uncertain failure require different next steps under [setup](setup.md). Complete supported work, identify the missing operation and retain the proposed design; do not silently broaden credentials or replace the requested result with a misleading view.

**Verify:** Reconcile the represented IDs and totals against the source. Check date placement, blanks, duplicates, labels and rendered readability when permitted. The Matrix contract inspected before 6 October 2026 was a cross-tab, not an aggregator; verify current capability before relying on that distinction.

## Produce a report from existing data

**Applicability:** Answer a question, compare groups or prepare a Report/Review or Dashboard/Monitoring result without changing the underlying system merely to report.

**Inspect:** Define the population, filters, period, timezone, units and source tables/views. Check pagination, inaccessible fields, duplicates, relationship expansion and the difference between a sample and a complete extraction. Determine which totals can be independently reconciled.

**Execute:** Read the supported data and calculate the requested result. Keep source references and retrieval time beside material conclusions. Use existing fields; a reporting request alone does not require a schema change, stored helper field, new view or automation. Label samples and incomplete coverage explicitly.

**Recover:** If a page or source is missing, separate the supported partial result from the unavailable population and name the specific next read/access needed. Do not extrapolate a partial count as complete. Reconcile inconsistent totals or duplicate IDs before stating a final metric.

**Verify:** Independently reconcile row counts, distinct IDs and relevant totals against the complete source population or another available authoritative count. Check representative records and edge cases, source references, rounding and labels. Report evidence limits and visual checks separately; a readable chart does not prove the underlying count.
