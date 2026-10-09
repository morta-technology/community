# Imports, migrations and retirement

Use these recipes when identity, meaning or dependencies change. [Setup](setup.md) covers capabilities and uncertain writes; [hub rules](hub-rules.md) establish migration scope and authority. Use the current provider's supported operations and safeguards, one writer per resource, and available locks/version checks. A read-then-write sequence alone is not atomic.

## Map and reconcile an import

**Applicability:** Bring records into an existing/new table, update from an external source or replace a table within an authorised migration.

**Inspect:** Define source population, row meaning, identity key, field mapping and intended conversion. Inspect all pages of target records and the current schema before deduplication. Decide the handling of duplicates, blanks, unknowns, ambiguous matches and existing human edits. Record the exact affected IDs/count and preserve appropriate source/recovery material; no mandatory backup table is implied.

**Execute:** Separate create, update, unchanged and unresolved cases using stable identity. Do not merge solely on a display name or row position. Use proven supported bulk operations for authorised batches where appropriate, with completed IDs/checkpoints retained. Keep source content separate from metadata and never fill unknown values by invention.

**Recover:** Reconcile partial or uncertain results against retained public IDs and source keys before another batch. Disable automatic replay of non-idempotent creates where the provider exposes that setting; otherwise avoid blind create retries. Retry only an operation whose idempotence/current state is established. Leave ambiguous records unresolved and continue independent unambiguous work.

**Verify:** Account for every source row as created, updated, unchanged, deliberately excluded or unresolved. Reconcile complete affected records, important unchanged fields, duplicate/orphan counts and destination totals; a populated destination is not proof of complete capture. Check affected relationships, views and documents before considering retirement, and label incomplete reconciliation plainly.

## Change a populated column kind

**Applicability:** Convert a field's storage kind or replace it with a differently typed field.

**Inspect:** Capture every original cell keyed by public row ID, including structured link labels/URLs and other nested content. Capture the field definition separately: formulas/script text, enabled flags, validation, relationships and affected views/imports/consumers. Define the intended conversion, including blanks, invalid and ambiguous values. Preserve the original recovery material under the task/hub's retention instruction.

**Execute:** Neutralise incompatible dependencies only within the scoped migration. Apply the supported kind change and inspect the immediate result before restoring anything. Restore or transform only the values that require it; restoring cell content is distinct from restoring formulas or other schema. Preserve row and resource IDs wherever the migration permits.

**Recover:** Compare the immediate result with ID-keyed originals. Reconcile missing or changed content using the defined mapping, and stop the dependent step if the result cannot be recovered safely. Do not replay a blanket restore that overwrites correct values or intervening edits. Keep the old field/recovery available until reconciliation and authorised retirement are complete.

**Verify:** Reconcile every affected value and unrelated field, then validate definitions, formulas, relationships, views and other consumers. Record unresolved exceptions. An observed link-to-text conversion on 8 October 2026 retained all previously populated cells immediately after the kind change; this is evidence for preserving and checking values, not a claim that all conversions erase data or that all conversions retain it.

## Change a controlled vocabulary

**Applicability:** Rename, replace or consolidate a status/category used in rows and dependent presentation/workflow logic.

**Inspect:** Define meaning and mapping for every affected value, including blanks and ambiguous records. Inventory physical and view-level options, rows, formulas, validation, colour rules, filters, sorts, named views, document embeds and supported automation dependencies. Respect ownership exclusions; identify consumers outside scope before writing.

**Execute:** Apply the agreed mapping while preserving stable row/resource IDs and unrelated content. Update dependent configuration only within the authorised scope. Reconcile options in physical schema and views, and use [custom ordering](presentation.md#apply-custom-row-ordering) for business order. No status vocabulary, rank-field name or prefix is supplied by this recipe.

**Recover:** Keep unresolved rows or inaccessible dependencies explicit; a partial migration is not complete. Reconcile uncertain writes by current IDs/state and repair only unintended changes. Do not automatically broaden permissions or delete an old value while retained consumers still need it.

**Verify:** Check every mapped value, actual ordered row IDs, view membership, labels/colours and affected document references. Test relevant formula/automation behaviour through an authorised route where supported. Retain a complete source view and preserve intentional arrangements. Report retained legacy values, deferred automation and incomplete visual checks separately from migrated data.

## Retire or replace a resource

**Applicability:** Remove, archive or replace a field, view, table or obsolete embedded control under explicit retirement scope.

**Inspect:** Establish exact retiring and surviving IDs. Reconcile migrated data first. Inspect consumers across native table responses, rich-text TABLE entities and SLICER controls, links, relationships, formulas, views and supported workflows. A document excluded from needing a replacement view may still reference the old one. Discovery of a dependency does not authorise deleting its document or surrounding text.

**Execute:** Rebind retained consumers to an equivalent authorised target. Detach only obsolete controls explicitly included in the retirement scope, preserving other section content and manual edits. Retire only after data and consumers are reconciled. Hiding a field is not deletion, and a replacement table having rows is not a reconciliation result.

**Recover:** If `resource_in_use` is returned, inspect and reconcile the remaining exact references before another retirement attempt. Do not repeatedly issue delete or broaden the operation to surrounding content. If a dependency cannot be read or updated, keep the old resource and report the specific blocked consumer/action while completing independent authorised work.

**Verify:** Read surviving response bindings, TABLE/SLICER controls, links and relationship values; check for dangling references and preserved surrounding text. Reconcile each source row for table replacement, and check views/relationships after archive, duplication or restore. Verify retained document rendering where permitted and distinguish retirement completion from remaining visual checks. The native-response plus rich-text-control dependency gap was observed in work reviewed on 9 October 2026 using 8 October evidence; inspect current representations rather than assuming one content surface is exhaustive.
