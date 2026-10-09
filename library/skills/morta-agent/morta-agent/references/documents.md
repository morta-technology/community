# Documents, navigation and embeds

Choose and combine the relevant [design patterns](design-patterns.md) for the audience and job. Use [setup](setup.md) for available operations and [hub rules](hub-rules.md) for adopted conventions. These recipes preserve existing identities and manual content while adapting the commissioned part of a document.

Historical observations below were assembled on 6 October 2026 from earlier Morta work. Check current endpoint/SDK types and permitted rendering evidence before treating a workaround as applicable.

## Structure and edit a document

**Applicability:** Build or amend narrative, guidance, navigation, review or operational sections.

**Inspect:** Read the actual section hierarchy, response types, body content, ordering and expansion settings. Identify manual examples and edits to preserve. Select useful design patterns by section; do not force the whole hub into one style. Separate structural headings from formatting inside a body.

**Execute:** Use native sections/subsections for structural headings and preserve existing section/response IDs for content edits. Keep the hierarchy shallow and remove generated placeholders from new sections. Expanded sections are the fallback for new documents, adjustable to the task/hub or selected template. Send one canonical rich-text representation through the supported contract. Choose Flexible for narrative, File Upload for evidence, Selection for choices and Signature for the configured acknowledgement when supported; inspect assignments and submitted-state behaviour.

**Recover:** Earlier competing structured blocks and fallback text flattened lists; reconcile the stored rich text and repair the affected content instead of replaying both representations. Create a response placeholder once and keep its ID. Earlier placeholders were readable before their update endpoint accepted configuration: use bounded read polling, then configure the same ID when ready. Stop if it remains unavailable and report that exact response as incomplete.

**Verify:** Read the hierarchy, section IDs/order, response bindings, rich-text structure and expansion settings. Confirm intended content and preserved manual material. Check actual lists, headings, links and response rendering through a permitted route. Report stored content and rendered usability separately when visual access is unavailable.

## Prepare and embed a table view

**Applicability:** Show authoritative table data inside a document, using a view suited to that document's readers.

**Inspect:** Define the intended records, visible fields, order, filters, sorts and grouping. Inspect existing views by ID, saved settings and actual row membership. A matching name is insufficient. Identify existing native Table responses and rich-text TABLE/SLICER controls so the edit preserves the right bindings and surrounding text.

**Execute:** Reuse a suitable view; otherwise prepare one purpose-named view within the authorised document task using [presentation](presentation.md), preserving shared source data and other consumers. Verify that view and bind its exact table/view IDs in a native Table response/embed. A dedicated table-only subsection is a useful default when the table is a separate working unit; adapt it when the document's selected pattern needs nearby explanation or context. A plain hyperlink is not a substitute for a requested native embed.

**Recover:** Reconcile an uncertain view or response create by its retained ID before retrying. If the needed view cannot be prepared, complete independent document work and report the embed incomplete; do not bind an arbitrary Default view. Earlier renderers ignored saved embed height. Inspect current rendering before promising resize; if height is fixed, show a complete decision unit and provide a route to the full register without hiding necessary evidence. View/control retirement follows [imports and migrations](imports-migrations.md#retire-or-replace-a-resource).

**Verify:** Read exact bindings, hierarchy and surrounding content. Reconcile displayed population and fields with the intended view, and inspect the actual embedded rendering through a permitted route. A stored URL or successful response update does not prove a usable native embed.

## Navigation, resource types and external embeds

**Applicability:** Organise accessible resources, link related material or configure an explicitly requested native external embed.

**Inspect:** Read existing navigation, landing links, resource identities and adopted grouping conventions. Resolve the canonical Resource Type source if one is used. Check the target and audience of every new link/embed, along with the available supported controls; navigation does not grant access. Keep access changes in [sharing](sharing.md).

**Execute:** Use a few meaningful first-level groups and descriptive links. Avoid pseudo-hierarchy or dummy resources solely to make folders appear. Use native iframe source/settings fields where supported, passing the source URL separately from settings instead of placing iframe HTML in a URL field. Preserve unrelated section text and links.

**Recover:** If the resource type or embed operation is absent, retain a specific navigation proposal and complete supported links/structure. If denied, identify the blocked action without broadening access. Reconcile an uncertain binding or grouping update against its exact resource ID before retrying; do not recreate the resource to fix a presentation setting.

**Verify:** Read target IDs, section links, type assignment and embed settings. Check the native selector as well as sidebar/grouping when a permitted route exists, and verify the intended external source and viewport. The Resource Type form inspected before 6 October 2026 was a table-backed reference, `table/<table-public-id>/<column-public-id>/<row-public-id>`, not a literal folder label. Resolve the current canonical source and contract before using it.
