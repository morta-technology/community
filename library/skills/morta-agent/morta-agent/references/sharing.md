# Audience, sharing and access limits

Use these recipes when a task concerns who should see or change a resource. Read [setup](setup.md) for the actual connector capabilities and [hub rules](hub-rules.md) for authority. A skill supplies instructions; it does not grant access or turn a saved policy into enforced permissions.

## Inspect audience and current access

**Applicability:** Review the audience of a resource, assess a sharing request or establish what the available connector can verify.

**Inspect:** Resolve the acting identity, exact hub/resource and intended audience. Inventory the available permission/sharing reads, current grants and inheritance only where the connector exposes them. Distinguish observed grants from effective access; a connector may not expose enough information to establish every person's access. A failed documentation lookup is not proof that no operation exists.

**Execute:** Use supported reads and describe the specific resource/action under review. Separate resource sharing from hub membership or invitation. Hidden columns, filtered views and embeds are presentation choices, not evidence of restricted access. If the task is an inspection, do not change permissions to test a theory.

**Recover:** If a read operation is absent, name the unobservable permission fact and the specific supported administrative check needed. If denied, report the exact failed read and access gap. For uncertain errors, inspect current capability/resource state before retrying. Continue independent checks; do not broaden credentials, invite the user to a whole hub or infer access from a similar resource.

**Verify:** Record which grants/settings were actually read, for which IDs and when, and which effective-access questions remain unresolved. Where a permitted supported route can test the intended audience, report that evidence separately. Do not claim that restricted presentation, a resource link or a successful owner read proves another person's access.

## Change sharing within scope

**Applicability:** Grant, change or revoke access to the exact audience/resource explicitly covered by the task and applicable authority.

**Inspect:** Confirm the precise subjects, resource scope, desired actions/roles and current grants. Read the current operation contract and determine whether it changes membership, sends an invitation/notification or affects inherited access. Identify the smallest supported change and the recovery path. Resolve material ambiguity before the dependent mutation; reuse sufficient existing authorisation rather than inventing a new approval gate.

**Execute:** Apply only the authorised grant/revocation through a supported operation. Retain returned grant/invitation/resource IDs. Do not treat a request to share one resource as permission to invite someone to the entire hub or to change unrelated grants. Sharing the instruction skill itself does not grant Morta access, and revocable access is not a guarantee that recipients cannot copy viewed information.

**Recover:** For an absent mutation operation, complete the prepared access specification and identify the exact administrative step still needed. For a denied operation, report the permission gap without switching identity or broadening credentials. For an uncertain invitation or grant, read the relevant state and retained IDs before retrying; avoid duplicate invitations. Reconcile unintended grants only within the established recovery authority and after checking intervening changes.

**Verify:** Read back changed and important unchanged grants. Distinguish a saved permission, invitation sent, invitation accepted and demonstrated effective access; claim only the states the available evidence supports. If recipient access or inheritance cannot be inspected, state that limitation and the specific remaining check. A filtered view or successful upload never substitutes for permission verification.
