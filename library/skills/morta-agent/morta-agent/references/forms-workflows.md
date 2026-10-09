# Forms, submissions and workflows

Use these recipes for intake and the supported progression of work. [Setup](setup.md) covers capability and error handling; [hub rules](hub-rules.md) establish state meaning and authority. Use [data modelling](data-model.md) for relationships/validation and [documents](documents.md) for native document responses.

The creation/guidance observations below were assembled on 6 October 2026 from earlier Morta work. Inspect the current endpoint/SDK and response behaviour before applying a workaround.

## Build or amend a form

**Applicability:** Collect one record through a Form view or amend an existing intake experience.

**Inspect:** Establish the contributor, target table and exact fields they can reasonably supply. Resolve required fields, relationships, initial values, guidance and submission destination. Inspect the existing form by ID and its underlying field/validation configuration. Distinguish internal administration from information the contributor needs to decide or provide.

**Execute:** Reuse or create the focused supported form, exposing only appropriate fields and requiring only necessary inputs. Set descriptions and guidance through supported operations. Supply required attribution, initial values and relationships explicitly when creating rows; creation through a filtered view historically did not inherit its filter values. Do not presume the current form/connector populates them either.

**Recover:** Retain the created view ID. Earlier creation paths dropped form descriptions and bulk view updates dropped field guidance; inspect the same resource and use a supported focused description update when needed. Reconcile an uncertain create before making another form. If form configuration is absent or denied, complete the supported schema/design work and name the specific remaining operation without claiming the form is ready.

**Verify:** Read back visible/required fields, descriptions, validation and destination. With authorised test data, check valid and invalid submissions and the complete resulting row, including fields omitted from the form. Verify rendered guidance and contributor experience through a permitted route; label that check incomplete when only stored settings were available.

## Submissions and transitions

**Applicability:** Submit a document/record, amend its assignment or move work through defined states.

**Inspect:** Read the current state, actual response/submission model, assigned roles and the hub's meaning of each transition. Check whether submission locks fields, triggers notifications, creates records or changes access, using available supported evidence. A status label is not necessarily a workflow transition. Identify which actor can perform the requested action and what evidence it requires.

**Execute:** Use the supported operation for the authorised transition and preserve response/row identities. Update only the intended fields or responses. Use configured Selection or Signature responses where the actual workflow calls for them; do not invent an acknowledgement or infer acceptance from completed data entry. Follow field purpose and the existing logging contract without prescribing a new history schema.

**Recover:** If the transition operation is absent, report the specific action that needs another supported route. If denied, identify the resource/action and missing access or authority without broadening credentials. After an uncertain response, read submission state and related records before retrying; inspect possible notifications or downstream effects where supported. Do not repeat a non-idempotent submission blindly.

**Verify:** Read the resulting state, actor/assignment and affected responses, plus important unchanged content. Check any observable locking or permitted downstream effect against the intended outcome. Distinguish persisted status, actual submission, required review and acceptance; report what could not be observed through the connector.

## Configure supported automation

**Applicability:** Create or amend an automation that the current Morta capability actually exposes and that the user has commissioned.

**Inspect:** Define the trigger, condition, target, action, owner, expected frequency and failure handling. Check the exact automation operations and permissions available; do not infer absence from a failed documentation search. Inspect existing rules to avoid duplicates, loops or competing writes. Establish whether running a test sends notifications or causes other external effects before selecting the test.

**Execute:** Configure the smallest supported automation within scope, preserving identifiers and existing working rules. An explicitly commissioned automation can establish and test its process; no separate mandatory manual trial is implied. Use the product's supported controls for activation and retries. Platform-specific external integrations and a new execution engine are outside this recipe; record them as separate work if needed.

**Recover:** For an absent operation, provide the prepared trigger/action specification and exact unsupported step, then continue independent supported work. For denied access, report the specific permission gap. For an uncertain create or activation, inspect the automation ID, current enabled state and any available run evidence before retrying. Do not invent operations, broaden credentials or create a second automation as a workaround.

**Verify:** Read back configuration and enabled state. Where an authorised safe test and run evidence are available, exercise a matching and non-matching case, inspect resulting records and confirm failure handling. If the connector lacks execution/history evidence, report configuration as saved and execution as unverified; do not equate an enabled flag with a successful run.
