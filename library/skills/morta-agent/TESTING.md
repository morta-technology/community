# Testing and maintenance

MortaAgent v0.5.0 is a **test candidate**. Results recorded on 9 October 2026:

| Check | Result |
| --- | --- |
| Agent decisions on synthetic resources | 21/21 scenarios passed: setup, scoped access, rules, reporting, ordering and migration safeguards |
| Independent ECC review | No actionable findings across all 12 instruction files, including the generic template |
| Community package | Links, configuration syntax, licence, ZIP contents and hashes verified |
| Live use | Claude installation, new Codex installation, live connector and visual checks remain unperformed |

The simulated results are one run per scenario, not a reliability guarantee. [Evaluation details](tests/evaluation-summary.json) record the outcomes and limitations.

## Try it in your app

Follow [Start Here](START-HERE.md), check the loaded version, then read one permitted resource. Verify a count or total against the source. Use an authorised disposable resource for later write tests.

## Check or rebuild the package

From this directory, using Python 3.11 or newer:

```sh
python build.py --check
python build.py
```

The first command verifies without writing; the second rebuilds the ZIP and [manifest](package-manifest.json). Neither connects to Morta or reruns agent scenarios. The community archive contains the 12 reviewed instruction files plus the repository's MIT notice.

After changing instructions, give a fresh assistant a case from the [setup](tests/setup-scenarios.json) or [editing](tests/editing-scenarios.json) scenarios, without the evaluation summary. Request a simulated response only, then review its decisions and calculations. Update the source, version, guides and recorded results together.

When reporting a problem, include the skill, app and connector versions, what you asked, and what happened. Keep credentials and private Morta content out of reports.
