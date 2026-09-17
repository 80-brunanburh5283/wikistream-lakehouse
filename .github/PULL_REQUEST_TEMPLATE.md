# What and why

<!-- One paragraph. What changes, and what problem it solves. -->

## How I verified it

<!--
Be specific. "Tests pass" is the weakest possible claim; CI already says that.
What did you actually observe? Paste the command and its output.

Good: "Ran `make up`, produced 4,300 events, killed the silver job with
`docker compose kill spark-silver` mid-batch, restarted, and
`make verify-no-duplicates` still exits 0. Silver went 4,301 -> 8,940 rows with
count(*) == count(distinct event_id) at both points."
-->

## Checklist

- [ ] `make lint` passes
- [ ] `make typecheck` passes
- [ ] `make test` passes (unit + integration)
- [ ] Commits follow Conventional Commits and the subject lines are under 72 characters
- [ ] No new secret, credential or personal data, and `.env` is not tracked
- [ ] All version pins are exact — no `latest` tag, no unpinned dependency

## If this touches the pipeline

- [ ] Docs updated: [`docs/correctness.md`](../blob/main/docs/correctness.md) if a
      guarantee changed, [`docs/data-contracts.md`](../blob/main/docs/data-contracts.md)
      if a column or grain changed, [`docs/runbook.md`](../blob/main/docs/runbook.md)
      if there is a new failure mode
- [ ] A new claim in the README has a command in the README that demonstrates it
- [ ] A non-obvious choice is recorded in [`DECISIONS.md`](../blob/main/DECISIONS.md)
      with the alternative I rejected

## Anything you would push back on

<!--
Optional, and the most useful section. What part of this are you least sure
about? Where would you welcome an argument?
-->
