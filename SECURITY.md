# Security policy

## What this project is

A local demonstration pipeline. It is not deployed anywhere, it has no users, it
stores no personal data of its own, and it has no production environment. There
is no hosted instance to attack.

That framing matters for triage: a finding here is a code-quality issue in a
reference implementation, not an incident.

## Credentials in this repository

The only credentials present are `minioadmin` / `minioadmin` in
[`.env.example`](.env.example). Those are the documented defaults of a MinIO
container listening on `localhost`, they grant access to nothing but an empty
local bucket, and they are not reusable anywhere. `.env` is gitignored.

CI runs `gitleaks` over the full history on every push. If you find something in
the history that looks like a real secret, that is a finding worth reporting —
see below.

The Terraform under [`infra/aws/`](infra/aws/) has never been applied and no
state file exists. It is validated statically in CI and holds no credentials.

## Reporting

Open a [GitHub issue](https://github.com/william-sarkar/wikistream-lakehouse/issues)
for anything that is not itself sensitive — which, given the above, is almost
everything.

For something you would rather not post publicly, use GitHub's
[private vulnerability reporting](https://github.com/william-sarkar/wikistream-lakehouse/security/advisories/new)
on this repository.

Please include the version or commit, what you ran, and what happened. I will
acknowledge within a week; I maintain this in my own time and cannot promise
faster.

## Supported versions

The `main` branch only. There are no releases and no backports.

## In scope

- Dependency vulnerabilities that are actually reachable from the code here.
- A credential, token or private URL committed anywhere in the history.
- A default in `docker-compose.yml` that exposes a service beyond `localhost`
  without saying so.
- An injection path in the Python or SQL — for example, a Spark SQL statement
  built by string interpolation from event data.

## Out of scope

- The MinIO defaults described above.
- Services bound to `localhost` being reachable from `localhost`.
- Missing authentication between components. There is none by design; the whole
  stack is one developer machine's loopback interface, and adding TLS and SASL
  between local containers would obscure the pipeline without protecting
  anything. Doing it properly is noted as a limitation in the README rather than
  pretended at here.
- Anything about the plan-only AWS module's runtime posture, since it has no
  runtime.
