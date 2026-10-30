# Pilot configuration placeholders

These files are examples to copy into the corresponding Gundi settings, not a
single importable integration or runtime defaults. Replace every placeholder
before running live actions. Keep real credentials in Gundi or your local
untracked environment file, not these examples.

| Setting | Example | Where to enter it |
| --- | --- | --- |
| OlmoEarth environment URL | `olmoearth-site.json` | Integration site/base URL |
| OlmoEarth API token | `olmoearth-auth.json` | Authenticate action |
| Project, model, area, cadence, classes and date semantics | `lcc-monitor.json` | Collect LCC Events action |
| Gundi OAuth client ID/secret, issuer/audience and API URLs | `../local/.env.local.example` | Copy to `local/.env.local` for the runner |

The Gundi environment example contains staging URL/client defaults and a secret
placeholder. Confirm all of those values with the Gundi team for your chosen
environment. Gundi credentials authenticate the runner to Gundi; the OlmoEarth
token authenticates requests to OlmoEarth. They are separate credentials.

The all-zero-prefixed UUIDs and small polygon in `lcc-monitor.json` are sample
values, not actual partner resources. `event_type` must match the configured
Gundi/EarthRanger route. EarthRanger destination credentials belong in that Gundi
route/destination, not in this connector.

Save Submit LCC Predictions with `run_on_schedule` disabled until the pilot is
ready. The `run_on_schedule` field in the Collect configuration only controls
collection; it does not enable prediction submission.

## Production pilot — supplied October 26, 2026

`lcc-monitor-production-pilot.json` records the project and model supplied by Rachel:

- Project: `6205704f-b09e-432b-af61-d68b1ebe018f`
- Model: `f96ef025-780e-4393-9083-e732a324670e`
- [Studio model page](https://olmoearth.allenai.org/projects/6205704f-b09e-432b-af61-d68b1ebe018f/models/f96ef025-780e-4393-9083-e732a324670e)

This is a configuration draft, not a ready-to-run integration. The AOI remains
the example polygon, and
cadence, start date, imagery lag and event type remain unconfirmed
example settings. Collection
is disabled on schedule in this draft; keep submission disabled too.

Confirm the production API base URL separately from the Studio UI URL, enter the
OlmoEarth token in Authenticate, and inspect the model's vector output and ISO
change-date semantics before running. Gundi credentials and destination setup
are still required. No live inference is initiated by adding this file.
