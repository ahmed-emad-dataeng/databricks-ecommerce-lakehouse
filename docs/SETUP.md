# Setup — M0 runbook

Everything needed to get from an empty Databricks Free Edition account to a
deployed job. Budget ~3 hours including the deploy spike.

Steps 1–3 need your accounts and cannot be automated.

---

## 1. Databricks Free Edition account

1. Sign up at <https://www.databricks.com/learn/free-edition>.
2. Note your workspace host — e.g. `https://dbc-xxxxxxxx-xxxx.cloud.databricks.com`.

### The LinkedIn limit increase is OPTIONAL

Databricks offers a "Verify with LinkedIn" option that **raises some usage
limits**. It is not required to sign up, to use Free Edition, or to complete this
project. If you cannot complete it, carry on — just follow the quota rules below
more strictly, since you will be working inside the lower limits.

If you do want it, note that LinkedIn has **two different features** and only one
of them is relevant:

| LinkedIn feature | Proves | Needs |
|---|---|---|
| **Workplace** verification | you work at a named company | a company work email, Entra Verified ID, or a LinkedIn Learning/Recruiter licence |
| **Identity** verification | you are a real person | a government ID + selfie. **No work email, no employer.** |

Databricks wants **identity** verification. If your profile lists a company as
your employer, LinkedIn's *workplace* flow will ask for an email at that
company's domain — which is a dead end for anyone who is not actually employed
there (e.g. a freelancer whose profile lists the platform they freelance on).
Skip that flow entirely.

Identity verification runs through **Persona** (100+ countries) or **CLEAR**
(US/Canada/Mexico only, plus a local phone number). Two things to know:

- It is available **only in the LinkedIn mobile app**, not the website. This is
  the usual reason people end up in the workplace flow by mistake.
- Persona generally wants an **NFC-enabled passport / e-passport**. A few
  countries also accept a driver's licence or national ID card; North America is
  more lenient. The name on the ID must match your LinkedIn profile name.

Separately, if your current position lists a company you are not employed by,
consider setting it to **"Self-employed"** or **"Freelance"** — LinkedIn supports
both, it stops the work-email prompt, and it is the more accurate description.

### Quota discipline — read this before you start building

Exceeding the fair-use quota **shuts down your compute for the rest of the day**,
and in extreme cases longer. Rules that keep you inside it:

- Stop the SQL warehouse manually when you finish a session. Set the shortest
  auto-stop it allows.
- Never leave a stream running. Everything here uses `Trigger.AvailableNow()`.
- Develop against `LIMIT`ed samples; run full loads deliberately.
- **Do not add a cron schedule during development.** Trigger the job by hand.
  The schedule block in `resources/ecommerce_pipeline.job.yml` is commented out
  for this reason.
- Keep the data at Olist scale (~550K rows). Do not scale it up "to test Spark".
- `geolocation` (~1M rows, two thirds of the raw dataset) is **excluded by
  default** in `src/config.py` because nothing in the model reads it. Leave it
  out unless you add a geographic question — this matters most if you are on the
  lower unverified limits.

---

## 2. Get the Olist data

Download **Brazilian E-Commerce Public Dataset by Olist** from Kaggle:
<https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce>

You cannot download it from inside a notebook — Free Edition restricts outbound
internet to a small set of trusted domains. Download to your laptop, then upload.

Expected nine files (~120 MB unzipped). You only need to upload **eight** —
`olist_geolocation_dataset.csv` is not loaded (see quota note above), so skipping
it also saves the upload:

```
olist_customers_dataset.csv
olist_geolocation_dataset.csv   <- not needed
olist_order_items_dataset.csv
olist_order_payments_dataset.csv
olist_order_reviews_dataset.csv
olist_orders_dataset.csv
olist_products_dataset.csv
olist_sellers_dataset.csv
product_category_name_translation.csv
```

Licensed **CC BY-NC-SA 4.0** — non-commercial use, attribution required. The
`.gitignore` excludes `*.csv` so the data never lands in the repo.

---

## 3. Install the Databricks CLI

**Already installed** — Databricks CLI v1.15.0, via:

```bash
winget install Databricks.DatabricksCLI --exact --accept-package-agreements --accept-source-agreements
```

winget added it to PATH, so `databricks` resolves once you restart your terminal
(or the Claude Code app). Until then the binary is at:

```
%LOCALAPPDATA%\Microsoft\WinGet\Packages\Databricks.DatabricksCLI_Microsoft.Winget.Source_8wekyb3d8bbwe\databricks.exe
```

`bundle validate` already parses `databricks.yml` and the job resource without
structural errors, so the config is sound before you authenticate. Deeper
task-level validation only runs against a real workspace.

Note the CLI calls these **Declarative Automation Bundles** — same
`databricks bundle` commands, renamed docs.

Then authenticate (creates a profile in `~/.databrickscfg`):

```bash
databricks auth login --host https://YOUR-WORKSPACE.cloud.databricks.com
```

Verify:

```bash
databricks current-user me
```

---

## 4. Create the catalog

Import `notebooks/00_setup_catalog.py` into the workspace and run it, or run the
SQL directly in a notebook:

```sql
CREATE CATALOG IF NOT EXISTS ecommerce_dev;
CREATE SCHEMA  IF NOT EXISTS ecommerce_dev.landing;
CREATE SCHEMA  IF NOT EXISTS ecommerce_dev.bronze;
CREATE SCHEMA  IF NOT EXISTS ecommerce_dev.silver;
CREATE SCHEMA  IF NOT EXISTS ecommerce_dev.gold;
CREATE SCHEMA  IF NOT EXISTS ecommerce_dev.ops;
CREATE VOLUME  IF NOT EXISTS ecommerce_dev.landing.raw;
```

Upload the eight needed CSVs to `/Volumes/ecommerce_dev/landing/raw/olist/` — via
Catalog Explorer's upload button, or:

```bash
databricks fs cp ./data/olist dbfs:/Volumes/ecommerce_dev/landing/raw/olist --recursive
```

---

## 5. The deploy spike — timeboxed to 60 minutes

The point is to **decide the deploy path quickly**, not to make bundles work at
any cost. The portfolio artifact is the pipeline; IaC is packaging. Three paths,
in order. Stop at whichever works.

### Path 1 — bundle from your laptop (preferred)

```bash
databricks bundle validate
databricks bundle deploy --target dev
```

**This is now very likely to just work.** The Terraform concern that originally
motivated a three-path spike no longer applies: CLI v1.15.0 defaults to the
`direct` deployment engine, not Terraform. Verified from the bundle schema:

> `engine`: The deployment engine to use. Valid values are `terraform` and
> `direct`. Takes priority over `DATABRICKS_BUNDLE_ENGINE`. **Default is
> `"direct"`.**

So there is no Terraform binary to download, which is what the older Free Edition
failure reports were about. Belt and braces: `releases.hashicorp.com` responds
HTTP 200 from this machine in ~0.8s, so even forcing `engine: terraform` would
work. Nothing Terraform-shaped is cached locally because nothing needs to be.

### Path 2 — deploy the bundle from the workspace UI

Databricks now supports creating and deploying bundle-managed assets from inside
the workspace. Use this if local auth fights back. Cross-workspace deploys are
not supported this way, which does not matter here — Free Edition has one
workspace, and dev/prod differ by catalog.

### Path 3 — jobs-as-JSON (fallback, zero risk)

At 60 minutes, stop. Export the job definition to `jobs/ecommerce_job.json`,
commit it, and deploy with:

```bash
databricks jobs create --json @jobs/ecommerce_job.json
# or, to update an existing job
databricks jobs reset --json @jobs/ecommerce_job.json
```

Still infrastructure-as-code, still version-controlled. Note the choice in
`docs/decisions.md` and move on to M1.

Realistically you should not reach this path. It stays documented because the
only genuinely untested step is `auth login` against a real workspace, and a
fallback that costs nothing to keep is worth keeping.

---

## 6. M1 gate — the thin vertical slice

**Do not proceed past this until it is green.** Run only the `orders` path end to
end: volume → bronze → silver → `fact_order` → one SQL query → one dashboard tile.

The gate: **a dashboard tile shows a number you can trace back to a specific row
in a raw CSV.** That proves the catalog, the volume, Auto Loader, the model, the
warehouse and AI/BI all work together — before you have nine tables' worth of
code depending on it.

Do not touch CDC, SCD2, Genie, orchestration or tests until this passes.

---

## 7. Known things to test early

Two assumptions in this codebase are unverified against a live Free Edition
workspace. Both have a defined fallback, so neither can stall you — but find out
in the first 20 minutes of the milestone rather than at the end.

| Assumption | Where | Fallback |
|---|---|---|
| Auto Loader checkpoints work in a UC volume on serverless | `src/bronze/ingest.py` | Set `INGEST_MODE = "copy_into"` — equally file-idempotent, no checkpoint needed |
| `auth login` succeeds against a Free Edition host | step 5 | Paths 2 then 3 above. The Terraform risk is resolved (see step 5); this is the only untested part left. |

---

## 8. Order of work

Milestones, with the hour-20 gate from the project plan:

| # | Milestone | real hours |
|---|---|---|
| M0 | Setup + deploy spike | 3 |
| M1 | Thin vertical slice **(gate)** | 4 |
| M2 | Bronze, all 9 tables | 3 |
| M3 | Silver + data quality | 6 |
| M4a | Gold core: 4 dims + 2 facts | 5 |
| M5 | CDC + SCD2 + idempotency proof | 4.5 |
| M6 | Lakeflow Job orchestration | 3 |
| | **— hour-20 gate: the project is now defensible —** | |
| M4b | Gold wave 2: `fact_payment` + 2 small dims | 1.5 |
| M7 | AI/BI dashboard + Genie | 3.5 |
| M8 | Tests + README | 3 |

Much of the code for M2–M6 is already written in `src/`. The remaining work is
running it against real data, fixing what breaks, and capturing the evidence
(screenshots, measured numbers) that the README marks as `TBD`.

If time runs short, cut in this order: streaming extension → `fact_review` →
M4b. **Never cut M8** — the README and the two documented traps are what
actually get read.
