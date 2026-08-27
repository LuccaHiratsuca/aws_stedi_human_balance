# Athena Query Screenshots

Evidence that each Glue table was created and returns the expected number of
rows. Capture each one from the **Athena query editor**, making sure both the
query text and the result row count are visible in the frame.

| File | Query to capture | Expected |
|------|------------------|----------|
| `customer_landing.png` | `SELECT COUNT(*) FROM stedi.customer_landing;` plus a `SELECT *` showing rows with a blank `sharewithresearchasofdate` | 956 |
| `accelerometer_landing.png` | `SELECT COUNT(*) FROM stedi.accelerometer_landing;` | 81273 |
| `step_trainer_landing.png` | `SELECT COUNT(*) FROM stedi.step_trainer_landing;` | 28680 |
| `customer_trusted.png` | `SELECT COUNT(*) FROM stedi.customer_trusted;` plus proof that no row has a blank `sharewithresearchasofdate` | 482 |
| `accelerometer_trusted.png` | `SELECT COUNT(*) FROM stedi.accelerometer_trusted;` | 32025 |
| `step_trainer_trusted.png` | `SELECT COUNT(*) FROM stedi.step_trainer_trusted;` | 13920 |
| `customers_curated.png` | `SELECT COUNT(*) FROM stedi.customers_curated;` | 464 |
| `machine_learning_curated.png` | `SELECT COUNT(*) FROM stedi.machine_learning_curated;` | 34437 |

The counts above are for the **stand-out** pipeline (consent-date filtering and
PII anonymisation enabled). See the root `README.md` for the baseline figures.

> `step_trainer_trusted` is 13920 rather than the 14460 the rubric lists. That
> is expected: this pipeline gates the IoT feed on `customers_curated` as the
> project instructions specify, while 14460 corresponds to gating on
> `customer_trusted`. The reasoning is in the root `README.md`.

Ready-to-paste queries live at the bottom of each file in [`../sql/`](../sql/).
