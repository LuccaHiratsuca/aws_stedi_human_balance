"""
Local validation harness for the STEDI lakehouse pipeline.
==========================================================
Replays every zone transition in pure Python against the JSON files in `data/`
and asserts the row count of each table. It is a fast, zero-cost sanity check
that the join and filter logic is right BEFORE spending Glue DPU-hours on it.

This is a development aid -- it is not deployed to AWS.

Usage:  python scripts/validate_pipeline.py
"""

import json
import pathlib
import sys

DATA = pathlib.Path(__file__).resolve().parent.parent / "data"

# Expected row counts for the stand-out variant of the pipeline
# (consent-date filtering + PII anonymisation).
EXPECTED = {
    "customer_landing": 956,
    "accelerometer_landing": 81273,
    "step_trainer_landing": 28680,
    "customer_trusted": 482,
    "accelerometer_trusted": 32025,
    "customers_curated": 464,
    "step_trainer_trusted": 14460,
    "machine_learning_curated": 34437,
}


def read_zone(*parts):
    """Load every newline-delimited JSON file under data/<parts...>/."""
    rows = []
    for path in sorted((DATA.joinpath(*parts)).glob("*.json")):
        with path.open(encoding="utf-8") as handle:
            rows.extend(json.loads(line) for line in handle if line.strip())
    return rows


def main():
    # ---------------- Landing zone ----------------
    customer_landing = read_zone("customer", "landing")
    accelerometer_landing = read_zone("accelerometer", "landing")
    step_trainer_landing = read_zone("step_trainer", "landing")

    # ---------------- customer_landing_to_trusted ----------------
    # WHERE sharewithresearchasofdate IS NOT NULL
    customer_trusted = [
        c for c in customer_landing if c.get("shareWithResearchAsOfDate") is not None
    ]

    # ---------------- accelerometer_landing_to_trusted ----------------
    # INNER JOIN ON user = email  AND  timestamp >= sharewithresearchasofdate
    consent_by_email = {c["email"]: c["shareWithResearchAsOfDate"] for c in customer_trusted}
    accelerometer_trusted = [
        a
        for a in accelerometer_landing
        if a["user"] in consent_by_email and a["timestamp"] >= consent_by_email[a["user"]]
    ]

    # ---------------- customer_trusted_to_curated ----------------
    # SELECT DISTINCT customer columns INNER JOIN accelerometer_trusted ON email = user
    emails_with_accel = {a["user"] for a in accelerometer_trusted}
    customers_curated = [c for c in customer_trusted if c["email"] in emails_with_accel]

    # ---------------- step_trainer_trusted ----------------
    # INNER JOIN customer_trusted ON serialnumber. Consent -- not curation -- is
    # the privacy gate for the IoT feed; see the docstring of
    # scripts/step_trainer_trusted.py for why.
    consenting_serials = {c["serialNumber"] for c in customer_trusted}
    step_trainer_trusted = [
        s for s in step_trainer_landing if s["serialNumber"] in consenting_serials
    ]

    # ---------------- machine_learning_curated ----------------
    # INNER JOIN accelerometer_trusted ON sensorreadingtime = timestamp, PII dropped
    accel_by_time = {}
    for a in accelerometer_trusted:
        accel_by_time.setdefault(a["timestamp"], []).append(a)

    machine_learning_curated = [
        {
            "sensorreadingtime": s["sensorReadingTime"],
            "serialnumber": s["serialNumber"],
            "distancefromobject": s["distanceFromObject"],
            "x": a["x"],
            "y": a["y"],
            "z": a["z"],
        }
        for s in step_trainer_trusted
        for a in accel_by_time.get(s["sensorReadingTime"], ())
    ]

    actual = {
        "customer_landing": len(customer_landing),
        "accelerometer_landing": len(accelerometer_landing),
        "step_trainer_landing": len(step_trainer_landing),
        "customer_trusted": len(customer_trusted),
        "accelerometer_trusted": len(accelerometer_trusted),
        "customers_curated": len(customers_curated),
        "step_trainer_trusted": len(step_trainer_trusted),
        "machine_learning_curated": len(machine_learning_curated),
    }

    print(f"{'TABLE':<28}{'EXPECTED':>10}{'ACTUAL':>10}   ")
    print("-" * 56)
    failures = 0
    for table, expected in EXPECTED.items():
        got = actual[table]
        ok = got == expected
        failures += not ok
        print(f"{table:<28}{expected:>10}{got:>10}   {'PASS' if ok else 'FAIL'}")

    # The anonymisation guarantee, asserted rather than assumed.
    leaked = {k for row in machine_learning_curated for k in row} & {"user", "email"}
    print("-" * 56)
    print(f"PII columns in machine_learning_curated: {sorted(leaked) or 'none'}")
    failures += bool(leaked)

    print("\n" + ("All checks passed." if not failures else f"{failures} check(s) FAILED."))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
