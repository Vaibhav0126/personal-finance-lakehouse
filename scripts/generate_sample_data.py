"""
Generate realistic fake expense exports for two roommates, Person A and
Person B, in the same shape as real Monee app exports (same header, "¥1,940"
style amounts, "August 31, 2026" dates) plus some deliberately messy rows.

Standard library only, deterministic (seeded), so CI and anyone cloning the
repo get identical files.

    python scripts/generate_sample_data.py              # -> data/sample/*.csv
    python scripts/generate_sample_data.py --months 6 --out /tmp/pf
"""
import argparse
import csv
import random
import uuid
from datetime import date, timedelta
from pathlib import Path

HEADER = [
    "Account name", "Category", "Description", "Person", "Date", "Time", "Amount",
    "Recurring", "Payment status", "moneeTransactionID", "moneeCategoryID",
    "moneeAccountID", "moneePersonID", "moneeTimeZone", "moneeAssociatedAccountID",
    "moneeAssociatedAmount",
]

PERSONAL = {
    "Food": [("Lunch bento", 650, 1200), ("Ramen", 900, 1400), ("Convenience store", 300, 900)],
    "Beverages": [("Coffee", 350, 600), ("Bubble tea", 500, 750)],
    "Body Care": [("Shampoo", 800, 1500), ("Haircut", 3000, 4500)],
    "Leisure": [("Cinema", 1900, 2200), ("Gym day pass", 1000, 1500), ("Karaoke", 1500, 3000)],
    "Gift": [("Birthday gift", 2000, 6000)],
}
SHARED = {
    "Food": [("Weekly groceries", 3000, 7000), ("Dinner out", 4000, 9000)],
    "Housing": [("Toilet paper and detergent", 800, 1600)],
    "Leisure": [("Netflix", 1490, 1490)],
}
MONTHLY_SHARED = [("Rent", "Housing", 45000), ("Electricity", "Housing", 6000), ("Internet", "Housing", 2750)]


def fmt_date(d: date) -> str:  # "August 31, 2026"
    return f"{d:%B} {d.day}, {d.year}"


def fmt_yen(amount: int) -> str:  # -1940 -> "-¥1,940", 1370 -> "¥1,370"
    return f"{'-' if amount < 0 else ''}¥{abs(amount):,}"


def generate(months: int, seed: int, start: date):
    rng = random.Random(seed)
    # Separate stream for ids/times so the generated purchases don't depend on them.
    meta = random.Random(seed + 1)

    def fake_id() -> str:
        return str(uuid.UUID(int=meta.getrandbits(128))).upper()

    person_ids = {p: (fake_id(), fake_id()) for p in ("Person A", "Person B")}  # (account, person)
    category_ids = {}

    def row(person, d, category, desc, amount, recurring="No", status="Paid"):
        account_id, person_id = person_ids[person]
        category_id = category_ids.setdefault(category.strip().lower(), fake_id())
        return {
            "Account name": f"{person}'s account", "Category": category, "Description": desc,
            "Person": person, "Date": fmt_date(d),
            "Time": f"{meta.randint(7, 22):02d}:{meta.randint(0, 59):02d}:{meta.randint(0, 59):02d}",
            "Amount": fmt_yen(amount), "Recurring": recurring, "Payment status": status,
            "moneeTransactionID": fake_id(), "moneeCategoryID": category_id,
            "moneeAccountID": account_id, "moneePersonID": person_id,
            "moneeTimeZone": "Asia/Tokyo", "moneeAssociatedAccountID": "",
            "moneeAssociatedAmount": "",
        }

    a_rows, b_rows = [], []
    people = (("Person A", a_rows), ("Person B", b_rows))

    for m in range(months):
        month_start = date(start.year + (start.month - 1 + m) // 12, (start.month - 1 + m) % 12 + 1, 1)

        # Fixed monthly bills, split 50/50 and logged by both on nearby days.
        for desc, cat, total in MONTHLY_SHARED:
            d = month_start + timedelta(days=rng.randint(0, 4))
            half = -round(total * rng.uniform(0.9, 1.1) / 2)
            for person, rows in people:
                rows.append(row(person, d + timedelta(days=rng.randint(0, 2)), cat, desc, half, "Yes"))

        # Ad-hoc shared purchases.
        for _ in range(rng.randint(4, 7)):
            cat = rng.choice(list(SHARED))
            desc, lo, hi = rng.choice(SHARED[cat])
            d = month_start + timedelta(days=rng.randint(0, 25))
            half = -round(rng.uniform(lo, hi) / 2)
            for person, rows in people:
                rows.append(row(person, d + timedelta(days=rng.randint(0, 2)), cat, desc, half))

        # Personal spending.
        for person, rows in people:
            for _ in range(rng.randint(12, 20)):
                cat = rng.choice(list(PERSONAL))
                desc, lo, hi = rng.choice(PERSONAL[cat])
                d = month_start + timedelta(days=rng.randint(0, 27))
                rows.append(row(person, d, cat, desc, -round(rng.uniform(lo, hi))))

    # Messy rows the pipeline must handle.
    d = start + timedelta(days=10)
    a_rows.append(row("Person A", d, "  food ", "  Onigiri ", -180))                    # needs trimming / title-casing
    a_rows.append(row("Person A", d, "Food", "", -420))                                 # empty description
    b_rows.append(row("Person B", d, "Leisure", "Concert refund", 5500))                # credit, not expense
    b_rows.append({**row("Person B", d, "Food", "Mystery", -300), "Date": "sometime in March"})  # -> quarantine
    b_rows.append({**row("Person B", d, "Beverages", "Vending machine", -160), "moneeTransactionID": ""})  # no source id -> hash
    a_rows.append(dict(a_rows[0]))                                                      # exact duplicate (same source id)

    return a_rows, b_rows


def write(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADER)
        writer.writeheader()
        writer.writerows(rows)


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--months", type=int, default=3)
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--start", default="2026-01-01", help="first month, YYYY-MM-DD")
    p.add_argument("--out", default=str(Path(__file__).resolve().parents[1] / "data" / "sample"))
    args = p.parse_args(argv)

    a_rows, b_rows = generate(args.months, args.seed, date.fromisoformat(args.start))
    out = Path(args.out)
    write(out / "person_a.csv", a_rows)
    write(out / "person_b.csv", b_rows)
    print(f"Wrote {len(a_rows)} rows to {out / 'person_a.csv'} and {len(b_rows)} rows to {out / 'person_b.csv'}")


if __name__ == "__main__":
    main()
