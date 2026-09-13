"""Generates synthetic job-market raw data at a scale that actually
justifies Spark over pandas -- large enough (50k+ rows) that a
DataFrame-API groupBy/window pipeline is a real design choice, not
theater. Clearly-synthetic data, same disclosure as every other
seed/fixture dataset in this author's other repos."""

import csv
import random
from datetime import date, timedelta

random.seed(42)

INDUSTRIES = ["Software", "Finance", "Healthcare", "Manufacturing", "Retail", "Logistics", "Energy"]
SENIORITIES = ["Junior", "Mid", "Senior", "Staff"]
SOURCES = ["greenhouse", "lever", "workable", "smartrecruiters", "linkedin"]
COMPANY_COUNT = 400
POSTING_COUNT = 60000

START_DATE = date(2025, 1, 1)
DAY_SPAN = 600


def main():
    with open("data/raw_companies.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["company_id", "industry", "size_band"])
        for company_id in range(1, COMPANY_COUNT + 1):
            writer.writerow([
                company_id,
                random.choice(INDUSTRIES),
                random.choice(["Small", "Mid", "Large"]),
            ])

    with open("data/raw_postings.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["posting_id", "company_id", "seniority", "source", "posted_date", "salary_usd"])
        for posting_id in range(1, POSTING_COUNT + 1):
            company_id = random.randint(1, COMPANY_COUNT)
            seniority = random.choice(SENIORITIES)
            base_salary = {"Junior": 75000, "Mid": 105000, "Senior": 145000, "Staff": 180000}[seniority]
            salary = base_salary + random.randint(-15000, 25000)
            posted_date = START_DATE + timedelta(days=random.randint(0, DAY_SPAN))
            writer.writerow([
                posting_id,
                company_id,
                seniority,
                random.choice(SOURCES),
                posted_date.isoformat(),
                salary,
            ])

    print(f"Wrote {COMPANY_COUNT} companies and {POSTING_COUNT} postings.")


if __name__ == "__main__":
    main()
