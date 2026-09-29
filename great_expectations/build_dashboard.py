"""Builds a real, static HTML data-quality dashboard from the Great
Expectations validation results and the PySpark job's own aggregate
output -- so a pipeline run's health is visible at a glance instead of
only as console PASS/FAIL lines nobody reads after the run finishes.

Run directly (`python great_expectations/build_dashboard.py`), matching
every other script in this pipeline -- not designed to be imported as
`great_expectations.build_dashboard`, since this directory shares its
name with the real installed `great_expectations` library and package
resolution across that shadow is not reliable to depend on.
"""

import sys
from datetime import datetime, timezone

import pandas as pd
from validate_outputs import validate_salary_stats, validate_top_postings


def _salary_bars(industry_avg):
    if industry_avg.empty:
        return "<p>No data.</p>"
    max_value = industry_avg.max() or 1
    max_width = 400
    rows = []
    for industry, avg_salary in industry_avg.items():
        width = round((avg_salary / max_value) * max_width, 1)
        rows.append(
            f'<div class="bar-row">'
            f'<span class="bar-label">{industry}</span>'
            f'<svg width="{max_width}" height="22" role="img" aria-label="{industry} average salary">'
            f'<rect width="{width}" height="22" fill="#2f6f4f"></rect>'
            f"</svg>"
            f'<span class="bar-value">${avg_salary:,.0f}</span>'
            f"</div>"
        )
    return "\n".join(rows)


def build(output_dir, dashboard_path):
    all_results = validate_salary_stats(output_dir) + validate_top_postings(output_dir)

    expectation_rows = [
        (r["expectation_config"]["expectation_type"], "PASS" if r["success"] else "FAIL")
        for r in all_results
    ]
    passed = sum(1 for _, status in expectation_rows if status == "PASS")
    total = len(expectation_rows)

    stats_df = pd.read_parquet(f"{output_dir}/salary_stats_by_industry_seniority")
    top_df = pd.read_parquet(f"{output_dir}/top_postings_by_industry")

    industry_avg = stats_df.groupby("industry")["avg_salary"].mean().sort_values(ascending=False)

    expectation_rows_html = "\n".join(
        f'<tr class="{"pass" if status == "PASS" else "fail"}">'
        f"<td>{expectation}</td><td>{status}</td></tr>"
        for expectation, status in expectation_rows
    )

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Job Market Lakehouse -- Data Quality Dashboard</title>
<style>
  body {{ font-family: -apple-system, "Segoe UI", sans-serif; margin: 2rem; color: #1a1a1a; }}
  h1 {{ margin-bottom: 0; }}
  .generated {{ color: #666; font-size: 0.85rem; margin-top: 0.25rem; }}
  .kpis {{ display: flex; gap: 1rem; margin: 1.5rem 0; flex-wrap: wrap; }}
  .kpi {{ border: 1px solid #ddd; border-radius: 8px; padding: 1rem 1.5rem; }}
  .kpi .value {{ font-size: 1.8rem; font-weight: 700; }}
  .kpi .label {{ color: #666; font-size: 0.85rem; }}
  table {{ border-collapse: collapse; width: 100%; max-width: 600px; margin-bottom: 2rem; }}
  th, td {{ text-align: left; padding: 0.4rem 0.6rem; border-bottom: 1px solid #eee; }}
  tr.pass td:last-child {{ color: #1a7a3a; font-weight: 600; }}
  tr.fail td:last-child {{ color: #b3261e; font-weight: 600; }}
  .bar-row {{ display: flex; align-items: center; gap: 0.75rem; margin-bottom: 0.4rem; }}
  .bar-label {{ width: 120px; }}
  .bar-value {{ color: #444; }}
</style>
</head>
<body>
  <h1>Job Market Lakehouse</h1>
  <p class="generated">Generated {generated_at} from the real PySpark output in {output_dir}</p>

  <div class="kpis">
    <div class="kpi"><div class="value">{passed}/{total}</div><div class="label">Expectations passed</div></div>
    <div class="kpi"><div class="value">{len(stats_df):,}</div><div class="label">Industry/seniority segments</div></div>
    <div class="kpi"><div class="value">{len(top_df):,}</div><div class="label">Top-ranked postings surfaced</div></div>
  </div>

  <h2>Great Expectations results</h2>
  <table>
    <thead><tr><th>Expectation</th><th>Result</th></tr></thead>
    <tbody>
{expectation_rows_html}
    </tbody>
  </table>

  <h2>Average salary by industry</h2>
  {_salary_bars(industry_avg)}
</body>
</html>
"""

    with open(dashboard_path, "w", encoding="utf-8") as f:
        f.write(html)

    return passed, total


def main():
    output_dir = sys.argv[1] if len(sys.argv) > 1 else "data/output"
    dashboard_path = sys.argv[2] if len(sys.argv) > 2 else f"{output_dir}/dashboard.html"

    passed, total = build(output_dir, dashboard_path)
    print(f"Wrote dashboard to {dashboard_path} ({passed}/{total} expectations passed).")
    if passed < total:
        sys.exit(1)


if __name__ == "__main__":
    main()
