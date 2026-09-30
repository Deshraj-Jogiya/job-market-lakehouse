from hadoop.mapper import map_line
from hadoop.reducer import reduce_lines


def test_mapper_skips_header_and_keys_by_month_and_source():
    assert map_line("posting_id,company_id,seniority,source,posted_date,salary_usd\n") is None
    assert map_line("7,3,Mid,lever,2025-06-14,101000\n") == "2025-06|lever\t1"
    assert map_line("broken,line\n") is None


def test_reducer_sums_sorted_keys():
    lines = ["2025-06|lever\t1\n", "2025-06|lever\t1\n", "2025-07|greenhouse\t1\n"]
    assert list(reduce_lines(lines)) == ["2025-06|lever\t2", "2025-07|greenhouse\t1"]


def test_mapper_and_reducer_end_to_end_match_a_plain_count():
    rows = ["posting_id,company_id,seniority,source,posted_date,salary_usd",
            "1,1,Mid,lever,2025-06-01,1", "2,1,Mid,lever,2025-06-20,1", "3,2,Staff,workable,2025-07-02,1"]
    mapped = sorted(filter(None, (map_line(r + "\n") for r in rows)))
    assert dict(x.split("\t") for x in reduce_lines(m + "\n" for m in mapped)) == {"2025-06|lever": "2", "2025-07|workable": "1"}
