"""
Live scorer for Problem 3 — WikiStream Analytics Pipeline.

Run any time:  python score.py  |  make score

At the end the full report is saved to:  score_report.txt
"""

import sys
import json
import datetime
import pytest

# ── Automated test → point mapping ───────────────────────────────────────────

SECTIONS = [
    {
        "title": "Task 1 — Parse Events (Kafka → clean DataFrame)",
        "tests": [
            ("tests/test_transforms.py::TestParseEvents::test_filters_non_edit_type",
             "type=new filtered out (only type=edit survives)",           6),
            ("tests/test_transforms.py::TestParseEvents::test_output_columns_present",
             "All 7 required output columns present",                     3),
            ("tests/test_transforms.py::TestParseEvents::test_event_time_is_timestamp_type",
             "event_time is TimestampType (not Long or String)",          5),
            ("tests/test_transforms.py::TestParseEvents::test_event_time_year_is_correct",
             "event_time parsed from meta.dt (not unix timestamp field)", 5),
            ("tests/test_transforms.py::TestParseEvents::test_bytes_old_and_new_extracted",
             "bytes_old/bytes_new from nested length struct",             3),
            ("tests/test_transforms.py::TestParseEvents::test_bot_flag_preserved",
             "bot boolean flag preserved correctly",                       3),
        ],
    },
    {
        "title": "Task 2 — Windowed Aggregation (5-min tumbling window)",
        "tests": [
            ("tests/test_transforms.py::TestBuildAggregation::test_output_columns_present",
             "All required output columns present",                       3),
            ("tests/test_transforms.py::TestBuildAggregation::test_window_start_is_timestamp",
             "window_start is TimestampType (struct flattened correctly)", 4),
            ("tests/test_transforms.py::TestBuildAggregation::test_one_row_per_wiki_per_window",
             "One row per (wiki, window) — no cross-wiki bleeding",       4),
            ("tests/test_transforms.py::TestBuildAggregation::test_edit_count_enwiki",
             "edit_count = 7 for enwiki",                                 4),
            ("tests/test_transforms.py::TestBuildAggregation::test_edit_count_dewiki",
             "edit_count = 3 for dewiki",                                 3),
            ("tests/test_transforms.py::TestBuildAggregation::test_bot_edit_count_zero_for_human_wiki",
             "bot_edit_count = 0 for all-human wiki",                    4),
            ("tests/test_transforms.py::TestBuildAggregation::test_bot_edit_count_for_bot_wiki",
             "bot_edit_count = 3 for all-bot wiki",                      4),
            ("tests/test_transforms.py::TestBuildAggregation::test_net_bytes_positive_when_growing",
             "net_bytes_added > 0 when pages grow",                       4),
            ("tests/test_transforms.py::TestBuildAggregation::test_net_bytes_negative_when_shrinking",
             "net_bytes_added = -30 (3 × -10 bytes)",                    5),
            ("tests/test_transforms.py::TestBuildAggregation::test_no_window_struct_in_output",
             "window struct flattened — no raw 'window' column",          2),
        ],
    },
]

MANUAL_SECTIONS = [
    {
        "title": "Task 0 — SparkSession Config",
        "max": 10,
        "criteria": [
            "Delta extensions set (DeltaSparkSessionExtension + DeltaCatalog)",
            "S3A endpoint set to MINIO_ENDPOINT",
            "path.style.access = true  ← critical for MinIO (not virtual-hosted)",
            "fs.s3a.impl set to S3AFileSystem",
            "Credentials (access.key / secret.key) from env vars, not hardcoded",
        ],
    },
    {
        "title": "Task 2 — Watermark Placement & Output Mode",
        "max": 8,
        "criteria": [
            "withWatermark() called BEFORE groupBy (not after)",
            "Output mode: 'update' (not 'complete' — would cause unbounded state)",
            "Can explain: late event arriving after watermark is dropped, not processed",
        ],
    },
    {
        "title": "Task 3 — Postgres foreachBatch",
        "max": 12,
        "criteria": [
            "Uses foreachBatch (not direct JDBC streaming sink — doesn't exist)",
            "Staging write uses mode='overwrite' (not 'append' — idempotent!)",
            "UPSERT SQL: ON CONFLICT (window_start, wiki) DO UPDATE SET ...",
            "checkpointLocation set for this query",
            "psycopg2 connection opened and closed correctly (no leak)",
        ],
    },
    {
        "title": "Task 4 — Delta Lake on MinIO",
        "max": 10,
        "criteria": [
            "format('delta') not format('parquet')",
            "outputMode: 'append' (raw events, no aggregation — correct)",
            "partitionBy('wiki') — not by title/user (cardinality awareness)",
            "Separate checkpointLocation from Task 3",
            "make check-minio shows _delta_log/ directory in bucket",
        ],
    },
    {
        "title": "Curveball A — Idempotent Postgres Writes",
        "max": 10,
        "criteria": [
            "Identifies mode='overwrite' on staging as the fix (or already had it)",
            "Explains WHY: Spark re-processes batches on crash recovery (at-least-once)",
            "Distinguishes: checkpointing = offset exactly-once, NOT sink exactly-once",
        ],
    },
    {
        "title": "Curveball B — Third Sink (wiki.en-only Kafka topic)",
        "max": 10,
        "criteria": [
            "Creates a NEW writeStream on parsed_df (independent query, no restart)",
            "Separate checkpointLocation (not reusing existing one)",
            "to_json(struct(...)) used for Kafka value serialization",
            "Filters wiki == 'enwiki' before writing",
            "awaitAnyTermination covers all 3 queries",
        ],
    },
]

AUTO_MAX             = sum(pts for s in SECTIONS for _, _, pts in s["tests"])
MANUAL_MAX           = sum(s["max"] for s in MANUAL_SECTIONS)
PASS_THRESHOLD_AUTO  = int(AUTO_MAX * 0.65)   # 65% of automated tests
PASS_THRESHOLD_TOTAL = 65                      # out of 100


# ── Pytest collector ──────────────────────────────────────────────────────────

class _Collector:
    def __init__(self):
        self.results: dict[str, bool] = {}

    def pytest_runtest_logreport(self, report):
        if report.when == "call":
            self.results[report.nodeid] = report.passed
        elif report.when == "setup" and report.failed:
            self.results[report.nodeid] = False


# ── Rendering ─────────────────────────────────────────────────────────────────

W = 70

def line(c="─"):  return c * W
def hdr(text):
    return f"│ {text}{' ' * (W - 2 - len(text))} │"
def row(label, pts, status, lw=46):
    return f"│  {label[:lw].ljust(lw)}  {pts.center(5)}  {status.ljust(10)} │"


def ask_manual():
    total = 0
    scores = {}
    print("\n┌" + line() + "┐")
    print(hdr(" MANUAL ASSESSMENT — Streaming & Curveball"))
    print("├" + line() + "┤")
    for s in MANUAL_SECTIONS:
        print(hdr(f" {s['title']}  (max {s['max']} pts)"))
        for c in s["criteria"]:
            print(hdr(f"   • {c}"))
        while True:
            try:
                val = int(input(f"│  Score (0–{s['max']}): ").strip())
                if 0 <= val <= s["max"]:
                    total += val
                    scores[s["title"]] = val
                    break
            except ValueError:
                pass
            print(f"│  Enter a number 0–{s['max']}")
        print("├" + line() + "┤")
    return total, scores


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    collector = _Collector()
    print("Running automated tests (SparkSession startup ~30 s) …\n")
    pytest.main(
        ["tests/test_transforms.py", "--tb=short", "-q",
         "--no-header", "--disable-warnings"],
        plugins=[collector],
    )

    print()
    print("┌" + line("═") + "┐")
    print(hdr(" WIKISTREAM PIPELINE — INTERVIEW SCORE REPORT"))
    print("├" + line("═") + "┤")

    auto_earned  = 0
    auto_details = {}   # section title → list of (display, earned, pts, status_str)

    for section in SECTIONS:
        print(hdr(f" {section['title']}"))
        print("├" + line() + "┤")
        print(row("Test", "Pts", "Result"))
        print("├" + line() + "┤")
        section_rows = []
        for node_id, display, pts in section["tests"]:
            passed  = collector.results.get(node_id)
            earned  = pts if passed is True else 0
            status  = "✓ pass" if passed is True else ("✗ FAIL" if passed is False else "– n/a")
            st_str  = "PASS" if passed is True else "FAIL"
            auto_earned += earned
            section_rows.append((display, earned, pts, st_str))
            print(row(display, f"{earned}/{pts}", status))
        auto_details[section["title"]] = section_rows
        print("├" + line() + "┤")

    auto_pct    = int(auto_earned / AUTO_MAX * 100)
    auto_status = "✓" if auto_earned >= PASS_THRESHOLD_AUTO else "✗"
    print(row("AUTOMATED SUBTOTAL", f"{auto_earned}/{AUTO_MAX}", f"{auto_status} {auto_pct}%"))
    print("└" + line("─") + "┘")
    print(f"\n  Automated threshold : {PASS_THRESHOLD_AUTO}/{AUTO_MAX}")

    # Collect candidate name for the report
    candidate = input("\nCandidate name (press Enter to skip): ").strip() or "Unknown"

    ans = input("Score manual sections now? (y/N): ").strip().lower()
    if ans == "y":
        manual, manual_scores = ask_manual()
        total   = auto_earned + manual
        verdict = "PASS" if total >= PASS_THRESHOLD_TOTAL else "FAIL"
        strong  = " — STRONG HIRE" if total >= 80 else ""
        print()
        print("┌" + line("═") + "┐")
        print(hdr(f" CANDIDATE   : {candidate}"))
        print(hdr(f" AUTOMATED   : {auto_earned} / {AUTO_MAX}"))
        print(hdr(f" MANUAL      : {manual} / {MANUAL_MAX}"))
        print(hdr(f" ────────────────────────────────────"))
        print(hdr(f" TOTAL       : {total} / 100"))
        print(hdr(f" VERDICT     : {verdict}{strong}"))
        print("└" + line("═") + "┘")

        _save_report(candidate, auto_earned, auto_details, manual, manual_scores, total, verdict + strong)
    else:
        print(f"\n  Automated: {auto_earned}/{AUTO_MAX}  |  run again with 'y' for full score\n")


def _save_report(candidate, auto_earned, auto_details, manual, manual_scores, total, verdict):
    """Write a plain-text + JSON report to score_report.txt"""
    ts  = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    out = []
    out.append("=" * 72)
    out.append(f"  WIKISTREAM INTERVIEW — SCORE REPORT")
    out.append(f"  Candidate : {candidate}")
    out.append(f"  Date/Time : {ts}")
    out.append("=" * 72)
    out.append("")
    out.append("AUTOMATED TESTS")
    out.append("-" * 50)
    for section_title, results in auto_details.items():
        out.append(f"\n  {section_title}")
        for display, earned, pts, status in results:
            out.append(f"    [{status}]  {display:<48}  {earned}/{pts}")
    out.append("")
    out.append("MANUAL ASSESSMENT")
    out.append("-" * 50)
    for title, score in manual_scores.items():
        max_pts = next(s["max"] for s in MANUAL_SECTIONS if s["title"] == title)
        out.append(f"  {title:<45}  {score}/{max_pts}")
    out.append("")
    out.append("=" * 72)
    out.append(f"  AUTOMATED  : {auto_earned} / {sum(pts for s in SECTIONS for _,_,pts in s['tests'])}")
    out.append(f"  MANUAL     : {manual} / {sum(s['max'] for s in MANUAL_SECTIONS)}")
    out.append(f"  TOTAL      : {total} / 100")
    out.append(f"  VERDICT    : {verdict}")
    out.append("=" * 72)

    report_text = "\n".join(out)
    with open("score_report.txt", "w") as f:
        f.write(report_text)

    # Also save machine-readable JSON
    with open("score_report.json", "w") as f:
        json.dump({
            "candidate":    candidate,
            "timestamp":    ts,
            "automated":    auto_earned,
            "manual":       manual,
            "total":        total,
            "verdict":      verdict,
            "auto_details": {
                s: [{"test": d, "earned": e, "max": p, "passed": st == "PASS"}
                    for d, e, p, st in r]
                for s, r in auto_details.items()
            },
            "manual_scores": manual_scores,
        }, f, indent=2)

    print(f"\n  Report saved → score_report.txt")
    print(f"  JSON saved  → score_report.json\n")


if __name__ == "__main__":
    main()
