"""
score_auto.py — Non-interactive automated scorer.

Runs the 18 unit tests, prints the result table, saves score_report.json,
and exits with:
  0  — automated pass threshold met  (≥ 40 / 62)
  1  — below threshold

Used by:
  • GitHub Actions  (runs automatically on push to solution/* branch)
  • make score-auto (quick check without manual prompts)
  • make score-branch BRANCH=solution/jane-doe  (full local scoring)
"""

import sys
import json
import datetime
import os
import pytest

# ── Re-use section/point definitions from score.py ───────────────────────────
from score import SECTIONS, MANUAL_SECTIONS, AUTO_MAX, PASS_THRESHOLD_AUTO, _Collector, W, line, hdr, row

PASS_THRESHOLD_TOTAL = 65


def run_auto(candidate: str = "") -> tuple[int, dict]:
    """Run automated tests. Returns (score, details_dict)."""
    collector = _Collector()
    pytest.main(
        ["tests/test_transforms.py", "--tb=short", "-q",
         "--no-header", "--disable-warnings"],
        plugins=[collector],
    )

    auto_earned  = 0
    auto_details = {}

    for section in SECTIONS:
        section_rows = []
        for node_id, display, pts in section["tests"]:
            passed = collector.results.get(node_id)
            earned = pts if passed is True else 0
            st_str = "PASS" if passed is True else "FAIL"
            auto_earned += earned
            section_rows.append((display, earned, pts, st_str))
        auto_details[section["title"]] = section_rows

    return auto_earned, auto_details


def print_report(candidate, auto_earned, auto_details):
    print()
    print("┌" + line("═") + "┐")
    print(hdr(" WIKISTREAM — AUTOMATED SCORE REPORT"))
    if candidate:
        print(hdr(f" Candidate : {candidate}"))
    print(hdr(f" Date/Time : {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}"))
    print("├" + line("═") + "┤")

    for section_title, results in auto_details.items():
        print(hdr(f" {section_title}"))
        print("├" + line() + "┤")
        print(row("Test", "Pts", "Result"))
        print("├" + line() + "┤")
        for display, earned, pts, st_str in results:
            status = "✓ pass" if st_str == "PASS" else "✗ FAIL"
            print(row(display, f"{earned}/{pts}", status))
        print("├" + line() + "┤")

    auto_pct    = int(auto_earned / AUTO_MAX * 100)
    verdict_str = "✓  AUTO PASS" if auto_earned >= PASS_THRESHOLD_AUTO else "✗  BELOW THRESHOLD"
    print(row("AUTOMATED TOTAL", f"{auto_earned}/{AUTO_MAX}", f"{auto_pct}%"))
    print("└" + line("═") + "┘")
    print(f"\n  Auto threshold : {PASS_THRESHOLD_AUTO} / {AUTO_MAX}")
    print(f"  Result         : {verdict_str}")
    print(f"\n  Manual scoring still needed: +{sum(s['max'] for s in MANUAL_SECTIONS)} pts possible")
    print(f"  Run: make score-branch  to complete the full score\n")


def save_json(candidate, auto_earned, auto_details):
    verdict = "AUTO_PASS" if auto_earned >= PASS_THRESHOLD_AUTO else "AUTO_FAIL"
    report = {
        "candidate":    candidate,
        "timestamp":    datetime.datetime.now().isoformat(),
        "automated":    auto_earned,
        "auto_max":     AUTO_MAX,
        "verdict":      verdict,
        "auto_details": {
            s: [{"test": d, "earned": e, "max": p, "passed": st == "PASS"}
                for d, e, p, st in r]
            for s, r in auto_details.items()
        },
    }
    with open("score_report.json", "w") as f:
        json.dump(report, f, indent=2)
    return verdict


def main():
    # Candidate name from env var (set by make score-branch) or empty
    candidate = os.getenv("CANDIDATE_NAME", "")

    print("Running automated tests …\n")
    auto_earned, auto_details = run_auto(candidate)
    print_report(candidate, auto_earned, auto_details)
    verdict = save_json(candidate, auto_earned, auto_details)

    sys.exit(0 if verdict == "AUTO_PASS" else 1)


if __name__ == "__main__":
    main()
