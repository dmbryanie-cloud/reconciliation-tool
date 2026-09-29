"""Run every test suite against a throwaway local Postgres. Exits non-zero if anything fails.

    python tests/run_all.py            # all suites
    python tests/run_all.py security   # suites whose name contains "security"

No suite talks to QuickBooks: every QuickBooks call is mocked.
"""
import glob
import os
import subprocess
import sys
import time

import harness

SUITES = sorted(glob.glob(os.path.join(harness.HERE, "suite_*.py")))


def main():
    wanted = sys.argv[1:]
    suites = [s for s in SUITES if not wanted or any(w in os.path.basename(s) for w in wanted)]
    if not suites:
        sys.exit(f"No suite matches {wanted}.")
    db = harness.ensure_db()   # one database for the whole run; each suite resets the schema
    results = []
    try:
        for path in suites:
            name = os.path.basename(path)[len("suite_"):-3]
            print(f"\n=== {name} " + "=" * (60 - len(name)), flush=True)
            t0 = time.time()
            env = dict(os.environ, PYTHONIOENCODING="utf-8")
            code = subprocess.run([sys.executable, "-u", path], cwd=harness.HERE, env=env).returncode
            results.append((name, code, time.time() - t0))
    finally:
        harness.stop_db(db)
    print("\n" + "=" * 64)
    for name, code, secs in results:
        print(f"{'PASS' if code == 0 else 'FAIL'}  {name:<28} {secs:5.1f}s")
    failed = [n for n, code, _ in results if code != 0]
    print(f"\n{len(results) - len(failed)} of {len(results)} suites passed.")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
