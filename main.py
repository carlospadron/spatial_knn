import argparse
import glob
import os
import pandas as pd

from runners import SCENARIOS, SOLUTION_NAMES, check, make_plot, run_scenario, update_readme


def main():
    scenario_names = [s["name"] for s in SCENARIOS]
    parser = argparse.ArgumentParser(description="Run KNN benchmarks")
    parser.add_argument(
        "--scenario",
        choices=scenario_names,
        default=None,
        help="Run a single named scenario (default: run all)",
    )
    parser.add_argument(
        "--solution",
        choices=SOLUTION_NAMES,
        nargs="+",
        default=None,
        metavar="SOLUTION",
        help=f"Run only specific solutions. Choices: {', '.join(SOLUTION_NAMES)}",
    )
    parser.add_argument(
        "--results-only",
        action="store_true",
        help="Skip benchmarks; regenerate plot and README from existing baselines.csv",
    )
    parser.add_argument(
        "--skip-reference",
        action="store_true",
        help="Skip recomputing the reference CSV if it already exists on disk",
    )
    args = parser.parse_args()

    if args.results_only:
        baselines = (
            pd.read_csv("baselines.csv")
            .groupby(["dataset", "test"], sort=False)["elapsed_s"]
            .min()
            .reset_index()
        )
        make_plot(baselines, "results.png")
        update_readme(baselines)
        return

    scenarios_to_run = [
        s for s in SCENARIOS if args.scenario is None or s["name"] == args.scenario
    ]
    solutions = set(args.solution) if args.solution else None

    scenario_references = {}
    for scenario in scenarios_to_run:
        ref = run_scenario(scenario, solutions=solutions, skip_reference=args.skip_reference)
        scenario_references[scenario["name"]] = ref

    # Cloud service checks (use White Horse reference for validation)
    reference = scenario_references.get("White Horse (small)", pd.DataFrame())
    for cloud_csv in ("big_query_result.csv", "redshift_result.csv", "athena_results.csv"):
        if os.path.exists(cloud_csv):
            check(cloud_csv, reference)
        else:
            print(f"SKIPPED (file not found): {cloud_csv}")

    if os.path.exists("snowflake_result.csv"):
        check("snowflake_result.csv", reference, lowercase_columns=True)
    else:
        print("SKIPPED (file not found): snowflake_result.csv")

    databricks_csvs = glob.glob("part-*.csv")
    if databricks_csvs:
        check(databricks_csvs[0], reference)
    else:
        print("SKIPPED (file not found): part-*.csv (Databricks)")

    # Load baselines and keep the best (minimum) elapsed_s per (dataset, test)
    baselines = (
        pd.read_csv("baselines.csv")
        .groupby(["dataset", "test"], sort=False)["elapsed_s"]
        .min()
        .reset_index()
    )

    make_plot(baselines, "results.png")
    update_readme(baselines)


if __name__ == "__main__":
    main()
