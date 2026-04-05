import argparse
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
    args = parser.parse_args()
    scenarios_to_run = [
        s for s in SCENARIOS if args.scenario is None or s["name"] == args.scenario
    ]
    solutions = set(args.solution) if args.solution else None

    scenario_references = {}
    for scenario in scenarios_to_run:
        ref = run_scenario(scenario, solutions=solutions)
        scenario_references[scenario["name"]] = ref

    # Cloud service checks (use White Horse reference for validation)
    reference = scenario_references.get("White Horse (small)", pd.DataFrame())
    for cloud_csv in ("big_query_result.csv", "redshift_result.csv", "athena_results.csv"):
        if os.path.exists(cloud_csv):
            check(cloud_csv, reference)
        else:
            print(f"SKIPPED (file not found): {cloud_csv}")

    if os.path.exists("snowflake_result.csv"):
        knn = pd.read_csv("snowflake_result.csv").rename(columns=lambda x: x.lower())
        merged = pd.merge(reference, knn, how="outer", on="origin")
        print(merged[merged["destination_x"] != merged["destination_y"]])
    else:
        print("SKIPPED (file not found): snowflake_result.csv")

    # update filename to match the actual Databricks output CSV
    databricks_csv = "part-00000-tid-83222403381116274-4d1c858d-cce3-41a1-a392-7cb7afb59909-633-1-c000.csv"
    if os.path.exists(databricks_csv):
        knn = pd.read_csv(databricks_csv)
        merged = pd.merge(reference, knn, how="outer", on="origin")
        print(merged[merged["destination_x"] != merged["destination_y"]])
    else:
        print(f"SKIPPED (file not found): {databricks_csv}")

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
