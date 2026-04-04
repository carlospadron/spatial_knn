import argparse
import pandas as pd

from runners import SCENARIOS, check, make_plot, run_scenario, update_readme


def main():
    scenario_names = [s["name"] for s in SCENARIOS]
    parser = argparse.ArgumentParser(description="Run KNN benchmarks")
    parser.add_argument(
        "--scenario",
        choices=scenario_names,
        default=None,
        help="Run a single named scenario (default: run all)",
    )
    args = parser.parse_args()
    scenarios_to_run = [
        s for s in SCENARIOS if args.scenario is None or s["name"] == args.scenario
    ]

    scenario_references = {}
    for scenario in scenarios_to_run:
        ref = run_scenario(scenario)
        scenario_references[scenario["name"]] = ref

    # Cloud service checks (use White Horse reference for validation)
    reference = scenario_references.get("White Horse (small)", pd.DataFrame())
    check("big_query_result.csv", reference)
    check("redshift_result.csv", reference)
    check("athena_results.csv", reference)

    knn = pd.read_csv("snowflake_result.csv").rename(columns=lambda x: x.lower())
    merged = pd.merge(reference, knn, how="outer", on="origin")
    print(merged[merged["destination_x"] != merged["destination_y"]])

    # update filename to match the actual Databricks output CSV
    knn = pd.read_csv(
        "part-00000-tid-83222403381116274-4d1c858d-cce3-41a1-a392-7cb7afb59909-633-1-c000.csv"
    )
    merged = pd.merge(reference, knn, how="outer", on="origin")
    print(merged[merged["destination_x"] != merged["destination_y"]])

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
