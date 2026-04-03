# %% [markdown]
# # imports

# %%
import os
import subprocess
import time
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pandas as pd


def run_script(script_path, extra_env=None):
    env = {**os.environ}
    if extra_env:
        env.update(extra_env)
    result = subprocess.run(
        ["uv", "run", "--env-file", ".env", script_path], capture_output=True, text=True, env=env
    )
    if result.returncode != 0:
        print(f"FAILED\n{result.stderr}")
        return None
    elapsed = pd.Timedelta(result.stdout.strip()).total_seconds()
    print(f"Elapsed: {result.stdout.strip()}")
    return elapsed


def run_script_docker(script_path, extra_env=None):
    """Run a Python script inside the sedona Docker container (Spark + Sedona pre-installed)."""
    env_args = []
    if extra_env:
        for k, v in extra_env.items():
            env_args += ["-e", f"{k}={v}"]
    start = time.time()
    result = subprocess.run(
        ["docker", "compose", "exec", "-T"] + env_args + ["sedona", "python3", script_path],
        capture_output=True,
        text=True,
    )
    elapsed = time.time() - start
    if result.stdout.strip():
        print(result.stdout.strip())
    if result.stderr.strip():
        print(result.stderr.strip())
    if result.returncode != 0:
        print(f"FAILED (exit code {result.returncode})")
        return None
    return elapsed


def run_kotlin():
    """Run the Kotlin Maven project inside the kotlin Docker container."""
    start = time.time()
    result = subprocess.run(
        [
            "docker",
            "compose",
            "run",
            "--rm",
            "-T",
            "kotlin",
            "mvn",
            "compile",
            "exec:java",
        ],
        capture_output=True,
        text=True,
    )
    elapsed = time.time() - start
    if result.stdout.strip():
        print(result.stdout.strip())
    if result.stderr.strip():
        print(result.stderr.strip())
    if result.returncode != 0:
        print(f"FAILED (exit code {result.returncode})")
        return None
    print(f"Elapsed: {elapsed:.1f}s")
    return elapsed


def run_scala():
    """Run the Scala sbt project inside the scala Docker container."""
    start = time.time()
    result = subprocess.run(
        ["docker", "compose", "run", "--rm", "-T", "scala", "sbt", "run"],
        capture_output=True,
        text=True,
    )
    elapsed = time.time() - start
    if result.stdout.strip():
        print(result.stdout.strip())
    if result.stderr.strip():
        print(result.stderr.strip())
    if result.returncode != 0:
        print(f"FAILED (exit code {result.returncode})")
        return None
    print(f"Elapsed: {elapsed:.1f}s")
    return elapsed


def run_rust():
    """Run the Rust project locally using cargo."""
    env_vars = {}
    with open(".env") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                env_vars[key.strip()] = val.strip()
    env = {**os.environ, **env_vars}
    start = time.time()
    result = subprocess.run(
        ["cargo", "run", "--release"],
        capture_output=True,
        text=True,
        env=env,
        cwd="rust",
    )
    elapsed = time.time() - start
    if result.stdout.strip():
        print(result.stdout.strip())
    if result.stderr.strip():
        print(result.stderr.strip())
    if result.returncode != 0:
        print(f"FAILED (exit code {result.returncode})")
        return None
    print(f"Elapsed: {elapsed:.1f}s")
    return elapsed


def run_csharp():
    """Run the C# project locally using dotnet."""
    start = time.time()
    result = subprocess.run(
        ["dotnet", "run", "--project", "csharp/knn_csharp.csproj"],
        capture_output=True,
        text=True,
    )
    elapsed = time.time() - start
    if result.stdout.strip():
        print(result.stdout.strip())
    if result.stderr.strip():
        print(result.stderr.strip())
    if result.returncode != 0:
        print(f"FAILED (exit code {result.returncode})")
        return None
    print(f"Elapsed: {elapsed:.1f}s")
    return elapsed


def run_go():
    """Run the Go project locally."""
    env_vars = {}
    with open(".env") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                env_vars[key.strip()] = val.strip()
    env = {**os.environ, **env_vars}
    start = time.time()
    result = subprocess.run(
        ["go", "run", "."], capture_output=True, text=True, env=env, cwd="go"
    )
    elapsed = time.time() - start
    if result.stdout.strip():
        print(result.stdout.strip())
    if result.stderr.strip():
        print(result.stderr.strip())
    if result.returncode != 0:
        print(f"FAILED (exit code {result.returncode})")
        return None
    print(f"Elapsed: {elapsed:.1f}s")
    return elapsed


def check(result_csv, ref):
    knn = pd.read_csv(result_csv)
    merged = pd.merge(ref, knn, how="outer", on="origin")
    mismatches = merged[merged["destination_x"] != merged["destination_y"]]
    if len(mismatches) == 0:
        print("✓ Results match reference")
    return mismatches


# Scenario definitions
SCENARIOS = [
    {
        "name": "White Horse (small)",
        "uprn_table": "os.open_uprn_white_horse",
        "codepoint_table": "os.code_point_open_white_horse",
        "result_suffix": "",
        "plot_file": "results_white_horse.png",
    },
    {
        "name": "Full GB (large)",
        "uprn_table": "os.os_open_uprn",
        "codepoint_table": "os.codepoint_polygons",
        "result_suffix": "_full",
        "plot_file": "results_full_gb.png",
    },
]


def run_scenario(scenario):
    env = {
        "UPRN_TABLE": scenario["uprn_table"],
        "CODEPOINT_TABLE": scenario["codepoint_table"],
    }
    sfx = scenario["result_suffix"]
    print(f"\n{'='*60}")
    print(f"Running scenario: {scenario['name']}")
    print(f"  UPRN table:      {scenario['uprn_table']}")
    print(f"  Codepoint table: {scenario['codepoint_table']}")
    print(f"{'='*60}\n")

    timings = []

    # %% [markdown]
    # # SQL nearest neighbour (distinct)

    print("--- SQL distinct ---")
    timings.append({"test": "SQL distinct", "elapsed_s": run_script("sql/sql_distinct/knn.py", extra_env=env)})
    reference = pd.read_csv("sql/sql_distinct/result.csv")

    # %% [markdown]
    # # SQL nearest neighbour (lateral)

    print("--- SQL lateral ---")
    timings.append({"test": "SQL lateral", "elapsed_s": run_script("sql/sql_lateral/knn.py", extra_env=env)})
    check("sql/sql_lateral/result.csv", reference)

    # %% [markdown]
    # # Geopandas sjoin_nearest

    print("--- GeoPandas sjoin_nearest ---")
    timings.append({"test": "Geopandas sjoin_nearest", "elapsed_s": run_script("python/geopandas/knn.py", extra_env=env)})
    check("python/geopandas/result.csv", reference)

    # %% [markdown]
    # # python nearest neighbour - shapely (all vs all)

    print("--- Shapely all vs all ---")
    timings.append({"test": "Shapely all vs all", "elapsed_s": run_script("python/shapely_all_vs_all/knn.py", extra_env=env)})
    check("python/shapely_all_vs_all/result.csv", reference)

    # %% [markdown]
    # # python nearest neighbour - shapely (strtree)

    print("--- Shapely strtree ---")
    timings.append({"test": "Shapely strtree", "elapsed_s": run_script("python/shapely_strtree/knn.py", extra_env=env)})
    check("python/shapely_strtree/result.csv", reference)

    # %% [markdown]
    # # Scikit-Learn

    print("--- Scikit-Learn ---")
    timings.append({"test": "Scikit-Learn nearest neighbour", "elapsed_s": run_script("python/sklearn/knn.py", extra_env=env)})
    check("python/sklearn/result.csv", reference)

    # %% [markdown]
    # # Apache Sedona partial sql

    print("--- Apache Sedona partial sql ---")
    timings.append({"test": "Apache Sedona partial sql", "elapsed_s": run_script_docker("python/sedona_partial/knn.py", extra_env=env)})
    check("python/sedona_partial/result.csv", reference)

    # %% [markdown]
    # # Apache Sedona pure sql

    print("--- Apache Sedona pure sql ---")
    timings.append({"test": "Apache Sedona pure sql", "elapsed_s": run_script_docker("python/sedona_pure/knn.py", extra_env=env)})
    check("python/sedona_pure/result.csv", reference)

    # %% [markdown]
    # # Apache Sedona KNN operator

    print("--- Apache Sedona st_knn ---")
    timings.append({"test": "Apache Sedona st_knn", "elapsed_s": run_script_docker("python/sedona_knn/knn.py", extra_env=env)})
    check("python/sedona_knn/result.csv", reference)

    # %% [markdown]
    # # Kotlin / Scala / Rust / C# / Go
    # (compiled-language runners pick up UPRN_TABLE / CODEPOINT_TABLE via injected env)

    print("--- Kotlin ---")
    timings.append({"test": "Kotlin", "elapsed_s": run_kotlin()})
    check("kotlin/kotlin_all_vs_all.csv", reference)
    check("kotlin/kotlin_tree.csv", reference)

    print("--- Scala ---")
    timings.append({"test": "Scala", "elapsed_s": run_scala()})
    check("scala/scala_all_vs_all.csv", reference)
    check("scala/scala_tree.csv", reference)

    print("--- Rust ---")
    timings.append({"test": "Rust", "elapsed_s": run_rust()})
    check("rust/rust_all_vs_all.csv", reference)
    check("rust/rust_tree.csv", reference)

    print("--- C# ---")
    timings.append({"test": "C#", "elapsed_s": run_csharp()})
    check("csharp_all_vs_all.csv", reference)
    check("csharp_tree.csv", reference)

    print("--- Go ---")
    timings.append({"test": "Go", "elapsed_s": run_go()})
    check("go/go_all_vs_all.csv", reference)
    check("go/go_tree.csv", reference)

    # %% [markdown]
    # # DuckDB

    print("--- DuckDB ---")
    timings.append({"test": "DuckDB", "elapsed_s": run_script("python/duckdb/knn.py", extra_env=env)})
    check("python/duckdb/result.csv", reference)

    # %% [markdown]
    # # SedonaDB

    print("--- SedonaDB ---")
    timings.append({"test": "SedonaDB", "elapsed_s": run_script("python/sedonadb/knn.py", extra_env=env)})
    check("python/sedonadb/result.csv", reference)

    csv_path = f"timings{sfx}.csv"
    pd.DataFrame(timings).to_csv(csv_path, index=False)
    print(f"\nTimings written to {csv_path}")

    return reference


# %%
# Run both scenarios
scenario_references = {}
for scenario in SCENARIOS:
    ref = run_scenario(scenario)
    scenario_references[scenario["name"]] = ref




# %% [markdown]
# # Cloud service checks (use White Horse reference for validation)

# %%
reference = scenario_references.get("White Horse (small)", pd.DataFrame())

check("big_query_result.csv", reference)
check("redshift_result.csv", reference)
check("athena_results.csv", reference)

knn = pd.read_csv("snowflake_result.csv").rename(columns=lambda x: x.lower())
merged = pd.merge(reference, knn, how="outer", on="origin")
merged[merged["destination_x"] != merged["destination_y"]]

# update filename to match the actual Databricks output CSV
knn = pd.read_csv(
    "part-00000-tid-83222403381116274-4d1c858d-cce3-41a1-a392-7cb7afb59909-633-1-c000.csv"
)
merged = pd.merge(reference, knn, how="outer", on="origin")
merged[merged["destination_x"] != merged["destination_y"]]


# %% [markdown]
# # Results — per-scenario benchmark tables and plots

# %%
# Baseline timings loaded from baselines.csv (dataset, test, elapsed_s)
_baselines = pd.read_csv("baselines.csv")
RESULTS_BY_SCENARIO = {
    dataset: grp[["test", "elapsed_s"]].to_dict("records")
    for dataset, grp in _baselines.groupby("dataset", sort=False)
}


def make_plot(filename="results.png"):
    import numpy as np

    df = _baselines[_baselines["test"] != "BigQuery (Slot time consumed)"].copy()
    datasets = df["dataset"].unique()
    colors = ["steelblue", "coral"]

    # Use the union of tests, ordered by the first dataset's elapsed_s
    order = (
        df[df["dataset"] == datasets[0]]
        .sort_values("elapsed_s")
        .set_index("test")["elapsed_s"]
    )
    all_tests = list(order.index)
    # Append any tests that only appear in other datasets
    for t in df["test"].unique():
        if t not in all_tests:
            all_tests.append(t)

    # Filter to tests that have at least one non-zero value
    all_tests = [t for t in all_tests if df[df["test"] == t]["elapsed_s"].max() > 0]

    n_tests = len(all_tests)
    n_ds = len(datasets)
    bar_h = 0.8 / n_ds
    y = np.arange(n_tests)

    fig, ax = plt.subplots(figsize=(12, max(6, n_tests * 0.5)))
    for i, (dataset, color) in enumerate(zip(datasets, colors)):
        vals = [
            df.loc[(df["dataset"] == dataset) & (df["test"] == t), "elapsed_s"].iloc[0]
            if len(df.loc[(df["dataset"] == dataset) & (df["test"] == t)]) > 0 else 0
            for t in all_tests
        ]
        bars = ax.barh(y + i * bar_h, vals, height=bar_h, label=dataset, color=color)
        max_val = max(v for v in vals if v > 0) if any(v > 0 for v in vals) else 1
        for bar, val in zip(bars, vals):
            if val > 0:
                label = f"{val:.2f}s" if val < 1 else f"{val:.0f}s"
                ax.text(
                    bar.get_width() + max_val * 0.01,
                    bar.get_y() + bar.get_height() / 2,
                    label,
                    va="center",
                    fontsize=7,
                )

    ax.set_yticks(y + bar_h * (n_ds - 1) / 2)
    ax.set_yticklabels(all_tests)
    ax.set_xlabel("Time (seconds)")
    ax.set_title("KNN benchmark — time by method (lower is better)")
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x:.0f}s"))
    ax.legend()
    max_overall = df["elapsed_s"].max()
    ax.set_xlim(0, max_overall * 1.18)
    plt.tight_layout()
    plt.savefig(filename, dpi=150)
    plt.show()
    print(f"Saved plot: {filename}")


# %%
# Generate per-scenario DataFrames for the README table
all_dfs = {}
for scenario in SCENARIOS:
    sname = scenario["name"]
    all_dfs[sname] = _baselines[_baselines["dataset"] == sname][["test", "elapsed_s"]].reset_index(drop=True)

make_plot("results.png")


# %%
# Write combined markdown table into README.md between markers
import re

marker_start = "<!-- RESULTS_START -->"
marker_end = "<!-- RESULTS_END -->"
sections = []
for sname, df in all_dfs.items():
    df_md = df.copy()
    df_md["elapsed_s"] = df_md["elapsed_s"].apply(
        lambda s: f"{s:.2f}s" if s < 1 else f"{s:.0f}s"
    )
    sections.append(f"## Results — {sname}\n\n{df_md.to_markdown(index=False)}")

new_section = f"{marker_start}\n" + "\n\n".join(sections) + f"\n{marker_end}"
readme = open("README.md").read()
if marker_start in readme:
    readme = re.sub(
        f"{marker_start}.*?{marker_end}", new_section, readme, flags=re.DOTALL
    )
else:
    readme = readme.rstrip() + "\n\n" + new_section + "\n"
open("README.md", "w").write(readme)

# %%

