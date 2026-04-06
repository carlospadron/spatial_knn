import os
import re
import subprocess
import time
import uuid
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import pandas as pd


_DEFAULT_TIMEOUT = 1800  # 30 min hard ceiling so nothing hangs forever


def _docker_compose_run(service, container_name, cmd_args, timeout=None):
    """Run a one-shot docker compose service by name; kill it cleanly on timeout."""
    effective_timeout = timeout if timeout is not None else _DEFAULT_TIMEOUT
    cmd = [
        "docker", "compose", "run", "--rm", "-T",
        "--name", container_name,
        service,
    ] + cmd_args
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        stdout, stderr = proc.communicate(timeout=effective_timeout)
        return proc.returncode, stdout, stderr
    except subprocess.TimeoutExpired:
        kill_result = subprocess.run(["docker", "kill", container_name], capture_output=True)
        if kill_result.returncode != 0:
            subprocess.run(["docker", "stop", "--time", "5", container_name], capture_output=True)
        proc.kill()
        proc.communicate()
        return None, "", ""


def run_script(
    script_path,
    uprn_table=None,
    codepoint_table=None,
    timeout=None,
    statement_timeout_ms=None,
):
    args = ["uv", "run", script_path]
    if uprn_table:
        args += ["--uprn-table", uprn_table]
    if codepoint_table:
        args += ["--codepoint-table", codepoint_table]
    if statement_timeout_ms:
        args += ["--statement-timeout", str(statement_timeout_ms)]
    name = f"knn_{uuid.uuid4().hex[:8]}"
    returncode, stdout, stderr = _docker_compose_run("python", name, args, timeout=timeout)
    if returncode is None:
        print(f"TIMEOUT after {timeout}s")
        return None
    if returncode != 0:
        print(f"FAILED\n{stderr}")
        return None
    elapsed = pd.Timedelta(stdout.strip()).total_seconds()
    print(f"Elapsed: {stdout.strip()}")
    return elapsed


def run_script_docker(script_path, uprn_table=None, codepoint_table=None, timeout=None):
    """Run a Python script inside a one-shot sedona Docker container (Spark + Sedona pre-installed)."""
    extra_args = []
    if uprn_table:
        extra_args += ["--uprn-table", uprn_table]
    if codepoint_table:
        extra_args += ["--codepoint-table", codepoint_table]
    name = f"knn_sedona_{uuid.uuid4().hex[:8]}"
    start = time.time()
    returncode, stdout, stderr = _docker_compose_run(
        "sedona", name, ["python3", script_path] + extra_args, timeout=timeout
    )
    if returncode is None:
        print(f"TIMEOUT after {timeout}s")
        return None
    elapsed = time.time() - start
    if stdout.strip():
        print(stdout.strip())
    if stderr.strip():
        print(stderr.strip())
    if returncode != 0:
        print(f"FAILED (exit code {returncode})")
        return None
    return elapsed


def run_kotlin(timeout=None):
    """Run the Kotlin KNN implementation inside Docker and return a dict of test name to elapsed seconds."""
    try:
        result = subprocess.run(
            ["docker", "compose", "run", "--rm", "-T", "kotlin", "mvn", "compile", "exec:java"],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        print(f"TIMEOUT after {timeout}s")
        return None
    if result.returncode != 0:
        print(f"FAILED (exit code {result.returncode})\n{result.stderr}")
        return None
    return pd.read_csv("kotlin/timings.csv").set_index("test")["elapsed_s"].to_dict()


def run_scala(timeout=None):
    """Run the Scala sbt project inside Docker and return a dict of test name to elapsed seconds."""
    try:
        result = subprocess.run(
            ["docker", "compose", "run", "--rm", "-T", "scala", "sbt", "run"],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        print(f"TIMEOUT after {timeout}s")
        return None
    if result.returncode != 0:
        print(f"FAILED (exit code {result.returncode})\n{result.stderr}")
        return None
    return pd.read_csv("scala/timings.csv").set_index("test")["elapsed_s"].to_dict()


def run_rust(timeout=None):
    """Run the Rust project inside Docker and return a dict of test name to elapsed seconds."""
    try:
        result = subprocess.run(
            ["docker", "compose", "run", "--rm", "-T", "rust", "cargo", "run", "--release"],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        print(f"TIMEOUT after {timeout}s")
        return None
    if result.returncode != 0:
        print(f"FAILED (exit code {result.returncode})\n{result.stderr}")
        return None
    return pd.read_csv("rust/timings.csv").set_index("test")["elapsed_s"].to_dict()


def run_csharp(timeout=None):
    """Run the C# project inside Docker and return a dict of test name to elapsed seconds."""
    try:
        result = subprocess.run(
            ["docker", "compose", "run", "--rm", "-T", "csharp",
             "dotnet", "run", "--project", "csharp/knn_csharp.csproj"],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        print(f"TIMEOUT after {timeout}s")
        return None
    if result.returncode != 0:
        print(f"FAILED (exit code {result.returncode})\n{result.stderr}")
        return None
    return pd.read_csv("csharp/timings.csv").set_index("test")["elapsed_s"].to_dict()


def run_go(timeout=None):
    """Run the Go project inside Docker and return a dict of test name to elapsed seconds."""
    try:
        result = subprocess.run(
            ["docker", "compose", "run", "--rm", "-T", "go", "go", "run", "."],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        print(f"TIMEOUT after {timeout}s")
        return None
    if result.returncode != 0:
        print(f"FAILED (exit code {result.returncode})\n{result.stderr}")
        return None
    return pd.read_csv("go/timings.csv").set_index("test")["elapsed_s"].to_dict()


def record_timing(dataset, test, elapsed_s):
    """Append one timing row to baselines.csv and print it."""
    print(f"  {test}: {elapsed_s:.3f}s")
    header = not os.path.exists("baselines.csv")
    pd.DataFrame([{"dataset": dataset, "test": test, "elapsed_s": elapsed_s}]).to_csv(
        "baselines.csv", mode="a", index=False, header=header
    )


def check(result_csv, ref):
    knn = pd.read_csv(result_csv)
    merged = pd.merge(ref, knn, how="outer", on="origin")
    mismatches = merged[merged["destination_x"] != merged["destination_y"]]
    if len(mismatches) == 0:
        print("✓ Results match reference")
    else:
        print(f"✗ {len(mismatches)} mismatches found in {result_csv}")
        print(mismatches.head(10).to_string())
    return mismatches


SCENARIOS = [
    {
        "name": "White Horse (small)",
        "uprn_table": "os.open_uprn_white_horse",
        "codepoint_table": "os.code_point_open_white_horse",
        "plot_file": "results_white_horse.png",
    },
    {
        "name": "Full GB (large)",
        "uprn_table": "os.os_open_uprn",
        "codepoint_table": "os.codepoint_polygons",
        "plot_file": "results_full_gb.png",
        "reference_csv": "rust/rust_tree.csv",
        "timeout": 1800,
    },
]


SOLUTION_NAMES = [
    "sql_distinct", "sql_lateral", "geopandas", "shapely_all_vs_all", "shapely_strtree",
    "sklearn", "sedona_partial", "sedona_pure", "sedona_knn",
    "kotlin", "scala", "rust", "csharp", "go", "duckdb", "sedonadb",
]


def run_scenario(scenario, solutions=None):
    uprn_table = scenario["uprn_table"]
    codepoint_table = scenario["codepoint_table"]
    print(f"\n{'=' * 60}")
    print(f"Running scenario: {scenario['name']}")
    print(f"  UPRN table:      {uprn_table}")
    print(f"  Codepoint table: {codepoint_table}")
    print(f"{'=' * 60}\n")

    dataset = scenario["name"]
    timeout = scenario.get("timeout")
    reference_csv = scenario.get("reference_csv")
    # Pass statement_timeout slightly under the wall-clock timeout so Postgres
    # cancels the query itself before the subprocess timeout fires.
    sql_timeout_ms = int(timeout * 950) if timeout else None
    _run = lambda name: solutions is None or name in solutions

    if reference_csv:
        print("--- Rust (generating reference) ---")
        results = run_rust(timeout=timeout)
        if results:
            for k, v in results.items():
                record_timing(dataset, k, v)
        reference = pd.read_csv(reference_csv)
    else:
        print("--- SQL distinct ---")
        elapsed = run_script(
            "sql/sql_distinct/knn.py",
            uprn_table,
            codepoint_table,
            timeout=timeout,
            statement_timeout_ms=sql_timeout_ms,
        )
        if elapsed is not None:
            record_timing(dataset, "SQL distinct", elapsed)
        reference = pd.read_csv("sql/sql_distinct/result.csv")

    if _run("sql_lateral"):
        print("--- SQL lateral ---")
        elapsed = run_script(
            "sql/sql_lateral/knn.py",
            uprn_table,
            codepoint_table,
            timeout=timeout,
            statement_timeout_ms=sql_timeout_ms,
        )
        if elapsed is not None:
            record_timing(dataset, "SQL lateral", elapsed)
            check("sql/sql_lateral/result.csv", reference)

    if _run("geopandas"):
        print("--- GeoPandas sjoin_nearest ---")
        elapsed = run_script("python/geopandas/knn.py", uprn_table, codepoint_table, timeout=timeout)
        if elapsed is not None:
            record_timing(dataset, "Geopandas sjoin_nearest", elapsed)
            check("python/geopandas/result.csv", reference)

    if _run("shapely_all_vs_all"):
        print("--- Shapely all vs all ---")
        elapsed = run_script(
            "python/shapely_all_vs_all/knn.py",
            uprn_table,
            codepoint_table,
            timeout=timeout,
        )
        if elapsed is not None:
            record_timing(dataset, "Shapely all vs all", elapsed)
            check("python/shapely_all_vs_all/result.csv", reference)

    if _run("shapely_strtree"):
        print("--- Shapely strtree ---")
        elapsed = run_script(
            "python/shapely_strtree/knn.py",
            uprn_table,
            codepoint_table,
            timeout=timeout,
        )
        if elapsed is not None:
            record_timing(dataset, "Shapely strtree", elapsed)
            check("python/shapely_strtree/result.csv", reference)

    if _run("sklearn"):
        print("--- Scikit-Learn ---")
        elapsed = run_script("python/sklearn/knn.py", uprn_table, codepoint_table, timeout=timeout)
        if elapsed is not None:
            record_timing(dataset, "Scikit-Learn nearest neighbour", elapsed)
            check("python/sklearn/result.csv", reference)

    if _run("sedona_partial"):
        print("--- Apache Sedona partial sql ---")
        elapsed = run_script_docker(
            "python/sedona_partial/knn.py",
            uprn_table,
            codepoint_table,
            timeout=timeout,
        )
        if elapsed is not None:
            record_timing(dataset, "Apache Sedona partial sql", elapsed)
            check("python/sedona_partial/result.csv", reference)

    if _run("sedona_pure"):
        print("--- Apache Sedona pure sql ---")
        elapsed = run_script_docker(
            "python/sedona_pure/knn.py",
            uprn_table,
            codepoint_table,
            timeout=timeout,
        )
        if elapsed is not None:
            record_timing(dataset, "Apache Sedona pure sql", elapsed)
            check("python/sedona_pure/result.csv", reference)

    if _run("sedona_knn"):
        print("--- Apache Sedona st_knn ---")
        elapsed = run_script_docker(
            "python/sedona_knn/knn.py", uprn_table, codepoint_table, timeout=timeout
        )
        if elapsed is not None:
            record_timing(dataset, "Apache Sedona st_knn", elapsed)
            check("python/sedona_knn/result.csv", reference)

    if _run("kotlin"):
        print("--- Kotlin ---")
        results = run_kotlin(timeout=timeout)
        if results is not None:
            for k, v in results.items():
                record_timing(dataset, k, v)
            check("kotlin/kotlin_all_vs_all.csv", reference)
            check("kotlin/kotlin_tree.csv", reference)

    if _run("scala"):
        print("--- Scala ---")
        results = run_scala(timeout=timeout)
        if results is not None:
            for k, v in results.items():
                record_timing(dataset, k, v)
            check("scala/scala_all_vs_all.csv", reference)
            check("scala/scala_tree.csv", reference)

    if _run("rust"):
        if not reference_csv:
            print("--- Rust ---")
            results = run_rust(timeout=timeout)
            if results is not None:
                for k, v in results.items():
                    record_timing(dataset, k, v)
                check("rust/rust_all_vs_all.csv", reference)
                check("rust/rust_tree.csv", reference)
        else:
            check("rust/rust_all_vs_all.csv", reference)
            check("rust/rust_tree.csv", reference)

    if _run("csharp"):
        print("--- C# ---")
        results = run_csharp(timeout=timeout)
        if results is not None:
            for k, v in results.items():
                record_timing(dataset, k, v)
            check("csharp_all_vs_all.csv", reference)
            check("csharp_tree.csv", reference)

    if _run("go"):
        print("--- Go ---")
        results = run_go(timeout=timeout)
        if results is not None:
            for k, v in results.items():
                record_timing(dataset, k, v)
            check("go/go_all_vs_all.csv", reference)
            check("go/go_tree.csv", reference)

    if _run("duckdb"):
        print("--- DuckDB ---")
        elapsed = run_script("python/duckdb/knn.py", uprn_table, codepoint_table, timeout=timeout)
        if elapsed is not None:
            record_timing(dataset, "DuckDB", elapsed)
            check("python/duckdb/result.csv", reference)

    if _run("sedonadb"):
        print("--- SedonaDB ---")
        elapsed = run_script("python/sedonadb/knn.py", uprn_table, codepoint_table, timeout=timeout)
        if elapsed is not None:
            record_timing(dataset, "SedonaDB", elapsed)
            check("python/sedonadb/result.csv", reference)

    return reference


def make_plot(baselines, filename="results.png"):
    df = baselines[baselines["test"] != "BigQuery (Slot time consumed)"].copy()
    datasets = df["dataset"].unique()
    colors = ["steelblue", "coral"]

    fig, axes = plt.subplots(1, len(datasets), figsize=(14 * len(datasets) // 2, 10), squeeze=False)

    for ax, dataset, color in zip(axes[0], datasets, colors):
        sub = df[df["dataset"] == dataset].copy()
        sub = sub[sub["elapsed_s"] > 0].sort_values("elapsed_s", ascending=True)
        tests = list(sub["test"])
        vals = list(sub["elapsed_s"])
        y = np.arange(len(tests))

        bars = ax.barh(y, vals, height=0.6, color=color)
        max_val = max(vals) if vals else 1
        for bar, val in zip(bars, vals):
            label = f"{val:.2f}s" if val < 1 else f"{val:.0f}s"
            ax.text(
                bar.get_width() + max_val * 0.01,
                bar.get_y() + bar.get_height() / 2,
                label,
                va="center",
                fontsize=8,
            )

        ax.set_yticks(y)
        ax.set_yticklabels(tests)
        ax.set_xlabel("Time (seconds)")
        ax.set_title(dataset)
        ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x:.0f}s"))
        ax.set_xlim(0, max_val * 1.2)

    fig.suptitle("KNN benchmark — time by method (lower is better)", fontsize=13)
    plt.tight_layout()
    plt.savefig(filename, dpi=150)
    plt.show()
    print(f"Saved plot: {filename}")


def update_readme(baselines):
    marker_start = "<!-- RESULTS_START -->"
    marker_end = "<!-- RESULTS_END -->"
    sections = []
    for dataset, grp in baselines.groupby("dataset", sort=False):
        df_md = grp[["test", "elapsed_s"]].copy()
        df_md["elapsed_s"] = df_md["elapsed_s"].apply(
            lambda s: f"{s:.2f}s" if s < 1 else f"{s:.0f}s"
        )
        sections.append(f"## Results — {dataset}\n\n{df_md.to_markdown(index=False)}")

    new_section = f"{marker_start}\n" + "\n\n".join(sections) + f"\n{marker_end}"
    readme = open("README.md").read()
    if marker_start in readme:
        readme = re.sub(
            f"{marker_start}.*?{marker_end}", new_section, readme, flags=re.DOTALL
        )
    else:
        readme = readme.rstrip() + "\n\n" + new_section + "\n"
    open("README.md", "w").write(readme)
