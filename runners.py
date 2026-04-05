import os
import subprocess
import time
import uuid
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import pandas as pd


def _docker_compose_run(service, container_name, cmd_args, timeout=None):
    """Run a one-shot docker compose service by name; kill it cleanly on timeout."""
    cmd = [
        "docker", "compose", "run", "--rm", "-T",
        "--name", container_name,
        service,
    ] + cmd_args
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        stdout, stderr = proc.communicate(timeout=timeout)
        return proc.returncode, stdout, stderr
    except subprocess.TimeoutExpired:
        subprocess.run(["docker", "kill", container_name], capture_output=True)
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
    """Run the Rust project locally using cargo and return a dict of test name to elapsed seconds."""
    env_vars = {}
    with open(".env") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                env_vars[key.strip()] = val.strip()
    env = {**os.environ, **env_vars}
    try:
        result = subprocess.run(
            ["cargo", "run", "--release"],
            capture_output=True,
            text=True,
            env=env,
            cwd="rust",
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
    """Run the C# project locally using dotnet and return a dict of test name to elapsed seconds."""
    
    try:
        result = subprocess.run(
            ["dotnet", "run", "--project", "csharp/knn_csharp.csproj"],
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
    """Run the Go project locally and return a dict of test name to elapsed seconds."""
    with open(".env") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                env_vars[key.strip()] = val.strip()
    env = {**os.environ, **env_vars}
    try:
        result = subprocess.run(
            ["go", "run", "."],
            capture_output=True,
            text=True,
            env=env,
            cwd="go",
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


def run_scenario(scenario):
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

    print("--- GeoPandas sjoin_nearest ---")
    elapsed = run_script("python/geopandas/knn.py", uprn_table, codepoint_table, timeout=timeout)
    if elapsed is not None:
        record_timing(dataset, "Geopandas sjoin_nearest", elapsed)
        check("python/geopandas/result.csv", reference)

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

    print("--- Scikit-Learn ---")
    elapsed = run_script("python/sklearn/knn.py", uprn_table, codepoint_table, timeout=timeout)
    if elapsed is not None:
        record_timing(dataset, "Scikit-Learn nearest neighbour", elapsed)
        check("python/sklearn/result.csv", reference)

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

    print("--- Apache Sedona st_knn ---")
    elapsed = run_script_docker(
        "python/sedona_knn/knn.py", uprn_table, codepoint_table, timeout=timeout
    )
    if elapsed is not None:
        record_timing(dataset, "Apache Sedona st_knn", elapsed)
        check("python/sedona_knn/result.csv", reference)

    print("--- Kotlin ---")
    results = run_kotlin(timeout=timeout)
    if results is not None:
        for k, v in results.items():
            record_timing(dataset, k, v)
        check("kotlin/kotlin_all_vs_all.csv", reference)
        check("kotlin/kotlin_tree.csv", reference)

    print("--- Scala ---")
    results = run_scala(timeout=timeout)
    if results is not None:
        for k, v in results.items():
            record_timing(dataset, k, v)
        check("scala/scala_all_vs_all.csv", reference)
        check("scala/scala_tree.csv", reference)

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

    print("--- C# ---")
    results = run_csharp(timeout=timeout)
    if results is not None:
        for k, v in results.items():
            record_timing(dataset, k, v)
        check("csharp_all_vs_all.csv", reference)
        check("csharp_tree.csv", reference)

    print("--- Go ---")
    results = run_go(timeout=timeout)
    if results is not None:
        for k, v in results.items():
            record_timing(dataset, k, v)
        check("go/go_all_vs_all.csv", reference)
        check("go/go_tree.csv", reference)

    print("--- DuckDB ---")
    elapsed = run_script("python/duckdb/knn.py", uprn_table, codepoint_table, timeout=timeout)
    if elapsed is not None:
        record_timing(dataset, "DuckDB", elapsed)
        check("python/duckdb/result.csv", reference)

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

    order = (
        df[df["dataset"] == datasets[0]]
        .sort_values("elapsed_s")
        .set_index("test")["elapsed_s"]
    )
    all_tests = list(order.index)
    for t in df["test"].unique():
        if t not in all_tests:
            all_tests.append(t)
    all_tests = [t for t in all_tests if df[df["test"] == t]["elapsed_s"].max() > 0]

    n_tests = len(all_tests)
    n_ds = len(datasets)
    bar_h = 0.8 / n_ds
    y = np.arange(n_tests)

    fig, ax = plt.subplots(figsize=(12, max(6, n_tests * 0.5)))
    for i, (dataset, color) in enumerate(zip(datasets, colors)):
        vals = [
            df.loc[(df["dataset"] == dataset) & (df["test"] == t), "elapsed_s"].iloc[0]
            if len(df.loc[(df["dataset"] == dataset) & (df["test"] == t)]) > 0
            else 0
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
    ax.set_xlim(0, df["elapsed_s"].max() * 1.18)
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
