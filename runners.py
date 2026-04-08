import csv
import os
import re
import subprocess
import time
import uuid
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import pandas as pd


_DEFAULT_TIMEOUT = 3600  # 1 hr hard ceiling so nothing hangs forever


def _docker_compose_run(service, container_name, cmd_args, timeout=None, extra_docker_args=None):
    """Run a one-shot docker compose service by name; kill it cleanly on timeout.

    Uses --no-deps so that docker compose run does not start/stop the postgres
    dependency — postgres must already be running (docker compose up -d postgres).
    """
    effective_timeout = timeout if timeout is not None else _DEFAULT_TIMEOUT
    cmd = [
        "docker", "compose", "run", "--rm", "-T", "--no-deps",
        "--name", container_name,
    ]
    if extra_docker_args:
        cmd += extra_docker_args
    cmd += [service] + cmd_args
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        stdout, stderr = proc.communicate(timeout=effective_timeout)
        return proc.returncode, stdout, stderr
    except subprocess.TimeoutExpired:
        # Kill the container first, then the CLI process.
        subprocess.run(["docker", "kill", container_name], capture_output=True)
        subprocess.run(["docker", "rm", "-f", container_name], capture_output=True)
        proc.kill()
        try:
            proc.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            proc.terminate()
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


def _env_args(uprn_table, codepoint_table):
    """Build docker -e flags for table overrides."""
    if not uprn_table:
        return []
    return ["-e", f"UPRN_TABLE={uprn_table}", "-e", f"CODEPOINT_TABLE={codepoint_table}"]


_COMPILED_LANGS = {
    "kotlin": {
        "service": "kotlin",
        "cmd": lambda mode: ["mvn", "compile", "exec:java", f"-Dexec.args={mode}"],
        "timings": "kotlin/timings.csv",
        "brute_csv": "kotlin/kotlin_all_vs_all.csv",
        "tree_csv": "kotlin/kotlin_tree.csv",
        "label": "Kotlin",
    },
    "scala": {
        "service": "scala",
        "cmd": lambda mode: ["sbt", f"run {mode}"],
        "timings": "scala/timings.csv",
        "brute_csv": "scala/scala_all_vs_all.csv",
        "tree_csv": "scala/scala_tree.csv",
        "label": "Scala",
    },
    "rust": {
        "service": "rust",
        "cmd": lambda mode: ["cargo", "run", "--release", "--", mode],
        "timings": "rust/timings.csv",
        "brute_csv": "rust/rust_all_vs_all.csv",
        "tree_csv": "rust/rust_tree.csv",
        "label": "Rust",
    },
    "csharp": {
        "service": "csharp",
        "cmd": lambda mode: ["dotnet", "run", "--project", "csharp/knn_csharp.csproj", "--", mode],
        "timings": "csharp/timings.csv",
        "brute_csv": "csharp_all_vs_all.csv",
        "tree_csv": "csharp_tree.csv",
        "label": "C#",
    },
    "go": {
        "service": "go",
        "cmd": lambda mode: ["go", "run", ".", mode],
        "timings": "go/timings.csv",
        "brute_csv": "go/go_all_vs_all.csv",
        "tree_csv": "go/go_tree.csv",
        "label": "Go",
    },
}


def _run_compiled(lang, timeout=None, uprn_table=None, codepoint_table=None, mode="both"):
    """Run a compiled-language KNN implementation inside Docker."""
    cfg = _COMPILED_LANGS[lang]
    name = f"knn_{lang}_{uuid.uuid4().hex[:8]}"
    returncode, stdout, stderr = _docker_compose_run(
        cfg["service"], name, cfg["cmd"](mode),
        timeout=timeout, extra_docker_args=_env_args(uprn_table, codepoint_table),
    )
    if returncode is None:
        print(f"TIMEOUT after {timeout}s")
        return None
    if returncode != 0:
        print(f"FAILED (exit code {returncode})\n{stderr}")
        return None
    return pd.read_csv(cfg["timings"]).set_index("test")["elapsed_s"].to_dict()


def record_timing(dataset, test, elapsed_s):
    """Append one timing row to baselines.csv and print it."""
    print(f"  {test}: {elapsed_s:.3f}s")
    header = not os.path.exists("baselines.csv")
    with open("baselines.csv", "a", newline="") as f:
        writer = csv.writer(f)
        if header:
            writer.writerow(["dataset", "test", "elapsed_s"])
        writer.writerow([dataset, test, elapsed_s])


def check(result_csv, ref, lowercase_columns=False):
    knn = pd.read_csv(result_csv)
    if lowercase_columns:
        knn = knn.rename(columns=str.lower)
    merged = pd.merge(ref, knn, how="outer", on="origin")
    mismatches = merged[merged["destination_x"] != merged["destination_y"]]
    if len(mismatches) == 0:
        print("✓ Results match reference")
    else:
        ties = mismatches[
            (mismatches["distance_x"] - mismatches["distance_y"]).abs() < 1e-6
        ]
        real = mismatches.drop(ties.index)
        if len(real) == 0:
            print(f"✓ Results match reference ({len(ties)} tie-breaking differences)")
        else:
            print(f"✗ {len(real)} mismatches found in {result_csv} ({len(ties)} tie-breaking)")
            print(real.head(10).to_string())
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
        "timeout": 3600,
    },
]


SOLUTION_NAMES = [
    "sql_distinct", "sql_lateral", "geopandas", "shapely_all_vs_all", "shapely_strtree",
    "sklearn", "sedona_partial", "sedona_pure", "sedona_knn",
    "kotlin_brute", "kotlin_tree", "scala_brute", "scala_tree",
    "rust_brute", "rust_tree", "csharp_brute", "csharp_tree",
    "go_brute", "go_tree", "duckdb", "sedonadb",
]


def _ensure_postgres():
    """Make sure the postgres service is running (--no-deps needs it pre-started)."""
    subprocess.run(
        ["docker", "compose", "up", "-d", "postgres"],
        capture_output=True,
    )


def run_scenario(scenario, solutions=None, skip_reference=False):
    _ensure_postgres()
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
        if skip_reference and os.path.exists(reference_csv):
            print(f"--- Skipping reference generation ({reference_csv} exists) ---")
        else:
            print("--- Rust tree (generating reference) ---")
            results = _run_compiled("rust", timeout=timeout, uprn_table=uprn_table, codepoint_table=codepoint_table, mode="tree")
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

    for lang, cfg in _COMPILED_LANGS.items():
        for mode, csv_key in [("brute", "brute_csv"), ("tree", "tree_csv")]:
            solution_name = f"{lang}_{mode}"
            if not _run(solution_name):
                continue
            # Rust is pre-run as reference for the large scenario
            if lang == "rust" and reference_csv:
                check(cfg[csv_key], reference)
                continue
            print(f"--- {cfg['label']} {mode} ---")
            results = _run_compiled(
                lang, timeout=timeout, uprn_table=uprn_table,
                codepoint_table=codepoint_table, mode=mode,
            )
            if results is not None:
                for k, v in results.items():
                    record_timing(dataset, k, v)
                check(cfg[csv_key], reference)

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
    plt.close()
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
    with open("README.md") as f:
        readme = f.read()
    if marker_start in readme:
        readme = re.sub(
            f"{marker_start}.*?{marker_end}", new_section, readme, flags=re.DOTALL
        )
    else:
        readme = readme.rstrip() + "\n\n" + new_section + "\n"
    with open("README.md", "w") as f:
        f.write(readme)
