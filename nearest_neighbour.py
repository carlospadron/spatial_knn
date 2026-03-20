# %% [markdown]
# # imports

# %%
import os
import subprocess
import time

import pandas as pd


def run_script(script_path):
    result = subprocess.run(
        ["uv", "run", "--env-file", ".env", script_path], capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"FAILED\n{result.stderr}")
    else:
        print(f"Elapsed: {result.stdout.strip()}")


def run_script_docker(script_path):
    """Run a Python script inside the sedona Docker container (Spark + Sedona pre-installed)."""
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "sedona", "python3", script_path],
        capture_output=True, text=True
    )
    if result.stdout.strip():
        print(result.stdout.strip())
    if result.stderr.strip():
        print(result.stderr.strip())
    if result.returncode != 0:
        print(f"FAILED (exit code {result.returncode})")


def run_kotlin():
    """Run the Kotlin Maven project inside the kotlin Docker container."""
    start = time.time()
    result = subprocess.run(
        ["docker", "compose", "run", "--rm", "-T", "kotlin", "mvn", "compile", "exec:java"],
        capture_output=True, text=True
    )
    elapsed = time.time() - start
    if result.stdout.strip():
        print(result.stdout.strip())
    if result.stderr.strip():
        print(result.stderr.strip())
    if result.returncode != 0:
        print(f"FAILED (exit code {result.returncode})")
    else:
        print(f"Elapsed: {elapsed:.1f}s")


def run_scala():
    """Run the Scala sbt project inside the scala Docker container."""
    start = time.time()
    result = subprocess.run(
        ["docker", "compose", "run", "--rm", "-T", "scala", "sbt", "run"],
        capture_output=True, text=True
    )
    elapsed = time.time() - start
    if result.stdout.strip():
        print(result.stdout.strip())
    if result.stderr.strip():
        print(result.stderr.strip())
    if result.returncode != 0:
        print(f"FAILED (exit code {result.returncode})")
    else:
        print(f"Elapsed: {elapsed:.1f}s")


def run_rust():
    """Run the Rust project locally using cargo."""
    env_vars = {}
    with open('.env') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, _, val = line.partition('=')
                env_vars[key.strip()] = val.strip()
    env = {**os.environ, **env_vars}
    start = time.time()
    result = subprocess.run(
        ["cargo", "run", "--release"],
        capture_output=True, text=True, env=env, cwd="rust"
    )
    elapsed = time.time() - start
    if result.stdout.strip():
        print(result.stdout.strip())
    if result.stderr.strip():
        print(result.stderr.strip())
    if result.returncode != 0:
        print(f"FAILED (exit code {result.returncode})")
    else:
        print(f"Elapsed: {elapsed:.1f}s")


def run_csharp():
    """Run the C# project locally using dotnet."""
    start = time.time()
    result = subprocess.run(
        ["dotnet", "run", "--project", "csharp/knn_csharp.csproj"],
        capture_output=True, text=True
    )
    elapsed = time.time() - start
    if result.stdout.strip():
        print(result.stdout.strip())
    if result.stderr.strip():
        print(result.stderr.strip())
    if result.returncode != 0:
        print(f"FAILED (exit code {result.returncode})")
    else:
        print(f"Elapsed: {elapsed:.1f}s")


def run_go():
    """Run the Go project locally."""
    env_vars = {}
    with open('.env') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, _, val = line.partition('=')
                env_vars[key.strip()] = val.strip()
    env = {**os.environ, **env_vars}
    start = time.time()
    result = subprocess.run(
        ["go", "run", "."],
        capture_output=True, text=True, env=env, cwd="go"
    )
    elapsed = time.time() - start
    if result.stdout.strip():
        print(result.stdout.strip())
    if result.stderr.strip():
        print(result.stderr.strip())
    if result.returncode != 0:
        print(f"FAILED (exit code {result.returncode})")
    else:
        print(f"Elapsed: {elapsed:.1f}s")


def check(result_csv, ref):
    knn = pd.read_csv(result_csv)
    merged = pd.merge(ref, knn, how='outer', on='origin')
    mismatches = merged[merged['destination_x'] != merged['destination_y']]
    if len(mismatches) == 0:
        print("✓ Results match reference")
    return mismatches


# %% [markdown]
# # SQL nearest neighbour (distinct)

# %% [markdown]
# postgresql

# %%
run_script('sql/sql_distinct/knn.py')
reference = pd.read_csv('sql/sql_distinct/result.csv')
reference


# %% [markdown]
# # SQL nearest neighbour (lateral)

# %%
run_script('sql/sql_lateral/knn.py')
check('sql/sql_lateral/result.csv', reference)


# %% [markdown]
# # Geopandas sjoin_nearest

# %%
run_script('python/geopandas/knn.py')
check('python/geopandas/result.csv', reference)


# %% [markdown]
# # python nearest neighbour - shapely (all vs all)

# %%
run_script('python/shapely_all_vs_all/knn.py')
check('python/shapely_all_vs_all/result.csv', reference)


# %% [markdown]
# # python nearest neighbour - shapely (strtree)

# %%
run_script('python/shapely_strtree/knn.py')
check('python/shapely_strtree/result.csv', reference)


# %% [markdown]
# # Scikit-Learn

# %%
run_script('python/sklearn/knn.py')
check('python/sklearn/result.csv', reference)


# %% [markdown]
# # Apache Sedona

# %% [markdown]
# ## partial sql

# %%
run_script_docker('python/sedona_partial/knn.py')
check('python/sedona_partial/result.csv', reference)


# %% [markdown]
# ## pure sql

# %%
run_script_docker('python/sedona_pure/knn.py')
check('python/sedona_pure/result.csv', reference)


# %% [markdown]
# ## KNN operator

# %%
run_script_docker('python/sedona_knn/knn.py')
check('python/sedona_knn/result.csv', reference)


# %% [markdown]
# # Kotlin all vs all

# %%
run_kotlin()
check('kotlin/kotlin_all_vs_all.csv', reference)


# %% [markdown]
# # Kotlin strtree

# %%
check('kotlin/kotlin_tree.csv', reference)


# %% [markdown]
# # Scala all vs all

# %%
run_scala()
check('scala/scala_all_vs_all.csv', reference)


# %% [markdown]
# # Scala strtree

# %%
check('scala/scala_tree.csv', reference)


# %% [markdown]
# # Rust all vs all

# %%
run_rust()
check('rust/rust_all_vs_all.csv', reference)


# %% [markdown]
# # Rust tree

# %%
check('rust/rust_tree.csv', reference)


# %% [markdown]
# # C# all vs all

# %%
run_csharp()
check('csharp_all_vs_all.csv', reference)


# %% [markdown]
# # C# tree

# %%
check('csharp_tree.csv', reference)


# %% [markdown]
# # Go all vs all

# %%
run_go()
check('go/go_all_vs_all.csv', reference)


# %% [markdown]
# # Go tree

# %%
check('go/go_tree.csv', reference)


# %% [markdown]
# # DuckDB

# %%
run_script('python/duckdb/knn.py')
check('python/duckdb/result.csv', reference)


# %% [markdown]
# # SedonaDB

# %%
run_script('python/sedonadb/knn.py')
check('python/sedonadb/result.csv', reference)

# %% [markdown]
# # BigQuery

# %%
check('big_query_result.csv', reference)


# %% [markdown]
# # Redshift

# %%
check('redshift_result.csv', reference)


# %% [markdown]
# # Athena

# %%
check('athena_results.csv', reference)


# %% [markdown]
# # Snowflake (x-small)

# %%
knn = pd.read_csv('snowflake_result.csv').rename(columns=lambda x: x.lower())
merged = pd.merge(reference, knn, how='outer', on='origin')
merged[merged['destination_x'] != merged['destination_y']]


# %% [markdown]
# # Databricks (Standard_DS3_v2)

# %%
# update filename to match the actual Databricks output CSV
knn = pd.read_csv('part-00000-tid-83222403381116274-4d1c858d-cce3-41a1-a392-7cb7afb59909-633-1-c000.csv')
merged = pd.merge(reference, knn, how='outer', on='origin')
merged[merged['destination_x'] != merged['destination_y']]


# %% [markdown]
# # Results

# %%
results = [
    {'test' : 'SQL distinct', 'time': pd.Timedelta('0 days 00:02:10.043192')},
    {'test' : 'SQL lateral', 'time': pd.Timedelta('0 days 00:01:02.638035')},
    {'test' : 'Geopandas sjoin_nearest', 'time': pd.Timedelta('0 days 00:00:01.791905')},
    {'test' : 'Shapely all vs all', 'time': pd.Timedelta('0 days 00:02:06.666341')},
    {'test' : 'Shapely strtree', 'time': pd.Timedelta('0 days 00:00:01.319851')},
    {'test' : 'Scikit-Learn nearest neighbour', 'time': pd.Timedelta('0 days 00:00:25.355890')},
    {'test' : 'Apache Sedona partial sql', 'time': pd.Timedelta('0 days 00:02:32.106516')},
    {'test' : 'Apache Sedona pure sql', 'time': pd.Timedelta('0 days 00:02:37.501045')},
    {'test' : 'Apache Sedona st_knn', 'time': pd.Timedelta('0 days 00:00:09.269729')},
    {'test' : 'kotlin all vs all', 'time': pd.Timedelta('0 days 00:00:32.706000')},
    {'test' : 'kotlin strtree', 'time': pd.Timedelta('0 days 00:00:03.627000')},
    {'test' : 'scala all vs all', 'time': pd.Timedelta('0 days 00:00:27.585000')},
    {'test' : 'scala strtree', 'time': pd.Timedelta('0 days 00:00:04.142000')},
    {'test' : 'rust all vs all', 'time': pd.Timedelta('0 days 00:00:04.044441')},
    {'test' : 'rust strtree', 'time': pd.Timedelta('0 days 00:00:00.346612')},
    {'test' : 'C# all vs all', 'time': pd.Timedelta('0 days 00:00:22.904955')},
    {'test' : 'C# strtree', 'time': pd.Timedelta('0 days 00:00:05.894800')},
    {'test' : 'Go all vs all', 'time': pd.Timedelta('0 days 00:00:00')},  # TODO: update with actual time
    {'test' : 'Go strtree', 'time': pd.Timedelta('0 days 00:00:00')},  # TODO: update with actual time
    {'test' : 'DuckDB', 'time': pd.Timedelta('0 days 00:00:17.465032')},
    {'test' : 'SedonaDB', 'time': pd.Timedelta('0 days 00:00:00.438286')},
    {'test' : 'BigQuery', 'time': pd.Timedelta('0 days 00:00:03')},
    {'test' : 'BigQuery (Slot time consumed)', 'time': pd.Timedelta('0 days 00:05:10')},
    {'test' : 'RedShift', 'time': pd.Timedelta('0 days 00:00:26')},
    {'test' : 'Athena', 'time': pd.Timedelta('0 days 00:01:50')},
    {'test' : 'Snowflake cartesian', 'time': pd.Timedelta('0 days 00:01:15')},
    {'test' : 'Snowflake h3', 'time': pd.Timedelta('0 days 00:00:45')},
    {'test' : 'Databricks pure sql', 'time': pd.Timedelta('0 days 00:01:00')}
]


# %%
pd.DataFrame(results).sort_values('time').drop_duplicates().reset_index(drop = True)


