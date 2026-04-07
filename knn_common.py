import argparse
import os

from sqlalchemy import create_engine


def get_parser():
    """Return an ArgumentParser with the common --uprn-table and --codepoint-table flags."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--uprn-table", default="os.open_uprn_white_horse")
    parser.add_argument("--codepoint-table", default="os.code_point_open_white_horse")
    return parser


def get_db_params():
    """Return a dict of database connection parameters from environment variables."""
    return {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432"),
        "database": os.getenv("DB_NAME", "gis"),
    }


def get_engine():
    """Build a SQLAlchemy engine from DB_* environment variables."""
    p = get_db_params()
    return create_engine(
        f"postgresql://{p['user']}:{p['password']}@{p['host']}:{p['port']}/{p['database']}"
    )


def get_sedona_context():
    """Build a Sedona Spark context with standard configuration."""
    from sedona.spark import SedonaContext

    config = (
        SedonaContext.builder()
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.7.5,"
            "org.apache.sedona:sedona-spark-3.5_2.12:1.7.0,"
            "org.datasyslab:geotools-wrapper:1.7.0-28.5",
        )
        .config(
            "spark.jars.repositories",
            "https://artifacts.unidata.ucar.edu/repository/unidata-all",
        )
        .config("spark.executor.memory", "12g")
        .config("spark.driver.memory", "12g")
        .getOrCreate()
    )
    return SedonaContext.create(config)
