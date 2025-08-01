import tempfile
from typing import Callable
from pyspark.sql import SparkSession


def spark_session():
    temp_dir = tempfile.mkdtemp(prefix="spark-warehouse-")
    # All these packages are used by Spark and must be flagged as open for Java versions > 11
    packages = [
        "java.base/sun.nio.ch=ALL-UNNAMED",
        "java.base/java.lang=ALL-UNNAMED",
        "java.base/java.util=ALL-UNNAMED",
    ]
    add_open_cmd = map(
        lambda package: f"--add_opens={package}", packages
    )  # the command line option: `--add-opens=...`
    session = (
        SparkSession.builder.appName("usdataexchange-test-application")  # type: ignore
        .master("local[1]")
        .config("spark.sql.warehouse.dir", temp_dir)
        .config(
            "spark.driver.extraJavaOptions", " ".join(add_open_cmd)
        )  # --add-opens=... --add-opens=... --add-opens=... etc.
        .getOrCreate()
    )
    return session


def finalize(session: SparkSession) -> Callable[[], object]:
    def fin():
        # Drop every existing table
        for table in session.catalog.listTables():
            session.catalog.dropTempView(table.name)
        session.stop()
        return None

    return fin


if __name__ == '__main__':
    session = spark_session()
    df = session.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
    print(df.collect())
