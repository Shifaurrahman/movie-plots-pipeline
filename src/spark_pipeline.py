"""
Spark ETL Pipeline for Wikipedia Movie Plots Dataset
Fixed for Windows: Resolves NativeIO$Windows.access0 error
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, regexp_replace, split, size, floor
)
from datetime import datetime
import json
from pathlib import Path


class SparkMoviePlotsPipeline:
    def __init__(self, input_file: str, output_dir: str = "output_spark"):
        self.input_file = input_file
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        self.spark = (
            SparkSession.builder
            .appName("MoviePlotsSparkETL")
            .master("local[*]")
            # Windows fixes
            .config("spark.hadoop.io.native.lib.available", "false")
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
            .config("spark.hadoop.hadoop.io.nativeio.access.check.enabled", "false")
            .config("spark.sql.sources.commitProtocolClass",
                    "org.apache.spark.internal.io.HadoopMapReduceCommitProtocol")
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate()
        )

        self.processed_df = None

    def ingest(self, row_limit: int = None):
        print(f"📥 Ingesting from {self.input_file}")
        df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(self.input_file)
        if row_limit:
            df = df.limit(row_limit)
        print(f"✅ Loaded {df.count()} rows")
        return df

    def clean_and_transform(self, df):
        print("\n🧹 Cleaning & transforming")

        # Standardize column names
        for c in df.columns:
            new_name = c.strip().lower().replace(" ", "_").replace("-", "_")
            df = df.withColumnRenamed(c, new_name)

        # Core cleaning
        df = df.filter(col("plot").isNotNull() & (col("plot") != ""))
        df = df.withColumn("plot_length", size(split(col("plot"), " ")))
        df = df.withColumn("title_clean", regexp_replace(lower(col("title")), r"[^\w\s]", ""))
        df = df.filter(col("plot_length") >= 50)
        df = df.withColumn("decade", (floor(col("release_year") / 10) * 10).cast("int"))

        self.processed_df = df.cache()
        print(f"✅ Final cleaned dataset: {self.processed_df.count()} rows")
        return self.processed_df

    def validate(self, df):
        print("\n✓ Validation checks")
        checks = {
            "no_null_title": df.filter(col("title").isNull()).count() == 0,
            "no_null_plot": df.filter(col("plot").isNull()).count() == 0,
            "plot_length_positive": df.filter(col("plot_length") <= 0).count() == 0,
            "min_row_threshold": df.count() >= 150,
            "has_decades": df.filter(col("decade").isNotNull()).count() > 0,
        }

        results = {
            "timestamp": datetime.now().isoformat(),
            "total_rows": df.count(),
            "checks": checks,
            "all_passed": all(checks.values())
        }

        validation_path = self.output_dir / "validation_results.json"
        with open(validation_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2)

        print(f"📄 Validation saved to {validation_path}")
        return results

    def store(self, df):
        print("\n💾 Writing partitioned Parquet")
        output_path = self.output_dir / "parquet"
        (
            df.write
            .mode("overwrite")
            .partitionBy("decade")
            .option("compression", "snappy")
            .parquet(str(output_path))
        )
        print(f"✅ Parquet saved to {output_path}")

    def search_keyword(self, keyword: str, top_n: int = 20, save_json: bool = True) -> dict:
        """
        Performs case-insensitive keyword search in plot.
        Returns and optionally saves minimal JSON in exactly the format you requested.
        """
        if self.processed_df is None:
            raise ValueError("Run clean_and_transform() first.")

        print(f"\n🔍 Searching for: '{keyword}'")

        lower_keyword = keyword.lower()
        search_df = (
            self.processed_df
            .filter(lower(col("plot")).contains(lower_keyword))
            .orderBy(col("plot_length").desc())  # Longer plots first
            .limit(top_n)
        )

        total_matches = search_df.count()

        # Collect results in required format
        results_list = [
            {
                "title": row["title"],
                "plot_length": row["plot_length"],
                "decade": row["decade"],
                "year": row["release_year"],
                "genre": row["genre"]
            }
            for row in search_df.select(
                "title", "plot_length", "decade", "release_year", "genre"
            ).collect()
        ]

        output = {
            "keyword": keyword,
            "total_matches": total_matches,
            "results": results_list
        }

        if save_json:
            json_path = self.output_dir / f"search_{keyword.lower()}.json"
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(output, f, indent=2, ensure_ascii=False)
            print(f"💾 Results saved to {json_path}")

        return output

    def run_full_pipeline(self):
        print("🚀 Running Full Pipeline\n" + "=" * 50)
        df = self.ingest()  # Full dataset
        df = self.clean_and_transform(df)
        self.validate(df)
        self.store(df)
        print("\n🎉 Pipeline completed!")


if __name__ == "__main__":
    pipeline = SparkMoviePlotsPipeline(
        input_file="wiki_movie_plots_deduped.csv"
    )

    # Uncomment to run full pipeline (recommended once)
    # pipeline.run_full_pipeline()

    # Quick test with limited rows
    df = pipeline.ingest(row_limit=2000)  # Adjust as needed
    df = pipeline.clean_and_transform(df)
    pipeline.validate(df)
    pipeline.store(df)

    # Example searches - produces exactly your desired JSON
    pipeline.search_keyword("telephone", top_n=20)
    pipeline.search_keyword("dream", top_n=20)