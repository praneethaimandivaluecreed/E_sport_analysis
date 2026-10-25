
from src.ingest import create_spark_session, load_all_datasets
from src.transform import transform_all_datasets
from src.clean import clean_all_datasets


def main():
    spark = create_spark_session()

    try:
        raw_datasets = load_all_datasets(spark)
        transformed_datasets = transform_all_datasets(raw_datasets)

        result = clean_all_datasets(transformed_datasets)

        print("\nCLEANING SUMMARY")
        print("=" * 60)

        for name, df in result["cleaned"].items():
            print(f"{name}: {df.count()} cleaned rows")

        print("\nQUARANTINE SUMMARY")
        print("=" * 60)

        for name, df in result["quarantined"].items():
            print(f"\n{name}: {df.count()} quarantined rows")

            if "quarantine_reason" in df.columns:
                (
                    df.groupBy("quarantine_reason")
                    .count()
                    .show(truncate=False)
                )

        print("\nInspecting quarantined records:")

        for name, df in result["quarantined"].items():
            print(f"\n--- {name} ---")
            df.show(5, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
