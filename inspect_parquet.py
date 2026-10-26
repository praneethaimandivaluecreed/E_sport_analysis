from pathlib import Path
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = PROJECT_ROOT / "outputs"

# Read a Parquet dataset directly from its folder.
def inspect_parquet(folder_path):
    folder_path = Path(folder_path)

    # Find the actual Parquet data files, ignoring CRC files.
    parquet_files = list(folder_path.glob("*.parquet"))

    if not parquet_files:
        # Spark commonly names its data files "part-...."
        parquet_files = [
            path for path in folder_path.glob("part-*")
            if path.is_file() and not path.name.endswith(".crc")
        ]

    if not parquet_files:
        print(f"\nNo Parquet data files found in: {folder_path}")
        return

    table = pq.read_table(parquet_files)

    print(f"\n{'=' * 65}")
    print(f"DATASET: {folder_path.relative_to(OUTPUT_DIR)}")
    print(f"ROWS: {table.num_rows}")
    print(f"COLUMNS: {table.num_columns}")
    print("\nSCHEMA:")
    print(table.schema)
    print("\nFIRST 10 ROWS:")
    print(table.slice(0, 10).to_pandas().to_string(index=False))


# Begin with the key analytical outputs.
for name in [
    "team_performance",
    "player_performance",
    "team_form_over_time",
]:
    inspect_parquet(OUTPUT_DIR / "analytics" / name)