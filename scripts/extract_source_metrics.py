import csv
import json
import sys
from pathlib import Path

from mlops.metrics import SOURCE_COLUMNS


def extract(source, destination):
    with (
        Path(source).open(encoding="utf-8") as input_file,
        Path(destination).open("w", encoding="utf-8", newline="") as output_file,
    ):
        writer = csv.writer(output_file)
        writer.writerow(SOURCE_COLUMNS)
        for line in input_file:
            if line.startswith("SOURCE_METRIC,"):
                row = next(csv.reader([line.removeprefix("SOURCE_METRIC,")]))
                if len(row) == len(SOURCE_COLUMNS):
                    writer.writerow(row)
                continue
            try:
                entry = json.loads(line)
                sample = entry["source_metrics"]
                writer.writerow([sample[column] for column in SOURCE_COLUMNS])
            except (ValueError, KeyError, TypeError):
                continue


if __name__ == "__main__":
    extract(sys.argv[1], sys.argv[2])
