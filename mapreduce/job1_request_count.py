"""MapReduce Job 1: Request count by service"""
import os
from collections import defaultdict
from utils.s3_reader import read_csv_from_s3


def ensure_output_dir():
    """Ensure output directory exists"""
    os.makedirs('outputs', exist_ok=True)


def main():
    ensure_output_dir()

    data = read_csv_from_s3()

    # Map phase: emit (service_name, 1)
    mapped = [(row['service_name'], 1) for row in data]

    # Reduce phase: sum counts per service
    result = defaultdict(int)
    for service, count in mapped:
        result[service] += count

    # Sort by count descending
    sorted_result = sorted(result.items(), key=lambda x: x[1], reverse=True)

    # Print to console
    for service, count in sorted_result:
        print(f"{service} {count}")

    # Save to file
    with open('outputs/output1_request_count.txt', 'w') as f:
        for service, count in sorted_result:
            f.write(f"{service} {count}\n")


if __name__ == "__main__":
    main()