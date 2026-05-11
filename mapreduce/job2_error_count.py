"""MapReduce Job 2: Server error count (500) by service"""
import os
from collections import defaultdict
from utils.s3_reader import read_csv_from_s3


def ensure_output_dir():
    """Ensure output directory exists"""
    os.makedirs('outputs', exist_ok=True)


def main():
    ensure_output_dir()

    data = read_csv_from_s3()

    # Map phase: emit (service_name, 1) only for 500 errors
    mapped = [(row['service_name'], 1) for row in data if row['status_code'] == '500']

    # Reduce phase: sum errors per service
    result = defaultdict(int)
    for service, count in mapped:
        result[service] += count

    # Print to console
    for service, count in sorted(result.items(), key=lambda x: x[1], reverse=True):
        print(f"{service} {count}")

    # Save to file
    with open('outputs/output2_error_count.txt', 'w') as f:
        for service, count in sorted(result.items(), key=lambda x: x[1], reverse=True):
            f.write(f"{service} {count}\n")


if __name__ == "__main__":
    main()