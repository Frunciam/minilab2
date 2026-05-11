"""MapReduce Job 3: Top 10 slow endpoints (response_time > 800ms)"""
import os
from collections import defaultdict
from utils.s3_reader import read_csv_from_s3


def ensure_output_dir():
    """Ensure output directory exists"""
    os.makedirs('outputs', exist_ok=True)


def main():
    ensure_output_dir()

    data = read_csv_from_s3()

    # Map phase: emit ((service, endpoint), 1) for slow requests
    mapped = []
    for row in data:
        if int(row['response_time_ms']) > 800:
            key = (row['service_name'], row['endpoint'])
            mapped.append((key, 1))

    # Reduce phase: sum slow requests per endpoint
    result = defaultdict(int)
    for key, count in mapped:
        result[key] += count

    # Sort and get top 10
    sorted_result = sorted(result.items(), key=lambda x: x[1], reverse=True)[:10]

    # Print to console
    for (service, endpoint), count in sorted_result:
        print(f"{service},{endpoint} {count}")

    # Save to file
    with open('outputs/output3_top10_slow.txt', 'w') as f:
        for (service, endpoint), count in sorted_result:
            f.write(f"{service},{endpoint} {count}\n")


if __name__ == "__main__":
    main()