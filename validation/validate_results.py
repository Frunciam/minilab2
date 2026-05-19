"""Validation script to verify MapReduce outputs against direct Python checks."""
import sys
from collections import Counter
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils.s3_reader import read_csv_from_s3


def read_output_counts(path):
    """Read output lines formatted as '<key> <count>'."""
    counts = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            key, count = line.rsplit(" ", 1)
            counts[key] = int(count)
    return counts


def read_output_lines(path):
    """Read non-empty output lines."""
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def print_comparison(title, expected, actual):
    print(f"\n{title}")
    print("Key | MapReduce result | Python check | Match")
    print("--- | ---: | ---: | ---")
    for key in sorted(expected):
        actual_count = actual.get(key)
        match = "Yes" if actual_count == expected[key] else "No"
        print(f"{key} | {actual_count} | {expected[key]} | {match}")


def print_ordered_comparison(title, expected, actual):
    print(f"\n{title}")
    print("Rank | Output result | Python check | Match")
    print("---: | --- | --- | ---")
    max_len = max(len(expected), len(actual))
    for index in range(max_len):
        expected_line = expected[index] if index < len(expected) else "<missing>"
        actual_line = actual[index] if index < len(actual) else "<missing>"
        match = "Yes" if actual_line == expected_line else "No"
        print(f"{index + 1} | {actual_line} | {expected_line} | {match}")


def get_expected_slow_endpoint_lines(data):
    slow_counts = Counter(
        f"{row['service_name']},{row['endpoint']}"
        for row in data
        if int(row["response_time_ms"]) > 800
    )
    return [f"{key} {count}" for key, count in slow_counts.most_common(10)]


def get_expected_degraded_lines(data):
    stats = defaultdict(lambda: {
        "total": 0,
        "slow": 0,
        "server_error": 0,
        "timeout": 0,
    })

    for row in data:
        service = row["service_name"]
        stats[service]["total"] += 1
        if int(row["response_time_ms"]) > 800:
            stats[service]["slow"] += 1
        if int(row["status_code"]) >= 500:
            stats[service]["server_error"] += 1
        if row.get("error_type", "") == "Timeout":
            stats[service]["timeout"] += 1

    degraded = []
    for service, service_stats in stats.items():
        total = service_stats["total"]
        slow_rate = service_stats["slow"] / total
        error_rate = service_stats["server_error"] / total

        if slow_rate > 0.2:
            degraded.append(f"{service}, high slow request rate")
        elif error_rate > 0.1:
            degraded.append(f"{service}, high server error rate")
        elif service_stats["timeout"] >= 5:
            degraded.append(f"{service}, repeated timeout errors")

    return degraded


def validate():
    """Validate MapReduce and Ray outputs with direct Python checks."""
    data = read_csv_from_s3()

    request_counts = Counter(row["service_name"] for row in data)
    server_error_counts = Counter(
        row["service_name"] for row in data if int(row["status_code"]) >= 500
    )

    mapreduce_request_counts = read_output_counts("outputs/output1_request_count.txt")
    mapreduce_error_counts = read_output_counts("outputs/output2_error_count.txt")
    mapreduce_slow_endpoint_lines = read_output_lines("outputs/output3_top10_slow.txt")
    ray_degraded_lines = read_output_lines("outputs/output4_degraded_services.txt")

    print_comparison("Output 1: Request Count by Service", request_counts, mapreduce_request_counts)
    print_comparison("Output 2: Server Error Count by Service", server_error_counts, mapreduce_error_counts)
    print_ordered_comparison(
        "Output 3: Top 10 Slow Endpoints",
        get_expected_slow_endpoint_lines(data),
        mapreduce_slow_endpoint_lines,
    )
    print_ordered_comparison(
        "Output 4: Ray Degraded Services",
        get_expected_degraded_lines(data),
        ray_degraded_lines,
    )


if __name__ == "__main__":
    validate()
