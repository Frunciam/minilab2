"""Ray parallel processing: Degraded service detection"""
import csv
from io import StringIO
from collections import defaultdict
import os
import time
import ray
from utils.s3_reader import BUCKET_NAME, FILE_KEY, REGION, get_s3_client


NUM_PARALLEL_TASKS = 4


def ensure_output_dir():
    """Ensure output directory exists"""
    os.makedirs('outputs', exist_ok=True)


def get_data_shards():
    """Read data from S3 and split into shards"""
    s3 = get_s3_client()
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=FILE_KEY)
    content = obj['Body'].read().decode('utf-8')
    lines = content.splitlines()
    header = lines[0]
    data_lines = lines[1:]

    chunk_size = len(data_lines) // NUM_PARALLEL_TASKS
    shards = []

    for i in range(NUM_PARALLEL_TASKS):
        start = i * chunk_size
        end = (i + 1) * chunk_size if i < NUM_PARALLEL_TASKS - 1 else len(data_lines)
        shard_lines = [header] + data_lines[start:end]
        shards.append("\n".join(shard_lines))

    return shards


# Define remote function
@ray.remote
def process_shard(shard_content):
    """Process a single data shard and return service statistics"""
    reader = csv.DictReader(StringIO(shard_content))
    stats = defaultdict(lambda: {
        'total': 0,
        'slow': 0,
        'server_error': 0,
        'timeout': 0
    })

    for row in reader:
        service = row['service_name']
        stats[service]['total'] += 1

        # Slow request: response_time > 800ms
        if int(row['response_time_ms']) > 800:
            stats[service]['slow'] += 1

        # Server error: status_code = 500
        if row['status_code'] == '500':
            stats[service]['server_error'] += 1

        # Timeout error
        if row.get('error_type', '') == 'Timeout':
            stats[service]['timeout'] += 1

    return dict(stats)


def merge_stats(all_stats):
    """Merge statistics from all shards"""
    merged = defaultdict(lambda: {
        'total': 0,
        'slow': 0,
        'server_error': 0,
        'timeout': 0
    })

    for stats in all_stats:
        for service, counts in stats.items():
            merged[service]['total'] += counts['total']
            merged[service]['slow'] += counts['slow']
            merged[service]['server_error'] += counts['server_error']
            merged[service]['timeout'] += counts['timeout']

    return merged


def detect_degraded_services(stats):
    """
    Detect degraded services based on conditions:
    1. Slow request rate > 20%
    2. Server error rate > 10%
    3. At least 5 timeout errors
    """
    degraded = []

    for service, s in stats.items():
        total = s['total']
        if total == 0:
            continue

        slow_rate = s['slow'] / total
        error_rate = s['server_error'] / total

        if slow_rate > 0.2:
            degraded.append((service, "high slow request rate"))
        elif error_rate > 0.1:
            degraded.append((service, "high server error rate"))
        elif s['timeout'] >= 5:
            degraded.append((service, "repeated timeout errors"))

    return degraded


def main():
    ensure_output_dir()

    # Initialize Ray
    ray.init(ignore_reinit_error=True)

    start_time = time.time()

    # Get data shards
    shards = get_data_shards()

    # Submit parallel tasks
    futures = [process_shard.remote(shard) for shard in shards]
    shard_results = ray.get(futures)

    # Merge results
    merged_stats = merge_stats(shard_results)

    # Detect degraded services
    degraded_services = detect_degraded_services(merged_stats)

    # Print results
    print("Degraded Service Detection Results:")
    if degraded_services:
        for service, reason in degraded_services:
            print(f"{service}, {reason}")
    else:
        print("No degraded services detected.")

    # Save to file
    with open('outputs/output4_degraded_services.txt', 'w') as f:
        for service, reason in degraded_services:
            f.write(f"{service}, {reason}\n")

    print(f"\nTime taken: {time.time() - start_time:.2f} seconds")

    ray.shutdown()


if __name__ == "__main__":
    main()