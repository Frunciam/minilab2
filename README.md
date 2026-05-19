# Mini-Project 2B: Cloud Service Log Analytics

This project analyzes a cloud service log dataset using Amazon S3 object storage, MapReduce-style baseline analytics, and a Ray-based parallel extension.

## Workflow

```text
Cloud service log dataset
-> Amazon S3 object storage
-> MapReduce-style baseline analytics
-> Ray degraded-service detection
-> validation and comparison
```

## Dataset

The dataset is a CSV file containing cloud service request logs.

Main fields used in the analysis:

| Field | Use |
| --- | --- |
| `service_name` | Count requests, server errors, and degraded services |
| `endpoint` | Identify slow endpoints |
| `status_code` | Detect server errors where `status_code >= 500` |
| `response_time_ms` | Detect slow requests where `response_time_ms > 800` |
| `error_type` | Detect repeated `Timeout` errors |
| `region` | Descriptive field for the log records |

## Cloud Object Storage

The dataset was uploaded to Amazon S3 before analysis.

The S3 bucket name, object key, region, and AWS credentials are configured in:

```text
utils/s3_reader.py
```

The configured S3 object is the cloud copy of:

```text
Comp3041J MiniProject 2 Dataset.csv
```

Amazon S3 is suitable for storing log data because it is scalable, durable, cost-effective, and designed for large semi-structured files that can be processed later by batch or parallel analytics programs.

The reader first tries to read the dataset from S3. If S3 access fails during local development, it falls back to a local CSV copy with the same file name in the project root.

## Project Structure

```text
minilab2-master/
|-- README.md
|-- requirements.txt
|-- Comp3041J MiniProject 2 Dataset.csv  (optional local fallback copy)
|-- mapreduce/
|   |-- job1_request_count.py
|   |-- job2_error_count.py
|   |-- job3_top10_slow.py
|   `-- run_all_mapreduce.py
|-- ray/
|   `-- degraded_detection.py
|-- utils/
|   `-- s3_reader.py
|-- validation/
|   `-- validate_results.py
`-- outputs/
    |-- output1_request_count.txt
    |-- output2_error_count.txt
    |-- output3_top10_slow.txt
    `-- output4_degraded_services.txt
```

## Requirements

Install dependencies:

```powershell
python -m pip install -r requirements.txt
```

The project uses:

- `boto3` for Amazon S3 access
- `ray` for parallel processing

## Running MapReduce-Style Jobs

Run all three baseline jobs:

```powershell
python mapreduce\run_all_mapreduce.py
```

These jobs implement explicit map and reduce phases in Python. They are a MapReduce-style baseline rather than a distributed Hadoop cluster job.

### Output 1: Request Count by Service

```text
auth-service 12121
order-service 10937
search-service 10616
notification-service 8412
payment-service 7914
```

### Output 2: Server Error Count by Service

Server errors are records where:

```text
status_code >= 500
```

Result:

```text
payment-service 1362
search-service 904
order-service 717
notification-service 436
auth-service 436
```

### Output 3: Top 10 Slow Endpoints

Slow requests are records where:

```text
response_time_ms > 800
```

Result:

```text
search-service,/search/results 1174
search-service,/search/filter 1165
search-service,/search 1123
search-service,/search/autocomplete 1119
payment-service,/payments 577
payment-service,/payments/refund 526
payment-service,/payments/status 525
payment-service,/payments/confirm 506
order-service,/orders 256
order-service,/orders/history 229
```

Recorded MapReduce runtime:

```text
Total time: 6.99 seconds
```

## Running Ray Degraded-Service Detection

Run the Ray extension:

```powershell
python ray\degraded_detection.py
```

The Ray implementation splits the input data into shards and processes them with a remote function using `@ray.remote`. Each remote task returns partial service statistics, and the driver program merges the partial results.

A service is marked as degraded if any of these conditions is true:

| Condition | Threshold |
| --- | --- |
| Slow request rate | `> 20%` |
| Server error rate | `> 10%` |
| Timeout errors | `>= 5` |

Ray output:

```text
search-service, high slow request rate
order-service, repeated timeout errors
payment-service, high slow request rate
notification-service, repeated timeout errors
auth-service, repeated timeout errors
```

Recorded Ray runtime:

```text
Time taken: 2.85 seconds
```

The run log also showed:

```text
Started a local Ray instance.
```

## Validation

Run validation:

```powershell
python validation\validate_results.py
```

The validation script recomputes the expected results directly in Python and compares them with the generated output files.

Validation coverage:

| Output | Validation method |
| --- | --- |
| Output 1 | Recount requests grouped by `service_name` |
| Output 2 | Recount records where `status_code >= 500`, grouped by `service_name` |
| Output 3 | Recount slow requests where `response_time_ms > 800`, grouped by `service_name, endpoint`, then compare the top 10 list |
| Output 4 | Recompute slow rate, server error rate, and timeout count for each service, then compare degraded-service decisions |

The validation output prints `Yes` in the `Match` column when the generated output matches the direct Python check.

## Output Files

Generated outputs are stored in:

```text
outputs/
```

| File | Description |
| --- | --- |
| `output1_request_count.txt` | Request count by service |
| `output2_error_count.txt` | Server error count by service |
| `output3_top10_slow.txt` | Top 10 slow endpoints |
| `output4_degraded_services.txt` | Ray degraded-service detection results |

## Runtime Environment

Recorded test environment:

- Windows PowerShell
- Python local execution
- Amazon S3 for cloud object storage
- Ray local mode
- 4 Ray remote tasks configured in `ray/degraded_detection.py`

## MapReduce-Style Baseline vs Ray

The MapReduce-style baseline is suitable for fixed batch aggregation tasks. Each job maps records to key-value pairs and reduces values by key. In this project it is used for request counts, server error counts, and slow endpoint counts.

Ray is more flexible for custom parallel analytics. It is used here to detect degraded services using multiple metrics in the same pass: slow request rate, server error rate, and timeout count. Ray remote tasks process data shards in parallel and return partial statistics that are merged by the main program.

## Submission Checklist

- Include the S3 upload screenshot.
- Include MapReduce runtime evidence.
- Include Ray runtime evidence.
- Include validation output evidence.
- Keep the group report anonymized.
- Do not include real student names, student IDs, group ID, repository usernames, or cloud account details in the report.
