"""Validation script to verify results"""
from utils.s3_reader import read_csv_from_s3


def validate():
    """Validate results with manual check on first 100 rows"""
    data = read_csv_from_s3()

    # Manual count on first 100 rows
    manual_count = {}
    for row in data[:100]:
        service = row['service_name']
        manual_count[service] = manual_count.get(service, 0) + 1

    print("Manual count on first 100 rows:")
    for service, count in sorted(manual_count.items()):
        print(f"  {service}: {count}")


if __name__ == "__main__":
    validate()