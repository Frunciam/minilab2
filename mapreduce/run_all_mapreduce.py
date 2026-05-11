"""Run all MapReduce jobs"""
import time
import job1_request_count
import job2_error_count
import job3_top10_slow


def main():
    start_time = time.time()

    print("Job 1: Request Count by Service")
    job1_request_count.main()

    print("\nJob 2: Server Error Count by Service")
    job2_error_count.main()

    print("\nJob 3: Top 10 Slow Endpoints")
    job3_top10_slow.main()

    print(f"\nTotal time: {time.time() - start_time:.2f} seconds")


if __name__ == "__main__":
    main()