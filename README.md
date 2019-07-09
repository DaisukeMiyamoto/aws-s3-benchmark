aws-s3-benchmark
---

Amazon S3 Upload/Download throughput benchmark

# requirements

```
pandas
numpy
requests
boto3
```

# usage

## configure conditions

- set role or credentials for instance to read/write s3 bucket
- edit settings in `s3bench.py`

```
s3_bucket_name = 'midaisuk-s3-test'
max_concurrency_list = [10, 100]
max_io_queue_list = [1000, 10000]
filesize_list = [1024, 10240, 102400]
threads_list = [1, 4, 16]
trial = 1
random_data = True
```

## run

`$ python3 s3bench.py`

