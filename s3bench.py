import sys
import os
import numpy
import boto3
import random
import string
import time


def generate_random_str(num):
    random_str = ''.join([random.choice(string.ascii_letters + string.digits) for i in range(num)])
    return random_str


def generate_dummy_file(filename, megabyte, random_data=True):
    with open(filename, 'wb') as file:
        for i in range(10):
            if random_data:
                file.write(numpy.random.bytes(megabyte * 1000 * 100))
            else:
                file.write(numpy.zeros(megabyte * 1000 * 100 / 8))
    
    
def measure_upload_speed(bucket_name, filename, max_concurrency=10, max_io_queue=100):
    s3 = boto3.client('s3')
    transfer_config = boto3.s3.transfer.TransferConfig(max_concurrency=max_concurrency, max_io_queue=max_io_queue)
    key_name = generate_random_str(20)

    # upload
    start = time.time()
    s3.upload_file(filename, bucket_name, key_name, Config=transfer_config)
    upload_time = time.time() - start

    # download
    start = time.time()
    s3.download_file(bucket_name, key_name, key_name, Config=transfer_config)
    download_time = time.time() - start
    
    # clean up
    os.remove(key_name)

    return upload_time, download_time


if __name__=='__main__':
    s3_bucket_name = 'midaisuk-s3-test'

    filesize_list = [100, 500, 1000]
    filename_template = '%dmb.tmp'

    max_concurrency = 100
    max_io_queue = 1000
    random_data = True
    
    print('Settings:')
    print(' * Max Concurrency: %d' % max_concurrency)
    print(' * Max IO Queue: %d' % max_io_queue)
    print(' * Random Data: %s' % random_data)
    
    for filesize in filesize_list:
        print('Testing %d MB:' % filesize)
        filename = filename_template % filesize

        # if not os.path.exists(filename):
        generate_dummy_file(filename, filesize, random_data=random_data)
            
        upload_time, download_time = measure_upload_speed(
            s3_bucket_name,
            filename,
            max_concurrency=max_concurrency, 
            max_io_queue=max_io_queue
            )
        
        print(' * Upload    %.2f Mbps (%.4f [sec])' % (filesize / upload_time * 8, upload_time))
        print(' * Download  %.2f Mbbps (%.4f [sec])' % (filesize / download_time * 8, download_time))
