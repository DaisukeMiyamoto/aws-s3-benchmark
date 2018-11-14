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


def generate_dummy_file(filename, megabyte):
    with open(filename, 'wb') as file:
        file.write(numpy.random.bytes(megabyte * 1024 * 1024))
    
    
def measure_upload_speed(bucket_name, filename):
    s3 = boto3.client('s3')
    key_name = generate_random_str(20)

    # upload
    start = time.time()
    s3.upload_file(filename, bucket_name, key_name)
    upload_time = time.time() - start

    # download
    start = time.time()
    s3.download_file(bucket_name, key_name, key_name)
    download_time = time.time() - start
    
    # clean up
    os.remove(key_name)

    return upload_time, download_time


if __name__=='__main__':
    s3_bucket_name = 'midaisuk-s3-test'

    filesize_list = [100, 500, 1000]
    filename_template = '%dmb.tmp'
    
    for filesize in filesize_list:
        print('Testing %d MB:' % filesize)
        filename = filename_template % filesize
        generate_dummy_file(filename, filesize)
        upload_time, download_time = measure_upload_speed(s3_bucket_name, filename)
        print(' * Upload    %.2f Mbit/sec (%.4f [sec])' % (filesize / upload_time * 8, upload_time))
        print(' * Download  %.2f Mbit/sec (%.4f [sec])' % (filesize / download_time * 8, download_time))
