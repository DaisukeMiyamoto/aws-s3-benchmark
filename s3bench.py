import sys
import os
import numpy
import boto3
from boto3.s3.transfer import TransferConfig
import random
import string
import time
import threading


class S3Benchmark():
    def __init__(self, bucket_name, max_concurrency=10, max_io_queue=100, random_data=True):
        self.bucket_name = bucket_name
        self.local_tmp_file_list = []
        self.s3_tmp_file_list = []
        self.transfer_config = TransferConfig(max_concurrency=max_concurrency, max_io_queue=max_io_queue)
        self.random_data = random_data

    def _generate_random_str(self, num):
        random_str = ''.join([random.choice(string.ascii_letters + string.digits) for i in range(num)])
        return random_str
    
    def _generate_dummy_file(self, filename, megabyte, random_data=True):
        with open(filename, 'wb') as file:
            for i in range(10):
                if random_data:
                    file.write(numpy.random.bytes(megabyte * 1000 * 100))
                else:
                    file.write(numpy.zeros(megabyte * 1000 * 100 / 8))
        
    def _measure_upload_speed(self, file_name):
        s3 = boto3.client('s3')
        start = time.time()
        s3.upload_file(file_name, self.bucket_name, file_name, Config=self.transfer_config)
        process_time = time.time() - start
        self.s3_tmp_file_list.append(file_name)
        return process_time

    def _measure_download_speed(self, file_name):
        s3 = boto3.client('s3')
        start = time.time()
        s3.download_file(self.bucket_name, file_name, 'down_' + file_name, Config=self.transfer_config)
        process_time = time.time() - start
        self.local_tmp_file_list.append('down_' + file_name)
        return process_time

    def run(self, file_size_mb):
        print('Testing %d MB:' % file_size_mb)

        local_file_name = self._generate_random_str(8) + '_%dmb.tmp' % file_size_mb
        self.local_tmp_file_list.append(local_file_name)

        # if not os.path.exists(filename):
        self._generate_dummy_file(local_file_name, file_size_mb, random_data=self.random_data)

        upload_time = self._measure_upload_speed(local_file_name)
        download_time = self._measure_download_speed(local_file_name)
            
        print(' * Upload    %.2f Mbps (%.4f [sec])' % (filesize / upload_time * 8, upload_time))
        print(' * Download  %.2f Mbbps (%.4f [sec])' % (filesize / download_time * 8, download_time))
        self._clean()

    def multi_run(self, num_threads, file_size_mb):
        print('Testing %d MB:' % file_size_mb)

        thread_list = []
        for i in range(num_threads):
            local_file_name = self._generate_random_str(8) + '_%d_%dmb.tmp' % (i, file_size_mb)
            self.local_tmp_file_list.append(local_file_name)
            self._generate_dummy_file(local_file_name, file_size_mb, random_data=self.random_data)
            
            thread = threading.Thread(target=self._measure_upload_speed, args=([local_file_name]))
            thread_list.append(thread)
        
        for thread in thread_list:
            thread.start()
        
        for thread in thread_list:
            thread.join()


    def _clean(self):
        s3 = boto3.client('s3')
        for tmp_file in self.local_tmp_file_list:
            os.remove(tmp_file)
        
        # for tmp_file in self.s3_tmp_file_list:
        #     s3.delete_object(self.bucket_name, tmp_file)

        self.local_tmp_file_list = []
        self.s3_tmp_file_list = []


if __name__=='__main__':
    s3_bucket_name = 'midaisuk-s3-test'

    filesize_list = [100, 500, 1000]
    threads_list = [1, 2, 4, 8]

    max_concurrency = 100
    max_io_queue = 1000
    random_data = True
    
    print('Settings:')
    print(' * Max Concurrency: %d' % max_concurrency)
    print(' * Max IO Queue: %d' % max_io_queue)
    print(' * Random Data: %s' % random_data)
    
    s3bench = S3Benchmark(s3_bucket_name,
        max_concurrency=max_concurrency,
        max_io_queue=max_io_queue,
        random_data=random_data
    )

    # for filesize in filesize_list:
    #     s3bench.run(filesize)

    
    for filesize in filesize_list:
        s3bench.multi_run(4, filesize)
