import sys
import os
import numpy
import boto3
from boto3.s3.transfer import TransferConfig
import random
import string
import time
import multiprocessing
import pandas as pd
import requests
import datetime


class BenchmarkRecorder:
    def __init__(self, col_names, add_metadata=True):
        metadata_col_names = ['instance_type', 'availability-zone', 'version', 'date', 'datetime']
        self.add_metadata = add_metadata
        if add_metadata:
            col_names.extend(metadata_col_names)

        self.df = pd.DataFrame(columns=col_names)
        self.col_names = col_names

    def _get_metadata(self, key):
        response = requests.get('http://169.254.169.254/latest/meta-data/' + key)
        return response.text
        
    def _get_metadata_record(self):
        metadata = [
            self._get_metadata('instance-type'),
            self._get_metadata('placement/availability-zone'),
            '2018-11-20',
            str(datetime.date.today()),
            str(datetime.datetime.now())
        ]
        return metadata
        
    def add_record(self, record):
        if self.add_metadata:
            record.extend(self._get_metadata_record())

        s = pd.Series(record, index=self.col_names)
        self.df = self.df.append(s, ignore_index=True)

    def write(self, filename, file_type='csv'):
        if file_type == 'csv':
            self.df.to_csv(filename)
        elif file_type == 'json':
            self.df.to_json(filename)
        else:
            print('Error: wrong output type')

    def upload_to_s3(self, tmp_filename, bucket_name, key_name, file_type='csv'):
        self.write(tmp_filename, file_type)
        s3 = boto3.client('s3')
        s3.upload_file(tmp_filename, bucket_name, key_name)
        print('Results uploaded to s3://%s/%s' % (bucket_name, key_name))


class S3Benchmark:
    def __init__(self, bucket_name, max_concurrency=10, max_io_queue=100, random_data=True, clean=True):
        self.bucket_name = bucket_name
        self.local_tmp_file_list = []
        self.s3_tmp_file_list = []
        self.transfer_config = TransferConfig(max_concurrency=max_concurrency, max_io_queue=max_io_queue)
        self.random_data = random_data
        self.clean = clean

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
        
    def _print_result(self, upload_speed, upload_time, download_speed, download_time):
        print(' * Upload    %.2f Mbps (%.4f [sec])' % (upload_speed, upload_time))
        print(' * Download  %.2f Mbps (%.4f [sec])' % (download_speed, download_time))
        
    def run(self, file_size_mb):
        print('Testing %d MB:' % file_size_mb)

        local_file_name = self._generate_random_str(8) + '_%dmb.tmp' % file_size_mb
        self.local_tmp_file_list.append(local_file_name)

        # if not os.path.exists(filename):
        self._generate_dummy_file(local_file_name, file_size_mb, random_data=self.random_data)

        upload_time = self._measure_upload_speed(local_file_name)
        download_time = self._measure_download_speed(local_file_name)

        self._print_result(filesize / upload_time * 8, upload_time, filesize / download_time * 8, download_time)

        if self.clean:
            self._clean()

        return upload_time, download_time

    def multi_run(self, num_threads, file_size_mb):
        print('Testing %d MB x %d Process:' % (file_size_mb, num_threads))

        random_str = self._generate_random_str(8)
        for i in range(num_threads):
            local_file_name = random_str + '_%d_%dmb.tmp' % (i, file_size_mb)
            self.local_tmp_file_list.append(local_file_name)
            self._generate_dummy_file(local_file_name, file_size_mb, random_data=self.random_data)
            
        pool = multiprocessing.Pool(num_threads)
        start = time.time()
        pool.map(self._measure_upload_speed, self.local_tmp_file_list)
        upload_time = time.time() - start
        pool.close()
        
        pool = multiprocessing.Pool(num_threads)
        start = time.time()
        pool.map(self._measure_download_speed, self.local_tmp_file_list)
        download_time = time.time() - start
        pool.close()

        self._print_result(filesize * num_threads / upload_time * 8, upload_time, filesize * num_threads  / download_time * 8, download_time)

        if self.clean:
            self._clean()

        return upload_time, download_time

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

    max_concurrency_list = [10, 100]
    max_io_queue_list = [1000, 10000]
    # filesize_list = [32, 64]
    filesize_list = [32, 64, 128, 256, 512, 1024, 2048]
    # threads_list = [1, 2]
    threads_list = [1, 2, 4, 8, 16, 32]
    trial = 3
    random_data = True
    
    recorder = BenchmarkRecorder([
        'trial',
        'file_size',
        'max_concurrency',
        'max_io_queue',
        'random_data',
        'num_process',
        'upload_time_sec',
        'download_time_sec',
        'upload_speed_mbps',
        'download_speed_mbps',
    ])

    for max_concurrency in max_concurrency_list:
        for max_io_queue in max_io_queue_list:
            for num_process in threads_list:
                for filesize in filesize_list:
                    print('Settings:')
                    print(' * Max Concurrency: %d' % max_concurrency)
                    print(' * Max IO Queue: %d' % max_io_queue)
                    print(' * Random Data: %s' % random_data)

                    for i in range(trial):
                        s3bench = S3Benchmark(s3_bucket_name,
                            max_concurrency=max_concurrency,
                            max_io_queue=max_io_queue,
                            random_data=random_data
                        )
                        
                        upload_time, download_time = s3bench.multi_run(num_process, filesize)
                        recorder.add_record([
                                trial,
                                filesize,
                                max_concurrency,
                                max_io_queue,
                                random_data,
                                num_process,
                                upload_time,
                                download_time,
                                filesize * num_process / upload_time * 8,
                                filesize * num_process  / download_time * 8,
                            ]
                        )

    print(recorder.df)
    today = datetime.date.today()
    recorder.upload_to_s3('result.tmp', 'midaisuk-benchmarks', 's3/throughput/' + str(today) + '.csv', file_type='csv')
