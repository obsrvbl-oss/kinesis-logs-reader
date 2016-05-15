#  Copyright 2016 Observable Networks
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import unicode_literals
from json import loads

from boto3.session import Session

from .utils import gunzip_bytes


class KinesisLogsReader(object):
    def __init__(self, stream_name, start_time=None, **kwargs):
        kinesis_client = kwargs.pop('kinesis_client', None)
        self.kinesis_client = kinesis_client or self._get_client(**kwargs)

        self.stream_name = stream_name
        self.shard_ids = list(self._get_shard_ids())

        self.shard_iterators = {}
        for shard_id in self.shard_ids:
            self.shard_iterators[shard_id] = self._get_shard_iterator(
                shard_id, start_time
            )

        self.shard_finished = {shard_id: False for shard_id in self.shard_ids}

    def __iter__(self):
        self.iterator = self._reader()
        return self

    def __next__(self):
        return next(self.iterator)

    def next(self):
        # For Python 2 compatibility
        return self.__next__()

    def _get_client(self, **kwargs):
        return Session(**kwargs).client('kinesis')

    def _get_shard_ids(self):
        paginator = self.kinesis_client.get_paginator('describe_stream')
        response_iterator = paginator.paginate(StreamName=self.stream_name)

        for response in response_iterator:
            for shard in response['StreamDescription']['Shards']:
                yield shard['ShardId']

    def _get_shard_iterator(self, shard_id, start_time=None):
        kwargs = {'StreamName': self.stream_name, 'ShardId': shard_id}
        if start_time is None:
            kwargs['ShardIteratorType'] = 'LATEST'
        else:
            kwargs['ShardIteratorType'] = 'AT_TIMESTAMP'
            kwargs['Timestamp'] = start_time

        response = self.kinesis_client.get_shard_iterator(**kwargs)
        return response['ShardIterator']

    def _read_shard(self, shard_id):
        iterator = self.shard_iterators[shard_id]
        response = self.kinesis_client.get_records(ShardIterator=iterator)

        self.shard_iterators[shard_id] = response['NextShardIterator']
        self.shard_finished[shard_id] = response['MillisBehindLatest'] == 0

        for record in response['Records']:
            gz_data = record['Data']
            raw_data = gunzip_bytes(gz_data)
            data = loads(raw_data.decode('utf-8'))

            if data['messageType'] != 'DATA_MESSAGE':
                continue

            for flow_record in data.get('logEvents', []):
                yield flow_record['extractedFields']

    def _reader(self):
        while True:
            for shard_id in self.shard_ids:
                for item in self._read_shard(shard_id):
                    yield item

            if all(self.shard_finished.values()):
                break
