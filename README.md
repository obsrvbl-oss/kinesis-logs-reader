## Introduction

[![Build Status](https://travis-ci.org/obsrvbl/kinesis-logs-reader.svg?branch=master)](https://travis-ci.org/obsrvbl/kinesis-logs-reader)
[![Coverage Status](https://coveralls.io/repos/obsrvbl/kinesis-logs-reader/badge.svg?branch=master&service=github)](https://coveralls.io/github/obsrvbl/kinesis-logs-reader?branch=master)

Amazon's CloudWatch Logs (CWL) service stores log data from many types of applications. For very large volumes of log data it may not be feasible to retrieve all records for processing using the standard CWL API. Fortunately, [CWL Subscriptions](https://aws.amazon.com/about-aws/whats-new/2015/06/amazon-cloudwatch-logs-subscriptions/) can automatically deliver log entries to Amazon Kinesis, which is suitable for real-time processing.

[Observable Networks](https://observable.net/) uses Kinesis to retrieve [VPC Flow Logs](http://docs.aws.amazon.com/AmazonVPC/latest/UserGuide/flow-logs.html), which are an input to endpoint modeling for network security monitoring.

This project contains a Python library and command line tool for retrieving log data from a Kinesis stream that's subscribed to a CloudWatch Logs group. The library builds on [boto3](https://github.com/boto/boto3) and should work on both Python 2.7 and 3.4+.

__Note__: The library is still experimental. Give it a try and file an issue or pull request if you have suggestions.

## Getting started

You'll want to set up a Kinesis subscription for your CWL group first. See [Amazon's documentation](http://docs.aws.amazon.com/AmazonCloudWatch/latest/DeveloperGuide/Subscriptions.html) for instructions on how to do that.

Once you have log data being delivered to your Kinesis stream, install this project:
```
git clone https://github.com/obsrvbl/kinesis-logs-reader.git
cd kinesis-logs-reader
python setup.py develop
```

## CLI Usage

`kinesis-logs-reader` provides a command line interface called `kinesis_logs_reader` that prints log records to stdout. It assumes your AWS credentials are available through environment variables, a boto configuration file, or through IAM metadata.

```
$ kinesis_logs_reader --start-time="2016-05-14 14:30:00" "vpc_flowlogs_stream"
account_id	action	bytes	dstaddr	dstport	end	interface_id	log_status	packets	protocol	srcaddr	srcport	start	version
12345678901	ACCEPT	228	198.51.100.1	123	1463236181	eni-25bed87f	OK	3	17	192.0.2.1	123	1463236035	2
12345678901	ACCEPT	312	192.0.2.1	0	1463236181	eni-25bed87f	OK	3	1	198.51.100.1	0	1463236035	2
12345678901	ACCEPT	6873	198.51.100.2	9224	1463236421	eni-25bed87f	OK	20	6	192.0.2.1	22	1463236375	2
12345678901	ACCEPT	2143	192.0.2.1	22	1463236421	eni-25bed87f	OK	16	6	198.51.100.2	9224	1463236375	2
```

The name of your Kinesis stream is required.
* The `--start-time` argument is optional; if not supplied the [`LATEST`](http://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetShardIterator.html) shard iterator will be used.
* The `--count` argument is optional; if not supplied `0` will be used and all records from the starting point to the end of the stream will be returned.

# Library usage

To read the latest logs, import `kinesis_logs_reader` and create a `kinesis_logs_reader.KinesisLogsReader` object. Loop over this iterable object to retrieve the `extractedFields` dictionary from each log event:

```python
import kinesis_logs_reader

reader = kinesis_logs_reader.KinesisLogsReader('vpc_flowlogs_stream')
for item in reader:
    print(item)
```

When initializing the `KinesisLogsReader` you may optionally pass:
* A `start_time` keyword, which is a Python `datatime.datetime` object.
* A `kinesis_client` keyword, if you need to set up a `boto3.client` instance yourself

If you don't pass a `kinesis_client` then all keyword arguments will be used when instantiating the boto3 [`Session`](http://boto3.readthedocs.io/en/latest/reference/core/session.html#boto3.session.Session).
