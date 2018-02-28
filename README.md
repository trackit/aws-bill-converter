# AWS Bill Converter

AWS can optionally export detailed billing data as CSV files in S3 buckets. Two
formats exist for this purpose, which are not interoperable. This script will
read such a file in the old format from standard input, convert it as best as
possible to the new format and write it to standard output.

In the process some data will be lost.

This tool was designed to use data in the old format with the reporting tool at
[trackit/aws-cost-report](https://github.com/trackit/aws-cost-report).
