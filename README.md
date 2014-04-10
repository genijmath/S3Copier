S3Copier
========

Copy files from S3 to HDFS, etc

I faced an issue outlined on [stackoverflow](http://stackoverflow.com/questions/14631152/copy-files-from-amazon-s3-to-hdfs-using-s3distcp-fails).
It is possible that it is resolved in current version of s3distcp/distcp utilities (I was using m1.small AWS instance),

s3copy.SimpleCopier invokes S3 API to copy data directly (no Map-Reduce)
