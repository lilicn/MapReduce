Introduction
============

Implement of distributed algorithms with MapReduce in Java
- dir: src
- language: Java

MapReduce function written in Python
- dir: python
- language: Python

Help
====
- Hadoop
If you want to write your own MapReduce function and figure out how to use Hadoop, please check the following url:
http://hadoop.apache.org/docs/r1.0.4/mapred_tutorial.html

- AWS
The easist way to use AWS is to use Elastic MapReduce. Here are some useful tips:
1. If you want to kill the hadoop job, while don't want to terminate the job flow, just type:
% hadoop job -kill job_id
2. Visit http://<master.public-dns-name.amazonaws.com>:9100/ to check the job tracker.
3. Visit http://<master.public-dns-name.amazonaws.com>:9101/ to check HDFS management.
4. SSH command to go into the master node from local machine: 
$ ssh -o "ServerAliveInterval 10" -i </path/to/saved/keypair/file.pem> hadoop@<master.public-dns-name.amazonaws.com>





