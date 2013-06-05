Introduction
============

Implement of distributed algorithms with MapReduce in Java
- dir: src
- language: Java

MapReduce function written in Python
- dir: python
- language: Python

AWS
===
The easist way to use AWS is to use Elastic MapReduce. Here are some useful tips:
- If you want to kill the hadoop job, while don't want to terminate the job flow, just type:
$ hadoop job -kill job_id
- Visit http://\<master.public-dns-name.amazonaws.com\>:9100/ to check the job tracker.
- Visit http://\<master.public-dns-name.amazonaws.com\>:9101/ to check HDFS management.
- SSH command to go into the master node from local machine: 
$ ssh -o "ServerAliveInterval 10" -i \</path/to/saved/keypair/file.pem\> hadoop@\<master.public-dns-name.amazonaws.com\>
- Copy file to local: hadoop dfs -copyToLocal /user/hadoop/example-results example-results
- Copy file from local: $ scp -o "ServerAliveInterval 10" -i \</path/to/saved/keypair/file.pem\> -r hadoop@\<master.public-dns-name.amazonaws.com\>:example-results 

Help
====
- More tips for the use of Hadoop can be found in http://hadoop.apache.org/docs/r1.0.4/mapred_tutorial.html
- More tips for the use of pig can be found in http://pig.apache.org/docs/r0.7.0/tutorial.html
- More tips for the use of AWS can be found in http://docs.aws.amazon.com/AWSEC2/latest/UserGuide
- More tips for the use of pig in AWS can be found in http://www.cs.washington.edu/education/courses/cse344/11au/hw/hw6/hw6-awsusage.html



