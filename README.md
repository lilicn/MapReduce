Introduction
============

Implement of distributed algorithms with MapReduce
- Degree counting (Degree.java)
- Single source shortest path (BFS.java and Initial.java)
- Triangle counting (Triangle1.java and Triangle2.java)
- Canopy blocking (Canopy1.java, Canopy12.java and Canopy2.java)

How to make .jar
================

Use the folowing commands to make jar (of course, you have to has the hadoop-core-version.jar)
- $ javac -classpath ${HADOOP_HOME}/hadoop-${HADOOP_VERSION}-core.jar -d packageName className.java 
- $ jar -cvf className.jar -C packageName/ .



