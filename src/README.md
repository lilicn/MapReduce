Introduction
============

Implement of distributed algorithms with MapReduce
- src/Degree counting (Degree.java)
- src/Single source shortest path (BFS.java and Initial.java)
- src/Triangle counting (Triangle1.java and Triangle2.java)
- src/Canopy blocking (Canopy1.java, Canopy12.java and Canopy2.java)
- Traditional blocking (WithBlocking.java)

How to make .jar
================

Use the folowing commands to make jar (of course, you have to has the hadoop-core-version.jar)
- $ javac -classpath ${HADOOP_HOME}/hadoop-${HADOOP_VERSION}-core.jar -d packageName className.java 
- $ jar -cvf className.jar -C packageName/ .




