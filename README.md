MapReduce
=========

Implement of distributed algorithms with MapReduce

Use the folowing commands to make jar (of course, you have to has the hadoop-core.jar):
$ javac -classpath ${HADOOP_HOME}/hadoop-${HADOOP_VERSION}-core.jar -d packageName className.java 
$ jar -cvf className.jar -C packageName/ .


1. Degree counting (Degree.java)
2. Single source shortest path (BFS.java and Initial.java)
3. Triangle counting (Triangle1.java and Triangle2.java)
4. Canopy blocking (Canopy1.java, Canopy12.java and Canopy2.java)
