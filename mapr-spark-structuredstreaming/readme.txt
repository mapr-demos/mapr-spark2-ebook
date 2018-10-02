
This example runs on MapR 6.0.1 ,  Spark 2.2.1 and greater

Install and fire up the Sandbox using the instructions here: http://maprdocs.mapr.com/home/SandboxHadoop/c_sandbox_overview.html. 

____________________________________________________________________

Step 1: Log into Sandbox, create data directory, MapR-ES Topic and MapR-DB table:

Use an SSH client such as Putty (Windows) or Terminal (Mac) to login. See below for an example:
use userid: mapr and password: mapr.

For VMWare use:  $ ssh mapr@ipaddress 

For Virtualbox use:  $ ssh mapr@127.0.0.1 -p 2222 

after logging into the sandbox At the Sandbox unix command line:
 
Create a directory for the data for this project

mkdir /mapr/demo.mapr.com/data

use the mapr command line interface to create a stream, a topic, get info and create a table:

maprcli stream create -path /apps/uberstream -produceperm p -consumeperm p -topicperm p
maprcli stream topic create -path /apps/uberstream -topic ubers  

to get info on the ubers topic :
maprcli stream topic info -path /apps/uberstream -topic ubers


Create the MapR-DB Table which will get written to

maprcli table create -path /apps/ubertable -tabletype json -defaultreadperm p -defaultwriteperm p
 
____________________________________________________________________

Step 2: Copy the data file to the MapR sandbox or your MapR cluster

 
Copy the data file from the project data folder to the sandbox using scp to this directory /user/mapr/data/uber.csv on the sandbox:

use userid: mapr and password: mapr.
For VMWare use:  $ scp  *.json  mapr@<ipaddress>:/home/mapr/. 

For Virtualbox use:  $ scp -P 2222 data/*.json  mapr@127.0.0.1:/mapr/demo.mapr.com/data/.

this will put the data file into the cluster directory: 
/mapr/<cluster-name>/data/

____________________________________________________________________

Step 3: To run the code in the Spark Shell:
 
/opt/mapr/spark/spark-2.2.1/bin/spark-shell --master local[2]
 
 - For Yarn you should change --master parameter to yarn-client - "--master yarn-client"

The you can copy paste code from the scripts directory to run the examples :

look at and copy code from these files:
 
Chapter 6 ClusterUber.scala

____________________________________________________________________

Step 4:  Copy the jar files

You can build this project with Maven using IDEs like Eclipse or NetBeans, and then copy the JAR file to your MapR Sandbox, or you can install Maven on your sandbox and build from the Linux command line, 
for more information on maven, eclipse or netbeans use google search. 

After building the project on your laptop, you can use scp to copy your JAR file from the project target folder to the MapR Sandbox:

From your laptop command line or with a scp tool :

use userid: mapr and password: mapr.
For VMWare use:  $ scp  nameoffile.jar  mapr@ipaddress:/home/mapr/. 

For Virtualbox use:  $ scp -P 2222 target/*.jar  mapr@127.0.0.1:/home/mapr/.  

this will put the jar file into the mapr user home directory: 
/home/mapr/
____________________________________________________________________

Step 3: Run the Spark k-means program which will create and save the machine learning model: 


From the Sandbox command line run the Spark program which will create and save the machine learning model:

/opt/mapr/spark/spark-2.2.1/bin/spark-submit --class sparkml.ClusterUber --master local[2]  mapr-spark-structuredstreaming-uber-1.0.jar 

This will read  from the file mfs:///mapr/demo.mapr.com/data/uber.csv  and save the model in mfs:///mapr/demo.mapr.com/data/ubermodel  

You can optionally pass the files  as input parameters <file modelpath>   (take a look at the code to see what it does)


Instead of running the application , you can also copy paste  the code from the scripts directory ClusterUber.scala into the spark shell 

/opt/mapr/spark/spark-2.2.1/bin/spark-shell --master local[2]
 
 - For Yarn you should change --master parameter to yarn-client - "--master yarn-client"

_________________________________________________________________________________  

Step 4: After creating and saving the k-means model, publish events to the topic 


Run the Streaming code to publish events to the topic:

java -cp ./mapr-spark-structuredstreaming-uber-1.0.jar:`mapr classpath` streams.MsgProducer

This client will read lines from the file in "/mapr/demo.mapr.com/data/uber.csv" and publish them to the topic /apps/uberstream:ubers. 
You can optionally pass the file and topic as input parameters <file topic> 

Optional: run the MapR Streams Java consumer to see what was published :

java -cp mapr-spark-structuredstreaming-uber-1.0.jar:`mapr classpath` streams.MsgConsumer 

_________________________________________________________________________________  

Step 5: Run the  the Spark Structured Streaming client to consume events enrich them and write them to MapR-DB
(in separate consoles if you want to run at the same time)


/opt/mapr/spark/spark-2.2.1/bin/spark-submit --class streaming.StructuredStreamingConsumer --master local[2] \
 mapr-spark-structuredstreaming-uber-1.0.jar 

This spark streaming client will consume from the topic /apps/uberstream:ubers, enrich from the saved model at
/mapr/demo.mapr.com/data/ubermodel and write to the table /apps/ubertable.
You can optionally pass the  input parameters <topic model table> 
 
You can use ctl-c to stop


Instead of running the application , you can also copy paste  the code from the scripts directory 
StructuredStreamingConsumer  into the spark shell 

/opt/mapr/spark/spark-2.2.1/bin/spark-shell --master local[2]
 
 - For Yarn you should change --master parameter to yarn-client - "--master yarn-client"
____________________________________________________________________

Step 6:  Spark SQL and  MapR-DB 

In another window while the Streaming code is running, run the code to Query from MapR-DB 

/opt/mapr/spark/spark-2.2.1/bin/spark-submit --class sparkmaprdb.QueryUber --master local[2] \
 mapr-spark-structuredstreaming-uber-1.0.jar 


____________________________________________________________________
Step 7: Use the Mapr-DB shell to query the data

start the hbase shell and scan to see results: 

$ /opt/mapr/bin/mapr dbshell

maprdb mapr:> jsonoptions --pretty true --withtags false

maprdb mapr:> find /apps/ubertable --limit 5

maprdb mapr:> find /apps/ubertable --where '{ "$eq" : {"cid":9} }' --limit 10

maprdb mapr:> find /apps/ubertable --where '{"$and":[{"$eq":{"base":"B02617"}},{ "$like" : {"_id":"9%"} }]}' --limit 10


_________________________________________________________________________________

to delete   after using :
Later to delete a topic:
maprcli stream topic delete -path /apps/uberstream -topic ubers  

Later to delete a table:
maprcli table delete -path /apps/ubertable 

to delete the checkpoint directory

hadoop fs -rmr /tmp/uberdb
_________________________________________________________________________________

