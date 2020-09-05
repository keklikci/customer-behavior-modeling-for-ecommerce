Spark Standalone Cluster ec-2 
==================================

### Prerequisites

1. Python 3.6.9 is installed on all your nodes.
2. SSH server is installed on the master node. If not, [follow the link](https://blog.insightdatascience.com/simply-install-spark-cluster-mode-341843a52b88).
3. Pyspark (3.0.0) is installed. If not, follow the same link.
4. Scala is installed. If not, follow the same link.
5. Java is installed. If not, follow the same link. The link installs openjdk-8 but this project uses openjdk 11.0.8. If you choose to install another version, make sure to set your $JAVA_HOME correctly.
6. **While configuring cluster, please either use only private (preferable) or public IP's. The project only uses private IP's.**
---

### ec-2 Ports 

From EC2 Management Console, go to ```Security Groups > Edit Inbound Rules > Add rule``` to enable the following ports. If you have more than one slave, port 8081 will be occupied by the first worker. If you have binding problems for rest of your slaves, you may also need to enable some other ports.

*Note that it is possible to have more than 1 worker instance on a slave node. If that is the case, then you do not need to enable other successive ports because in a sense you have two separate workers; considering you allocated your resources correctly.* **For a comprehensive resource allocation guideline,** [follow the link](https://spoddutur.github.io/spark-notes/distribution_of_executors_cores_and_memory_for_spark_application.html).

1. To start a spark master, ```Custom TCP - TCP - 7077 - Custom - 0.0.0.0/0 ```
2. To start a spark worker, ```Custom TCP - TCP - 8081 - Custom - 0.0.0.0/0 ```
3. To access spark jobs UI, ```Custom TCP - TCP - 4040 - Custom - 0.0.0.0/0 ```
4. To access spark master UI, ```Custom TCP - TCP - 8080 - Custom - 0.0.0.0/0 ```
5. To access spark history history server UI, ```Custom TCP - TCP - 18080 - Custom - 0.0.0.0/0```
---
### Keyless SSH Set-Up

1. Go to ssh directory and run the following command to create RSA key-pair.
```bash
cd ~/.ssh
ssh-keygen -t rsa -P ""
```
2. Name your private key ```id_rsa``` and public key ```id_rsa.pub```.
3. Logged in as the root user (ubuntu), distribute your public key to each slave node.
```bash
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
```
---
### Environment Variables (run the commands in order)

1. Run the following command to define $SPARK_HOME. This is the directory you will start your master and slaves.
```bash
export SPARK_HOME=/home/ubuntu/spark-3.0.0-bin-hadoop2.7
```
2. Run the following command to define PYTHONPATH.
```bash
export PYTHONPATH=$SPARK_HOME/python:
```
3. Run the following command to enable network access to JVM, add py4j to your PYTHONPATH.
```bash
export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH
```
---
### Edit /etc/hosts 

1. Run the following command.
```bash
sudo nano -w /etc/hosts
```
2. Add the private IP address of the spark master to the file.
```bash
127.0.0.1 localhost
XXX.XX.X.XX master
```
---
### Configuring Cluster

1. Go to your spark conf directory by running the following command.
```bash
cd $SPARK_HOME/conf
```
2. Here you have to create your own configuration files by using the following templates; slaves.template, spark-defaults.conf.template, spark-env.sh.template, log4j.properties.template. Copy the contents of the templates to separate files by only removing .template from the filenames as follows:
```bash
cp slaves.template slaves 
cp spark-defaults.conf.template spark-defaults.conf
cp spark-env.sh.template spark-env.sh
cp log4j.properties.template log4j.properties
```
3. Now, you have your configuration files but all spark parameters inside are commented out. Please leave ```log4j.properties``` as it is since it's the only file that is not commented out. The remaining files, however, must be edited.
4. Go to your ```slaves``` file and add the private IP of your slave like the following:
```bash
# A Spark Worker will be started on each of the machines listed below.
XXX.XX.XX.46
```
5. Go to your ```spark-defaults.conf``` file and set the following:
```bash
spark.eventLog.enabled          true
spark.driver.memory             1g
spark.serializer                org.apache.spark.serializer.KryoSerializer
spark.master                    spark://master:7077
spark.eventLog.dir              file:///tmp/spark-events
spark.history.fs.logDirectory   file:///tmp/spark-events
spark.executor.cores            5
spark.executor.instances        5
spark.executor.memory           20g
```
6. Go to your ```spark-env.sh``` file and set the following:
```bash
SPARK_MASTER_HOST=XXX.XX.XX.61
JAVA_HOME=/usr/lib/jvm/java-1.11.0-openjdk-amd64
PYSPARK_PYTHON=python3
SPARK_WORKER_CORES=16
SPARK_WORKER_INSTANCES=2
```
---
### Start master, slave, history server
1. Start your master by running the following commands.
```bash
cd $SPARK_HOME
sbin/start-master.sh
```
2. In your $SPARK_HOME, ```logs``` directory is created with the master's log. Examine that log. If everything is fine, then it should say "ALIVE" for the master. If not, please go over the previous steps one by one. You can now navigate to the master web UI on (http://ec2-XX-XXX-XXX-14.us-east-2.compute.amazonaws.com:8080) ```ec2-XX-XXX-XXX-14.us-east-2.compute.amazonaws.com``` is the public DNS of the master.
3. In your $SPARK_HOME, start your slave by running the following command. The master-url is ```spark://master:7077``` and is displayed on the master web UI, **master** is the hostname defined in ```/etc/hosts```.
```bash
sbin/start-slave.sh spark://master:7077
```
4. If successfully started, you can access the worker web UI at (http://ec2-XX-XXX-XXX-14.us-east-2.compute.amazonaws.com:8081), else please refer to **step 2**.
---
### Submitting Jobs to Cluster 
1. Before submitting your application, start a history-server(Why? Please refer to **step 3**). Run the following commands from your $SPARK_HOME. It is essential to run the command ```mkdir /tmp/spark-events``` before this step. Spark logs are automatically saved in this directory. If this directory is not created, spark will throw an error when you start the history server.
```bash
mkdir /tmp/spark-events
sbin/start-history-server.sh
```
2. In your $SPARK_HOME, run the following command to lauch your application to cluster. Here, **node-count** is # of slaves + 1 master node. 
```bash
bin/spark-submit --master spark://master:7077 /path/to/your/script node-count /path/to/inputfile
```
**Example** 
```bash
bin/spark-submit --master spark://master:7077 /home/matalay/Workspace/feature_extraction_spark_pipeline.py 2 /data/insider/largedata.parquet
```
3. If successfully launched, go to the jobs web UI at (http://ec2-XX-XXX-XXX-14.us-east-2.compute.amazonaws.com:4040). This web UI is accessible until the termination of your code, i.e sc.stop() is invoked. However, after termination you could view info related to executors, storage, environment, etc at your history-server url (explained in the following section).
---
### Monitoring Cluster
1. After the termination of spark context, (http://ec2-XX-XXX-XXX-14.us-east-2.compute.amazonaws.com:4040) is no longer accessible. However, you can access all spark job logs at (http://ec2-XX-XXX-XXX-14.us-east-2.compute.amazonaws.com:18080). This is how you will be monitoring your application after the termination of your code.
2. History server fetches spark logs from ```/tmp/spark-events``` directory. You could clear the history server web UI if you submit too many applications by clearing those logs with ```rm -r /tmp/spark-events```.
---
### Stop master, slave, history server
1. Run the following commands to stop the master and slave.
```bash
sbin/stop-master.sh
sbin/stop-slave.sh
```
2. After running each command, remember that you will lose access to the web UI of the instance you stop.
3. If you no longer desire to look at the history-server, stop the history server by running the following command.
```bash
sbin/stop-history-server.sh
```
4. After stopping all instances, it's convenient to delete the ```$SPARK_HOME/logs```directory. When you re-launch an application, it will automatically be re-generated.
---
