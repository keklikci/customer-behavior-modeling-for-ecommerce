Spark Standalone Cluster ec-2 
==================================

### Prerequisites

1. SSH server is installed on the master node. If not, [follow the link](https://blog.insightdatascience.com/simply-install-spark-cluster-mode-341843a52b88) 
2. Pyspark (3.0.0) is installed. If not, follow the same link.
3. Scala is installed. If not, follow the same link.
4. Java is installed. If not, follow the same link. The link installs openjdk-8 but this project uses openjdk 11.0.8. If you choose to install another version, make sure to set your $JAVA_HOME correctly.
---
### ec-2 Ports 

1. From EC2 Management Console, go to Security Groups > Edit Inbound Rules > Add rule to enable the following ports. If you have more than one slave, port 8081 will be occupied by the first worker. If you have binding problems for rest of the slaves, you may also need to enable some other ports.

*for park-shell-jobs*
```bash
Custom TCP - TCP - 4040 - Custom - 0.0.0.0/0 
```
*for spark-master-ui-port*
```bash
Custom TCP - TCP - 8080 - Custom - 0.0.0.0/0 
```
*for spark-history-server*
```bash
Custom TCP - TCP - 18080 - Custom - 0.0.0.0/0 
```
*for spark-master-port*
```bash
Custom TCP - TCP - 7077 - Custom - 0.0.0.0/0 
```
*for spark-worker-ui-port*
```bash
Custom TCP - TCP - 8081 - Custom - 0.0.0.0/0 
```
---
### Keyless SSH Set-Up

1. Go to ssh directory and run the followin command to create RSA key-pair.
```bash
cd ~/.ssh
ssh-keygen -t rsa -P ""
```
2. Logged in as the root user (ubuntu), distribute your public key to each slave node.
```bash
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
```
---
### Environment Variables (run the commands in order)

1. Run the following command to define $SPARK_HOME. This is the directory you will your master and slaves.
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
XXX.XX.X.61 master
```
---

# TODO
> Cluster Set-Up
> Configuring Cluster
> Monitoring Cluster
> URLS
> History Server
> Logs (spark-events) and Logs (master, worker, history server)


