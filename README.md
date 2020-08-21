Spark Standalone Cluster ec-2 
==================================

### Prerequisites

1. SSH server is installed on the master node. If not, [follow the link](https://blog.insightdatascience.com/simply-install-spark-cluster-mode-341843a52b88) 
2. Pyspark (3.0.0) is installed. If not, follow the same link.
---

### Keyless SSH Set-Up

1. Go to ssh directory.
```bash
cd ~/.ssh
```
2.


---

> ```Cluster Set-Up```

> ```Keyless SSH Set-Up```

> ```Environment Variables```
> ```Configuring Cluster```
> ```Monitoring Cluster```



## For Windows OS 
-------------------------------------------------------------------------
### Creating virtual environment 
Install pipenv to create the virtual environment.This will create Pipfile (and possibly) Pipfile.lock files which support high-level packaging. You could work with pipenv inside an already existing virtual environment, in that case pipenv will automatically use the virtualenv you are in, your program will run smoothly.
```bash
pip install pipenv
```
Go to directory that you want to create the virtual environment and create the environment.
```bash
pipenv shell
```
Install pyspark
```bash
pipenv install pyspark
```
Run feature_extraction_spark_pipeline.py file
```bash
python feature_extraction_spark_pipeline.py
```


## For MacOS / Linux
-----------------------------------------------------------------------
### Creating virtual environment 
Install pipenv to create the virtual environment.This will create Pipfile (and possibly) Pipfile.lock files which support high-level packaging. You could work with pipenv inside an already existing virtual environment, in that case pipenv will automatically use the virtualenv you are in, your program will run smoothly.
```bash
brew install pipenv
```
Go to directory that you want to create the virtual environment  and create the environment.
```bash
pipenv shell
```
Install pyspark
```bash
pipenv install pyspark
```
Run feature_extraction_spark_pipeline.py file
```bash
python feature_extraction_spark_pipeline.py
```
