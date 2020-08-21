Spark Standalone Cluster ec-2 
==================================

Prerequisites

1. SSH server is installed on the master node. If not, [follow the link](https://blog.insightdatascience.com/simply-install-spark-cluster-mode-341843a52b88).
2. Pyspark (3.0.0) is installed. 
3. Click the **Edit** button.
4. Delete the following text: *Delete this line to make a change to the README from Bitbucket.*
5. After making your change, click **Commit** and then **Commit** again in the dialog. The commit page will open and you’ll see the change you just made.
6. Go back to the **Source** page.

---
This README.md is for running ```feature_extraction_spark_pipeline.py``` on AWS ec-2 instances. Sections include;

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
