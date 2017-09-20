```bash
./bin/spark-submit --master local[4] --jars jars/elasticsearch-hadoop-2.2.0.jar /home/pyspark-experiments/es_spark_test.py
```

Install numpy
```bash
pip install --user numpy scipy matplotlib ipython jupyter pandas sympy nose
```
or
```bash
sudo apt-get install python-numpy python-scipy python-matplotlib ipython ipython-notebook python-pandas python-sympy python-nose
```


Resources Used:
 - https://qbox.io/blog/building-an-elasticsearch-index-with-python?utm_source=qbox.io&utm_medium=article&utm_campaign=elasticsearch-in-apache-spark-python
 - https://www.kaggle.com/annavictoria/speed-dating-experiment
 - https://spark.apache.org/docs/2.1.0/mllib-decision-tree.html