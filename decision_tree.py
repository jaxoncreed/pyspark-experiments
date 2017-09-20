from pyspark import SparkContext, SparkConf

import numpy
print('Printing Here')
print(numpy.version.version)

if __name__ == "__main__":

    conf = SparkConf().setAppName("ESTest")
    sc = SparkContext(conf=conf)

    q ="""{
      "query": {
        "match_all": {}
      }
    }"""

    es_read_conf = {
        "es.nodes" : "159.203.184.137",
        "es.port" : "9200",
        "es.resource" : "dating/date",
        "es.query": q
    }

    es_write_conf = {
        "es.nodes": "159.203.184.137",
        "es.port" : "9200",
        "es.resource" : "dating/value_counts"
    }

    es_rdd = sc.newAPIHadoopRDD(
        inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
        keyClass="org.apache.hadoop.io.NullWritable",
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
        conf=es_read_conf)


    # ALL the ML Stuff


    from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
    from pyspark.mllib.util import MLUtils

    # Load and parse the data file into an RDD of LabeledPoint.
    # Split the data into training and test sets (30% held out for testing)
    (trainingData, testData) = es_rdd.randomSplit([0.7, 0.3])

    # Train a DecisionTree model.
    #  Empty categoricalFeaturesInfo indicates all features are continuous.
    model = DecisionTree.trainClassifier(trainingData, numClasses=2, categoricalFeaturesInfo={},
                                        impurity='gini', maxDepth=5, maxBins=32)

    # Evaluate model on test instances and compute test error
    predictions = model.predict(testData.map(lambda x: x.features))
    labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
    testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testData.count())
    print('Test Error = ' + str(testErr))
    print('Learned classification tree model:')
    print(model.toDebugString())

    # Save and load model
    model.save(sc, "/home/pyspark-experiments/data/myDecisionTreeClassificationModel")
    sameModel = DecisionTreeModel.load(sc, "target/tmp/myDecisionTreeClassificationModel")