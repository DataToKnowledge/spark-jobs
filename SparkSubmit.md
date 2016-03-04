# To Submit a job

There are a few options available that are specific to the cluster manager that is being used. For example, with a Spark standalone cluster with cluster deploy mode, you can also specify --supervise to make sure that the driver is automatically restarted if it fails with non-zero exit code. To enumerate all such options available to spark-submit, run it with --help. Here are a few examples of common options:

```bash



# Run on a Spark standalone cluster in client deploy mode
./bin/spark-submit \
  --class it.dtk.streaming.ExtractQueryTerms \
  --master spark://spark-master-0:7077 \
  --executor-memory 1G \
  --total-executor-cores 3 \
  ./spark-jobs-assembly-0.1.0.jar \
  prod

# Run on a Spark standalone cluster in cluster deploy mode with supervise
./bin/spark-submit \
  --class it.dtk.streaming.ExtractQueryTerms \
  --master spark://spark-master-0:7077 \
  --deploy-mode cluster \
  --supervise \
  --executor-memory 1G \
  --total-executor-cores 3 \
  ./spark-jobs-assembly-0.1.0.jar \
  prod

# Run on a YARN cluster
export HADOOP_CONF_DIR=XXX
./bin/spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master yarn \
  --deploy-mode cluster \  # can be client for client mode
  --executor-memory 20G \
  --num-executors 50 \
  /path/to/examples.jar \
  1000

# Run a Python application on a Spark standalone cluster
./bin/spark-submit \
  --master spark://207.184.161.138:7077 \
  examples/src/main/python/pi.py \
  1000

# Run on a Mesos cluster in cluster deploy mode with supervise
./bin/spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master mesos://207.184.161.138:7077 \
  --deploy-mode cluster
  --supervise
  --executor-memory 20G \
  --total-executor-cores 100 \
  http://path/to/examples.jar \
  1000

```