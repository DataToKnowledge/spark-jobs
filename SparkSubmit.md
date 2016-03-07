# To Submit a job

There are a few options available that are specific to the cluster manager that is being used. For example, with a Spark standalone cluster with cluster deploy mode, you can also specify --supervise to make sure that the driver is automatically restarted if it fails with non-zero exit code. To enumerate all such options available to spark-submit, run it with --help. Here are a few examples of common options:

## Run ExtractQueryTerms

### Client Mode

This is useful if you want to try the job

```bash

# Run on a Spark standalone cluster in client deploy mode
./bin/spark-submit \
  --class it.dtk.streaming.ExtractQueryTerms \
  --master spark://spark-master-0:7077 \
  --executor-memory 512M \
  --total-executor-cores 2 \
  ./spark-jobs-assembly-0.1.0.jar \
  prod

```

Moreover you can use local[2] as master to run all the workers locally


### Cluster Mode

```bash

# Run on a Spark standalone cluster in cluster deploy mode with supervise
./bin/spark-submit \
  --class it.dtk.streaming.ExtractQueryTerms \
  --master spark://spark-master-0:6066  \
  --deploy-mode cluster \
  --supervise \
  --executor-memory 1G \
  --total-executor-cores 2 \
  ./spark-jobs-assembly-0.1.0.jar \
  prod

```

## Run ExtractFeeds

### Client Mode

This is useful if you want to try the job

```bash

# Run on a Spark standalone cluster in client deploy mode
./bin/spark-submit \
  --class it.dtk.streaming.ExtractFeeds \
  --master spark://spark-master-0:7077 \
  --executor-memory 1G \
  --total-executor-cores 2 \
  ./spark-jobs-assembly-0.1.0.jar \
  prod

```

Moreover you can use local[2] as master to run all the workers locally


### Cluster Mode

```bash

# Run on a Spark standalone cluster in cluster deploy mode with supervise
./bin/spark-submit \
  --class it.dtk.streaming.ExtractFeeds \
  --master spark://spark-master-0:6066  \
  --deploy-mode cluster \
  --supervise \
  --executor-memory 1G \
  --total-executor-cores 2 \
  ./spark-jobs-assembly-0.1.0.jar \
  prod

```


## Run TagArticles

### Client Mode

This is useful if you want to try the job

```bash

# Run on a Spark standalone cluster in client deploy mode
./bin/spark-submit \
  --class it.dtk.streaming.TagArticles \
  --master spark://spark-master-0:7077 \
  --executor-memory 512M \
  --total-executor-cores 2 \
  ./spark-jobs-assembly-0.1.0.jar \
  prod

```

Moreover you can use local[2] as master to run all the workers locally

```bash

# Run on a Spark standalone cluster in client deploy mode
./bin/spark-submit \
  --class it.dtk.streaming.TagArticles \
  --master local[2] \
  --executor-memory 1G \
  --total-executor-cores 2 \
  ./spark-jobs-assembly-0.1.0.jar \
  prod

```

### Cluster Mode

```bash

# Run on a Spark standalone cluster in cluster deploy mode with supervise
./bin/spark-submit \
  --class it.dtk.streaming.TagArticles \
  --master spark://spark-master-0:6066  \
  --deploy-mode cluster \
  --supervise \
  --executor-memory 1G \
  --total-executor-cores 2 \
  ./spark-jobs-assembly-0.1.0.jar \
  prod

```
