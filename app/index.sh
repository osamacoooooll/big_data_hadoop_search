#!/bin/bash
echo "Running MapReduce indexing using Hadoop Streaming"

HDFS_INPUT="/data"
HDFS_OUTPUT="/index/output"

echo "Using HDFS input: $HDFS_INPUT"

# Clean previous output
hdfs dfs -rm -r -f $HDFS_OUTPUT

# Run the Hadoop streaming job
mapred streaming \
  -input $HDFS_INPUT \
  -output $HDFS_OUTPUT \
  -mapper "python3 mapper1.py" \
  -reducer "python3 reducer1.py" \
  -file mapreduce/mapper1.py \
  -file mapreduce/reducer1.py 

# View output
echo -e "\nIndexing complete. Output:"
hdfs dfs -cat $HDFS_OUTPUT/part-* > index_output.txt

# Run the Cassandra loader
python3 app.py index_output.txt