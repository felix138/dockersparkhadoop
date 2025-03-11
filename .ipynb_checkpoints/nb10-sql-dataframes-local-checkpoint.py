# coding:utf8
import os
import sys
import subprocess

from pyarrow import duration
from pyspark import SparkConf, SparkContext
from pyspark.python.pyspark.shell import sqlContext
from pyspark.sql import SparkSession, Row
from time import time
import urllib.request

# 清除可能指向 Docker 的环境变量
if 'SPARK_MASTER' in os.environ:
    del os.environ['SPARK_MASTER']

if 'SPARK_HOME' in os.environ:
    del os.environ['SPARK_HOME']

if 'SPARK_CONF_DIR' in os.environ:
    del os.environ['SPARK_CONF_DIR']
# check exist for alive sparksession

# 设置 Python 解释器路径，确保使用 Conda 环境中的 Python

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
existing_spark = SparkSession.getActiveSession()
existing_spark.stop()

# setup spark config and using local spark
conf = (
    SparkConf()
    .setAppName("RDD whole1 files")
    .setMaster("local[*]")  # 确保使用本地模式，不连接到 Docker
    .set("spark.executor.memory", "14g")  # 设置内存
    .set("spark.driver.memory", "8g")
    .set("spark.network.timeout", "600s")  # 增加网络超时
    .set("spark.executorEnv.PYSPARK_PYTHON", sys.executable)
    .set("spark.yarn.appMasterEnv.PYSPARK_PYTHON", sys.executable)
    .set("spark.executorEnv.PYTHONHASHSEED", "0")  # 保证所有节点使用相同的Python环境
    .set("spark.python.worker.reuse", "true")
)
print(f"Driver Python version: {sys.version}")
worker_version = subprocess.check_output([os.environ.get('PYSPARK_PYTHON'), '--version'],
                                         stderr=subprocess.STDOUT).decode('utf-8').strip()
print(f"Worker Python version: {worker_version}")

spark = SparkSession.builder.config(conf=conf).getOrCreate()

sc = spark.sparkContext

print("Spark application is running locally")
print(f"Using Python interpreter: {sys.executable}")

# 下载文件到本地路径
local_file_path = "C:/root/kddcup.data_10_percent.gz"
urllib.request.urlretrieve("http://kdd.ics.uci.edu/databases/kddcup99/kddcup.data_10_percent.gz", local_file_path)

# 使用 format 方法构造数据文件路径，确保以 file:/// 开头
data_file = "file:///{}".format(local_file_path.replace("\\", "/"))

print("Data file path:", data_file)

# loading data and with setup num of Partitions
raw_data = sc.textFile(data_file, minPartitions=10).cache()
# take top 10 row
sample_data = raw_data.take(10)
for line in sample_data:
    print(line)

csv_data = raw_data.map(lambda l: l.split(","))
row_data = csv_data.map(lambda p: Row(
    duration=int(p[0]),
    protocol_type=p[1],
    service=p[2],
    flag=p[3],
    src_bytes=int(p[4]),
    dst_bytes=int(p[5])
))

# 创建 DataFrame 并注册为临时表
interaction_df = spark.createDataFrame(row_data)
interaction_df.createOrReplaceTempView("interactions")

# 执行 SQL 查询
tcp_interactions = spark.sql("""
    SELECT duration, dst_bytes 
    FROM interactions 
    WHERE protocol_type = 'tcp' 
    AND duration > 1000 
    AND dst_bytes = 0
""")

# show result
tcp_interactions.show()

# to show data Schema
interaction_df.printSchema()
# Output duration together with dst_bytes
# tcp_interactions_out = tcp_interactions.map(lambda p: "Duration: {},Dest.bytes {}".format(p.duration,p.dst_bytes))
tcp_interactions_out = tcp_interactions.rdd.map(
    lambda p: "Duration: {}, Dest.bytes: {}".format(p['duration'], p['dst_bytes']))
for ti_out in tcp_interactions_out.collect():
    print(ti_out)
# Queries as DataFrame operations
t1 = time()
interaction_df.select("protocol_type", "duration", "dst_bytes").groupBy("protocol_type").count().show()
t2 = time()
tt = t2 - t1
print("Query performed in {} seconds".format(round(tt, 3)))


def get_label_type(label):
    if label != "normal":
        return "attack"
    else:
        return "normal"


row_label_data = csv_data.map(lambda p: Row(
    duration=int(p[0]),
    protocol_type=p[1],
    service=p[2],
    flag=p[3],
    src_bytes=int(p[4]),
    dst_bytes=int(p[5]),
    label=get_label_type(p[41])
)
                              )

interaction_labeled_df = spark.createDataFrame(row_label_data)

t1 = time()
interaction_labeled_df.select("label").groupBy("label").count().show()
t2 = time()
tt = t2 - t1
print("Query performed in {} seconds".format(round(tt, 3)))


interaction_labeled_df.select("label","protocol_type","dst_bytes").groupBy("label","protocol_type",interaction_labeled_df.dst_bytes == 0).count().show()

sc.stop()
spark.stop()
