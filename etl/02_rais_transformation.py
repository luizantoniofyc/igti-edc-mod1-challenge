import sys
import logging
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Configuracao de logs de aplicacao
logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger('datalake_rais_ingestion')
logger.setLevel(logging.DEBUG)

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sparkContext = SparkContext()
glueContext = GlueContext(sparkContext)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger.info("Reading data from rais...")
rais = (
    spark
    .read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ";")
    .option("encoding", "latin1")
    .load("s3://datalake-luizantoniolima-igti-challenge/raw-data/rais/")
)

logger.info("Writing rais data as parquet...")
(
    rais
    .write
    .mode("overwrite")
    .format("parquet")
    .partitionBy('ano', 'uf')
    .save("s3://datalake-luizantoniolima-igti-challenge/staging/rais")
)