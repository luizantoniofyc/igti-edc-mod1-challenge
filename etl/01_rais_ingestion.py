from distutils.log import error
from urllib.request import urlretrieve
import py7zr
import os
import sys
import logging
import boto3
from pyspark.sql import functions as f
from pyspark.sql import SparkSession

# Configuracao de logs de aplicacao
logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger('datalake_rais_ingestion')
logger.setLevel(logging.DEBUG)

basepath = "./opt/ml/processing/output"
dlpath = f"{basepath}/rais"

names = [
     "RAIS_VINC_PUB_CENTRO_OESTE.7z",
    #  "RAIS_VINC_PUB_NORDESTE.7z",
    #  "RAIS_VINC_PUB_NORTE.7z",
    #  "RAIS_VINC_PUB_SUL.7z",
    #  "RAIS_VINC_PUB_MG_ES_RJ.7z",
    #  "RAIS_VINC_PUB_SP.7z",
]

urls = [
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2020/RAIS_VINC_PUB_CENTRO_OESTE.7z",
    # "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2020/RAIS_VINC_PUB_NORDESTE.7z",
    # "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2020/RAIS_VINC_PUB_NORTE.7z",
    # "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2020/RAIS_VINC_PUB_SUL.7z",
    # "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2020/RAIS_VINC_PUB_MG_ES_RJ.7z",
    # "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2020/RAIS_VINC_PUB_SP.7z",
]

logger.info("Creating spark session...")
spark = (
    SparkSession.builder.appName("ChallengeSpark")
    .getOrCreate()
)

s3_client = boto3.client('s3')

def upload_raw_data(url, name):
    logger.info(f"Uploading from {url}")
    try:
        filename = dlpath + '/' + name
        urlretrieve(url, filename=filename)
        s3_client.upload_file(filename, "datalake-luizantoniolima-igti-challenge", f"raw-data/rais/{name}")
        os.remove(filename)
        logger.info(f"Successfully uploaded {name} to s3.")
    except Exception as e:
        logger.error("Error on uploading raw data to s3: " + str(e))

def download_raw_data(name):
    logger.info(f"Downloading {name}")
    try:
        filename = dlpath + '/' + name
        s3_client.download_file("datalake-luizantoniolima-igti-challenge", f"raw-data/rais/{name}", filename)
        archive = py7zr.SevenZipFile(filename)
        archive.extractall(path=dlpath)
        archive.close()
        transformar_dados(filename)
        os.remove(filename)
        logger.info(f"Successfully downloaded {name} from s3.")
    except Exception as e:
        logger.error("Error on downloading raw data from s3: " + str(e))

def transformar_dados(filename):
    try:
        logger.info("Reading data from rais...")
        rais = (
            spark
            .read
            .format("csv")
            .option("header", True)
            .option("inferSchema", True)
            .option("delimiter", ";")
            .option("encoding", "latin1")
            .load(filename)
            # .load("s3://datalake-luizantoniolima-igti-challenge/raw-data/rais/")
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
    except Exception as e:
        logger.error("Error on reading or writing data from spark: " + str(e))
    
if __name__ == "__main__":
    os.makedirs(dlpath, exist_ok=True)

    for i in range(len(urls)):
        upload_raw_data(urls[i], names[i])

    for name in range(len(names)):
        
        res = download_raw_data(names[i])
        logger.info(res)

    logger.info("DONE")