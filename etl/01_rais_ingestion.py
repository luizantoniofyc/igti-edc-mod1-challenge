from urllib.request import urlretrieve
import py7zr
import os
import sys
import logging
import boto3

# Configuracao de logs de aplicacao
logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger('datalake_rais_ingestion')
logger.setLevel(logging.DEBUG)

basepath = "./opt/ml/processing/output"
dlpath = f"{basepath}/rais"

names = [
     "RAIS_VINC_PUB_CENTRO_OESTE.7z",
     "RAIS_VINC_PUB_NORDESTE.7z",
     "RAIS_VINC_PUB_NORTE.7z",
     "RAIS_VINC_PUB_SUL.7z",
     "RAIS_VINC_PUB_MG_ES_RJ.7z",
     "RAIS_VINC_PUB_SP.7z",
]

urls = [
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2020/RAIS_VINC_PUB_CENTRO_OESTE.7z",
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2020/RAIS_VINC_PUB_NORDESTE.7z",
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2020/RAIS_VINC_PUB_NORTE.7z",
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2020/RAIS_VINC_PUB_SUL.7z",
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2020/RAIS_VINC_PUB_MG_ES_RJ.7z",
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2020/RAIS_VINC_PUB_SP.7z",
]

s3_client = boto3.client('s3')

def upload_file_s3(filename, name):
    s3_client.upload_file(
        filename, 
        "datalake-luizantoniolima-igti-challenge", 
        f"raw-data/rais/{name}")

def upload_raw_data(url, name):
    logger.info(f"Uploading from {url}")
    try:
        filename = dlpath + '/' + name
        urlretrieve(url, filename=filename)
        archive = py7zr.SevenZipFile(filename)
        archive.extractall(path=dlpath)
        archive.close()
        upload_file_s3(filename, archive.filename)
        os.remove(filename)
        os.remove(archive.filename)
        logger.info(f"Successfully uploaded {archive.filename} to s3.")
    except Exception as e:
        logger.error("Error on uploading raw data to s3: " + str(e))

if __name__ == "__main__":
    os.makedirs(dlpath, exist_ok=True)

    for i in range(len(urls)):
        upload_raw_data(urls[i], names[i])

    logger.info("DONE")