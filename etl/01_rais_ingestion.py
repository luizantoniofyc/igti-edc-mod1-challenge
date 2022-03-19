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

filenames = [
     "RAIS_VINC_PUB_CENTRO_OESTE.txt",
     "RAIS_VINC_PUB_NORDESTE.txt",
     "RAIS_VINC_PUB_NORTE.txt",
     "RAIS_VINC_PUB_SUL.txt",
     "RAIS_VINC_PUB_MG_ES_RJ.txt",
     "RAIS_VINC_PUB_SP.txt",
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

def upload_file_s3(fullpath, name):
    s3_client.upload_file(
        fullpath, 
        "datalake-luizantoniolima-igti-challenge", 
        f"raw-data/rais/{name}")

def download_from_rais(url, name):
    try:
        logger.info(f"Downloading and extracting from {url}")
        filename = dlpath + '/' + name
        urlretrieve(url, filename=filename)
        archive = py7zr.SevenZipFile(filename)
        archive.extractall(path=dlpath)
        archive.close()
        logger.info(f"Successfully downloaded and extracted {archive.filename}.")
    except Exception as e:
        logger.error(f"Error on downloading or extracting from {url}: " + str(e))

def upload_to_s3(name):
    try:
        logger.info(f"Uploading {name} to s3.")
        filename = dlpath + '/' + name
        upload_file_s3(filename, name)
        logger.info(f"Successfully uploaded {name} to s3.")
    except Exception as e:
        logger.error(f"Error on uploading {name} to s3: " + str(e))

if __name__ == "__main__":
    os.makedirs(dlpath, exist_ok=True)

    for i in range(len(urls)):
        download_from_rais(urls[i], names[i])

    for i in range(len(filenames)):
        upload_to_s3(filenames[i])

    logger.info("DONE")