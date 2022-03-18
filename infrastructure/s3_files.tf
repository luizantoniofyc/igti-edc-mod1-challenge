resource "aws_s3_bucket_object" "raw_data" {
    bucket = aws_s3_bucket.dl.id
    key    = "/raw-data/rais/"
    acl    = "private"
    source = "null"
}

resource "aws_s3_bucket_object" "staging" {
    bucket = aws_s3_bucket.dl.id
    key    = "/staging/rais/"
    acl    = "private"
    source = "null"
}

resource "aws_s3_bucket_object" "rais_ingestion" {
  bucket = aws_s3_bucket.dl.id
  key    = "glue-job/pyspark/01_rais_ingestion.py"
  acl    = "private"
  source = "../etl/01_rais_ingestion.py"
  etag   = filemd5("../etl/01_rais_ingestion.py")
}

resource "aws_s3_bucket_object" "rais_transformation" {
  bucket = aws_s3_bucket.dl.id
  key    = "glue-job/pyspark/02_rais_transformation.py"
  acl    = "private"
  source = "../etl/02_rais_transformation.py"
  etag   = filemd5("../etl/02_rais_transformation.py")
}

resource "aws_s3_bucket_object" "build_lambda_package" {
  bucket = aws_s3_bucket.dl.id
  key    = "glue-job/build_lambda_package.sh"
  acl    = "private"
  source = "../scripts/build_lambda_package.sh"
  etag   = filemd5("../scripts/build_lambda_package.sh")
}