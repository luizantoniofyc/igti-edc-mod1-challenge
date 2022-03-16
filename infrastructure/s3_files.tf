resource "aws_s3_bucket_object" "rais_ingestion" {
  bucket = aws_s3_bucket.dl.id
  key    = "emr-code/pyspark/01_rais_ingestion.py"
  acl    = "private"
  source = "../etl/01_rais_ingestion.py"
  etag   = filemd5("../etl/01_rais_ingestion.py")
}

resource "aws_s3_bucket_object" "build_lambda_package" {
  bucket = aws_s3_bucket.dl.id
  key    = "emr-code/build_lambda_package.sh"
  acl    = "private"
  source = "../scripts/build_lambda_package.sh"
  etag   = filemd5("../scripts/build_lambda_package.sh")
}