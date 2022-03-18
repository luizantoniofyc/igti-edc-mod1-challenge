resource "aws_glue_job" "glue_job_rais" {
  name = "glue-job-rais"
  role_arn = aws_iam_role.lambda.arn

  command {
    script_location = "s3://${aws_s3_bucket_object.rais_transformation.key}"
  }
}

resource "aws_glue_catalog_database" "rais_catalog_database" {
  name = "rais-catalog-database"
}

resource "aws_glue_catalog_table" "rais_catalog_table" {
  name = "rais"
  database_name = aws_glue_catalog_database.rais_catalog_database.name
}

resource "aws_glue_crawler" "rais_glue_crawler" {
  database_name = aws_glue_catalog_database.rais_catalog_database.name
  name = "rais-glue-crawler"
  role = aws_iam_role.lambda.arn

  catalog_target {
    database_name = aws_glue_catalog_database.rais_catalog_database.name
    tables = [aws_glue_catalog_table.rais_catalog_table.name]
  }

  schema_change_policy {
    delete_behavior = "LOG"
  }

  configuration = <<EOF
{
  "Version":1.0,
  "Grouping": {
    "TableGroupingPolicy": "CombineCompatibleSchemas"
  }
}
EOF
} 