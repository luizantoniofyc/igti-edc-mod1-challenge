import boto3

def handler(event, context):
    """
    Lambda function that starts a job flow in GLUE for data transformation. 
    """
    job_name = 'glue-job-rais'

    client = boto3.client('glue', region_name='us-east-1')

    job_run_id = client.start_job_run(JobName=job_name)

    running = True
    while running:
        running = getStatus()
        
    crawler_run_id = client.start_crawler(
        Name='rais-glue-crawler'
    )

    def getStatus():
        status_detail = client.get_job_run(JobName=job_name, RunId = job_run_id.get("JobRunId"))
        status = status_detail.get("JobRun").get("JobRunState")
        if (status == 'SUCCEEDED'):
            return False
        return True

    return {
        'statusCode': 200,
        'body': f"Started glue crawler {crawler_run_id}"
    }