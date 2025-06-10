from main import run_etl

def lambda_handler(event, context):
    run_etl()
    return {
        'statusCode': 200,
        'body': 'ETL job executed successfully'
    }
