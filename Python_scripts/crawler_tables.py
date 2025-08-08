import boto3


glue_conn = boto3.client("glue")

def create_crawler_table(crawler_name, db_name = '', desc='', s3_path = ''):
    response = glue_conn.create_crawler(
            Name = crawler_name,
            Role = "arn:aws:iam::466414787742:role/GluePOCETLRole",
            DatabaseName = db_name,
            Description = desc,
            Targets = {
                "S3Targets": [
                    {
                        "Path" : s3_path
                    }
                ]
            }
    )
    return response


def lambda_handler(event, context):
    crawler_tables_di = {"poc-automotive-crawler" : {
                                                        "db_name": "poc-etl-db",
                                                        "description": "Create the Automotive crawler",
                                                        "s3_path": "s3://glue-crawler-bucket-28/Automotive/"
                                                    }, 
                            "poc-baby_and_maternity-crawler": {
                                                        "db_name": "poc-etl-db",
                                                        "description": "Create the Baby And Maternity crawler",
                                                        "s3_path": "s3://glue-crawler-bucket-28/Baby_Maternity/"
                                                    }, 
                            "poc-bags_and_luggage-crawler": {
                                                        "db_name": "poc-etl-db",
                                                        "description": "Create the Bags And Luggage crawler",
                                                        "s3_path": "s3://glue-crawler-bucket-28/Bags_Luggage/"
                                                    }}

    for key, val in crawler_tables_di.items():
        db_name, desc, s3_path = tuple(map(val.get, ["db_name", "description", "s3_path"]))
        response  = create_crawler_table(key, db_name, desc, s3_path)