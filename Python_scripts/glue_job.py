from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import sys


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)


appliances_frame = glue_context.create_dynamic_frame.from_catalog(
        database = "poc-etl-db",
        table_name = "ecom_appliances"
)

appliances_df = appliances_frame.toDF()

appliances_df.show()
job.commit()


