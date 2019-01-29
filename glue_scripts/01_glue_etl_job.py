import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "landing", table_name = "playbacks", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "landing", table_name = "playbacks_small", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("userid", "string", "userid", "string"), ("timestamp", "string", "timestamp", "string"), ("artname", "string", "artname", "string"), ("traname", "string", "traname", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("userid", "string", "userid", "string"), ("timestamp", "string", "timestamp", "string"), ("artname", "string", "artname", "string"), ("traname", "string", "traname", "string")], transformation_ctx = "applymapping1")
## @type: SelectFields
## @args: [paths = ["userid", "timestamp", "artname", "traname"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]Í
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["userid", "timestamp", "artname", "traname"], transformation_ctx = "selectfields2")
## @type: ResolveChoice
## @args: [choice = "MATCH_CATALOG", database = "refined", table_name = "results", transformation_ctx = "resolvechoice3"]
## @return: resolvechoice3
## @inputs: [frame = selectfields2]
resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "refined", table_name = "results", transformation_ctx = "resolvechoice3")
## @type: DataSink
## @args: [database = "refined", table_name = "results", transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = resolvechoice3]
datasink4 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice3, database = "refined", table_name = "results", transformation_ctx = "datasink4")
job.commit()