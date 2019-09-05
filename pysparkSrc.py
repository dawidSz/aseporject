import json

from pyspark.sql import types, SQLContext
from pyspark import SparkContext


def convert_data(srcFilename, dstFilename):
    with open(srcFilename, 'r') as src:
        collectedData = json.load(src)['data']

    with open(dstFilename, 'w') as dst:
        for record in collectedData:
            convertedRecord = '{},{},{}\n'.format(record[1], record[2], record[3])
            dst.write(convertedRecord)


if __name__ == '__main__':
    spark_ctx = SparkContext()
    sql_ctx = SQLContext(spark_ctx)

    headings = types.StructType([types.StructField('company', types.StringType()),
                                 types.StructField('city', types.StringType()),
                                 types.StructField('name', types.StringType())])

    rdd = spark_ctx.textFile('s3://projektase/preparedData.txt').map(lambda record: record.split(','))
    dataframe = sql_ctx.createDataFrame(rdd, headings)
    sql_ctx.registerDataFrameAsTable(dataframe, 'project_data')

    SQL_querry = 'SELECT company, city, name, COUNT(*) FROM project_data GROUP BY company, city, name HAVING COUNT(*)>0'

    res = sql_ctx.sql(SQL_querry).collect()

    res_headings = types.StructType([types.StructField('company', types.StringType()),
                                     types.StructField('city', types.StringType()),
                                     types.StructField('name', types.StringType()),
                                     types.StructField('occ', types.StringType())])

    sql_ctx.createDataFrame(res, res_headings).write.csv('s3://projektase/results')
