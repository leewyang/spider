
import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("spider-datagen")
    .config("spark.jars", "/home/leey/devpub/spider/sqlite-jdbc-3.45.2.0.jar")
    .config("spark.driver.extraClassPath", "/home/leey/devpub/spider/sqlite-jdbc-3.45.2.0.jar")
    .getOrCreate()
)

# CLASS

# Code snippet generated with Databricks Labs Data Generator (`dbldatagen`) DataAnalyzer class
# Install with `pip install dbldatagen` or in notebook with `%pip install dbldatagen`
# See the following resources for more details:
#
#   Getting Started - [https://databrickslabs.github.io/dbldatagen/public_docs/APIDOCS.html]
#   Github project - [https://github.com/databrickslabs/dbldatagen]
#
import dbldatagen as dg
import pyspark.sql.types

# Column definitions are stubs only - modify to generate correct data  
#
generation_spec = (
    dg.DataGenerator(sparkSession=spark, 
                     name='synthetic_data', 
                     rows=10000,
                     random=True,
                     )
    .withColumn('CLASS_CODE', 'string', template=r'\\w')
    .withColumn('CRS_CODE', 'string', template=r'\\w')
    .withColumn('CLASS_SECTION', 'string', template=r'\\w')
    .withColumn('CLASS_TIME', 'string', template=r'\\w')
    .withColumn('CLASS_ROOM', 'string', template=r'\\w')
    .withColumn('PROF_NUM', 'bigint', minValue=105, maxValue=342)
    )
df_synthetic = generation_spec.build()
df_synthetic.write.mode('overwrite').parquet('CLASS.parquet')

# COURSE

# Code snippet generated with Databricks Labs Data Generator (`dbldatagen`) DataAnalyzer class
# Install with `pip install dbldatagen` or in notebook with `%pip install dbldatagen`
# See the following resources for more details:
#
#   Getting Started - [https://databrickslabs.github.io/dbldatagen/public_docs/APIDOCS.html]
#   Github project - [https://github.com/databrickslabs/dbldatagen]
#
import dbldatagen as dg
import pyspark.sql.types

# Column definitions are stubs only - modify to generate correct data  
#
generation_spec = (
    dg.DataGenerator(sparkSession=spark, 
                     name='synthetic_data', 
                     rows=10000,
                     random=True,
                     )
    .withColumn('CRS_CODE', 'string', template=r'\\w')
    .withColumn('DEPT_CODE', 'string', template=r'\\w')
    .withColumn('CRS_DESCRIPTION', 'string', template=r'\\w')
    .withColumn('CRS_CREDIT', 'float', minValue=3.0, maxValue=4.0, step=0.1)
    )
df_synthetic = generation_spec.build()
df_synthetic.write.mode('overwrite').parquet('COURSE.parquet')

# DEPARTMENT

# Code snippet generated with Databricks Labs Data Generator (`dbldatagen`) DataAnalyzer class
# Install with `pip install dbldatagen` or in notebook with `%pip install dbldatagen`
# See the following resources for more details:
#
#   Getting Started - [https://databrickslabs.github.io/dbldatagen/public_docs/APIDOCS.html]
#   Github project - [https://github.com/databrickslabs/dbldatagen]
#
import dbldatagen as dg
import pyspark.sql.types

# Column definitions are stubs only - modify to generate correct data  
#
generation_spec = (
    dg.DataGenerator(sparkSession=spark, 
                     name='synthetic_data', 
                     rows=10000,
                     random=True,
                     )
    .withColumn('DEPT_CODE', 'string', template=r'\\w')
    .withColumn('DEPT_NAME', 'string', template=r'\\w')
    .withColumn('SCHOOL_CODE', 'string', template=r'\\w')
    .withColumn('EMP_NUM', 'bigint', minValue=103, maxValue=435)
    .withColumn('DEPT_ADDRESS', 'string', template=r'\\w')
    .withColumn('DEPT_EXTENSION', 'string', template=r'\\w')
    )
df_synthetic = generation_spec.build()
df_synthetic.write.mode('overwrite').parquet('DEPARTMENT.parquet')

# EMPLOYEE

# Code snippet generated with Databricks Labs Data Generator (`dbldatagen`) DataAnalyzer class
# Install with `pip install dbldatagen` or in notebook with `%pip install dbldatagen`
# See the following resources for more details:
#
#   Getting Started - [https://databrickslabs.github.io/dbldatagen/public_docs/APIDOCS.html]
#   Github project - [https://github.com/databrickslabs/dbldatagen]
#
import dbldatagen as dg
import pyspark.sql.types

# Column definitions are stubs only - modify to generate correct data  
#
generation_spec = (
    dg.DataGenerator(sparkSession=spark, 
                     name='synthetic_data', 
                     rows=10000,
                     random=True,
                     )
    .withColumn('EMP_NUM', 'bigint', minValue=100, maxValue=435)
    .withColumn('EMP_LNAME', 'string', template=r'\\w')
    .withColumn('EMP_FNAME', 'string', template=r'\\w')
    .withColumn('EMP_INITIAL', 'string', template=r'\\w')
    .withColumn('EMP_JOBCODE', 'string', template=r'\\w')
    .withColumn('EMP_HIREDATE', 'string', template=r'\\w')
    .withColumn('EMP_DOB', 'string', template=r'\\w')
    )
df_synthetic = generation_spec.build()
df_synthetic.write.mode('overwrite').parquet('EMPLOYEE.parquet')

# ENROLL

# Code snippet generated with Databricks Labs Data Generator (`dbldatagen`) DataAnalyzer class
# Install with `pip install dbldatagen` or in notebook with `%pip install dbldatagen`
# See the following resources for more details:
#
#   Getting Started - [https://databrickslabs.github.io/dbldatagen/public_docs/APIDOCS.html]
#   Github project - [https://github.com/databrickslabs/dbldatagen]
#
import dbldatagen as dg
import pyspark.sql.types

# Column definitions are stubs only - modify to generate correct data  
#
generation_spec = (
    dg.DataGenerator(sparkSession=spark, 
                     name='synthetic_data', 
                     rows=10000,
                     random=True,
                     )
    .withColumn('CLASS_CODE', 'string', template=r'\\w')
    .withColumn('STU_NUM', 'bigint', minValue=321452, maxValue=324257)
    .withColumn('ENROLL_GRADE', 'string', template=r'\\w')
    )
df_synthetic = generation_spec.build()
df_synthetic.write.mode('overwrite').parquet('ENROLL.parquet')

# PROFESSOR

# Code snippet generated with Databricks Labs Data Generator (`dbldatagen`) DataAnalyzer class
# Install with `pip install dbldatagen` or in notebook with `%pip install dbldatagen`
# See the following resources for more details:
#
#   Getting Started - [https://databrickslabs.github.io/dbldatagen/public_docs/APIDOCS.html]
#   Github project - [https://github.com/databrickslabs/dbldatagen]
#
import dbldatagen as dg
import pyspark.sql.types

# Column definitions are stubs only - modify to generate correct data  
#
generation_spec = (
    dg.DataGenerator(sparkSession=spark, 
                     name='synthetic_data', 
                     rows=10000,
                     random=True,
                     )
    .withColumn('EMP_NUM', 'bigint', minValue=103, maxValue=435)
    .withColumn('DEPT_CODE', 'string', template=r'\\w')
    .withColumn('PROF_OFFICE', 'string', template=r'\\w')
    .withColumn('PROF_EXTENSION', 'string', template=r'\\w')
    .withColumn('PROF_HIGH_DEGREE', 'string', template=r'\\w')
    )
df_synthetic = generation_spec.build()
df_synthetic.write.mode('overwrite').parquet('PROFESSOR.parquet')

# STUDENT

# Code snippet generated with Databricks Labs Data Generator (`dbldatagen`) DataAnalyzer class
# Install with `pip install dbldatagen` or in notebook with `%pip install dbldatagen`
# See the following resources for more details:
#
#   Getting Started - [https://databrickslabs.github.io/dbldatagen/public_docs/APIDOCS.html]
#   Github project - [https://github.com/databrickslabs/dbldatagen]
#
import dbldatagen as dg
import pyspark.sql.types

# Column definitions are stubs only - modify to generate correct data  
#
generation_spec = (
    dg.DataGenerator(sparkSession=spark, 
                     name='synthetic_data', 
                     rows=10000,
                     random=True,
                     )
    .withColumn('STU_NUM', 'bigint', minValue=321452, maxValue=324299)
    .withColumn('STU_LNAME', 'string', template=r'\\w')
    .withColumn('STU_FNAME', 'string', template=r'\\w')
    .withColumn('STU_INIT', 'string', template=r'\\w')
    .withColumn('STU_DOB', 'string', template=r'\\w')
    .withColumn('STU_HRS', 'bigint', minValue=15, maxValue=120)
    .withColumn('STU_CLASS', 'string', template=r'\\w')
    .withColumn('STU_GPA', 'float', minValue=2.11, maxValue=3.87, step=0.1)
    .withColumn('STU_TRANSFER', 'decimal(38,18)', minValue=0E-18, maxValue=1.000000000000000000)
    .withColumn('DEPT_CODE', 'string', template=r'\\w')
    .withColumn('STU_PHONE', 'string', template=r'\\w')
    .withColumn('PROF_NUM', 'bigint', minValue=199, maxValue=311)
    )
df_synthetic = generation_spec.build()
df_synthetic.write.mode('overwrite').parquet('STUDENT.parquet')

