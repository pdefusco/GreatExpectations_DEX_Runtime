#****************************************************************************
# (C) Cloudera, Inc. 2020-2022
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# #  Author(s): Paul de Fusco
#***************************************************************************/

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_suite import ExpectationSuite
import time

run_id = f'{time.time()}'
print(run_id)

data_lake_name = "s3a://go01-demo/" #Edit config value with your CDP Data Lake name from the CDP Management Console

spark = SparkSession.builder.appName('INGEST').config("spark.kubernetes.access.hadoopFileSystems", data_lake_name).getOrCreate()

# Read data from existing spark table
df = spark.sql("SELECT * FROM default.LC_table")
df.show(10)

df = df.select("acc_now_delinq", "acc_open_past_24mths", "addr_state", "all_util", "annual_inc",\
                    "annual_inc_joint", "application_type", "avg_cur_bal", "bc_open_to_buy", "bc_util", "chargeoff_within_12_mths",
                      "delinq_2yrs", "delinq_amnt", "desc", "dti", "dti_joint", "earliest_cr_line", "emp_length", "emp_title", "funded_amnt",
                       "funded_amnt_inv","grade","home_ownership","id","il_util","initial_list_status","inq_fi","inq_last_12m","inq_last_6mths",
                       "installment","int_rate","issue_d","loan_amnt","loan_status","max_bal_bc","member_id","mo_sin_old_il_acct","mo_sin_old_rev_tl_op",
                       "mo_sin_rcnt_rev_tl_op","mo_sin_rcnt_tl")
                       
# If you don't have a table you can use the data in the data folder:
# Upload to cloud storage first and then access with
#df = spark.read.option("inferSchema" , "true").option("header", "true").csv(data_lake_name+"/path/to/lending.csv")

# creating GE wrapper around spark dataframe
gdf = SparkDFDataset(df)

# gdf holds referance to df as gdf.spark_df
# we can access the df as gdf.spark_df

gdf.spark_df.show(10)

expectation_suite_based_on_profiling, validation_result_based_on_profiling = gdf.profile(BasicDatasetProfiler)
# expectation_suite_based_on_profiling is of type great_expectations.core.expectation_suite.ExpectationSuite
# validation_result_based_on_profiling is of type great_expectations.core.expectation_validation_result.ExpectationSuiteValidationResult

print(expectation_suite_based_on_profiling)

# profiling adds the expectations to the default expectation
print(gdf.expectation_suite_name)
print(expectation_suite_based_on_profiling.expectation_suite_name)


# edit the existing expectation
expectation_suite_based_on_profiling.add_expectation(
          expectation_configuration=ExpectationConfiguration(expectation_type="expect_column_values_to_not_be_null",
                                                                       kwargs={'column': 'avg_cur_bal'}), overwrite_existing=True)
expectation_suite_based_on_profiling.remove_expectation(
  expectation_configuration=ExpectationConfiguration(expectation_type="expect_table_row_count_to_be_between",
                                                                       kwargs= {"min_value": 0,
                                                                                "max_value": None
                                                                                }))

# create custom validation suite
custom_expectation_suite = ExpectationSuite(expectation_suite_name="lending.custom")

# now we will add expectations
custom_expectation_suite.add_expectation(ExpectationConfiguration(expectation_type="expect_column_values_to_be_between",
                                                                 kwargs={'column': 'annual_inc', 'min_value': 1, 'max_value':1000000},
                                                                 meta={'reason': 'month should always be in between 1 and 12'}))

custom_validation = gdf.validate(custom_expectation_suite, run_id=run_id)
custom_validation
