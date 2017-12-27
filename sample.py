# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import split
import sys
import os
import logging
from datetime import datetime
import collections
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql.types import *
from py4j.protocol import Py4JJavaError
from pyspark.sql.functions import lit
from pyspark.sql.functions import col

BAELocation = sys.argv[1]
LocationMaster = sys.argv[2]
MultitrackerGoogle = sys.argv[3]
SpringMobileStoreList = sys.argv[4]
OutputException = sys.argv[5]
OutputException1=sys.argv[6]
OutputException2=sys.argv[7]
OutputException3=sys.argv[8]
OutputException4=sys.argv[9]
OutputException5=sys.argv[10]
OutputException6=sys.argv[11]
OutputException7=sys.argv[12]
OutputException8=sys.argv[14]
OutputException8=sys.argv[15]
Fileformat1=sys.argv[16]
Fileformat2=sys.argv[17]
Fileformat3=sys.argv[18]
Fileformat4=sys.argv[19]
NullFound1=sys.argv[20]
NullFound2=sys.argv[21]
NullFound3=sys.argv[22]
NullFound4=sys.argv[23]
NullFound5=sys.argv[24]
NullFound6=sys.argv[25]

spark = SparkSession.builder.\
    appName("LocationMasterRQ4Parquet").getOrCreate()


class Error(Exception):
    """Base class for other exceptions"""
    pass


class NullValueError1(Error):
    """Raised StoreType not null"""
    pass


try:
    dfBAELocation = spark.read.format("com.databricks.spark.csv").\
        option("header", "true").\
        option("treatEmptyValuesAsNulls", "true").\
        option("inferSchema", "true").\
        load(BAELocation)
except Exception as err:
    print('GameStopFile format Exception') 
    dfOutput1= spark.read.format("com.databricks.spark.csv").\
        option("header", "false").\
        option("treatEmptyValuesAsNulls", "true").\
        option("inferSchema", "true").\
        load(Fileformat1)
           
    dfOutput1.coalesce(1). \
            write.format("com.databricks.spark.csv").\
            option("header", "true").mode("overwrite").save(OutputException)

try:
    dfLocationMaster = spark.read.format("com.databricks.spark.csv").\
        option("header", "true").\
        option("treatEmptyValuesAsNulls", "true").\
        option("inferSchema", "true").\
        load(LocationMaster)
except Exception as err:
    print('GameStopFile format Exception') 
    dfOutput2= spark.read.format("com.databricks.spark.csv").\
        option("header", "false").\
        option("treatEmptyValuesAsNulls", "true").\
        option("inferSchema", "true").\
        load(Fileformat2)
           
    dfOutput2.coalesce(1). \
            write.format("com.databricks.spark.csv").\
            option("header", "true").mode("overwrite").save(OutputException1)


try:
    dfMultitrackerGoogle = spark.read.format("com.databricks.spark.csv").\
        option("header", "true").\
        option("treatEmptyValuesAsNulls", "true").\
        option("inferSchema", "true").\
        load(MultitrackerGoogle)
except Exception as err:
    print('GameStopFile format Exception') 
    dfOutput3= spark.read.format("com.databricks.spark.csv").\
        option("header", "false").\
        option("treatEmptyValuesAsNulls", "true").\
        option("inferSchema", "true").\
        load(Fileformat3)
           
    dfOutput3.coalesce(1). \
            write.format("com.databricks.spark.csv").\
            option("header", "true").mode("overwrite").save(OutputException2)

try:
    dfSpringMobileStoreList = spark.read.format("com.databricks.spark.csv").\
        option("header", "true").\
        option("treatEmptyValuesAsNulls", "true").\
        option("inferSchema", "true").\
        load(SpringMobileStoreList)
except Exception as err:
    print('GameStopFile format Exception') 
    dfOutput4= spark.read.format("com.databricks.spark.csv").\
        option("header", "false").\
        option("treatEmptyValuesAsNulls", "true").\
        option("inferSchema", "true").\
        load(Fileformat4)
           
    dfOutput4.coalesce(1). \
            write.format("com.databricks.spark.csv").\
            option("header", "true").mode("overwrite").save(OutputException3)

try:
    StoreNumbernullcount = dfBAELocation.filter(dfBAELocation['Store Number'].isNull()).count()
    if StoreNumbernullcount > 0:
        raise NullValueError1

except NullValueError1 as ve:
    print('Store Number have NULL records') 
    dfOutput5= spark.read.format("com.databricks.spark.csv").\
        option("header", "false").\
        option("treatEmptyValuesAsNulls", "true").\
        option("inferSchema", "true").\
        load(NullFound1)
           
    dfOutput5.coalesce(1). \
            write.format("com.databricks.spark.csv").\
            option("header", "true").mode("overwrite").save(OutputException1)

try:
    BAEWorkdayIDnullcount = dfBAELocation.filter(dfBAELocation['BAEWorkdayID'].isNull()).count()
    if BAEWorkdayIDnullcount > 0:
        raise NullValueError1

except NullValueError1 as ve:
    print('BAEWorkdayID have NULL records') 
    dfOutput2= spark.read.format("com.databricks.spark.csv").\
        option("header", "false").\
        option("treatEmptyValuesAsNulls", "true").\
        option("inferSchema", "true").\
        load(NullFound1)
           
    dfOutput2.coalesce(1). \
            write.format("com.databricks.spark.csv").\
            option("header", "true").mode("overwrite").save(OutputException1)

try:
    BSISWorkdayIDnullcount = dfBAELocation.filter(dfBAELocation['BSISWorkdayID'].isNull()).count()
    if BSISWorkdayIDnullcount > 0:
        raise NullValueError1

except NullValueError1 as ve:
    print('BSISWorkdayID have NULL records') 
    dfOutput2= spark.read.format("com.databricks.spark.csv").\
        option("header", "false").\
        option("treatEmptyValuesAsNulls", "true").\
        option("inferSchema", "true").\
        load(NullFound1)
           
    dfOutput2.coalesce(1). \
            write.format("com.databricks.spark.csv").\
            option("header", "true").mode("overwrite").save(OutputException1)


try:
    StoreTypenullcount = dfLocationMaster.filter(dfLocationMaster['StoreType'].isNull()).count()
    if StoreTypenullcount > 0:
        raise NullValueError1

except NullValueError1 as ve:
    print('StoreType have NULL records') 
    dfOutput2= spark.read.format("com.databricks.spark.csv").\
        option("header", "false").\
        option("treatEmptyValuesAsNulls", "true").\
        option("inferSchema", "true").\
        load(NullFound1)
           
    dfOutput2.coalesce(1). \
            write.format("com.databricks.spark.csv").\
            option("header", "true").mode("overwrite").save(OutputException1)


try:
    storeIDnullcount = dfLocationMaster.filter(dfLocationMaster['StoreID'].isNull()).count()
    if storeIDnullcount > 0:
        raise NullValueError1

except NullValueError1 as ve:
    print('StoreID have NULL records') 
    dfOutput2= spark.read.format("com.databricks.spark.csv").\
        option("header", "false").\
        option("treatEmptyValuesAsNulls", "true").\
        option("inferSchema", "true").\
        load(NullFound1)
           
    dfOutput2.coalesce(1). \
            write.format("com.databricks.spark.csv").\
            option("header", "true").mode("overwrite").save(OutputException1)


try:
    Abbreviationnullcount = dfLocationMaster.filter(dfLocationMaster['Abbreviation'].isNull()).count()
    if Abbreviationnullcount > 0:
        raise NullValueError1

except NullValueError1 as ve:
    print('Abbreviation have NULL records') 
    dfOutput3= spark.read.format("com.databricks.spark.csv").\
        option("header", "false").\
        option("treatEmptyValuesAsNulls", "true").\
        option("inferSchema", "true").\
        load(NullFound2)
           
    dfOutput3.coalesce(1). \
            write.format("com.databricks.spark.csv").\
            option("header", "true").mode("overwrite").save(OutputException2)
spark.stop()
