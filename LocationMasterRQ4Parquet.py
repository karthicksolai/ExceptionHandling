# -*- coding: utf-8 -*-
#This module for KPI "# of leads >=10 GA"  for report 
#from __future__ import print_function
from pyspark.sql import SparkSession,SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import split
import sys,os
import logging
from datetime import datetime
import collections
from pyspark.sql.types import StructType,StringType,IntegerType
from pyspark.sql.types import *
from py4j.protocol import Py4JJavaError
from pyspark.sql.functions import lit
       
#if len(sys.argv) != 2:
#    exit()

LocationMasterList = sys.argv[1]
BAELocation = sys.argv[2]
DealerCodes = sys.argv[3]
MultiTrackerpath = sys.argv[4]
SpringMobileStoreList = sys.argv[5]
Storeoutput = sys.argv[6]
StoreFileTime = sys.argv[7]

#for S3 run.....
spark = SparkSession.builder.\
        appName("LocationMasterRQ4Parquet").getOrCreate()

#exception code testing

try:
  
  


#########################################################################################################
# Reading the source data files #
#########################################################################################################
    try:
        
        dfLocationMaster = spark.read.format("com.databricks.spark.csv").\
                        option("header", "true").\
                        option("treatEmptyValuesAsNulls", "true").\
                        option("inferSchema", "true").\
                        load(LocationMasterList)

    except     (FileNotFoundError, IOError):

        print("Wrong file or file path")

                        
    fields = LocationMasterList.split('\\')
    newformat = ''
    if len(fields) > 0:
        filenamefield = fields[len(fields)-1].split('_')
        if len(filenamefield) > 0 :
            datefield = filenamefield[len(filenamefield)-1].split('.')
            oldformat = datefield[0]
            print(oldformat)
            datetimeobject = datetime.strptime(oldformat,'%Y%m%d%H%M')
            newformat = datetimeobject.strftime('%m/%d/%Y')
            print(newformat)
        
    dfLocationMaster = dfLocationMaster.withColumn('report_date',lit(newformat))

    try:
        
        
    
    dfBAE = spark.read.format("com.databricks.spark.csv").\
                option("header", "true").\
                option("treatEmptyValuesAsNulls", "true").\
                option("inferSchema", "true").\
                load(BAELocation)
    
    dfBAE = dfBAE.withColumnRenamed("Store Number", "StoreNo")
    
    
    dfDealer = spark.read.format("com.databricks.spark.csv").\
                option("header", "true").\
                option("treatEmptyValuesAsNulls", "true").\
                option("inferSchema", "true").\
                load(DealerCodes)
    
    #24
    dfDealer = dfDealer.withColumnRenamed("Dealer Code", "DealerCode").\
                withColumnRenamed("Loc #", "Loc#").\
                withColumnRenamed("Retail IQ", "RetailIQ").\
                withColumnRenamed("ATT Mkt Abbrev", "ATTMktAbbrev").\
                withColumnRenamed("ATT Market Name", "ATTMarketName").\
                withColumnRenamed("Dispute Mkt", "DisputeMkt").\
                withColumnRenamed("WS Expires", "WSExpires").\
                withColumnRenamed("Footprint Level", "FootprintLevel").\
                withColumnRenamed("Business Expert", "BusinessExpert").\
                withColumnRenamed("DF Code", "DFCode").\
                withColumnRenamed("Old 2", "Old2").\
                withColumnRenamed("ATT Location Name", "ATTLocationName").\
                withColumnRenamed("ATT Location ID", "ATTLocationID").\
                withColumnRenamed("ATT Region", "ATTRegion").\
                withColumnRenamed("Open Date", "OpenDate").\
                withColumnRenamed("Close Date", "CloseDate").\
                withColumnRenamed("DC Origin", "DCOrigin").\
                withColumnRenamed("Store Origin", "StoreOrigin").\
                withColumnRenamed("Acquisition Origin", "AcquisitionOrigin").\
                withColumnRenamed("TB Loc", "TBLoc").\
                withColumnRenamed("SMF Mapping", "SMFMapping").\
                withColumnRenamed("SMF Market", "SMFMarket").\
                withColumnRenamed("DC status", "DCstatus").\
                withColumnRenamed("Sorting Rank", "SortingRank").\
                withColumnRenamed("Rank Description", "RankDescription")
    
    
                
    dfMultiTracker = spark.read.format("com.databricks.spark.csv").\
  		option("header", "true").\
                    option("treatEmptyValuesAsNulls", "true").\
                    option("inferSchema", "true").\
                    load(MultiTrackerpath)
#load("GS_FR_SF\\GS_SrcClean\\multiTracker_2017-08-23T11_40_14.048Z.csv")
                    
                                
    dfMultiTracker = dfMultiTracker.withColumnRenamed("Formula Link", "FormulaLink").\
                withColumnRenamed("AT&T Region", "AT&TRegion").\
                withColumnRenamed("AT&T Market", "AT&TMarket").\
                withColumnRenamed("Spring Market", "SpringMarket").\
                withColumnRenamed("Store Name", "StoreName").\
                withColumnRenamed("Street Address", "StreetAddress").\
                withColumnRenamed("Square Feet", "Square Feet").\
                withColumnRenamed("Total Monthly Rent", "TotalMonthlyRent").\
                withColumnRenamed("Lease Expiration", "LeaseExpiration").\
                withColumnRenamed("June 2017 Total Ops", "June2017TotalOps").\
                withColumnRenamed("Average Last 12 Months Ops", "AverageLast12MonthsOps").\
                withColumnRenamed("Average Traffic Count Last 12 Months", "AverageTrafficCountLast12Months").\
                withColumnRenamed("June SMF", "JuneSMF").\
                withColumnRenamed("Dealer Code", "DealerCode").\
                withColumnRenamed("Exterior Photo", "ExteriorPhoto").\
                withColumnRenamed("Interior Photo", "InteriorPhoto").\
                withColumnRenamed("Build Type", "BuildType").\
                withColumnRenamed("Store Type", "StoreType").\
                withColumnRenamed("C&C Designation", "C&CDesignation").\
                withColumnRenamed("Remodel or Open Date", "RemodelorOpenDate").\
                withColumnRenamed("Authorized RetailerTagLine", "AuthorizedRetailerTagLine").\
                withColumnRenamed("Pylon/Monument Panels", "Pylon/MonumentPanels").\
                withColumnRenamed("Selling Walls", "SellingWalls").\
                withColumnRenamed("Memorable Accessory Wall", "MemorableAccessoryWall").\
                withColumnRenamed("Cash Wrap Expansion", "CashWrapExpansion").\
                withColumnRenamed("Window Wrap Grpahics", "WindowWrapGrpahics").\
                withColumnRenamed("Live DTV", "LiveDTV").\
                withColumnRenamed("Learning Tables", "LearningTables").\
                withColumnRenamed("Community Table", "CommunityTable").\
                withColumnRenamed("Diamond Displays", "DiamondDisplays").\
                withColumnRenamed("C Fixtures", "CFixtures").\
                withColumnRenamed("TIO Kiosk", "TIOKiosk").\
                withColumnRenamed("Approved for Flex Blade", "ApprovedforFlexBlade").\
                withColumnRenamed("Cap Index Score", "CapIndexScore").\
                withColumnRenamed("Selling Walls Notes", "SellingWallsNotes")
    
#dfMultiTracker.printSchema() 
    SpringMobile_Schema = StructType() .\
                        add("StoreNo", StringType(), True). \
                        add("StoreName", StringType(), True). \
                        add("Address", StringType(), True). \
                        add("City", StringType(), True). \
                        add("State", StringType(), True). \
                        add("Zip", StringType(), True). \
                        add("Market", StringType(), True). \
                        add("Region", StringType(), True). \
                        add("District", StringType(), True). \
                        add("OpenDate", StringType(), True). \
                        add("MarketVP", StringType(), True). \
                        add("RegionDirector", StringType(), True). \
                        add("DistrictManager", StringType(), True). \
                        add("Useless", StringType(), True). \
                        add("Classification", StringType(), True). \
                        add("AcquisitionName", StringType(), True). \
                        add("StoreTier", StringType(), True). \
                        add("SqFt", StringType(), True). \
                        add("SqFtRange", StringType(), True). \
                        add("ClosedDate", StringType(), True). \
                        add("Status", StringType(), True). \
                        add("Attribute", StringType(), True)
    
    dfSpringMobile = spark.read.format("com.crealytics.spark.excel").\
                option("location", SpringMobileStoreList).\
                option("sheetName", "StoreSummary").\
                option("treatEmptyValuesAsNulls", "true").\
                option("addColorColumns", "false").\
                option("inferSchema", "false").\
                schema(SpringMobile_Schema). \
                option("spark.read.simpleMode","true"). \
                option("useHeader", "true").\
                load("com.databricks.spark.csv")
                    
    dfSpringMobile = dfSpringMobile.withColumnRenamed("Store #", "Store").\
                withColumnRenamed("Store Name", "StoreName").\
                withColumnRenamed("Open Date", "OpenDate").\
                withColumnRenamed("Market VP", "MarketVP").\
                withColumnRenamed("Region Director", "RegionDirector").\
                withColumnRenamed("District Manager", "DistrictManager").\
                withColumnRenamed("Acquisition Name", "AcquisitionName").\
                withColumnRenamed("Store Tier", "StoreTier").\
                withColumnRenamed("Sq Ft", "SqFt").\
                withColumnRenamed("Sq Ft Range", "SqFtRange").\
                withColumnRenamed("Closed Date", "ClosedDate")
                                
#########################################################################################################
# Reading the source data files #
#########################################################################################################

    todayyear = datetime.now().strftime('%Y')
    todaymonth = datetime.now().strftime('%m')
    TodayFolderName = datetime.now().strftime('%Y%m%d%H%M')
    dfLocationMaster.coalesce(1).select("*"). \
    write.parquet(Storeoutput + '/' + todayyear + '/' + todaymonth + '/' + 'location' + StoreFileTime);
    dfBAE.coalesce(1).select("*"). \
    write.parquet(Storeoutput + '/' + todayyear + '/' + todaymonth + '/' + 'BAE' + StoreFileTime);
    dfDealer.coalesce(1).select("*"). \
    write.parquet(Storeoutput + '/' + todayyear + '/' + todaymonth + '/' + 'Dealer' + StoreFileTime);
    
    #dfMultiTracker.coalesce(1).select("*").write.parquet("output");	
    dfMultiTracker.coalesce(1).select("*"). \
    write.parquet(Storeoutput + '/' + todayyear + '/' + todaymonth + '/' + 'multiTracker' + StoreFileTime);
    
    dfSpringMobile.coalesce(1).select("*"). \
    write.parquet(Storeoutput + '/' + todayyear + '/' + todaymonth + '/' + 'springMobile' + StoreFileTime);

except Exception as err:
    print('GameStop Fileformat Exception'.format(str(err)))
    sys.exit()
spark.stop()





        

        
