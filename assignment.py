from typing import List, Dict, Union
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
import argparse


class CaseStudy:
    def __init__(self, input_directory : str, output_directory: str):
        self.spark = (SparkSession
                        .builder
                        .master("local[2]")
                        .appName("CaseStudy")
                        .getOrCreate())
        self.spark.sparkContext.setLogLevel("ERROR")
        self.input_directory = input_directory
        self.output_directory = output_directory

    def create_schema(self):
        self.chargeSchema = StructType([
            StructField("CRASH_ID", IntegerType()),
            StructField("UNIT_NBR", IntegerType()),
            StructField("PRSN_NBR", IntegerType()),
            StructField("CHARGE", StringType()),
            StructField("CITATION_NBR", StringType())
        ])
        self.damagesSchema = StructType([
                StructField("CRASH_ID", IntegerType()),
                StructField("DAMAGED_PROPERTY", StringType())
        ])
        self.endorseSchema = StructType([
                StructField("CRASH_ID", IntegerType()),
                StructField("UNIT_NBR", IntegerType()),
                StructField("DRVR_LIC_ENDORS_ID", StringType())
        ])
        self.primaryPersonSchema = StructType([
                StructField("CRASH_ID", IntegerType()),
                StructField("UNIT_NBR", IntegerType()),
                StructField("PRSN_NBR", IntegerType()),
                StructField("PRSN_TYPE_ID", StringType()),
                StructField("PRSN_OCCPNT_POS_ID", StringType()),
                StructField("PRSN_INJRY_SEV_ID", StringType()),
                StructField("PRSN_AGE", StringType()),
                StructField("PRSN_ETHNICITY_ID", StringType()),
                StructField("PRSN_GNDR_ID", StringType()),
                StructField("PRSN_EJCT_ID", StringType()),
                StructField("PRSN_REST_ID", StringType()),
                StructField("PRSN_AIRBAG_ID", StringType()),
                StructField("PRSN_HELMET_ID", StringType()),
                StructField("PRSN_SOL_FL", StringType()),
                StructField("PRSN_ALC_SPEC_TYPE_ID", StringType()),
                StructField("PRSN_ALC_RSLT_ID", StringType()),
                StructField("PRSN_BAC_TEST_RSLT", StringType()),
                StructField("PRSN_DRG_SPEC_TYPE_ID", StringType()),
                StructField("PRSN_DRG_RSLT_ID", StringType()),
                StructField("DRVR_DRG_CAT_1_ID", StringType()),
                StructField("PRSN_DEATH_TIME", StringType()),
                StructField("INCAP_INJRY_CNT", IntegerType()),
                StructField("NONINCAP_INJRY_CNT", IntegerType()),
                StructField("POSS_INJRY_CNT", IntegerType()),
                StructField("NON_INJRY_CNT", IntegerType()),
                StructField("UNKN_INJRY_CNT", IntegerType()),
                StructField("TOT_INJRY_CNT", IntegerType()),
                StructField("DEATH_CNT", IntegerType()),
                StructField("DRVR_LIC_TYPE_ID", StringType()),
                StructField("DRVR_LIC_STATE_ID", StringType()),
                StructField("DRVR_LIC_CLS_ID", StringType()),
                StructField("DRVR_ZIP", StringType())
        ])
        self.restrictSchema = StructType([
                StructField("CRASH_ID", IntegerType()),
                StructField("UNIT_NBR", IntegerType()),
                StructField("DRVR_LIC_RESTRIC_ID", StringType())
        ])
        self.unitsSchema = StructType([
                StructField("CRASH_ID", IntegerType()),
                StructField("UNIT_NBR", IntegerType()),
                StructField("UNIT_DESC_ID", StringType()),
                StructField("VEH_PARKED_FL", StringType()),
                StructField("VEH_HNR_FL", StringType()),
                StructField("VEH_LIC_STATE_ID", StringType()),
                StructField("VIN", StringType()),
                StructField("VEH_MOD_YEAR", StringType()),
                StructField("VEH_COLOR_ID", StringType()),
                StructField("VEH_MAKE_ID", StringType()),
                StructField("VEH_MOD_ID", StringType()),
                StructField("VEH_BODY_STYL_ID", StringType()),
                StructField("EMER_RESPNDR_FL", StringType()),
                StructField("OWNR_ZIP", StringType()),
                StructField("FIN_RESP_PROOF_ID", StringType()),
                StructField("FIN_RESP_TYPE_ID", StringType()),
                StructField("VEH_DMAG_AREA_1_ID", StringType()),
                StructField("VEH_DMAG_SCL_1_ID", StringType()),
                StructField("FORCE_DIR_1_ID", StringType()),
                StructField("VEH_DMAG_AREA_2_ID", StringType()),
                StructField("VEH_DMAG_SCL_2_ID", StringType()),
                StructField("FORCE_DIR_2_ID", StringType()),
                StructField("VEH_INVENTORIED_FL", StringType()),
                StructField("VEH_TRANSP_NAME", StringType()),
                StructField("VEH_TRANSP_DEST", StringType()),
                StructField("CONTRIB_FACTR_1_ID", StringType()),
                StructField("CONTRIB_FACTR_2_ID", StringType()),
                StructField("CONTRIB_FACTR_P1_ID", StringType()),
                StructField("VEH_TRVL_DIR_ID", StringType()),
                StructField("FIRST_HARM_EVT_INV_ID", StringType()),
                StructField("INCAP_INJRY_CNT", IntegerType()),
                StructField("NONINCAP_INJRY_CNT", IntegerType()),
                StructField("POSS_INJRY_CNT", IntegerType()),
                StructField("NON_INJRY_CNT", IntegerType()),
                StructField("UNKN_INJRY_CNT", IntegerType()),
                StructField("TOT_INJRY_CNT", IntegerType()),
                StructField("DEATH_CNT", IntegerType())
        ])


    def transform_existing_schema(self, df:DataFrame, col_types:Dict[str, Union[IntegerType, StringType, FloatType]]):
        cols = [
        F.col(i) if i not in col_types else F.when(F.col(i)=="NA",F.lit(None)).
                        otherwise(F.col(i)).cast(col_types[i])
        for i in df.columns
        ]
        return df.select(*cols)

    def read_from_csv(self):
        self.chargesDF = self.spark.read.csv(self.input_directory+"/Charges_use.csv", 
                                schema=self.chargeSchema, header=True)
        self.damagesDF = self.spark.read.csv(self.input_directory+"/Damages_use.csv", 
                                schema=self.damagesSchema, header=True)
        self.endorseDF = self.spark.read.csv(self.input_directory+"/Endorse_use.csv", 
                                schema=self.endorseSchema, header=True)
        self.primaryPersonRawDF = self.spark.read.csv(self.input_directory+"/Primary_Person_use.csv", 
                                schema=self.primaryPersonSchema, header=True)
        self.restrictDF = self.spark.read.csv(self.input_directory+"/Restrict_use.csv", 
                                schema=self.restrictSchema, header=True)
        self.unitsRawDF = self.spark.read.csv(self.input_directory+"/Units_use.csv", 
                                schema=self.unitsSchema, header=True)
        

    def analysis1(self) -> DataFrame:
        return self.primaryPersonDF.where(
                      (F.col("PRSN_INJRY_SEV_ID")=="KILLED")
                      & 
                      (F.col("PRSN_GNDR_ID")=="MALE")).\
                      select(F.countDistinct("CRASH_ID").
                             alias("crashes_count"))

    def analysis2(self) -> DataFrame:
        return self.unitsDF.where(F.col("VEH_BODY_STYL_ID").
                    isin(["MOTORCYCLE", "POLICE MOTORCYCLE"])).\
                    select(F.countDistinct("CRASH_ID").
                            alias("motorcycle_crash_count"))

    def analysis3(self) -> DataFrame:
        return self.primaryPersonDF.where(
                (F.col("PRSN_GNDR_ID") == "FEMALE")).\
                groupBy("DRVR_LIC_STATE_ID").\
                agg(F.countDistinct("CRASH_ID").alias("crash_count")).\
                orderBy(F.col("crash_count").desc()).\
                limit(1).select("DRVR_LIC_STATE_ID")

    def analysis4(self) -> DataFrame:
        vehicleIdTotalInjury = self.unitsDF.groupBy("VEH_MAKE_ID").\
                        agg(F.sum(
                            F.expr("TOT_INJRY_CNT + DEATH_CNT")).
                            alias("Sum_injury"))

        win = Window.orderBy(F.col("Sum_injury").desc())

        return vehicleIdTotalInjury.select("VEH_MAKE_ID",
                            "Sum_injury",
                            F.row_number().over(win).
                            alias("Row_number")
                            ).where(F.col("Row_number").
                            isin(list(range(5,16)))).\
                            select("VEH_MAKE_ID","Sum_injury")

    def analysis5(self) -> DataFrame :
        styleEthnicityData = self.primaryPersonDF.join(self.unitsDF, 
                     self.primaryPersonDF["CRASH_ID"]==self.unitsDF["CRASH_ID"],
                     "right").drop(self.unitsDF["CRASH_ID"]).\
                     select("CRASH_ID","VEH_BODY_STYL_ID","PRSN_ETHNICITY_ID")

        styleEthnicityDataGroupedDF = styleEthnicityData.\
                        groupBy("VEH_BODY_STYL_ID","PRSN_ETHNICITY_ID").\
                           agg(F.count("CRASH_ID").alias("crashes"))

        ethnic_win = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(F.col("crashes").desc())

        return styleEthnicityDataGroupedDF.withColumn(
                                    "Row_num", 
                                    F.dense_rank().over(ethnic_win)).\
                                    where(F.col("Row_num") == 1).\
                                    orderBy(F.col("crashes").desc()).\
                                    select("VEH_BODY_STYL_ID",
                                        "PRSN_ETHNICITY_ID",
                                           "crashes")

    def analysis6(self) -> DataFrame:
        alcoholContributingDF = self.unitsDF.where(
        (F.col("CONTRIB_FACTR_1_ID").contains("ALCOHOL"))
            | 
        (F.col("CONTRIB_FACTR_1_ID").contains("DRINKING"))
            |
        (F.col("CONTRIB_FACTR_2_ID").contains("ALCOHOL")) 
            | 
        (F.col("CONTRIB_FACTR_2_ID").contains("DRINKING")))

        return self.primaryPersonDF.where(F.col("DRVR_ZIP").isNotNull()).\
                    join(F.broadcast(alcoholContributingDF),
                    alcoholContributingDF["CRASH_ID"] == 
                    self.primaryPersonDF["CRASH_ID"]).\
                    drop(alcoholContributingDF["CRASH_ID"]).\
                    groupBy("DRVR_ZIP").\
                    agg(F.countDistinct("CRASH_ID").
                    alias("count_crash")).\
                    orderBy(F.col("count_crash").desc()).limit(5).\
                    select("DRVR_ZIP")

    def analysis7(self) -> DataFrame:
        distinctDamageIds = self.damagesDF.select("CRASH_ID").distinct()

        unitsChangedDF = self.unitsDF.where(F.col("FIN_RESP_TYPE_ID").
                                contains("INSURANCE")).\
                select("CRASH_ID",
                F.when(F.regexp_extract("VEH_DMAG_SCL_1_ID","\d",0) != "", 
                F.regexp_extract("VEH_DMAG_SCL_1_ID","\d",0)).
                otherwise(F.lit(None)).cast(IntegerType()).alias("DMAG_1"),
                F.when(F.regexp_extract("VEH_DMAG_SCL_2_ID","\d",0) != "", 
                    F.regexp_extract("VEH_DMAG_SCL_2_ID","\d",0)).
                otherwise(F.lit(None)).cast(IntegerType()).alias("DMAG_2"))

        return unitsChangedDF.join(F.broadcast(distinctDamageIds),
                            distinctDamageIds["CRASH_ID"]==
                            unitsChangedDF["CRASH_ID"],
                            "left_anti").drop(distinctDamageIds["CRASH_ID"]).\
                            where((F.col("DMAG_1") > 4) 
                            | (F.col("DMAG_2") > 4)).\
                            agg(F.countDistinct("CRASH_ID").
                                alias("Count_distinct_crashes"))

    def analysis8(self) -> DataFrame:
        colorByCountTop10 = self.unitsDF.groupBy("VEH_COLOR_ID").count().\
                            orderBy(F.col("count").desc()).\
                            limit(10).select("VEH_COLOR_ID").\
                            rdd.flatMap(lambda x: x).\
                            collect()
        # WHERE isin 25 states LIST
        statesByOffenceTop25 = self.unitsDF.join(self.chargesDF,
                     self.unitsDF["CRASH_ID"]==self.chargesDF["CRASH_ID"],
                    ).drop(self.chargesDF["CRASH_ID"]).\
                    groupBy("VEH_LIC_STATE_ID").agg(
                        F.count("CHARGE").
                      alias("count_offences")).\
                    orderBy(F.col("count_offences").desc()).\
                    select("VEH_LIC_STATE_ID").\
                    limit(25).rdd.flatMap(lambda x: x).\
                                    collect()
        # Step 3
        speedingCharges = self.chargesDF.where(F.col("CHARGE").contains("SPEED"))

        unitsFilteredByColorStates = self.unitsDF.where(
            (F.col("VEH_LIC_STATE_ID").isin(statesByOffenceTop25))
            &
            (F.col("VEH_COLOR_ID").isin(colorByCountTop10)))

        licensedDrivers = self.primaryPersonDF.where(
                        (~F.col("DRVR_LIC_TYPE_ID").isin(["UNLICENSED","NA","UNKNOWN"]))
                        & 
                        (~F.col("DRVR_LIC_CLS_ID").isin(["UNLICENSED"]))
                        &
                        (F.col("PRSN_TYPE_ID").contains("DRIVER"))
                        )

        return unitsFilteredByColorStates.join(licensedDrivers,
            unitsFilteredByColorStates["CRASH_ID"]==licensedDrivers["CRASH_ID"]
            ).drop(licensedDrivers["CRASH_ID"]).\
            join(F.broadcast(speedingCharges),
               unitsFilteredByColorStates["CRASH_ID"]==speedingCharges["CRASH_ID"]
            ).drop(speedingCharges["CRASH_ID"]).\
            groupBy("VEH_MAKE_ID").agg(F.count("CHARGE").
                      alias("speeding_offences")).\
            orderBy(F.col("speeding_offences").desc()).limit(5)

    def run(self):
        self.create_schema()
        self.read_from_csv()
        primaryPersonSchemaChanges = {"PRSN_AGE": IntegerType(), 
                                      "PRSN_BAC_TEST_RSLT": FloatType()}
        self.primaryPersonDF = self.transform_existing_schema(
                                        self.primaryPersonRawDF,
                                        primaryPersonSchemaChanges)
        unitsSchemaChanges = {"FIN_RESP_PROOF_ID": IntegerType(),
                              "FORCE_DIR_1_ID": IntegerType(),
                              "FORCE_DIR_2_ID": IntegerType()}
        self.unitsDF = self.transform_existing_schema(
                                        self.unitsRawDF,
                                        unitsSchemaChanges)

        self.analysis1().coalesce(1).write.\
                csv(self.output_directory+"/malesKilled-q1", 
                    header=True, mode="overwrite")
        print("File from analysis 1 written.")
        self.analysis2().coalesce(1).write.\
                csv(self.output_directory+"/twoWheelersCrashed-q2", 
                    header=True, mode="overwrite")
        print("File from analysis 2 written.")
        self.analysis3().coalesce(1).write.\
                csv(self.output_directory+"/accidentProneStateWomen-q3", 
                    header=True, mode="overwrite")
        print("File from analysis 3 written.")
        self.analysis4().coalesce(1).write.\
                csv(self.output_directory+"/top5to15thVehMakeIds-q4", 
                    header=True, mode="overwrite")
        print("File from analysis 4 written.")
        self.analysis5().coalesce(1).write.\
                csv(self.output_directory+"/bodyStyleTopEthnicGroup-q5", 
                    header=True, mode="overwrite")
        print("File from analysis 5 written.")
        self.analysis6().coalesce(1).write.\
                csv(self.output_directory+"/accidentDueToAlcoholTop5Zip-q6", 
                    header=True, mode="overwrite")
        print("File from analysis 6 written.")
        self.analysis7().coalesce(1).write.\
                csv(self.output_directory+"/crashesWithoutDamage-q7", 
                    header=True, mode="overwrite")
        print("File from analysis 7 written.")
        self.analysis8().coalesce(1).write.\
                csv(self.output_directory+"/top5VehicleMakeIds-q8", 
                    header=True, mode="overwrite")
        print("File from analysis 8 written.")

        
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_dir", help="directory where data exists")
    parser.add_argument("--output_dir", help="directory where output data will be written")
    args = parser.parse_args()
    cs_obj = CaseStudy(input_directory=args.input_dir, 
                        output_directory=args.output_dir)
    cs_obj.run()