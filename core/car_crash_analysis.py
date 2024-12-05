from pyspark.sql import SparkSession, DataFrame, Row, Window, functions as F
from pyspark.sql.functions import desc, col, isnan, row_number
import os
from helpers.utils import load_csv_data_to_df, save_dataframe_to_file


class BCGxCarCrashAnalysis:
    def __init__(self, spark, config):
        file_path = config.get("data_file_name")
        self.chargesDf = load_csv_data_to_df(spark, file_path.get("Charges"))
        self.damagesDf = load_csv_data_to_df(spark, file_path.get("Damages"))
        self.endorseDf = load_csv_data_to_df(spark, file_path.get("Endorse"))
        self.primaryPersonDf = load_csv_data_to_df(spark, file_path.get("Primary_Person"))
        self.unitsDf = load_csv_data_to_df(spark, file_path.get("Units"))
        self.restrictDf = load_csv_data_to_df(spark, file_path.get("Restrict"))
    
    def getAllDataFrames(self, limit_rows=20):
        """
        Returns all DataFrames as a dictionary with an optional limit.
        Only returns the first `limit_rows` rows for each DataFrame.
        """
        return {
            "chargesDf": self.chargesDf.limit(limit_rows),
            "damagesDf": self.damagesDf.limit(limit_rows),
            "endorseDf": self.endorseDf.limit(limit_rows),
            "primaryPersonDf": self.primaryPersonDf.limit(limit_rows),
            "unitsDf": self.unitsDf.limit(limit_rows),
            "restrictDf": self.restrictDf.limit(limit_rows),
        }
    
    def get_crashes_with_male_deaths(self, save_df_to_path: str, file_format: str):
        """
        Find the number of crashes (accidents) in which number of males killed are greater than 2?
        """
        df = self.primaryPersonDf.filter((col("PRSN_GNDR_ID") == "MALE") & (col("DEATH_CNT") > 2))
        save_dataframe_to_file(df, save_df_to_path, file_format)
        return df.count()

    def count_two_wheeler_crashes(self, save_df_to_path: str, file_format: str):
        """
        How many two wheelers are booked for crashes? 
        """
        df = self.unitsDf.filter(col("VEH_BODY_STYL_ID").like("%MOTORCYCLE%"))
        save_dataframe_to_file(df, save_df_to_path, file_format)
        return df.count()

    def top_5_vehicle_makes_driver_death_no_airbag(self, save_df_to_path: str, file_format: str):
        """
        Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.
        """
        df = (
            self.unitsDf.join(self.primaryPersonDf, on=["CRASH_ID"], how="inner")
            .filter(
                (col("PRSN_INJRY_SEV_ID") == "KILLED") &
                (col("PRSN_AIRBAG_ID") == "NOT DEPLOYED") &
                (col("VEH_MAKE_ID") != "NA")
            )
            .groupBy("VEH_MAKE_ID")
            .count()
            .withColumnRenamed("count", "vehicle_count")
            .orderBy(col("vehicle_count").desc())
            .limit(5)
        )
        save_dataframe_to_file(df, save_df_to_path, file_format)
        return df

    def count_vehicles_valid_license_hit_and_run(self, save_df_to_path: str, file_format: str):
        """
        Determine number of Vehicles with driver having valid licences involved in hit and run?
        """
        df = (
            self.unitsDf.select("CRASH_ID", "VEH_HNR_FL")
            .join(self.primaryPersonDf.select("CRASH_ID", "DRVR_LIC_TYPE_ID"), on=["CRASH_ID"], how="inner")
            .filter(
                (col("VEH_HNR_FL") == "Y") &
                (col("DRVR_LIC_TYPE_ID").isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]))
            )
        )
        save_dataframe_to_file(df, save_df_to_path, file_format)
        return df.count()

    def accidents_no_females_by_state(self, save_df_to_path: str, file_format: str):
        """
        Which state has highest number of accidents in which females are not involved? 
        """
        df = (
            self.primaryPersonDf.filter(col("PRSN_GNDR_ID") != "FEMALE")
            .groupBy("DRVR_LIC_STATE_ID")
            .count()
            .withColumnRenamed("count", "driver_count")
            .orderBy(col("driver_count").desc())
        )
        save_dataframe_to_file(df, save_df_to_path, file_format)
        return df

    def top_3_to_5_vehicle_makes_injuries(self, save_df_to_path: str, file_format: str):
        """
        Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        """
        df = (self.unitsDf.filter(self.unitsDf.VEH_MAKE_ID != "NA")
            .withColumn("TOT_CASUALTIES_CNT", col("TOT_INJRY_CNT") + col("DEATH_CNT"))
            .groupby("VEH_MAKE_ID")
            .sum("TOT_CASUALTIES_CNT")
            .withColumnRenamed("sum(TOT_CASUALTIES_CNT)", "TOT_CASUALTIES_CNT_AGG")
            .orderBy(col("TOT_CASUALTIES_CNT_AGG").desc())
        )
            
        # Adding ranking and filtering the top 3 to 5 rows
        window_spec = Window.orderBy(col("TOT_CASUALTIES_CNT_AGG").desc())
        df_with_rank = df.withColumn("rank", row_number().over(window_spec))
        df_top_3_to_5 = df_with_rank.filter((col("rank") >= 3) & (col("rank") <= 5))
        save_dataframe_to_file(df_top_3_to_5, save_df_to_path, file_format)
        return df_top_3_to_5

    def top_ethnicity_by_body_style(self, save_df_to_path: str, file_format: str):
        """
        For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
        """
        w = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())
    
        df = (
            self.unitsDf.join(self.primaryPersonDf, on=["CRASH_ID"], how="inner")
            .filter(
                ~self.unitsDf["VEH_BODY_STYL_ID"].isin(
                    ["NA", "UNKNOWN", "NOT REPORTED", "OTHER  (EXPLAIN IN NARRATIVE)"]
                )
            )
            .filter(~self.primaryPersonDf["PRSN_ETHNICITY_ID"].isin(["NA", "UNKNOWN"]))
            .groupby("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")
            .agg(F.count("*").alias("count"))
            .withColumn("row", row_number().over(w))
            .filter(col("row") == 1)
            .drop("row", "count")
        )
        save_dataframe_to_file(df, save_df_to_path, file_format)
        return df
    
    def top_5_zip_codes_alcohol_crashes(self, save_df_to_path: str, file_format: str):
        """
        Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
        """
        df = (
            self.unitsDf.join(self.primaryPersonDf, on=["CRASH_ID"], how="inner")
            .dropna(subset=["DRVR_ZIP"])
            .filter(
                (
                    col("CONTRIB_FACTR_1_ID").isNotNull() & col("CONTRIB_FACTR_1_ID").contains("ALCOHOL")
                )
                | (
                    col("CONTRIB_FACTR_2_ID").isNotNull() & col("CONTRIB_FACTR_2_ID").contains("ALCOHOL")
                )
            )
            .groupby("DRVR_ZIP")
            .count()
            .orderBy(col("count").desc())
            .limit(5)
        )
        save_dataframe_to_file(df, save_df_to_path, file_format)
        return df

    def count_crashes_no_damage_high_level_insured(self, save_df_to_path: str, file_format: str):
        """
        Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
        """
        df = (
            self.damagesDf.join(self.unitsDf, on=["CRASH_ID"], how="inner")
            .filter(
                (
                    (col("VEH_DMAG_SCL_1_ID").cast("int") > 4)
                    & ~col("VEH_DMAG_SCL_1_ID").isin(["NA", "NO DAMAGE", "INVALID VALUE"])
                )
                | (
                    (col("VEH_DMAG_SCL_2_ID").cast("int") > 4)
                    & ~col("VEH_DMAG_SCL_2_ID").isin(["NA", "NO DAMAGE", "INVALID VALUE"])
                )
            )
            .filter(col("DAMAGED_PROPERTY") == "NONE")
            .filter(col("FIN_RESP_TYPE_ID") == "PROOF OF LIABILITY INSURANCE")
        )
        save_dataframe_to_file(df, save_df_to_path, file_format)
        return df.collect()

    def top_5_vehicle_makes_speeding_licensed_top_colors_states(self, save_df_to_path: str, file_format: str):
        """
        Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)
        """
        # Getting top 25 states with VEH_LIC_STATE_ID that is not null
        top_25_state_list = [
            row[0]
            for row in self.unitsDf.filter(
                col("VEH_LIC_STATE_ID").cast("int").isNotNull()
            )
            .groupBy("VEH_LIC_STATE_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(25)
            .collect()
        ]
        
        # Getting top 10 most used vehicle colors that are not "NA"
        top_10_used_vehicle_colors = [
            row[0]
            for row in self.unitsDf.filter(self.unitsDf.VEH_COLOR_ID != "NA")
            .groupBy("VEH_COLOR_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(10)
            .collect()
        ]
        
        # Main DataFrame logic
        df = (
            self.chargesDf.join(self.primaryPersonDf, on=["CRASH_ID"], how="inner")
            .join(self.unitsDf, on=["CRASH_ID"], how="inner")
            .filter(self.chargesDf.CHARGE.contains("SPEED"))
            .filter(
                self.primaryPersonDf.DRVR_LIC_TYPE_ID.isin(
                    ["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]
                )
            )
            .filter(self.unitsDf.VEH_COLOR_ID.isin(top_10_used_vehicle_colors))
            .filter(self.unitsDf.VEH_LIC_STATE_ID.isin(top_25_state_list))
            .groupBy("VEH_MAKE_ID")
            .count()
            .withColumnRenamed("count", "vehicle_count")
            .orderBy(col("vehicle_count").desc())
            .limit(5)
        )
        save_dataframe_to_file(df, save_df_to_path, file_format)
        return df