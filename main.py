from pyspark.sql import SparkSession
import os
from helpers.utils import set_bcg_directory, parse_arguments, load_csv_data_to_df, read_json
from core.car_crash_analysis import BCGxCarCrashAnalysis
import datetime

if __name__ == "__main__":
    # Setting up the current working directory
    set_bcg_directory()

    config_path = parse_arguments()
    config = read_json(config_path)

    # Initializing the log file path
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    event_log_dir = os.path.join(config.get("logs_path"), f"logs_{current_date}")

    # Ensuring the log directory exists or creating it if it doesn't
    os.makedirs(event_log_dir, exist_ok=True)

    # Initializing a Spark session with the necessary configurations
    spark = SparkSession.builder \
            .appName("BCGxCarCrashAnalysis") \
            .config("spark.eventLog.enabled", "true") \
            .config("spark.eventLog.dir", event_log_dir) \
            .getOrCreate()

    # Setting the Spark logging level to show only errors
    spark.sparkContext.setLogLevel("ERROR")
    
    file_path = config.get("data_file_name")
    save_df_to_path = config.get("output_path")
    file_format = config.get("file_format")

    analysis = BCGxCarCrashAnalysis(spark, config)
    
    # Displaying the first 10 rows of each DataFrame
    all_dataframes = analysis.getAllDataFrames()
    for name, df in all_dataframes.items():
        print(f"Showing data for {name}:")
        df.show(10)
    
    # Analytics 1: Find the number of crashes (accidents) in which number of males killed are greater than 2?
    print("Q1: Number of crashes having males killed greater than 2:", 
          analysis.get_crashes_with_male_deaths(save_df_to_path.get("task_1"), file_format))

    # Analysis 2: How many two wheelers are booked for crashes? 
    print("Q2: Number of two wheelers crashes:", 
        analysis.count_two_wheeler_crashes(save_df_to_path.get("task_2"), file_format))

    # Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.
    print("Q3: Top 5 Vehicle makes of the car with crashes and death:")
    analysis.top_5_vehicle_makes_driver_death_no_airbag(save_df_to_path.get("task_3"), file_format).show()

    # Analysis 4: Determine number of Vehicles with driver having valid licences involved in hit and run? 
    print("Q4: Number of Vehicles with driver having valid licences involved in hit and run:",
    analysis.count_vehicles_valid_license_hit_and_run(save_df_to_path.get("task_4"), file_format))

    # Analysis 5: Which state has highest number of accidents in which females are not involved? 
    print("Q5: States having highest number of accidents in which females are not involved:")
    analysis.accidents_no_females_by_state(save_df_to_path.get("task_5"), file_format).show()

    # Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
    print("Q6: Top 3 to 5 Vehicle contributing to the injuries including death:") 
    analysis.top_3_to_5_vehicle_makes_injuries(save_df_to_path.get("task_6"), file_format).show()

    # Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
    print("Q7: All the body styles involved in crashes, mention the top ethnic user group of each unique body style:")
    analysis.top_ethnicity_by_body_style(save_df_to_path.get("task_7"), file_format).show()

    # Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
    print("Q8: Top 5 zip codes with highest number crashes with alcohols as the CF for a crash:")
    analysis.top_5_zip_codes_alcohol_crashes(save_df_to_path.get("task_8"), file_format).show()

    # Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
    print("Q9: Distinct Crash IDs with no damage, damage level > 4, and insured cars:",
    analysis.count_crashes_no_damage_high_level_insured(save_df_to_path.get("task_9"), file_format))

    # Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)
    print("Q10: Top 5 Vehicle Makes with speeding charges, licensed drivers, top 10 vehicle colors, and cars from top 25 states with most offences:")
    analysis.top_5_vehicle_makes_speeding_licensed_top_colors_states(save_df_to_path.get("task_10"), file_format).show()

    spark.stop()