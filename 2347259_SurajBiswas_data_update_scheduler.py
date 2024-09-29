import pandas as pd
from pymongo import MongoClient
import schedule
import time
import os

# MongoDB connection function
def connect_mongo():
    try:
        client = MongoClient("mongodb://localhost:27017/")  
        db = client["aviation_data"]
        collection = db["flights"]
        print("MongoDB connected successfully.")
        return collection
    except Exception as e:
        print(f"Failed to connect to MongoDB: {e}")
        return None

# Function to replace MongoDB collection with the new CSV data
def replace_mongo_data(csv_path):
    """Replace the entire MongoDB collection with the data from the CSV file.

    Args:
        csv_path (str): Path to the CSV file.
    """
    collection = connect_mongo()
    if collection is None:
        return False

    try:
        # Load the CSV data
        df = pd.read_csv(csv_path)
        
        if df.empty:
            print("CSV file is empty. No data inserted.")
            return False

        # Ensure the dataframe only has the expected columns
        expected_columns = {"DepartureDate", "ArrivalDate", "DepartureTime", "ArrivalTime", "DelayMinutes", "FlightNumber", "Airline"} 
        extra_columns = set(df.columns) - expected_columns
        if extra_columns:
            print(f"Extra columns detected in CSV: {extra_columns}. These columns will be ignored.")
            df = df.drop(columns=extra_columns)

        # Convert dataframe to a list of dictionaries for insertion
        records = df.to_dict("records")

        # First, clear the entire collection
        collection.delete_many({})
        print("MongoDB collection cleared.")

        # Insert the new data
        collection.insert_many(records)
        print(f"Inserted {len(records)} new records into MongoDB.")
        
        return True
    except Exception as e:
        print(f"Failed to replace data in MongoDB: {e}")
        return False

# Scheduler job to run daily at 8 AM
def schedule_daily_update():
    """Scheduled job to update MongoDB with the latest CSV data."""
    try:
        # Define the path to your CSV file
        csv_path = 'aviation_data.csv' 

        # Check if the file exists before attempting to load it
        if not os.path.exists(csv_path):
            print(f"CSV file not found at {csv_path}. Make sure the path is correct.")
            return
        
        # Replace MongoDB with the CSV data
        success = replace_mongo_data(csv_path)
        if success:
            print("Scheduled data update completed.")
        else:
            print("Scheduled data update failed.")
    except Exception as e:
        print(f"Error during scheduled update: {e}")

# Function to start the scheduler
def run_scheduler():
    # Schedule the update to run every day at 8 AM
    schedule.every().day.at("08:00").do(schedule_daily_update)

    print("Scheduler started. Waiting for the next job at 8 AM...")

    while True:
        # Run the scheduled task only once at 8 AM daily
        schedule.run_pending()
        time.sleep(60)  # Check once every minute for any pending jobs

if __name__ == "__main__":
    run_scheduler()
