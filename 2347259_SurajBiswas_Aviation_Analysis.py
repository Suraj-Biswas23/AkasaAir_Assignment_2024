import pandas as pd
from pymongo import MongoClient
import streamlit as st
import plotly.express as px
from concurrent.futures import ThreadPoolExecutor
import traceback
from datetime import datetime

# Set page config
st.set_page_config(page_title="Aviation Data Analysis", page_icon="✈️", layout="wide")

# Connect to MongoDB
@st.cache_resource
def connect_mongo():
    """Connect to the MongoDB database.

    Returns:
        MongoDB collection object if the connection is successful, else None.
    """
    try:
        client = MongoClient("mongodb://localhost:27017/")
        db = client['aviation']
        collection = db['original_flights']
        return collection
    except Exception as e:
        st.error(f"Failed to connect to MongoDB: {str(e)}")
        return None
    
# Function to create a temporary collection for uploaded data
def create_temp_collection():
    """
    Creates a temporary MongoDB collection for storing uploaded flight data.

    Returns:
        temp_collection: The MongoDB collection object for 
        the temporary storage of uploaded data, or None if 
        the collection creation fails.
    """
    try:
        client = MongoClient("mongodb://localhost:27017/")
        db = client['aviation']
        temp_collection = db['temp_flights']
        return temp_collection
    except Exception as e:
        st.error(f"Failed to create temporary collection: {str(e)}")
        return None
    
def create_cleaned_data_collection():
    """
    Create a MongoDB collection for cleaned flight data.

    Returns:
        pymongo.collection.Collection: The cleaned_flights collection if successful, 
        None if there was an error.

    Raises:
        Exception: Displays an error in the Streamlit UI if connection fails.
    """
    try:
        client = MongoClient("mongodb://localhost:27017/")
        db = client['aviation']
        cleaned_collection = db['cleaned_flights']
        return cleaned_collection
    except Exception as e:
        st.error(f"Failed to create cleaned data collection: {str(e)}")
        return None
    
def save_analysis_results_to_mongo(avg_delay_by_airline, delay_distribution, avg_delay_by_departure_time):
    """
    Save analysis results to MongoDB, including the current date.

    Args:
        avg_delay_by_airline (pd.Series): Average delay grouped by airline.
        delay_distribution (pd.Series): Distribution of delays.
        avg_delay_by_departure_time (pd.Series): Average delay grouped by departure time.

    Returns:
        None: Displays success or error message in the Streamlit UI.

    Raises:
        Exception: Displays an error in the Streamlit UI if the save operation fails.
    """
    try:
        client = MongoClient("mongodb://localhost:27017/")
        db = client['aviation']
        analysis_collection = db['analysis_results']

        # Convert dictionaries to have string keys
        def convert_keys_to_string(data):
            if isinstance(data, dict):
                return {str(k): convert_keys_to_string(v) for k, v in data.items()}
            elif isinstance(data, list):
                return [convert_keys_to_string(i) for i in data]
            else:
                return data

        # Prepare the data to store in MongoDB with string keys
        analysis_data = {
            "analysis_date": datetime.now().isoformat(),  # Store the current date and time
            "avg_delay_by_airline": convert_keys_to_string(avg_delay_by_airline.to_dict()),
            "delay_distribution": convert_keys_to_string(delay_distribution.to_dict()),
            "avg_delay_by_departure_time": convert_keys_to_string(avg_delay_by_departure_time.to_dict())
        }

        # Insert the data into MongoDB without replacing existing data
        analysis_collection.insert_one(analysis_data)
        st.success("Analysis results saved to MongoDB!")
    except Exception as e:
        st.error(f"Failed to save analysis results to MongoDB: {str(e)}")

# Load dataset into a pandas DataFrame from the original collection
@st.cache_data
def load_data(original=True):
    """Load flight data from MongoDB into a DataFrame.

    Args:
        original (bool): If True, load from the original collection, else from temporary collection.

    Returns:
        pd.DataFrame: DataFrame containing flight data.
    """
    if original:
        flights_collection = connect_mongo()
    else:
        flights_collection = create_temp_collection()
        
    if flights_collection is not None:
        try:
            data = list(flights_collection.find())
            df = pd.DataFrame(data)
            return df
        except Exception as e:
            st.error(f"Failed to load data from MongoDB: {str(e)}")
            return None
    return None

# Function to insert data into the temporary collection
def insert_data_to_temp_mongo(df):
    """Insert DataFrame data into a temporary MongoDB collection after clearing existing data.

    Args:
        df (pd.DataFrame): DataFrame containing flight data.

    Returns:
        bool: True if insertion is successful, else False.
    """
    temp_collection = create_temp_collection()
    if temp_collection is not None:
        try:
            temp_collection.delete_many({})  # Clear existing data
            temp_collection.insert_many(df.to_dict('records'))
            return True
        except Exception as e:
            st.error(f"Failed to insert data into temporary MongoDB collection: {str(e)}")
            return False
    return False

# Validate CSV file
def validate_csv(df):
    """Validate that the required columns are present in the DataFrame.

    Args:
        df (pd.DataFrame): DataFrame containing flight data.

    Returns:
        bool: True if all required columns are present, else False.
    """
    required_columns = ['DepartureDate', 'ArrivalDate', 'DepartureTime', 'ArrivalTime', 'DelayMinutes', 'FlightNumber', 'Airline']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        st.error(f"The CSV file is missing the following required columns: {', '.join(missing_columns)}")
        return False
    return True

def process_chunk(chunk):
    """Process a chunk of flight data to clean and normalize it.

    Args:
        chunk (pd.DataFrame): A chunk of the flight data DataFrame.

    Returns:
        pd.DataFrame: Processed chunk of flight data, or None if an error occurs.
    """
    # Check if the chunk is empty
    if chunk.empty:
        return chunk
    
    try:
        # Convert DepartureDate and ArrivalDate to YYYY-MM-DD format
        chunk['DepartureDate'] = pd.to_datetime(chunk['DepartureDate'], format='%m/%d/%Y').dt.strftime('%Y-%m-%d')
        chunk['ArrivalDate'] = pd.to_datetime(chunk['ArrivalDate'], format='%m/%d/%Y').dt.strftime('%Y-%m-%d')

        # Convert DepartureTime and ArrivalTime to 24-hour format
        chunk['DepartureTime'] = pd.to_datetime(chunk['DepartureTime'], format='%I:%M %p').dt.strftime('%H:%M')
        chunk['ArrivalTime'] = pd.to_datetime(chunk['ArrivalTime'], format='%I:%M %p').dt.strftime('%H:%M')

        # Handle missing DelayMinutes
        chunk['DelayMinutes'] = chunk['DelayMinutes'].fillna(0)  # Use 0 to indicate missing data or no delay
        chunk['DelayStatus'] = chunk['DelayMinutes'].apply(lambda x: 'On Time' if x == 0 else 'Delayed')


        # Combine dates and times for proper duration calculation
        chunk['DepartureDateTime'] = pd.to_datetime(chunk['DepartureDate'] + ' ' + chunk['DepartureTime'])
        chunk['ArrivalDateTime'] = pd.to_datetime(chunk['ArrivalDate'] + ' ' + chunk['ArrivalTime'])

        # Correct any inconsistencies or errors in times
        inconsistent_time_mask = chunk['ArrivalDateTime'] < chunk['DepartureDateTime']
        if inconsistent_time_mask.any():
            chunk.loc[inconsistent_time_mask, 'ArrivalDate'] = (pd.to_datetime(chunk.loc[inconsistent_time_mask, 'ArrivalDate']) + pd.Timedelta(days=1)).dt.strftime('%Y-%m-%d')
            chunk.loc[inconsistent_time_mask, 'ArrivalDateTime'] = pd.to_datetime(chunk.loc[inconsistent_time_mask, 'ArrivalDate'] + ' ' + chunk.loc[inconsistent_time_mask, 'ArrivalTime'])

        # Calculate flight duration in minutes
        chunk['FlightDuration'] = ((chunk['ArrivalDateTime'] - chunk['DepartureDateTime']).dt.total_seconds() / 60).round().astype(int)
        chunk.loc[chunk['FlightDuration'] < 0, 'FlightDuration'] += 24 * 60

        return chunk
    except Exception as e:
        st.error(f"Error in processing data chunk: {str(e)}")
        st.error(traceback.format_exc())
        return None

def clean_and_normalize_data(df, chunk_size=5000):
    """Clean and normalize the flight data by processing it in chunks.

    Args:
        df (pd.DataFrame): DataFrame containing raw flight data.
        chunk_size (int): Number of rows to process in each chunk.

    Returns:
        pd.DataFrame: Cleaned and normalized flight data.
    """
    try:
        # Split the dataframe into chunks
        num_chunks = len(df) // chunk_size + 1
        chunks = [df.iloc[i*chunk_size:(i+1)*chunk_size] for i in range(num_chunks)]

        # Use ThreadPoolExecutor to parallelize chunk processing
        with ThreadPoolExecutor() as executor:
            processed_chunks = list(executor.map(process_chunk, chunks))

        # Filter out None values from processed_chunks
        processed_chunks = [chunk for chunk in processed_chunks if chunk is not None]

        # Concatenate processed chunks back into a single DataFrame
        cleaned_df = pd.concat(processed_chunks).reset_index(drop=True)

        # Detect and remove overlapping flights within the same airline and flight number
        cleaned_df.sort_values(by=['FlightNumber', 'DepartureDateTime'], inplace=True)

        # Create a list to store the indexes of rows to be removed
        rows_to_remove = []
        for i in range(1, len(cleaned_df)):
            prev_row = cleaned_df.iloc[i - 1]
            curr_row = cleaned_df.iloc[i]
            if (prev_row['FlightNumber'] == curr_row['FlightNumber']) and (prev_row['Airline'] == curr_row['Airline']):
                if curr_row['DepartureDateTime'] < prev_row['ArrivalDateTime']:
                    if prev_row['FlightDuration'] > curr_row['FlightDuration']:
                        rows_to_remove.append(prev_row.name)
                    else:
                        rows_to_remove.append(curr_row.name)

        # Store removed entries for summary without DepartureDateTime and ArrivalDateTime
        removed_entries = cleaned_df.loc[rows_to_remove].copy()  # Create a copy of removed entries
        if not removed_entries.empty:
            # Drop the unwanted columns before displaying
            removed_entries = removed_entries.drop(columns=['DepartureDateTime', 'ArrivalDateTime'], errors='ignore')
            st.write("Removed Duplicate/Overlapping Entries:")
            st.write(removed_entries)

        # Drop overlapping rows
        cleaned_df.drop(rows_to_remove, inplace=True)

        if '_id' in cleaned_df.columns:
            cleaned_df = cleaned_df.drop('_id', axis=1)

        cleaned_df = cleaned_df.drop(columns=['DepartureDateTime', 'ArrivalDateTime'], errors='ignore')
        grouped_df = cleaned_df.groupby('Airline', as_index=False).apply(lambda x: x.reset_index(drop=True)).reset_index(drop=True)

        cleaned_collection = create_cleaned_data_collection()
        if cleaned_collection is not None:
            try:
                cleaned_collection.delete_many({})  # Clear existing cleaned data
                cleaned_collection.insert_many(cleaned_df.to_dict('records'))
                st.success("Cleaned data saved successfully to MongoDB!")
            except Exception as e:
                st.error(f"Failed to save cleaned data to MongoDB: {str(e)}")

        return grouped_df
    except Exception as e:
        st.error(f"Error in data cleaning and normalization: {str(e)}")
        st.error(traceback.format_exc())
        return None

# Analyze the cleaned dataset
def analyze_data(df):
    """Analyze flight data to calculate average delays and distributions.

    Args:
        df (pd.DataFrame): Cleaned DataFrame of flight data.

    Returns:
        tuple: (avg_delay_by_airline, delay_distribution, avg_delay_by_departure_time)
            - avg_delay_by_airline (pd.Series): Average delay per airline.
            - delay_distribution (pd.Series): Summary statistics of delays.
            - avg_delay_by_departure_time (pd.Series): Average delay per departure hour.
    """
    try:
        avg_delay_by_airline = df.groupby('Airline')['DelayMinutes'].mean().sort_values(ascending=False)
        delay_distribution = df['DelayMinutes'].describe()
        departure_hours = pd.to_datetime(df['DepartureTime'].astype(str), format='%H:%M').dt.hour
        avg_delay_by_departure_time = df.groupby(departure_hours)['DelayMinutes'].mean()
        avg_delay_by_departure_time.index.name = 'DepartureHour'
        return avg_delay_by_airline, delay_distribution, avg_delay_by_departure_time
    except Exception as e:
        st.error(f"Error in data analysis: {str(e)}")
        return None, None, None

# Visualize the analysis results
def visualize_data(avg_delay_by_airline, delay_by_departure_time, df):
    """Generate visualizations for average delays by airline and by departure hour.

    Args:
        avg_delay_by_airline (pd.Series): Average delay per airline.
        avg_delay_by_departure_time (pd.Series): Average delay per departure hour.
    """
    try:
        # Bar Chart: Average Delay by Airline
        st.markdown('<p class="step-header">Average Delay by Airline</p>', unsafe_allow_html=True)
        fig = px.bar(
            x=avg_delay_by_airline.index, 
            y=avg_delay_by_airline.values,
            labels={'x': 'Airline', 'y': 'Average Delay (Minutes)'},
            title='Average Delay by Airline',
            color=avg_delay_by_airline.values,
            color_continuous_scale='Viridis'
        )
        fig.update_layout(
            xaxis_tickangle=-45,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font_color='#fafafa'
        )
        st.plotly_chart(fig, use_container_width=True)

        # Histogram: Distribution of Delays
        st.markdown('<p class="step-header">Distribution of Delays</p>', unsafe_allow_html=True)
        fig = px.histogram(
            df, x='DelayMinutes',
            nbins=30,
            labels={'DelayMinutes': 'Delay (Minutes)', 'count': 'Frequency'},
            title='Distribution of Delays',
            color_discrete_sequence=['#636EFA']
        )
        fig.update_layout(
            bargap=0.1,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font_color='#fafafa'
        )
        st.plotly_chart(fig, use_container_width=True)

        # Line Chart: Average Delay by Departure Hour
        st.markdown('<p class="step-header">Average Delay by Departure Time</p>', unsafe_allow_html=True)
        fig = px.line(
            x=delay_by_departure_time.index, 
            y=delay_by_departure_time.values,
            labels={'x': 'Hour of Day', 'y': 'Average Delay (Minutes)'},
            title='Average Delay by Departure Time',
        )
        fig.update_traces(line_color='#636EFA')
        fig.update_layout(
            xaxis_tickmode='linear',
            xaxis_tick0=0,
            xaxis_dtick=1,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font_color='#fafafa'
        )
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Error in data visualization: {str(e)}")

# Function to create a pie chart using Plotly
def plot_airline_distribution(grouped_df):
    """Create a pie chart visualizing the distribution of flights by airline.

    Args:
        grouped_df (pd.DataFrame): DataFrame containing flight data, with an 'Airline' column.

    Raises:
        Exception: If there's an error in plotting, it displays an error message in Streamlit.
    """
    try:
        airline_counts = grouped_df['Airline'].value_counts().reset_index()
        airline_counts.columns = ['Airline', 'Count']

        fig = px.pie(airline_counts, values='Count', names='Airline', title='Distribution of Flights by Airline', 
                     hole=0.3)  # Adding a hole to create a donut chart
        st.plotly_chart(fig)  # Display the plot in Streamlit
    except Exception as e:
        st.error(f"Error in plotting airline distribution: {str(e)}")

# Function to create histogram
def plot_duration_vs_delay(df):
    """Create a histogram showing the relationship between flight duration and delay minutes.

    Args:
        df (pd.DataFrame): DataFrame containing flight data, must include 'FlightDuration', 'DelayMinutes', and 'Airline' columns.

    Raises:
        Exception: If there's an error in plotting, it displays an error message in Streamlit.
    """
    try:
        st.markdown('<p class="step-header">Flight Duration vs. Delay Minutes</p>', unsafe_allow_html=True)
        fig = px.histogram(df, 
                           x='FlightDuration', 
                           y='DelayMinutes', 
                           color='Airline', 
                           title='Flight Duration vs. Delay Minutes',
                           labels={
                               'FlightDuration': 'Flight Duration (minutes)',
                               'DelayMinutes': 'Delay Minutes'
                           },
                           barmode='group',  # Group the bars for better comparison
                           opacity=0.7)  # Set opacity for better visibility
        st.plotly_chart(fig)
    except Exception as e:
        st.error(f"Error in plotting duration vs delay: {str(e)}")

def common_analysis(df, data_source):
    """
    Perform common analysis on flight data, including cleaning, analyzing, and visualizing.

    Args:
        df (pd.DataFrame): The input flight data as a DataFrame.
        data_source (str): The source name of the data for display purposes.

    Returns:
        None: Displays results and visualizations in the Streamlit UI.

    Raises:
        None: Error handling is done within the function for user feedback.
    """
    # Remove the '_id' column if it exists
    if '_id' in df.columns:
        df = df.drop('_id', axis=1)

    st.success(f"{data_source} data loaded successfully!")
    st.write("Dataset sample:")
    st.write(df.head())

    # Clean and normalize data
    st.markdown('<p class="step-header">Step 1: Clean and Normalize Data</p>', unsafe_allow_html=True)
    with st.spinner(f"Cleaning and normalizing {data_source} data..."):
        cleaned_df = clean_and_normalize_data(df)
    if cleaned_df is None:
        return
    st.success(f"{data_source} data cleaned and normalized!")
    st.write("Cleaned dataset sample:")
    st.write(cleaned_df.head())

    # Sidebar for data overview
    st.sidebar.header(f"{data_source} Data Overview")
    st.sidebar.write(f"Total flights: {len(cleaned_df)}")
    st.sidebar.write(f"Date range: {cleaned_df['DepartureDate'].min()} to {cleaned_df['DepartureDate'].max()}")
    st.sidebar.write(f"Airlines: {', '.join(cleaned_df['Airline'].unique())}")

    # Analyze data
    st.markdown('<p class="step-header">Step 2: Analyze Data</p>', unsafe_allow_html=True)
    with st.spinner(f"Analyzing {data_source} data..."):
        avg_delay_by_airline, delay_distribution, delay_by_departure_time = analyze_data(cleaned_df)
    if avg_delay_by_airline is None or delay_distribution is None or delay_by_departure_time is None:
        return
    st.success(f"{data_source} data analysis complete!")

    # Save analysis results to MongoDB
    save_analysis_results_to_mongo(avg_delay_by_airline, delay_distribution, delay_by_departure_time)

    # Display analysis results
    st.write(f"**Average Delay by Airline ({data_source} Data):**")
    st.write(avg_delay_by_airline)
    st.write(f"**Delay Distribution ({data_source} Data):**")
    st.write(delay_distribution)
    st.write(f"**Average Delay by Departure Time ({data_source} Data):**")
    st.write(delay_by_departure_time)

    # Display summary statistics
    st.markdown('<p class="step-header">Summary Statistics</p>', unsafe_allow_html=True)
    col1, col2, col3 = st.columns(3)
    col1.metric("Average Delay", f"{delay_distribution['mean']:.2f} minutes")
    col2.metric("Median Delay", f"{delay_distribution['50%']:.2f} minutes")
    col3.metric("Max Delay", f"{delay_distribution['max']:.2f} minutes")

    # Visualize data
    st.markdown('<p class="step-header">Step 3: Data Visualization</p>', unsafe_allow_html=True)
    plot_airline_distribution(cleaned_df)
    visualize_data(avg_delay_by_airline, delay_by_departure_time, cleaned_df)
    plot_duration_vs_delay(cleaned_df)

    # Show cleaned raw data
    st.markdown('<p class="step-header">Step 4: Cleaned Raw Data (Optional)</p>', unsafe_allow_html=True)
    if st.checkbox("Show cleaned raw data"):
        show_paginated_data(cleaned_df)

def show_paginated_data(df):
    """
    Display paginated data from a DataFrame in the Streamlit UI.

    Args:
        df (pd.DataFrame): The DataFrame to display and paginate.

    Returns:
        None: Displays the current page of data and a download button for the entire DataFrame as CSV.
    
    This function allows users to navigate through pages of data and download the full dataset.
    """
    page_size = 10
    total_pages = (len(df) // page_size) + (1 if len(df) % page_size > 0 else 0)

    if 'current_page' not in st.session_state:
        st.session_state.current_page = 1

    col1, col2, col3 = st.columns([1, 2, 1])

    with col1:
        if st.button("Previous Page"):
            if st.session_state.current_page > 1:
                st.session_state.current_page -= 1

    with col2:
        st.markdown(f"**Current Page:** {st.session_state.current_page} of {total_pages}")

    with col3:
        if st.button("Next Page"):
            if st.session_state.current_page < total_pages:
                st.session_state.current_page += 1

    start_index = (st.session_state.current_page - 1) * page_size
    end_index = start_index + page_size
    current_data = df.iloc[start_index:end_index]
    st.write(current_data)

    csv = df.to_csv(index=False)
    st.download_button(
        label="Download as CSV",
        data=csv,
        file_name='cleaned_flights_data.csv',
        mime='text/csv',
    )

def show_original_data_analysis():
    """
    Load and analyze original data from MongoDB for visualization in the Streamlit UI.

    This function retrieves the original dataset from the MongoDB collection,
    performs common analysis, and displays results in the Streamlit app.

    Returns:
        None: Displays a warning if no data is available, otherwise proceeds with analysis.
    """
    st.markdown('<p class="step-header">Step 1: Load Data</p>', unsafe_allow_html=True)
    with st.spinner("Loading original data from MongoDB..."):
        df = load_data()  # Load from the original collection
    if df is None:
        st.warning("No data available in the original collection.")
        return
    
    common_analysis(df, "Original")

def show_upload_data_analysis():
    """
    Handles the upload and analysis of CSV files in the Streamlit app.

    Prompts the user to upload a CSV file, validates the data, and
    inserts it into a temporary MongoDB collection for further analysis.
    If successful, it loads the data and performs common analysis.

    Returns:
        None: Displays error messages or information prompts based on
        the upload status and data validation.
    """
    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

    if uploaded_file is not None:
        try:
            df_uploaded = pd.read_csv(uploaded_file)

            if not validate_csv(df_uploaded):
                return

            with st.spinner("Inserting uploaded data into temporary MongoDB collection..."):
                if not insert_data_to_temp_mongo(df_uploaded):
                    return

            with st.spinner("Loading uploaded data..."):
                df = load_data(original=False)  # Load from temporary collection
            if df is None:
                return

            common_analysis(df, "Uploaded")

        except Exception as e:
            st.error(f"An unexpected error occurred while processing the uploaded file: {str(e)}")
            st.error(traceback.format_exc())
    else:
        st.info("Please upload a CSV file to begin or view analysis from the original data.")

def show_page(page):
    """
    Displays the appropriate analysis page based on user selection.

    Parameters:
        page (str): The name of the page to display. Can be either 
        "Original Data Analysis" or "Upload Data Analysis".

    Returns:
        None: The function directly modifies the Streamlit UI by 
        calling the corresponding analysis function for the selected page.
    """
    if page == "Original Data Analysis":
        show_original_data_analysis()
    elif page == "Upload Data Analysis":
        show_upload_data_analysis()
       
# Streamlit app layout
def main():
    """Main function to run the Streamlit app.

    1. Upload CSV data.
    2. Insert data into temporary MongoDB collection.(If file uploaded)
    3. Load data from MongoDB.
    4. Clean and normalize data.
    5. Analyze data.
    6. Create visualizations.
    """
    st.title(f"✈️ Aviation Data Analysis")

    st.markdown("""This dashboard presents an analysis of aviation data, focusing on flight delays, durations, and trends.""")

    # Sidebar for user details
    st.sidebar.header("About the Analyst")
    st.sidebar.write("Name: Suraj Biswas")  
    st.sidebar.write("College: Christ(Deemed to be University)")  
    
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox("Select a page", ["Original Data Analysis", "Upload Data Analysis"])
    
    show_page(page)  # Call the function based on the selected page

if __name__ == "__main__":
    main()