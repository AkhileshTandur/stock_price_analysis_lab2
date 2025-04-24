from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd

# Constants from Airflow Variables
SNOWFLAKE_CONN = "snowflake_conn"
SYMBOLS = ["NVDA"]
FORECASTING_PERIODS = 7

# Snowflake schema details from Variables
DATABASE = Variable.get("snowflake_database", default_var="DEV")
SCHEMA_RAW = Variable.get("snowflake_schema_raw", default_var="RAW")
SCHEMA_ML = Variable.get("snowflake_schema_ml", default_var="RAW")

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Function to get Snowflake cursor
def get_snowflake_cursor():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN)
    return hook.get_conn().cursor()

# Task to extract data using yfinance
@task
def extract_data(symbol):
    ticker = yf.Ticker(symbol)
    return ticker.history(period="180d").reset_index()

# Task to transform data
@task
def transform_data(hist_df, symbol):
    records = []
    for _, row in hist_df.iterrows():
        records.append((
            symbol,
            row['Open'],
            row['High'],
            row['Low'],
            row['Close'],
            row['Volume'],
            row['Date'].date()
        ))
    return records

# Task to load data into Snowflake
@task
def load_data(records, symbol):
    cursor = get_snowflake_cursor()
    try:
        cursor.execute("BEGIN")
        cursor.execute(f"DELETE FROM {DATABASE}.{SCHEMA_RAW}.STOCK_PRICE WHERE SYMBOL = %s", (symbol,))
        cursor.executemany(
            f"""INSERT INTO {DATABASE}.{SCHEMA_RAW}.STOCK_PRICE 
               (SYMBOL, OPEN, HIGH, LOW, CLOSE, VOLUME, DATE) 
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            records
        )
        cursor.execute("COMMIT")
    except Exception as e:
        cursor.execute("ROLLBACK")
        raise e
    finally:
        cursor.close()

# Task to train the ML model (creates a view for each symbol)
@task
def train_model(symbol):
    """Prepare data for forecasting"""
    cursor = get_snowflake_cursor()
    try:
        # Create the view in a schema we know we have access to
        view_name = f"MARKET_DATA_VIEW_{symbol}"
        cursor.execute(f"""
            CREATE OR REPLACE VIEW {DATABASE}.{SCHEMA_ML}.{view_name} AS
            SELECT DATE, CLOSE, SYMBOL
            FROM {DATABASE}.{SCHEMA_RAW}.STOCK_PRICE
            WHERE SYMBOL = %s
            ORDER BY DATE
        """, (symbol,))
        
        print(f"Successfully created view {DATABASE}.{SCHEMA_ML}.{view_name}")
        return True
    except Exception as e:
        print(f"View creation failed: {str(e)}")
        raise
    finally:
        cursor.close()
        
# Task to generate forecasts
@task
def generate_forecast(symbol):
    """Generate predictions using a simple forecasting method"""
    cursor = get_snowflake_cursor()
    try:
        # First, check if the market data view exists
        view_name = f"MARKET_DATA_VIEW_{symbol}"
        cursor.execute(f"SHOW VIEWS LIKE '{view_name}' IN SCHEMA {DATABASE}.{SCHEMA_ML}")
        view_exists = cursor.fetchone() is not None
        
        if not view_exists:
            raise Exception(f"Market data view for {symbol} doesn't exist. Run train_model first.")
        
        # Get the latest price and calculate a simple moving average
        cursor.execute(f"""
            SELECT 
                MAX(DATE) as latest_date,
                (SELECT CLOSE FROM {DATABASE}.{SCHEMA_ML}.{view_name} WHERE SYMBOL = '{symbol}' ORDER BY DATE DESC LIMIT 1) as latest_price,
                AVG(CLOSE) as avg_price,
                STDDEV(CLOSE) as std_price
            FROM {DATABASE}.{SCHEMA_ML}.{view_name}
            WHERE SYMBOL = '{symbol}'
            AND DATE >= DATEADD(day, -30, CURRENT_DATE())
        """)
        
        result = cursor.fetchone()
        latest_date = result[0]
        latest_price = result[1]
        avg_price = result[2]
        std_price = result[3] if result[3] is not None else avg_price * 0.05
        
        print(f"Latest date: {latest_date}, Latest price: {latest_price}, Avg price: {avg_price}, Std: {std_price}")
        
        # Create a temporary table with forecast dates
        forecast_dates_table = f"TEMP_FORECAST_DATES_{symbol}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        cursor.execute(f"""
            CREATE TEMPORARY TABLE {forecast_dates_table} (
                FORECAST_DATE DATE
            )
        """)
        
        # Insert forecast dates (one by one to avoid using generators)
        date_values = []
        for i in range(FORECASTING_PERIODS):
            next_date = (datetime.strptime(str(latest_date), "%Y-%m-%d") + timedelta(days=i+1)).strftime("%Y-%m-%d")
            date_values.append(f"('{next_date}')")
        
        cursor.execute(f"""
            INSERT INTO {forecast_dates_table} (FORECAST_DATE)
            VALUES {','.join(date_values)}
        """)
        
        # Delete existing forecasts for this symbol
        cursor.execute(f"""
            DELETE FROM {DATABASE}.{SCHEMA_ML}.FORECAST_RESULTS
            WHERE SYMBOL = '{symbol}'
        """)
        
        # Insert new forecasts
        cursor.execute(f"""
            INSERT INTO {DATABASE}.{SCHEMA_ML}.FORECAST_RESULTS (
                SYMBOL, DATE, FORECAST, LOWER_BOUND, UPPER_BOUND
            )
            SELECT 
                '{symbol}' AS SYMBOL,
                fd.FORECAST_DATE AS DATE,
                {latest_price} * (1 + (DATEDIFF(day, '{latest_date}', fd.FORECAST_DATE) * 0.002)) AS FORECAST,
                {latest_price} * (1 + (DATEDIFF(day, '{latest_date}', fd.FORECAST_DATE) * 0.002)) - {std_price} AS LOWER_BOUND,
                {latest_price} * (1 + (DATEDIFF(day, '{latest_date}', fd.FORECAST_DATE) * 0.002)) + {std_price} AS UPPER_BOUND
            FROM {forecast_dates_table} fd
        """)
        
        # Drop temporary table
        cursor.execute(f"DROP TABLE IF EXISTS {forecast_dates_table}")
        
        return True
    except Exception as e:
        print(f"Forecast generation failed: {str(e)}")
        raise
    finally:
        cursor.close()

# Define the DAG
with DAG(
    dag_id="stock_price_prediction_pipeline",
    default_args=default_args,
    description="Stock Price Prediction Pipeline for multiple companies",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:
    
    # Process each symbol
    for symbol in SYMBOLS:
        # ETL Pipeline for each symbol
        hist_data = extract_data(symbol)
        transformed_data = transform_data(hist_data, symbol)
        load_task = load_data(transformed_data, symbol)
        
        # ML Pipeline for each symbol
        train_task = train_model(symbol)
        forecast_task = generate_forecast(symbol)
        
        # Define dependencies for this symbol's pipeline
        load_task >> train_task >> forecast_task