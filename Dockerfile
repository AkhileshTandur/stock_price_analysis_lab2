FROM apache/airflow:2.10.5-python3.9

# Install dependencies
RUN pip install yfinance
RUN pip install sentence-transformers

RUN pip uninstall -y pinecone-client
RUN pip install pinecone
RUN pip install dbt-snowflake
