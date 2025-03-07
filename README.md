# Biometric ETL Script

This script extracts, transforms, and loads biometric login data from MSSQL to PostgreSQL.

## Setup Instructions

### 1. Create a `.env` file

Create a `.env` file in the project root directory and add the following environment variables:


# MSSQL Configuration

`MSSQL_SERVER=localhost`
`MSSQL_DATABASE=biometric`
`MSSQL_USERNAME=sa`
`MSSQL_PASSWORD=YourStrong@Passw0rd`


# PostgreSQL Configuration
`POSTGRES_HOST=localhost`
`POSTGRES_DATABASE=ems`
`POSTGRES_USERNAME=postgres`
`POSTGRES_PASSWORD=postgres`
`POSTGRES_PORT=5432`



### 2. Install the required packages
`pip install -r requirements.txt`


### 3. Execute the script
`python main.py`

