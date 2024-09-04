# Oracle to MongoDB Data Migration Service

## Overview
This service reads data from an Oracle database, transforms the data into BSON (Binary JSON) format, and sends it to MongoDB.

## Setup Instructions

### 1. Configure Database Connections
Before running the service, you need to configure your connection credentials for both Oracle and MongoDB. 
- Place your credentials in the `funcoes.py` file. Make sure you have the correct connection strings for both databases.

### 2. Define Oracle Tables for Migration
Specify which Oracle tables you want to migrate to MongoDB.
- Edit the `config.json` file to include the table name, schema, MongoDB database, and collection name.

### 3. Install Dependencies
Ensure that your environment has all the necessary dependencies.
- You can install them by running:
  ```bash
  pip install -r requirements.txt
