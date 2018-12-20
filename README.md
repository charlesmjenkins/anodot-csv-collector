# Anodot CSV Collector :: On-Premises Product

- **Author**: C.J. Jenkins - Solution Engineer - Anodot
- **Current Version:** 1.0 "MVP"
- **Description:** This program takes in a setting file that defines the parameters of a stream of CSV files. These CSV files each represent a time bucket of data samples to be sent to Anodot. The user can integrate this script into a cron job for continuous ingestion of data into Anodot.
- **Technologies:** Python 3
- **Python Libraries Used:**
    - argparse, json, sys, os, logging, time, glob, csv, requests, datetime

## Feature, Bug Fix, and Refactoring Requests:
- None at this time.

## Test System:
- MacOS 10.13.4
- Python 3.7.0

## Getting Started:
1. Populate settings JSON file
```
{
    "filenamePrefix": "prefix", <-- file prefix string, no underscores
    "filename": "filename", <-- filename string, no underscores
    "collectionInterval": "1M", <-- "1M", "5M", "1H", "1D", or "1W"
    "timestamp": "mytimestampcolumnheader", <-- any string (name of the timestamp column in your CSVs)
    "timestampFormat": "yyyy/mm/dd hh:mm:ss", <-- "epoch_seconds", "yyyy/mm/dd hh:mm:ss", or "yyyy-mm-dd hh:mm:ss"
    "measures": {
        "m1":"counter", <-- "counter" or "gauge" for each; define as many measures as you like in this dictionary
        "m2":"gauge"
    },
    "dimensions": [
        "d1", <-- define as many dimensions as you like in this list
        "d2",
        "ver"
    ],
    "samplesPerPost": 1000, <-- integer for how many samples to include per HTTP POST
    "timeBetweenPosts": 0.6, <-- integer for how long to wait between HTTP POSTs (samples per second must align with Anodot account settings; default is no more than 2000 samples per second)
    "token": "mytoken", <-- Anodot account token string
    "sysTokenVariableName": "ANODOT_TOKEN", <-- sys variable string to store token [currently NOT implemented]
    "fillMissingMeasureValuesWithZero": true, <-- true (zero fill missing measures) or false (skip ingestion of this sample)
    "fillMissingDimensionValuesWithNone": true <-- true (fill missing dimensions with "none") or false (skip ingestion of this dimension)
}
```
2. Upload CSVs to ingest to the 'incoming' directory
    - Incoming files MUST be named in alignment with your settings definitions.
        - Example: prefix_filename_1M_2018_01_01_00_00_00.csv
    - Incoming files' samples MUST be sorted in ascending order of timestamp prior to ingestion (unsorted data will not ingest accurately into Anodot)

3. Run with this command in your terminal:
```
python3 csv-collector.py [settings_filename].json
```

## Other Notes:
- The CSV files move from the 'incoming' folder to 'processing' to 'complete'. The program will delete any files within 'complete' older than 7 days.

- The run_logs directory will contain detailed log files for every time the program is executed. The program will delete any files within 'run_logs' older than 7 days.
