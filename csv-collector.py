# Anodot CSV Collector :: On Premises Product
# Author: C.J. Jenkins - Solution Engineer - Anodot
#
# Description:
# This program takes in a setting file that defines
# the parameters of a stream of CSV files. These CSV
# files each represent a time bucket of data samples
# to be sent to Anodot. The user can integrate this
# script into a cron job for continuous ingestion
# of data into Anodot.
#
# Getting Started:
# 1. Populate settings JSON file
# 2. Upload CSVs to the 'incoming' directory
# 3. Run: python3 csv-collector.py [settings_filename].json
# ---------------------------------------------------------

# Imports
import argparse, json, sys, os, logging, time, glob, csv
import requests
import copy
from datetime import datetime
from datetime import timezone
from time import sleep

LOG_RETENTION_DAYS = 7

root = logging.getLogger()
root.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
logging.basicConfig(filename='run_logs/'+datetime.now().strftime('%Y-%m-%d_%H-%M-%S')+'.log', filemode='w', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
handler.setFormatter(formatter)
root.addHandler(handler)

logging.info('Starting run.')

# Classes
class Stream:
    def __init__(self, settings):
        """
        Intializes a Stream object based on settings file definitions
        """
        # File naming convention for reference: prefix_filename_1M_2018_01_01_00_00_00.csv
        self.filenamePrefix = settings["filenamePrefix"]
        self.filename = settings["filename"]
        self.collectionInterval = settings["collectionInterval"]
        self.timestamp = settings["timestamp"]
        self.timestampFormat = settings["timestampFormat"]
        self.measures = settings["measures"]
        self.dimensions = settings["dimensions"]
        self.samplesPerPost = settings["samplesPerPost"]
        self.timeBetweenPosts = settings["timeBetweenPosts"]
        self.token = settings["token"]
        self.sysTokenVariableName = settings["sysTokenVariableName"]
        self.fillMissingMeasureValuesWithZero = settings["fillMissingMeasureValuesWithZero"]
        self.fillMissingDimensionValuesWithNone = settings["fillMissingDimensionValuesWithNone"]
        self.total_samples_processed = 0

    def settings_are_valid(self):
        """
        Returns true if the settings contained in the Stream object are valid
        """
        is_valid = True

        # filenamePrefix
        if not isinstance(self.filenamePrefix, str):
            logging.error("Setting 'filenamePrefix' must be a string.")
            is_valid = False
        elif "_" in self.filenamePrefix:
            logging.error("Setting 'filenamePrefix' must not contain underscores.")
            is_valid = False

        # filename
        if not isinstance(self.filename, str):
            logging.error("Setting 'filename' must be a string.")
            is_valid = False
        elif "_" in self.filename:
            logging.error("Setting 'filename' must not contain underscores.")
            is_valid = False

        # collectionInterval
        if not isinstance(self.collectionInterval, str):
            logging.error("Setting 'collectionInterval' must be a string.")
            is_valid = False
        elif "_" in self.collectionInterval:
            logging.error("Setting 'collectionInterval' must not contain underscores.")
            is_valid = False
        elif not (self.collectionInterval == "1M" \
               or self.collectionInterval == "5M" \
               or self.collectionInterval == "1H" \
               or self.collectionInterval == "1D" \
               or self.collectionInterval == "1W"):
            logging.error("Setting 'collectionInterval' must be set to either '1M', '5M', '1H', '1D', or '1W'.")
            is_valid = False

        # timestamp
        if not isinstance(self.timestamp, str):
            logging.error("Setting 'timestamp' must be a string.")
            is_valid = False

        # timestampFormat
        if not isinstance(self.timestampFormat, str):
            logging.error("Setting 'timestampFormat' must be a string.")
            is_valid = False
        elif not (self.timestampFormat == "epoch_seconds" \
               or self.timestampFormat == "yyyy/mm/dd hh:mm:ss" \
               or self.timestampFormat == "yyyy-mm-dd hh:mm:ss"):
            logging.error("Setting 'timestampFormat' must be set to either 'epoch_seconds', 'yyyy/mm/dd hh:mm:ss', or 'yyyy-mm-dd hh:mm:ss'.")
            is_valid = False

        # measures
        if not isinstance(self.measures, dict):
            logging.error("Setting 'measures' must be a dictionary.")
            is_valid = False
        for meas,agg in self.measures.items():
            if agg not in ["counter", "gauge"]:
                logging.error("Measure '"+meas+"' must have an associated aggregation string of either 'counter' or 'gauge'.")
                is_valid = False

        # dimensions
        if not isinstance(self.dimensions, list):
            logging.error("Setting 'dimensions' must be an array.")
            is_valid = False
        i = 0
        for d in self.dimensions:
            i += 1
            if not isinstance(d, str):
                logging.error("Setting 'dimensions' item ["+str(i)+"] must be a string.")
                is_valid = False

        # samplesPerPost
        if not isinstance(self.samplesPerPost, int):
            logging.error("Setting 'samplesPerPost' must be an integer.")
            is_valid = False
        elif self.samplesPerPost > 9000:
            logging.error("Setting 'samplesPerPost' must be <= 1000.")
            is_valid = False

        # timeBetweenPosts
        if not (isinstance(self.timeBetweenPosts, float) or isinstance(self.timeBetweenPosts, int)):
            logging.error("Setting 'timeBetweenPosts' must be a float or integer.")
            is_valid = False

        # token
        if not isinstance(self.token, str):
            logging.error("Setting 'token' must be a string.")
            is_valid = False

        # sysTokenVariableName
        if not isinstance(self.sysTokenVariableName, str):
            logging.error("Setting 'sysTokenVariableName' must be a string.")
            is_valid = False

        # fillMissingMeasureValuesWithZero
        if not (self.fillMissingMeasureValuesWithZero, bool):
            logging.error("Setting 'fillMissingMeasureValuesWithZero' must be a boolean value 'true' or 'false' and not a string.")
            is_valid = False

        # fillMissingDimensionValuesWithNone
        if not isinstance(self.fillMissingDimensionValuesWithNone, bool):
            logging.error("Setting 'fillMissingDimensionValuesWithNone' must be a boolean value 'true' or 'false' and not a string.")
            is_valid = False

        return is_valid

    def ingest_data(self):
        """
        Iterates through all target files, extracts their samples, and sends them to Anodot
        """
        # Build overall filename prefix
        combined_filename = self.filenamePrefix+"_"+self.filename+"_"+self.collectionInterval+"_"
        logging.info("Preparing to ingest latest files beginning with '"+combined_filename+"'.")

        # Find latest files that match combined file prefix, sort by file time bucket, start ingesting each file
        try:
            list_of_files = glob.glob('incoming/'+combined_filename+'*.csv')
            list_of_files.sort()
            logging.info("Latest files found that match settings: "+str(list_of_files)+".")
        except:
            logging.error("No files found with names that match settings.")
            exit()

        # Move files to processing directory
        files_to_process = []
        for file in list_of_files:
            file_split = file.split("/")
            os.rename(file, "processing/"+file_split[1])
            files_to_process.append("processing/"+file_split[1])
        logging.info("Moved files to process to 'processing' directory.")

        # Instantiate list to contain data to be pushed per POST
        data_list = []

        # Keep track of how many samples we successfully POST to Anodot
        samples_sent_successfully = 0

        for file_to_process in files_to_process:
            # Check that timestamp in file name is valid
            try:
                tokens = file_to_process.split("_")
                file_timestamp = datetime(year=int(tokens[3]),month=int(tokens[4]),day=int(tokens[5]),hour=int(tokens[6]),minute=int(tokens[7]),second=int(tokens[8].split(".")[0]))
                logging.info("Timestamp in file's name is valid.")
            except:
                logging.error("Timestamp in file's name is invalid.")
                exit()

            # Open CSV
            try:
                f = open(file_to_process)
                logging.info("Successfully opened '"+file_to_process+"'.")
            except:
                logging.error("Could not open '"+file_to_process+"'.")
                exit()

            # Read CSV
            with f:
                found_all_measures = True
                found_all_dimensions = True
                definitions_to_columns_map = {}

                reader = csv.reader(f)

                header = next(reader)

                # Check for duplicate headers
                s = set(header)
                if len(s) != len(header):
                    logging.error("Duplicate headers exist.")
                    exit()

                # Check that timestamp header in settings appears in file
                if self.timestamp in header:
                    logging.info("Found timestamp in file header.")
                else:
                    logging.error("Missing timestamp in file header.")
                    exit()

                # Keep track of which column contains the timestamps
                definitions_to_columns_map[self.timestamp] = header.index(self.timestamp)

                # Check that all measures in settings appear in file
                for m in self.measures.keys():
                    if m not in header:
                        logging.error("Could not find measure '"+m+"' in file header.")
                        found_all_measures = False
                    else:
                        # Keep track of which columns contain each measure
                        definitions_to_columns_map[m] = header.index(m)

                if found_all_measures:
                    logging.info("Found all measures in file header.")
                else:
                    logging.error("Missing measures in file header.")
                    exit()

                # Check that all dimensions in settings appear in file
                for d in self.dimensions:
                    if d not in header:
                        logging.error("Could not find dimension '"+d+"' in file header.")
                        found_all_dimensions = False
                    else:
                        # Keep track of which columns contain each dimension
                        definitions_to_columns_map[d] = header.index(d)

                if found_all_dimensions:
                    logging.info("Found all dimensions in file header.")
                else:
                    logging.error("Missing dimensions in file header.")
                    exit()

                # Iterate through each row in the file
                line_number = 0
                for row in reader:
                    line_number += 1

                    # Check if row has correct number of values
                    if len(row) != len(header):
                        logging.error("Line "+str(line_number)+" does not have the same number of values as the header. Skipping this line.")
                        continue

                    # Iterate through each measure, assign it its dimensions, and generate data dictionary for the sample in the standard Anodot format
                    for m in self.measures:
                        data = {}
                        data["properties"] = {}
                        data["properties"]["what"] = m
                        for d in self.dimensions:
                            # Replace missing dimension values with 'none' if settings dictate as such, otherwise skip missing dimensions
                            if row[definitions_to_columns_map[d]] == "":
                                if self.fillMissingDimensionValuesWithNone:
                                    row[definitions_to_columns_map[d]] = "none"
                                else:
                                    logging.error("Line "+str(line_number)+", dimension '"+d+"' does not have a value. Skipping this dimension for this line.")
                                    continue
                            data["properties"][d] = row[definitions_to_columns_map[d]]
                        data["properties"]["target_type"] = self.measures[m]
                        data["tags"] = {}

                        # Check timestamp
                        if self.timestampFormat == "epoch_seconds":
                            try:
                                row_timestamp = datetime.fromtimestamp(int(row[definitions_to_columns_map[self.timestamp]]))
                                row_epoch = int(row_timestamp.timestamp())
                            except:
                                logging.error("Line "+str(line_number)+"'s timestamp does not match 'timestampFormat' or is otherwise invalid. Skipping this line.")
                                continue
                        elif self.timestampFormat == "yyyy/mm/dd hh:mm:ss":
                            try:
                                row_timestamp = datetime.strptime(row[definitions_to_columns_map[self.timestamp]], '%Y/%m/%d %H:%M:%S')
                                row_epoch = int(row_timestamp.replace(tzinfo=timezone.utc).timestamp())
                            except:
                                logging.error("Line "+str(line_number)+"'s timestamp does not match 'timestampFormat' or is otherwise invalid. Skipping this line.")
                                continue
                        elif self.timestampFormat == "yyyy-mm-dd hh:mm:ss":
                            try:
                                row_timestamp = datetime.strptime(row[definitions_to_columns_map[self.timestamp]], '%Y-%m-%d %H:%M:%S')
                                row_epoch = int(row_timestamp.replace(tzinfo=timezone.utc).timestamp())
                            except:
                                logging.error("Line "+str(line_number)+"'s timestamp does not match 'timestampFormat' or is otherwise invalid. Skipping this line.")
                                continue

                        # Add timestamp to data dictionary
                        data["timestamp"] = row_epoch

                        # Replace missing measure values with 0.0 if settings dictate as such, otherwise skip missing measures
                        if row[definitions_to_columns_map[m]] == "":
                            if self.fillMissingMeasureValuesWithZero:
                                row[definitions_to_columns_map[m]] = 0.0
                            else:
                                logging.error("Line "+str(line_number)+", measure '"+m+"' does not have a value. Skipping this measure for this line.")
                                continue

                        # Add value to data dictionary
                        data["value"] = row[definitions_to_columns_map[m]]

                        # Add completed sample dictionary to the list of samples we are preparing to send
                        data_list.append(data)

                    # POST chunk of samples
                    if len(data_list) == self.samplesPerPost:
                        self.total_samples_processed += len(data_list)
                        logging.info("Data compiled. Preparing to send "+str(len(data_list))+" samples to Anodot.")
                        samples_sent_successfully += send_to_anodot(data_list, self.token)
                        sleep(self.timeBetweenPosts)
                        data_list = []

                # POST remaining samples
                if len(data_list) > 0:
                    self.total_samples_processed += len(data_list)
                    logging.info("Data compiled. Preparing to send "+str(len(data_list))+" samples to Anodot.")
                    samples_sent_successfully += send_to_anodot(data_list, self.token)
                    sleep(self.timeBetweenPosts)
                    data_list = []

            # Move processed file to complete directory
            file_to_process_split = file_to_process.split("/")
            os.rename(file_to_process, "complete/"+file_to_process_split[1])
            logging.info("Moved '"+file_to_process_split[1]+"' to the 'complete' directory.")

        logging.info("Total Samples Processed: " + str(self.total_samples_processed))

def send_to_anodot(data_list, token):
    """
    Attempts to send list of samples to Anodot and returns the number of samples successfully sent.
    """
    anodot_domain = "api-poc.anodot.com"
    full_url = "https://" + anodot_domain + "/api/v1/metrics?token=" + token + "&protocol=anodot20"
    headers = {'Content-Type':'application/json'}
    resp = requests.post(full_url, headers=headers, data=str(data_list), timeout=120)
    if resp.status_code == 200:
        logging.info("POST to Anodot successful.")
        return len(data_list)
    else:
        logging.error("POST to Anodot NOT successful.")
        return 0

# Functions
def cleanup(number_of_days, path):
    """
    Removes files from the passed in path that are older than or equal
    to the number_of_days
    """
    time_in_secs = time.time() - (number_of_days * 24 * 60 * 60)
    for root, dirs, files in os.walk(path, topdown=False):
        for file_ in files:
            full_path = os.path.join(root, file_)
            stat = os.stat(full_path)

            if stat.st_mtime <=  time_in_secs:
                os.remove(full_path)

def instantiate_stream():
    """
    Returns Stream object based on settings file
    """
    parser = argparse.ArgumentParser(description='CSV Collector :: Anodot On-Premises Product')
    parser.add_argument('settings', metavar='settings', help='A JSON file containing configuration details for the stream.')
    args = parser.parse_args()
    settings = None
    new_stream = None

    try:
        with open(args.settings, encoding="utf-8") as f:
            try:
                settings = json.loads(f.read())
            except ValueError:
                logging.error("Could not load settings from file. Ensure JSON is valid.")
    except:
        logging.error("Could not open settings file.")
        exit()

    try:
        logging.info("Opened settings file successfully. Creating stream object.")
        new_stream = Stream(settings)
    except:
        logging.error("Could not build stream object based on settings provided.")
        exit()

    return new_stream

# Main
def main(settings):
    logging.info("Attempting run with settings file: '"+settings[0]+"'.")
    new_stream = instantiate_stream()

    logging.info("Checking stream settings.")
    if new_stream.settings_are_valid():
        logging.info("Stream settings valid.")
    else:
        logging.error("Stream settings invalid.")
        exit()

    logging.info("Attempting to ingest data.")
    new_stream.ingest_data()

    # Delete log files older than specified number of days
    cleanup(LOG_RETENTION_DAYS,"run_logs")

    # Delete processed CSVs older than specified number of days
    cleanup(LOG_RETENTION_DAYS,"complete")

if __name__ == "__main__":
    main(sys.argv[1:])
