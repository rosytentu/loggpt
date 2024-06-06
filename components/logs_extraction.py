from elasticsearch import Elasticsearch
import os
from logger import logger
from utilities.config import DataConstants

class ElasticSearchProcessor:
    @staticmethod
    def read_last_timestamp(timestamp_file):
        try:
            if os.path.exists(timestamp_file):
                with open(timestamp_file, 'r') as file:
                    return file.read().strip()
            else:
                return "0"  # Unix epoch
        except Exception as e:
            logger.error(f"Error occurred while reading last timestamp: {e}")
            return "0"

    @staticmethod
    def write_last_timestamp(timestamp, timestamp_file):
        try:
            with open(timestamp_file, 'w') as file:
                file.write(timestamp)
        except Exception as e:
            logger.error(f"Error occurred while writing last timestamp: {e}")

    @staticmethod
    def process_logs(index_name, timestamp_file):
        try:
            data_constants = DataConstants()
            es = Elasticsearch([data_constants.es_url], http_auth=(data_constants.es_user, data_constants.es_password))
            
            # Read the last timestamp
            last_timestamp = ElasticSearchProcessor.read_last_timestamp(timestamp_file)

            # Retrieve logs based on last timestamp
            if last_timestamp == "0":
                results = es.search(index=index_name, body={"query": {"match_all": {}}}, size=10000)
            else:
            # Retrieve only the updated logs
                if last_timestamp:
                # Retrieve only the updated logs
                    results = es.search(index=index_name, body={"query": {"range": {"@timestamp": {"gt": last_timestamp}}}}, size=10000)
                else:
                    # If the last timestamp is empty, retrieve all logs
                    results = es.search(index=index_name, body={"query": {"match_all": {}}}, size=10000)

            # Process the logs
            hits = results['hits']['hits']
            timestamps = []

            for hit in hits:
                timestamp = hit['_source']['@timestamp']
                timestamps.append(timestamp)

            # Find the most recent timestamp
            most_recent_timestamp = max(timestamps)

            # Write the most recent timestamp to file
            ElasticSearchProcessor.write_last_timestamp(most_recent_timestamp, timestamp_file)

            return hits 
        except Exception as e:
            logger.error(f"Error occurred while processing logs: {e}")
            return []



