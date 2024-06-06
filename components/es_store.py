from components.logs_extraction import ElasticSearchProcessor  
from components.embedding_model import EmbeddingModel  
from langchain_elasticsearch import ElasticsearchStore
import time
from utilities.config import DataConstants

class ElasticStore:
    @staticmethod
    def store_data(message_list, metadata_list):
        data_constants = DataConstants()
        embeddings = EmbeddingModel.initialize_model()
        db = ElasticsearchStore.from_texts(
            message_list,
            embeddings,
            metadatas=metadata_list,
            es_url=data_constants.es_url,
            index_name=data_constants.embeddings_index,
            es_user=data_constants.es_user,
            es_password=data_constants.es_password,
            strategy=ElasticsearchStore.ApproxRetrievalStrategy(),
        )
        db.client.indices.refresh(index=data_constants.embeddings_index)
    @staticmethod
    def continuous_process(index_name, interval, timestamp_file):
        while True:
            # Process logs using ElasticSearchProcessor
            logs = ElasticSearchProcessor.process_logs(index_name, timestamp_file)
            chunk_list = []
            metadata_list = []

            # Loop through the documents and append the "message" field value to message_list
            # and the "timestamp" field value to metadata_list
            for document in logs:
                message = document["_source"].get("message")
                event = document["_source"].get("event", {})
                event_code = event.get("code")
                event_provider = event.get("provider")
                log = document["_source"].get("log", {})
                log_level = log.get("level")

                host = document["_source"].get("host", {})
                host_name = host.get("name")
                timestamp = document["_source"].get("@timestamp")
               
                if timestamp and message and event_code and event_provider and log_level and host_name:
                    formatted_entry = (
                        f"timestamp: {timestamp} "
                        f"log_level: {log_level} "
                        f"message: {message} "
                        f"event_code: {event_code} "
                        f"event_provider: {event_provider} "
                        f"host_name: {host_name}"
                    )
                    chunk_list.append(formatted_entry)
                    metadata_list.append({"timestamp": timestamp})

            # Store the data
            if chunk_list and metadata_list:
                ElasticStore.store_data(chunk_list, metadata_list)
            else:
                print("no new logs found to embedd")
           
            # Wait for the specified interval before the next iteration
            time.sleep(interval)