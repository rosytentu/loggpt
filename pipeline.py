from components.es_store import ElasticStore
from utilities.config import DataConstants
from components.logs_extraction import ElasticSearchProcessor  
from components.embedding_model import EmbeddingModel  
from langchain_elasticsearch import ElasticsearchStore

class Pipelinee:
    @staticmethod
    def run_pipeline():
        try:
            data_constants=DataConstants()
            store=ElasticStore()
            index_name = data_constants.logs_index
            interval = 40  # Time interval in seconds
            timestamp_file = "last_processed_timestamp.txt"
            ElasticStore.continuous_process(index_name, interval, timestamp_file)
        except Exception as e:
            logger.error(f"Error in pipeline execution: {e}")