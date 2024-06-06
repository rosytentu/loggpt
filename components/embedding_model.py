from langchain.embeddings import HuggingFaceBgeEmbeddings
from logger import logger

class EmbeddingModel:
    @staticmethod
    def initialize_model():
        try:
            # Define the model parameters
            model_name = "BAAI/bge-large-en-v1.5"
            encode_kwargs = {'normalize_embeddings': True} 
            logger.info("Initializing embedding model with parameters: model_name={}, encode_kwargs={}".format(model_name, encode_kwargs))
            # Create and return the model instance
            return HuggingFaceBgeEmbeddings(
                model_name=model_name,
                encode_kwargs=encode_kwargs
            )
        except Exception as e:
            logger.error(f"Error initializing embedding model: {e}")
            return None