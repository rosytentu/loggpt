from langchain.vectorstores.elasticsearch import ElasticsearchStore
from components.embedding_model import EmbeddingModel
from dotenv import load_dotenv
from langchain.retrievers import TimeWeightedVectorStoreRetriever
from utilities.config import DataConstants
from elasticsearch import Elasticsearch
import os
from nltk.corpus import stopwords
from elasticsearch import Elasticsearch
import nltk
nltk.download('stopwords')
import logger
 
class Retriever:
    @staticmethod
    def get_retriever():
        try:
            data_constants=DataConstants()
            embeddings = EmbeddingModel.initialize_model()
            db = ElasticsearchStore(
                es_url=data_constants.es_url,
                index_name=data_constants.embeddings_index,
                embedding=embeddings,
                es_user=data_constants.es_user,
                es_password=data_constants.es_password,
                strategy=ElasticsearchStore.ApproxRetrievalStrategy(),
            )
            retriever=db.as_retriever(search_type="similarity", search_kwargs={"k": 10})
            return retriever
        except Exception as e:
            logger.error(f"Error initializing retriever: {e}")
            return None
    @staticmethod
    def preprocessed_query(query):
        user_query = query
 
        # Tokenize the user query
        tokens = user_query.lower().split()
 
        # Get English stop words
        stop_words = set(stopwords.words('english'))
 
        # Filter out stop words from the query
        filtered_query = [word for word in tokens if word not in stop_words]
 
        # Join the filtered words into a single string
        filtered_query_str = ' '.join(filtered_query)
        return filtered_query_str
 
    @staticmethod
    def get_logs(query):
        es = Elasticsearch(
                    hosts=["http://localhost:9200"],
                    http_auth=("elastic", "pO49bI_PWYj4jcVLFyiI")
                )
        preprocessed_query= Retriever.preprocessed_query(query)
        index_name = 'test-basic-done4'
        search_query = {
            "query": {
                "match": {
                    "text": preprocessed_query  # Replace with the actual query term
                }
            },
            "sort": [
                {
                    "metadata.timestamp": {
                        "order": "desc"
                    }
                }
            ],
            "size": 10
        }
 
        # Execute the search query
        result = es.search(index=index_name, body=search_query)
        relevant_docs = [hit["_source"]["text"] for hit in result["hits"]["hits"]]
 
        return relevant_docs
    @staticmethod
    def get_recent_logs():
        es = Elasticsearch(
            hosts=["http://localhost:9200"],
            http_auth=("elastic", "pO49bI_PWYj4jcVLFyiI")
        )
        data_constants=DataConstants()
        query = {
        "size": 10,  # Get the specified number of documents
        "sort": [{"metadata.timestamp": {"order": "desc"}}],  # Sort by timestamp in descending order
        "query": {"match_all": {}}  # Match all documents
         }
 
        # Perform the Elasticsearch search
        result = es.search(index=data_constants.embeddings_index, body=query)
 
        # Extract the text field from the top 'size' documents
        relevant_docs = [hit["_source"]["text"] for hit in result["hits"]["hits"]]
 
        return relevant_docs
    @staticmethod
    def get_oldest_logs():
        es = Elasticsearch(
            hosts=["http://localhost:9200"],
            http_auth=("elastic", "pO49bI_PWYj4jcVLFyiI")
        )
        data_constants = DataConstants()
        query = {
            "size": 10,  # Get the specified number of documents
            "sort": [{"metadata.timestamp": {"order": "asc"}}],  # Sort by timestamp in ascending order to get the oldest logs
            "query": {"match_all": {}}  # Match all documents
        }
 
        # Perform the Elasticsearch search
        result = es.search(index=data_constants.embeddings_index, body=query)
 
        # Extract the text field from the top 'size' documents
        relevant_docs = [hit["_source"]["text"] for hit in result["hits"]["hits"]]
 
        return relevant_docs