from elasticsearch import Elasticsearch
import time
class EmbeddingsDeletion:
    @staticmethod
    def count_old_documents(index_name):
        es = Elasticsearch(
            hosts=["http://localhost:9200"],
            basic_auth=("elastic", "pO49bI_PWYj4jcVLFyiI")
        )

        # Elasticsearch query to count documents older than 30 minutes
        count_query = {
            "query": {
                "range": {
                    "metadata.timestamp": {
                        "lt": "now-2d/d"  
                    }
                }
            }
        }

        # Get the count of documents to be deleted
        count_response = es.count(index=index_name, body=count_query)
        num_docs_to_delete = count_response['count']

        return num_docs_to_delete

    @staticmethod
    def delete_old_documents(index_name):
        es = Elasticsearch(
            hosts=["http://localhost:9200"],
            http_auth=("elastic", "pO49bI_PWYj4jcVLFyiI")
        )

        # Elasticsearch query to delete documents older than 30 minutes
        delete_query = {
            "query": {
                "range": {
                    "metadata.timestamp": {
                        "lt": "now-2d/d"  
                    }
                }
            }
        }

        # Perform the delete_by_query operation
        delete_response = es.delete_by_query(index=index_name, body=delete_query)
        num_docs_deleted = delete_response['deleted']
        print(f"Deletion successful: {num_docs_deleted} documents deleted.")

if __name__ == "__main__":
    index_name = "test-basic-done4"
    num_docs_to_delete = EmbeddingsDeletion.count_old_documents(index_name)
    if num_docs_to_delete > 0:
        print(f"Number of documents older than 7 days: {num_docs_to_delete}")
        EmbeddingsDeletion.delete_old_documents(index_name)
