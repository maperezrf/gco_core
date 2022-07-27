from google.cloud import bigquery
import google.auth 

class GCLOUD:

    def __init__(self) -> None:
        credentials, project = google.auth.default()
        self.bqclient = bigquery.Client(credentials=credentials)

    def get_query(self, query):
        query_string = query
        return self.bqclient.query(query_string).result().to_dataframe()

    def get_table(self):
        pass

    def get_client(self):
        return self.bqclient

    def get_table(self):
        pass

