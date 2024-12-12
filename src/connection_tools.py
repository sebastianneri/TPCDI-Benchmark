def create_gcs_client():
    credentials_dict = {
      "type": "service_account",
      "project_id": "bdma-371020",
      "private_key_id": "",
      "private_key": "",
      "client_email": "tpcdi-databricks-bdma@bdma-371020.iam.gserviceaccount.com",
      "client_id": "103179657336858348005",
      "auth_uri": "https://accounts.google.com/o/oauth2/auth",
      "token_uri": "https://oauth2.googleapis.com/token",
      "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
      "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/tpcdi-databricks-bdma%40bdma-371020.iam.gserviceaccount.com"
    }
    credentials = service_account.Credentials.from_service_account_info(credentials_dict)
    client = storage.Client(credentials=credentials)
    return client