import logging
import requests
import pandas as pd
import azure.functions as func
import base64
import os
import json
from datetime import datetime
from azure.communication.email import EmailClient

class StockReceiptProcessor:
    def __init__(self, req: func.HttpRequest):
        self.req = req
        self.auth_header = req.headers.get("Authorization")
        self.json_data = None
        self.df = None

    def unauthorized_response(self):
        return func.HttpResponse(
            json.dumps({
                "status": False,
                "error": {
                    "code": 401,
                    "message": "Unauthorized"
                }
            }),
            status_code=401,
            mimetype="application/json"
        )

    def validate_credentials(self):
        if not self.auth_header or not self.auth_header.startswith("Basic "):
            return False

        credentials = base64.b64decode(self.auth_header[6:]).decode("utf-8").split(":")
        login = credentials[0]
        password = credentials[1]
        if login != os.environ.get("API_USERNAME") or password != os.environ.get("PASSWORD"):
            return False

        return True

    def get_json_data(self):
        try:
            self.json_data = self.req.get_json()
            logging.info(self.json_data)
            if not self.json_data:
                return False
            # Convert JSON data to DataFrame and store in self.df
            df = pd.DataFrame(self.json_data)
            self.df = df
            logging.info(self.df)
            return True
        except Exception as e:
            logging.error(f"Invalid JSON data: {str(e)}")
            return False

    def process_data_frame(self):
        df = self.df
        try:
            # Extract required data from nested "items" field
            df["quantity"] = df["items"].apply(lambda x: x['quantity'])
            df["barcode"] = df["items"].apply(lambda x: x['barcode'])
            df["batchnumber"] = df["items"].apply(lambda x: x['batchnumber'])
            df["codentifier"] = df["items"].apply(lambda x: x['codentifier'] if x['codentifier'] != 'nan' else "")

            del df["items"]

            # Rename columns and add additional columns
            df.columns = [
                "GL_REFINTERNE", "GL_DEPOT", "GP_ETABLISSEMENT", "GP_DATELIVRAISON", "GP_DATEPIECE", "GL_QTEFACT",
                "GA_CODEBARRE", "batchnumber", "GA_IDSERIE"
            ]
            df["PREFIXE"] = "BLFC1_"
            df["end"] = ""
            df["end1"] = ""
            del df["batchnumber"]
            cols = [
                "PREFIXE", "GL_REFINTERNE", "GP_DATEPIECE", "GP_DATELIVRAISON", "GL_DEPOT", "GP_ETABLISSEMENT",
                "GA_CODEBARRE", "GA_IDSERIE", "GL_QTEFACT", "end", "end1"
            ]
            df = df[cols]

            # Format date columns
            df["GP_DATEPIECE"] = df["GP_DATEPIECE"].str[-2:] + '/' + df["GP_DATEPIECE"].str[5:7] + '/' + df["GP_DATEPIECE"].str[:4]
            df["GP_DATELIVRAISON"] = df["GP_DATELIVRAISON"].str[-2:] + '/' + df["GP_DATELIVRAISON"].str[5:7] + '/' + df["GP_DATELIVRAISON"].str[:4]
            self.df = df
            logging(self.df)
            return df

        except KeyError as e:
            logging.error(f"KeyError: {str(e)}")
            raise Exception("Missing key in JSON data.")



    def upload_blob(self, data, ref):
        time = datetime.now()
        blob_service_uri = os.environ.get("blobServiceUri")
        container_name = os.environ.get("Container_name")
        day_time = time.strftime("%Y_%m_%d_%H_%M_%S_")
        blob_name = f"in/data_{ref}_{day_time}.csv"
        container_url = f"{blob_service_uri}{container_name}"
        sas_token = os.environ.get("Sas_Token")
        url = f"{container_url}/{blob_name}{sas_token}"

        try:
            # response = requests.put(
            #     url,
            #     data=data,
            #     headers={"x-ms-blob-type": "BlockBlob"}
            # )
            # response.raise_for_status()
            # logging.info("Blob created successfully.")
            logging.info("upload blob")
            logging.info(data)
            return True
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to create blob. Error: {str(e)}")
            return False

    def send_email(self, subject, body, df=None):
        try:
            email_client = EmailClient.from_connection_string("endpoint=https://adfariontestemail.unitedstates.communication.azure.com/;accesskey=XuR3TYpb0NLF3Hh0JHgrryf9cqerQASZZcp7xIYallulvyUmbIgPJQQJ99AFACULyCpUVXMwAAAAAZCS9Jrq")
            df_bytes = df.to_csv(index=False).encode() if df is not None else b''
            file_bytes_b64 = base64.b64encode(df_bytes).decode()
            # logging(file_bytes_b64)
            message = {
                "content": {
                    "subject": subject,
                    "plainText": body
                },
                "recipients": {
                    "to": [
                        {"address": "yahiachames@gmail.com"},
                     
                    ]
                },
                "senderAddress": "DoNotReply@378ddca7-14a5-4e11-a78d-ea183ea21e01.azurecomm.net",
                "attachments": [
                    {
                        "name": "Received_Stock.csv",
                        "contentType": "text/csv",
                        "contentInBase64": file_bytes_b64
                    }
                ]
            }
            poller = email_client.begin_send(message)
            logging.info("Email sent successfully.")
        except Exception as ex:
            logging.error(f"Exception while sending email: {ex}")
