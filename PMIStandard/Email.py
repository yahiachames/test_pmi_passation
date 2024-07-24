import logging
import base64
import pandas as pd
from io import BytesIO

import uuid
from datetime import datetime
from azure.communication.email import EmailClient
from typing import List
from azure.core.credentials import AzureKeyCredential

class Email:
     def __init__(self,connection_string) -> None:
        self.connection_string = connection_string


     def convert_df_to_excel_bytes(self,df):
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False)
        buffer.seek(0)  # Move the cursor to the start of the buffer
        return buffer.getvalue() 
    

     def send_email_with_attachments(self, file_bytes_list: List[bytes], filenames: List[str], subject: str, body: str, recipient: str, sender: str, cc: List[str] = None):
        """
        Sends an email with one or more Excel file attachments.

        :param file_bytes_list: List of bytes of the Excel files to attach.
        :type file_bytes_list: List[bytes]
        :param filenames: List of filenames for the attachments.
        :type filenames: List[str]
        :param subject: The subject of the email.
        :type subject: str
        :param body: The body of the email (plain text).
        :type body: str
        :param recipient: The recipient email address.
        :type recipient: str
        :param sender: The sender email address.
        :type sender: str
        :param cc: List of CC email addresses. Defaults to None.
        :type cc: List[str], optional

        :return: None
        """
        logging.info("Initializing EmailClient.")
        email_client = EmailClient.from_connection_string(self.connection_string)

        if len(file_bytes_list) > 0:
            try:
                attachments = []
                for file_bytes, filename in zip(file_bytes_list, filenames):
                    logging.info(f"Encoding file {filename} to base64.")
                    file_bytes_b64 = base64.b64encode(file_bytes).decode()
                    attachments.append({
                        "name": filename,
                        "contentType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        "contentInBase64": file_bytes_b64
                    })

                message = {
                    "content": {
                        "subject": subject,
                        "plainText": body,
                        "html": f"<html><p>{body}</p></html>"
                    },
                    "recipients": {
                        "to": [
                            {"address": recipient }
                        ],
                        "cc": [{"address": cc_email} for cc_email in cc] if cc else []
                    },
                    "senderAddress": sender,
                    "attachments": attachments
                }

                logging.info("Sending email.")
                poller = email_client.begin_send(message)
                result = poller.result()
                logging.info(f"Email sent successfully: {result}")
            except Exception as e:
                logging.error(f"An error occurred while sending the email: {e}")
        else:
            logging.info("Skipping email sending because there are no files to attach.")

     def check_and_send_alerts(self, df: pd.DataFrame, file_name: str, recipient: str, sender: str, cc: List[str] = None):
        """
        Checks for an empty DataFrame or all rows failing validation and sends an alert if either condition is met.

        :param df: The DataFrame to check.
        :type df: pd.DataFrame
        :param file_name: The name of the file being processed.
        :type file_name: str
        :param recipient: The recipient email address.
        :type recipient: str
        :param sender: The sender email address.
        :type sender: str
        :param cc: List of CC email addresses. Defaults to None.
        :type cc: List[str], optional

        :return: None
        """
        subject = ""
        body = "there is no rows in the file"
        file_bytes_list = [self.convert_df_to_excel_bytes(df)]
        filenames = [f"{file_name}.xlsx"]

        # Check for empty DataFrame
        if df.empty:
            subject = "Alert: Empty Data File"
            body = f"The file {file_name} is empty."
            self.send_email_with_attachments(file_bytes_list, filenames, subject, body, recipient, sender, cc)
            return

        # Check for all rows invalid (assuming 'status' column indicates invalid rows)
        if df['status'].apply(lambda x: x != 'clean').all():
            subject = "Alert: All Rows Failed Validation"
            body = f"All rows in the file {file_name} failed validation."
            self.send_email_with_attachments(file_bytes_list, filenames, subject, body, recipient, sender, cc)
            return

        logging.info("No alerts sent as the DataFrame is not empty and not all rows failed validation.")   