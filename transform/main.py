import azure.functions as func
import json
import logging
import pandas as pd
from pathlib import Path
import os
from zeep.plugins import HistoryPlugin
from PMIStandard.PDF_Generator.PDFConfiguration import PDFConfiguration
from PMIStandard.Authentication import Authentication
from PMIStandard.AzureStorage.TableStorage import AzureDataTablesClient
from PMIStandard.AzureStorage.BlobStorage import AzureBlobProcessor
from PMIStandard.CegidWebService.CegidWebService import CegidWebService
from lxml import etree
import xml.etree.ElementTree as ET
from datetime import datetime,timedelta
from decimal import Decimal
import math

def main(req: func.HttpRequest) -> func.HttpResponse:

    try : 

        data = req.get_json()

        return func.HttpResponse(
            json.dumps({"status": 300, "message": data}),
            status_code=300,
            mimetype="application/json"
        )

            
    except Exception as e:
            # Handle the exception and send it as a message
            error_message = f"internal error occurred: {str(e)}"
            return func.HttpResponse(
            json.dumps({"status": 500, "message": error_message}),
            status_code=500,
            mimetype="application/json"
        )
    # Save PDF to file
 