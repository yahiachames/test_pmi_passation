import azure.functions as func
import json
import logging
import pandas as pd
from .RecieveStock import StockReceiptProcessor

def main(req: func.HttpRequest) -> func.HttpResponse:
    processor = StockReceiptProcessor(req)
    
    if not processor.validate_credentials():
        return processor.unauthorized_response()

    if not processor.get_json_data():
        processor.send_email("Stock Receipt Error: Invalid JSON data.", pd.DataFrame())
        return func.HttpResponse(
            json.dumps({"status": False, "error": {"code": 400, "message": "Invalid JSON data."}}),
            status_code=400,
            mimetype="application/json"
        )

    try:
        processor.process_data_frame()
        data = processor.df.to_csv(sep=";", index=False, encoding='utf-8-sig')
        logging.info("data")
        logging.info(data)
        print(data)
        ref = processor.json_data['internal_reference']

        if processor.upload_blob(data, ref):
            processor.send_email(f"Stock Receipt Created on Middleware for {ref}", processor.df)
            return func.HttpResponse(
                json.dumps({"status": True, "content": "Receive stock file is generated successfully."}),
                status_code=200,
                mimetype="application/json"
            )
        else:
            # processor.send_email(f"Stock Receipt Error {ref} – Failed to create file on Middleware", processor.df)
            return func.HttpResponse(
                json.dumps({"status": False, "error": {"code": 500, "message": "Failed to create blob."}}),
                status_code=500,
                mimetype="application/json"
            )
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        processor.send_email(f"An error occurred during processing ASN. Error: {str(e)}", pd.DataFrame())
        return func.HttpResponse(
            json.dumps({"status": False, "error": {"code": 500, "message": "An error occurred during processing."}}),
            status_code=500,
            mimetype="application/json"
        )
