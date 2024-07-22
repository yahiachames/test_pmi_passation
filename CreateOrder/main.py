import azure.functions as func
import json
import logging
import pandas as pd


def main(req: func.HttpRequest) -> func.HttpResponse:
    request_test = req.get_json()
    if not request_test:
            # send_email("Stock Receipt Error : Invalid JSON data.")  # Send email notification for invalid JSON data
            return func.HttpResponse(
                json.dumps({
                    "status": False,
                    "error": {
                        "code": 400,
                        "message": "Invalid JSON data."
                    }
                }),
                status_code=400,
                mimetype="application/json"
            )
    print(request_test)
    return func.HttpResponse(
            json.dumps({"status": True, "payload": request_test}),
            status_code=500,
            mimetype="application/json"
        )
