import json
import logging
import azure.functions as func
import base64
import os


class Authentication:
    def __init__(self, req) -> None:
        self.req = req

    def authenticate(self):
        try : 
            logging.info("executed auth")
            logging.info(self.req.headers)
            # Check the Authorization header for valid credentials
            auth_header = self.req.headers.get("Authorization")
            logging.info("header auth")
            if not auth_header or not auth_header.startswith("Basic "):
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

            # Extract and validate the credentials from the Authorization header
            credentials = base64.b64decode(auth_header[6:]).decode("utf-8").split(":")
            login = credentials[0]
            password = credentials[1]
            logging.info(f"{login} {password}")
            if login != os.environ.get("API_USERNAME") or password != os.environ.get("PASSWORD"):
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
            return None
        except : 
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

     