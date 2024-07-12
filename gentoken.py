import base64
import json
url = "http://localhost:7071/api/HttpTrigger1"
username = "chames"
password = "yahia"
auth_string = f"{username}:{password}"
auth_header = base64.b64encode(auth_string.encode()).decode()
print(auth_header)