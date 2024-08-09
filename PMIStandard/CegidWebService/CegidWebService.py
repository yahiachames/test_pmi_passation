from zeep import Client
from zeep.transports import Transport
from zeep.plugins import HistoryPlugin
from zeep.settings import Settings
from requests.auth import HTTPBasicAuth
import requests
import os
import logging
from lxml import etree
import xml.etree.ElementTree as ET
from zeep import helpers
import json

class CegidWebService:
    def __init__(self, wsdl_url, username, password,DatabaseId , fileLink="") -> None:
        """
        Initializes the CegidWebService with the given WSDL URL, username, and password.

        Args:
            wsdl_url (str): The URL to the WSDL of the web service.
            username (str): The username for authentication.
            password (str): The password for authentication.
        """
        self.wsdl_url = wsdl_url
        self.username = username
        self.password = password
        self.client = {}
        self.DatabaseId = DatabaseId
        self.history  = None
        self.fileLink = fileLink

    def create_soap_connector(self):
        """
        Creates and returns a SOAP client using the given WSDL URL.
        """
                # Create a session and set up basic authentication
        session = requests.Session()
        session.auth = HTTPBasicAuth(self.username, self.password)
        history = HistoryPlugin()
        headerArr = {}
        settings = Settings(strict=False, xml_huge_tree=True,extra_http_headers=headerArr)
        # Create a transport with the session
        transport = Transport(session=session   )

        # Create the SOAP client
        client = Client(wsdl=self.wsdl_url, transport=transport,plugins=[history],
                    settings=settings)
        logging.info("hello world")
        request_data = {
    'text': 'chames Message',  # Text to pass for testing the consumption
    'clientContext': {
        'DatabaseId': 	self.DatabaseId  # You can set this if needed
    }
}

        
        logging.info(client.service._operations)
        logging.info(client.service.HelloWorld(**request_data))

        self.client = client
        self.history = history
        return history

    def create_order_document(self, document_type, data):
        """
        Creates a new sales document in the system based on the document type.

        Args:
            document_type (str): The type of document to create ('return', 'sales', or 'replacement').
            data (dict): The data required to create the document.

        Returns:
            dict: The response from the web service containing the created document details.
        """
        logging.info("salemou alykom order type nivau ali")
        logging.info(document_type)
        document_type = document_type.lower()  # Convert to lowercase
        if document_type == 'return':
            return self.create_return_document(data,document_type)
            
        elif document_type == 'sale':
            return self.create_sales_document(data,document_type)
          
        elif document_type == 'replacement':
            return self.create_replacement_document(data,document_type)
           
        else:
            raise ValueError("Invalid document type. Expected 'return', 'sales', or 'replacement'.")

    def create_return_document(self, data, document_type):
        """
        Creates a return document in the system.

        Args:
            data (dict): The data required to create the return document.

        Returns:
            dict: The response from the web service containing the created return document details.
        """
        
        logging.info("iq payload from return") 
        logging.info(data)
        logging.info(data["items"])
        request_data = {
            "createRequest": {
                "DeliveryAddress": {
                    "FirstName": data.get("firstName", ""),
                    "LastName": data.get("lastName", "")
                },
                "Header": {
                    "Active": 1,
                    "Comment": self.fileLink,
                    "CustomerId": "SC00004000",
                    "Date": data.get("orderDate", ""),
                    "ExternalReference": data.get("orderNumber", "").replace('RET', ''),
                    "InternalReference": data.get("orderNumber", ""),
                    "OmniChannel": {
                        "BillingStatus": "Totally",
                        "DeliveryType": "ShipByCentral",
                        "FollowUpStatus": "Validated",
                        "PaymentStatus": "Totally",
                        "ReturnStatus": "NotReturned",
                        "ShippingStatus": "Totally"
                    },
                    "Origin": "ECommerce",
                    "StoreId": data.get("store_id", "IQST01"),
                    "Type": "Receipt",
                    "UserDefinedTables": {
                        "UserDefinedTable": {
                            "Id": 1,
                            "Value": "SAL05"
                        }
                    },
                    "WarehouseId": data.get("warehouseId", "IQWH01")
                },
                "Lines": { "Create_Line": self.create_lines(data["items"], "0") },
                "Payments": {
                    "Create_Payment": self.create_payments(data,document_type)},
                "ShippingTaxes": {
                    "Create_ShippingTax": {
                        "Amount": 0,
                        "Id": "FPORT"
                    }
                }
            },
            "clientContext": {
                "DatabaseId": self.DatabaseId 
            }
        }

        try:
            logging.info("return payload")
            logging.info(request_data)
            response = self.client.service.Create(**request_data)
            logging.info(response)
            logging.info("Request:\n%s", self.history.last_sent['envelope'])  
            logging.info("Response:\n%s", self.history.last_received['envelope'])
            return (200, "success", json.loads(json.dumps(helpers.serialize_object(response))))
        except Exception as e:
            logging.error("Exception occurred", exc_info=True)
            error_message = etree.tostring(self.history.last_received["envelope"], encoding="unicode", pretty_print=True)
            root = ET.fromstring(error_message)

            # Find the error message elements
            error_elements = root.findall(".//{http://www.cegid.fr/fault}Message")

            # Extract the desired error messages
            error_messages = []
            for element in error_elements:
                message = element.text.strip()
                if "-" in message:
                    error_lines = message.split("\n")
                    for line in error_lines:
                        if "-" in line:
                            error_message = line.strip().lstrip("- ")
                            error_message = error_message.split('- ')[1]
                            error_messages.append(error_message)
            
            if error_messages:
                final_error_message = "\n".join(error_messages)
            else:
                final_error_message = "An unknown error occurred."

            logging.info(final_error_message)   
            return (500, final_error_message, None) 
    def create_sales_document(self, data,document_type):
        """
        Creates a sales document in the system.

        Args:
            data (dict): The data required to create the sales document.

        Returns:
            dict: The response from the web service containing the created sales document details.
        """
        logging.info("iq payload from sales") 
        logging.info(document_type)
        logging.info(data)
        logging.info(data["items"])
        request_data = {
            "createRequest": {
                "DeliveryAddress": {
                    "FirstName": data.get("firstName", ""),
                    "LastName": data.get("lastName", "")
                },
                "Header": {
                    "Active": 1,
                    "Comment": self.fileLink,
                    "CustomerId": "SC00004000",
                    "Date": data.get("orderDate", ""),
                    "InternalReference": data.get("orderNumber", ""),
                    "OmniChannel": {
                        "BillingStatus": "Totally",
                        "DeliveryType": "ShipByCentral",
                        "FollowUpStatus": "Validated",
                        "PaymentStatus": "Totally",
                        "ReturnStatus": "NotReturned",
                        "ShippingStatus": "Totally"
                    },
                    "Origin": "ECommerce",
                    "StoreId": data.get("store_id", "IQST01"),
                    "Type": "Receipt",
                    "UserDefinedTables": {
                        "UserDefinedTable": {
                            "Id": 1,
                            "Value": "SAL01"
                        }
                    },
                    "WarehouseId": data.get("warehouseId", "IQWH01")
                },
                "Lines": {  "Create_Line"  : self.create_lines(data["items"],data["shippingCost"] if "shippingCost" in data else "0" )},
                "Payments": {
                    "Create_Payment": self.create_payments(data,document_type)
                    
                },
                "ShippingTaxes": {
                    "Create_ShippingTax": {
                        "Amount": 0,
                        "Id": "FPORT"
                    }
                }
            },
            "clientContext": {
                "DatabaseId": self.DatabaseId 
            }
        }
        try:
            logging.info("sale payload")
            logging.info(request_data)
            response = self.client.service.Create(**request_data)
            logging.info(response)
            logging.info(
                "Request:\n%s"
                , self.history.last_sent[
                'envelope'
            ])  
                    
            logging.info(
                "Response:\n%s"
                , self.history.last_received[
                'envelope'
            ])
            return (200,"success",json.loads(json.dumps(helpers.serialize_object(response))))
        except:
            error_message = etree.tostring(self.history.last_received["envelope"], encoding="unicode", pretty_print=True)
            root = ET.fromstring(error_message)

            # Find the error message elements
            error_elements = root.findall(".//{http://www.cegid.fr/fault}Message")

            # Extract the desired error messages
            for element in error_elements:
                message = element.text.strip()
                if "-" in message:
                    error_lines = message.split("\n")
                    for line in error_lines:
                        if "-" in line:
                            error_message = line.strip().lstrip("- ")
                            error_message = error_message.split('- ')[1]
            logging.info(error_message) 
            return (500,error_message , None)  
    def create_payments(self , data , orderType):
 
            items = data["items"]
            payments = []
            total_discount = 0
            payments.append({
                        "Amount": data.get("totalAmount", ""),
                        "CurrencyId": "AED",
                        "DueDate": data.get("orderDate", ""),
                        "Id": 1,
                        "IsReceivedPayment": 0,
                        "MethodId": data.get("paymentType", "")
                    })
            logging.info("salemou alykom orderType create_payments")
            logging.info(orderType)
            if orderType == "sale"  : 
                for item in items:
                    originalPrice = float(item["originalPrice"])
                    finalPrice = float(item["finalPrice"])
  
                    if ((originalPrice - finalPrice != 0) and item["promotionId"] != "" ) :
                        difference =  (originalPrice - finalPrice ) * float(item["quantity"])
                        total_discount += difference
                        logging.info("checking calc")
                        logging.info(f"{difference} , {total_discount},{data['promotionId']}")
                
                payments.append({  "Amount": total_discount,
                                "CurrencyId": "AED",
                                "DueDate": data.get("orderDate", ""),
                                "Id": 1,
                                "IsReceivedPayment": 0,
                                "MethodId": "10"
                            })
            return payments
        




    def create_replacement_document(self, data , document_type):
        """
        Creates a replacement document in the system.

        Args:
            data (dict): The data required to create the replacement document.

        Returns:
            dict: The response from the web service containing the created replacement document details.
        """
        request_data = {
            "createRequest": {
                "DeliveryAddress": {
                    "FirstName": data.get("firstName", ""),
                    "LastName": data.get("lastName", "")
                },
                "Header": {
                    "Active": 1,
                    "Comment": self.fileLink,
                    "CustomerId": "SC00004000",
                    "Date": data.get("orderDate", ""),
                    "ExternalReference": data.get("orderNumber", "").replace('REP', ''),
                    "InternalReference": data.get("orderNumber", ""),
                    "OmniChannel": {
                        "BillingStatus": "Totally",
                        "DeliveryType": "ShipByCentral",
                        "FollowUpStatus": "Validated",
                        "PaymentStatus": "Totally",
                        "ReturnStatus": "NotReturned",
                        "ShippingStatus": "Totally"
                    },
                    "Origin": "ECommerce",
                    "StoreId": data.get("store_id", "IQST01"),
                    "Type": "Receipt",
                    "UserDefinedTables": {
                        "UserDefinedTable": {
                            "Id": 1,
                            "Value": "SAL03"
                        }
                    },
                    "WarehouseId": data.get("warehouseId", "IQWH01")
                },
                "Lines": {  "Create_Line"  : self.create_lines(data["items"],"0")},
                "Payments": {
                    "Create_Payment": self.create_payments(data,document_type)
                },
                "ShippingTaxes": {
                    "Create_ShippingTax": {
                        "Amount": 0,
                        "Id": "FPORT"
                    }
                }
            },
            "clientContext": {
                "DatabaseId": self.DatabaseId 
            }
        }
        logging.info("replacement payload")
        logging.info(request_data)
        try:
            logging.info(request_data)
            response = self.client.service.Create(**request_data)
            logging.info(response)
            logging.info(
                "Request:\n%s"
                , self.history.last_sent[
                'envelope'
            ])  
                    
            logging.info(
                "Response:\n%s"
                , self.history.last_received[
                'envelope'
            ])
            return (200,"success",json.loads(json.dumps(helpers.serialize_object(response))))
        except:
            error_message = etree.tostring(self.history.last_received["envelope"], encoding="unicode", pretty_print=True)
            root = ET.fromstring(error_message)

            # Find the error message elements
            error_elements = root.findall(".//{http://www.cegid.fr/fault}Message")

            # Extract the desired error messages
            for element in error_elements:
                message = element.text.strip()
                if "-" in message:
                    error_lines = message.split("\n")
                    for line in error_lines:
                        if "-" in line:
                            error_message = line.strip().lstrip("- ")
                            error_message = error_message.split('- ')[1]
            logging.info(error_message) 
            return (500,error_message,None)

    def create_lines(self, list_of_item, shippingCost="0"):
        """
        Creates lines for the sales document.

        Args:
            list_of_item (list): The list of items to include in the document.
            shippingCost (str): The shipping cost to include in the document.

        Returns:
            list: The list of lines formatted for the request.
        """
        lines = []
        for item in list_of_item:
            lines.append(
               {
                   "ExternalReference" : float(item.get("originalPrice", "")) - float(item.get("finalPrice", "")),
                    "ItemIdentifier": {
                        "Reference": item.get("sku", "")
                    },
                    "NetUnitPrice": item.get("finalPrice", ""),
                    "Origin": "ECommerce",
                    "Quantity": item.get("quantity", ""),
                    "SerialNumberId": item.get("codentifier", ""),
                    "UnitPrice": item.get("originalPrice", ""),
                    "CatalogReference": item.get("promotionId", "")
                }
            )
        # if shippingCost != "0" and  shippingCost != "":
        #     lines.append({
        #             "ItemIdentifier": {
        #                 "Reference": "2222222222222"
        #             },
        #             "NetUnitPrice": shippingCost,
        #             "Origin": "ECommerce",
        #             "Quantity": "1",
        #             "SerialNumberId": "",
        #             "UnitPrice": shippingCost,
        #             "CatalogReference": ""
        #         })

        return lines
    
    def transfer(self,lines_items, orderDate="", orderNumber=""):
        """
        Constructs the JSON request data for the transfer service.

        Args:
            lines_items (list): The list of line items to include in the transfer.
            orderDate (str): The date of the order in DD-MM-YYYY format.
            orderNumber (str): The number of the order.
            **kwargs: Additional keyword arguments.

        Returns:
            dict: The JSON request data for the transfer service.
        """
        request_data = {
            "Request": {
                "Header": {
                    "CurrencyId": "AED",
                    "Date": orderDate,
                    "DocumentTypeToCreate": "SentTransfer",
                    "ExternalReference": f"exter{orderNumber}",
                    "FollowUpReference": f"follow{orderNumber}",
                    "InternalReference": f"trans{orderNumber}",
                    "Recipient": {
                        "StoreId": "IQST01",
                        "WarehouseId": "IQWHDF",
                    },
                    "Sender": {
                        "StoreId": "IQST01",
                        "WarehouseId": "IQWH01",
                    },
                    "TaxIncluded": False,
                },
                "Lines": []
            }
             
        }


        for line in lines_items:
            if int(line["quantity"]) < 0:
                request_data["Request"]["Lines"].append({
                    "Line": {
                        "ItemIdentifier": {
                            "Reference": line["sku"]
                        },
                        "Quantity": 1,
                        "SenderWarehouseId":"IQWH01",
                        "SerialNumberId": line["codentifier"],
                        "UnitPriceBase": line["originalPrice"]
                    }
                })
        clientContext = {
            "Context": {
                "DatabaseId": self.DatabaseId 
            }
        }

        try:

            logging.info("transsfer client.service._operations")
            logging.info(self.client.service._operations)
            response = self.client.service.Create(**request_data,**clientContext)
            logging.info(response)
            logging.info("Request:\n%s", self.history.last_sent['envelope'])  
            logging.info("Response:\n%s", self.history.last_received['envelope'])
            return (200,"success",json.loads(json.dumps(helpers.serialize_object(response))))
        except:
            error_message = etree.tostring(self.history.last_received["envelope"], encoding="unicode", pretty_print=True)
            root = ET.fromstring(error_message)
            error_elements = root.findall(".//{http://www.cegid.fr/fault}Message")
            for element in error_elements:
                message = element.text.strip()
                if "-" in message:
                    error_lines = message.split("\n")
                    for line in error_lines:
                        if "-" in line:
                            error_message = line.strip().lstrip("- ")
                            error_message = error_message.split('- ')[1]
            logging.info(error_message) 
            return (500,error_message , None) 
        
    def get_by_key_document(self, data):
        """
        Retrieves a document by key from the system.

        Args:
            data (dict): The data required to retrieve the document.

        Returns:
            dict: The response from the web service containing the document details.
        """
        request_data = {
          
                "searchRequest": {
                    "Key": {
                        "Number": data["Number"],
                        "Stump": data["Stump"],
                        "Type": data["Type"]
                    }
                }
            } 
        client_context = { "clientContext": {
                    "DatabaseId": self.DatabaseId
                }}
        

        try:
            response = self.client.service.GetByKey(**request_data , **client_context)
            logging.info(response)
            # logging.info("Request:\n%s", self.history.last_sent['envelope'])  
            # logging.info("Response:\n%s", self.history.last_received['envelope'])
 
            return (200,"success",dict(helpers.serialize_object(response)))
        except:
            error_message = etree.tostring(self.history.last_received["envelope"], encoding="unicode", pretty_print=True)
            root = ET.fromstring(error_message)

            # Find the error message elements
            error_elements = root.findall(".//{http://www.cegid.fr/fault}Message")

            # Extract the desired error messages
            for element in error_elements:
                message = element.text.strip()
                if "-" in message:
                    error_lines = message.split("\n")
                    for line in error_lines:
                        if "-" in line:
                            error_message = line.strip().lstrip("- ")
                            error_message = error_message.split('- ')[1]
            logging.info(error_message) 
            return (500,error_message , None)    
