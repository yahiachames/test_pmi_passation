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

def main(req: func.HttpRequest) -> func.HttpResponse:

    try : 
        auth = Authentication(req)
        logging.info(req)
        auth_result = auth.authenticate()

        if auth_result :
            return auth_result
        data = req.get_json()

        #Transformation
        data["orderDate"] = data["orderDate"][-4:] + '-' + data["orderDate"][3:5] + '-' + data["orderDate"][0:2]
        logging.info(data["orderDate"])
        if data['orderNumber'].startswith("RET"):
            data["orderType"] = "return"
        data["orderType"] = data["orderType"].lower()

        logging.info("iq payload")
        logging.info(data)

        #fileLink
        current_date = datetime.now().strftime("%d-%m-%Y")
        fileLink = os.environ.get("fileLink") + data["orderType"] + "/" + current_date + "/" + data["orderNumber"] + ".pdf"


        #table_client
        tableClient = AzureDataTablesClient("OrderIds",os.environ.get("storage_account_connections_tring"))
        tableClient.connect_table_service()
        entity_properties = {
            'PartitionKey': 'Invoices',
            'RowKey': data['orderNumber']
        }
        tableClient.create_entity(entity_properties)

        #cegid create connection
        data["DatabaseId"]  = os.environ.get("database_id")
        cegidWebService = CegidWebService(os.environ.get("cegid_sale_wsdl") , os.environ.get("cegid_soap_username") , os.environ.get("cegid_soap_password"),data["DatabaseId"],fileLink)
        history = cegidWebService.create_soap_connector()    



        #connect to blob azure
        blobClient = AzureBlobProcessor("$web",data["orderType"],os.environ.get("static_blob_connection_string"))
        

        #create order web services
        response_create_rep = cegidWebService.create_order_document(data["orderType"], data)
        logging.info(response_create_rep)
        status_code = response_create_rep[0]
        response_msg = response_create_rep[1]
        response_content = response_create_rep[2]


        # transfer in case creaet order of type replacement
        if data["orderType"].lower() == "replacement":
            cegidWebServiceTransfer = CegidWebService(os.environ.get("cegid_sale_transfert") , os.environ.get("cegid_soap_username") , os.environ.get("cegid_soap_password"),data["DatabaseId"])
            history = cegidWebServiceTransfer.create_soap_connector()   
            response_create_transfer = cegidWebServiceTransfer.transfer(data["items"], data["orderDate"] , data["orderNumber"])
            status_code = (status_code + response_create_transfer[0]) / 2
            response_msg = f"replacement {response_msg} transfer {response_create_transfer[1]}"
            logging.info(response_msg)


        



        if status_code == 200:
            #cegid web service get by key
            logging.info("response_content")
            logging.info(response_content)

            Numbera = response_content["Number"]
            Stumpa = response_content["Stump"]
            Typea = response_content["Type"]


            #cegid web service get by key
            cegidWebServiceGetByKey = CegidWebService(os.environ.get("cegid_sale_wsdl") , os.environ.get("cegid_soap_username") , os.environ.get("cegid_soap_password"),data["DatabaseId"])
            history = cegidWebServiceGetByKey.create_soap_connector() 
            response_get_item = cegidWebServiceGetByKey.get_by_key_document(response_content) 
            logging.info("response_get_item")
            logging.info(response_get_item[2])
            logging.info(type(response_get_item[2]))
            data_dict = response_get_item[2]



            data_dict2 = {
        'DeliveryAddress': {
            'City': None,
            'ContactNumber': 0,
            'CountryId': None,
            'CountryIdType': 'Internal',
            'Email': None,
            'ExternalReference': None,
            'FirstName': 'السيد السيد',
            'Individual': True,
            'LastName': None,
            'Line1': None,
            'Line2': None,
            'Line3': None,
            'PhoneNumber': None,
            'Region': None,
            'TitleId': None,
            'ZipCode': None
        },
        'Header': {
            'Active': True,
            'Comment': 'https://adfariontest.blob.core.windows.net/adfariontest//tmp/00344122.pdf',
            'CurrencyId': 'AED',
            'CustomerId': 'SC00004000',
            'Date': datetime(2024, 7, 24, 0, 0),
            'InternalReference': '00344122',
            'Key': {
                'Number': 44,
                'Stump': 'IQST01',
                'Type': 'Receipt'
            },
            'OmniChannel': {
                'BillingStatus': None,
                'CancelDate': None,
                'CancelReasonId': None,
                'CancelStatus': None,
                'Comment': None,
                'DeliveryStoreId': None,
                'DeliveryType': None,
                'DeliveryWarehouseId': None,
                'FollowUpStatus': None,
                'GiftMessage': None,
                'GiftMessageType': None,
                'LockingDate': None,
                'OriginStoreId': None,
                'OriginalDocument': None,
                'PaymentMethodId': None,
                'PaymentStatus': None,
                'PreferCustomerDelivery': None,
                'ReturnStatus': None,
                'ReturnType': None,
                'ShippingStatus': None,
                'Tracking': None,
                'Transporter': None
            },
            'Origin': 'ECommerce',
            'SalesPersonId': None,
            'Status': 'None',
            'StoreId': 'IQST01',
            'TaxExcludedTotalAmount': Decimal('8.5700'),
            'TaxIncludedTotalAmount': Decimal('9.0000'),
            'TotalQuantity': Decimal('2.0000'),
            'UserDefinedDates': {
                'UserDefinedDate': [
                    {
                        'Id': '1',
                        'Value': datetime(1900, 1, 1, 0, 0)
                    },
                    {
                        'Id': '2',
                        'Value': datetime(1900, 1, 1, 0, 0)
                    },
                    {
                        'Id': '3',
                        'Value': datetime(1900, 1, 1, 0, 0)
                    }
                ]
            },
            'UserDefinedTables': {
                'UserDefinedTable': [
                    {
                        'Id': '1',
                        'Value': 'SAL01'
                    },
                    {
                        'Id': '2',
                        'Value': None
                    },
                    {
                        'Id': '3',
                        'Value': None
                    }
                ]
            },
            'UserFields': None,
            'WarehouseId': 'IQWH01'
        },
        'InvoicingAddress': {
            'City': None,
            'ContactNumber': None,
            'CountryId': None,
            'CountryIdType': None,
            'Email': None,
            'ExternalReference': None,
            'FirstName': None,
            'Individual': None,
            'LastName': None,
            'Line1': None,
            'Line2': None,
            'Line3': None,
            'PhoneNumber': None,
            'Region': None,
            'TitleId': None,
            'ZipCode': None
        },
        'Lines': {
            'Get_Line': [
                {
                    'CatalogReference': None,
                    'Comment': None,
                    'ComplementaryDescription': 'HEETS DIMENSIONS AMMIL BUNDLE (10)',
                    'DeliveryDate': datetime(2024, 7, 24, 0, 0),
                    'DiscountTypeId': None,
                    'ExternalReference': None,
                    'InitialDeliveryDate': datetime(2024, 7, 24, 0, 0),
                    'ItemCode': 'PH0111CT',
                    'ItemId': 'PH0111CT                         X',
                    'ItemReference': '7622100750239',
                    'Label': 'HEETS - AMMIL MNT',
                    'MovementReasonId': None,
                    'OmniChannel': {
                        'FollowUpStatus': None,
                        'WarehouseId': None
                    },
                    'Origin': 'ECommerce',
                    'PackageReference': None,
                    'Quantity': Decimal('1.0000'),
                    'Rank': 1,
                    'SalesPersonId': None,
                    'SerialNumberId': 'SE000003',
                    'TaxExcludedNetUnitPrice': Decimal('6.6700'),
                    'TaxExcludedUnitPrice': Decimal('6.6700'),
                    'TaxIncludedNetUnitPrice': Decimal('7.0000'),
                    'TaxIncludedUnitPrice': Decimal('7.0000'),
                    'TaxeNotCalculated': False,
                    'WarehouseId': 'IQWH01'
                },
                {
                    'CatalogReference': None,
                    'Comment': None,
                    'ComplementaryDescription': None,
                    'DeliveryDate': datetime(2024, 7, 24, 0, 0),
                    'DiscountTypeId': None,
                    'ExternalReference': None,
                    'InitialDeliveryDate': datetime(2024, 7, 24, 0, 0),
                    'ItemCode': 'SHIPPING COST',
                    'ItemId': 'SHIPPING COST                    X',
                    'ItemReference': '2222222222222',
                    'Label': 'shipping cost',
                    'MovementReasonId': None,
                    'OmniChannel': {
                        'FollowUpStatus': None,
                        'WarehouseId': None
                    },
                    'Origin': 'ECommerce',
                    'PackageReference': None,
                    'Quantity': Decimal('1.0000'),
                    'Rank': 2,
                    'SalesPersonId': None,
                    'SerialNumberId': None,
                    'TaxExcludedNetUnitPrice': Decimal('1.9000'),
                    'TaxExcludedUnitPrice': Decimal('1.9000'),
                    'TaxIncludedNetUnitPrice': Decimal('2.0000'),
                    'TaxIncludedUnitPrice': Decimal('2.0000'),
                    'TaxeNotCalculated': False,
                    'WarehouseId': 'IQWH01'
                }
            ]
        },
        'Payments': {
            'Get_Payment': [
                {
                    'Amount': Decimal('15.0000'),
                    'CashAmount': Decimal('15.0000'),
                    'Code': 'EMC',
                    'CreditCard': {
                        'AuthorizationNumber': None,
                        'TransactionIssuer': None,
                        'TransactionNumber': None,
                        'TransactionReference': None
                    },
                    'CustomerCredit': {
                        'BalanceAmount': None
                    },
                    'CustomerOrderReference': None,
                    'Date': datetime(2024, 7, 24, 0, 0),
                    'DetailExternalReference': None,
                    'DocumentNumber': 'IQ-000023',
                    'DiscountAmount': Decimal('0.0000'),
                    'ExternalReference': None,
                    'GiftVoucher': {
                        'AuthorizationNumber': None
                    },
                    'ReturnAuthorization': None
                }
            ]
        },
        'Rounding': {
            'Amount': Decimal('0.4300'),
            'IsRounded': False
        },
        'Status': 'Created'
    }       
            delivery_address = data_dict['DeliveryAddress']
            header = data_dict["Header"]
            last_name = delivery_address.get('LastName', " ")
            if last_name == "WALK IN CUSTOMER" or last_name == None:
                last_name = ""
            first_name = delivery_address.get('FirstName', " ")
            customer_full_name = f"{first_name} {last_name}".strip()
            invoice_time = datetime.now() + timedelta(hours=3)

            def process_data(data):
                formatted_lines = []
                Totale_orig = 0
                total_discount = 0 
                total_after_discount = 0
                total_before_tva = 0
                total_tva = 0
                total_after_tva = 0

                Amount_excl = data["Header"]["TaxExcludedTotalAmount"]
                VAT = data["Header"]["TaxIncludedTotalAmount"] - data["Header"]["TaxExcludedTotalAmount"]
                total_after_vat = data["Header"]['TaxIncludedTotalAmount']

                for item in data['Lines']['Get_Line']:
                    try:
                        reference = item['ItemReference']
                        description = item['Label']
                        
                        tax_included_price = round(float(item['TaxIncludedNetUnitPrice']), 3)
                        tax_excluded_price = round(float(item['TaxExcludedUnitPrice']), 3)
                        value_hors_tva = round(float(item['TaxExcludedNetUnitPrice']), 3)
                        quantity = int(float(item['Quantity']))
                        total_after_discount += value_hors_tva * quantity
                        tva = tax_excluded_price - value_hors_tva

                        orig_price = f'{tax_excluded_price:.2f}'
                        value_hors_tva_str = f'{value_hors_tva:.2f}'
                        tva_str = f'{tva:.2f}'

                        total = round(tax_excluded_price * quantity, 3)
                        total_str = f'{total:.2f}'
                        Totale_orig += value_hors_tva

                        # Totale_orig.append(total_str)

                        first_line = [reference, description, quantity, orig_price, value_hors_tva_str, tva_str, total_str]
                        formatted_lines.append(first_line)

                        if 'ComplementaryDescription' in item and item['ComplementaryDescription']:
                            arabic_text = item['ComplementaryDescription']
                            formatted_lines.append(['', f'{arabic_text} arabic', '', '', '', '', ''])

                        if 'SerialNumberId' in item and item['SerialNumberId']:
                            item['CatalogReference'] = "AED50"
                            serial_number = item['SerialNumberId']
                            formatted_lines.append(['', serial_number, '', '', '', '', ''])

                        if 'CatalogReference' in item and item['CatalogReference']:
                            voucher_number = item['CatalogReference']
                            formatted_lines.append(['', f'{voucher_number} voucher applied', '', '', '', '', ''])

                        if item['TaxIncludedNetUnitPrice'] != item['TaxIncludedUnitPrice']:
                            discount_amount = float(item['TaxIncludedUnitPrice']) - tax_included_price
                            discount_perc = round((discount_amount / float(item['TaxIncludedUnitPrice'])) * 100, 1)
                            discount_total = round(discount_amount * quantity, 3)
                            total_discount += item['TaxIncludedUnitPrice'] * quantity
                            formatted_lines.append(['', f'DISCOUNT on {discount_perc}% -{discount_amount:.3f}', '', '', '', '', ''])
                            formatted_lines.append(['', f'Orig. Amount: {float(item["TaxIncludedUnitPrice"]):.3f}', '', '', '', '', ''])
                    except (KeyError, ValueError) as e:
                        logging.info(f"Error processing item: {item}, error: {e}")

                if formatted_lines and 'shipping cost' in formatted_lines[-1]:
                    formatted_lines[-1][formatted_lines[-1].index('shipping cost')] = 'Shipment fees'
                Totale_orig = f'{Totale_orig:.2f}'
                total_discount = f'{total_discount:.2f}'
                total_after_discount = f'{total_after_discount:.2f}'
                total_before_tva = f'{total_before_tva:.2f}'
                total_tva = f'{total_tva:.2f}'
                total_after_vat = f'{total_after_vat:.2f}'
                VAT = f'{VAT:.2f}'
                Amount_excl = f'{Amount_excl:.2f}'
                return formatted_lines , Totale_orig , total_discount , total_after_discount , total_before_tva , total_tva , total_after_tva , Amount_excl , VAT , total_after_vat 

            formatted_lines , Totale_orig , total_discount , total_after_discount , total_before_tva , total_tva , total_after_tva , Amount_excl , VAT , total_after_vat  = process_data(data_dict)
            logging.info(formatted_lines)

    

            def generate_pdf_with_barcode(
                base_dir: str,
                template_file: str,
                font_path: str,
                output_file: str,
                barcode_code: str,
                barcode_output_dir: str,
                sample_data: list,
                data: dict
            ) -> bytes:
            

                # Instantiate the PDF configuration service
                pdf_service = PDFConfiguration(
                    base_dir=base_dir,
                    template_file=template_file,
                    font_path=font_path,
                    output_file=output_file
                )

                # Generate and modify the barcode
                barcode_path = os.path.join(barcode_output_dir, barcode_code)
                modified_barcode_buffer, modified_barcode_path = pdf_service.generate_and_modify_barcode(barcode_code, barcode_path)

                # Update data dictionary with barcode path
                data.update({
                    "barcode_path": f"file://{modified_barcode_path}",
                    "font_path": font_path
                })

                # Generate PDF
                pdf_bytes = pdf_service.generate_pdf(data,font_path)
                
                return pdf_bytes
            


            BASE_DIR = Path(__file__).resolve().parent.parent
            BASE_DIR = str(BASE_DIR)
            FONT_PATH = os.path.join(BASE_DIR, "PMIStandard","PDF_Generator", 'fonts')
            TEMPLATE_FILE = "template_pmi.html"
            OUTPUT_FILE = "output_jinja_new_font.pdf"
            BARCODE_CODE = "123456781234"
            BARCODE_OUTPUT_DIR = '/tmp'
            SAMPLE_DATA = [
                ['7622100690917', 'IQOS ORIGINALS ONE MOBILITY KIT - SLATE', 1, '9.600', '12', '12', '9.600'],
                ['', 'أيقوص أوريجينالز وان موبايلتي كيت - arabic', '', '', '', '', ''],
                ['', 'AKXVA31C185P', '', '', '', '', ''],
                ['2222222222222', 'shipping cost', 1, '0.000', '12', '12', '0.000'],
                # Add more data as needed
            ]
            DATA = {
                "data": formatted_lines,
                "image_path": f"file://{os.path.join(BASE_DIR, 'PMIStandard' , 'PDF_Generator', 'assets', 'ALAB_Logo.png')}",
                "customer_name": customer_full_name,
                "invoice_number": Numbera,
                "invoice_date": header.get("Date","").strftime("%B %d, %Y"),
                "invoice_time":invoice_time.strftime("%I:%M %p"),
                "invoice_code": data["orderNumber"],
                "order_type": data["orderType"],
                'method_of_payment': 'Credit Card',
                'amount': '100.00',
                'quantity': '5',
                'total_original_amount': Totale_orig,
                'total_discount': total_discount,
                'total_after_discount': total_after_discount,
                'amount_excl_vat': Amount_excl,
                'vat_amount': VAT,
                'total_after_vat': total_after_vat,
            
            
            }




            # Generate PDF
            pdf_bytes = generate_pdf_with_barcode(
                base_dir=BASE_DIR,
                template_file=TEMPLATE_FILE,
                font_path=FONT_PATH,
                output_file=OUTPUT_FILE,
                barcode_code=BARCODE_CODE,
                barcode_output_dir=BARCODE_OUTPUT_DIR,
                sample_data=SAMPLE_DATA,
                data=DATA
            )
            blobClient.insertOneFile(data["orderNumber" ] + ".pdf",pdf_bytes)
            logging.info("after insertion in azure")
            
            # return func.HttpResponse(
            #     body=pdf_bytes,
            #     headers={
            #         "Content-Type": "application/pdf",
            #         "Content-Disposition": "attachment; filename=output_jinja_new_font.pdf"
            #     },
            #     status_code=200
            # )

            return func.HttpResponse(
            json.dumps({"status": status_code, "content" : {"message": response_msg , 
                                                            "fileLink" : fileLink , 
                                                            "orderDate" : data["orderDate"] , 
                                                            "orderNumber" : data["orderNumber"] ,
                                                            "invoiceNumber" : Numbera
                                                            
                                                            }}),
            status_code=200,
            mimetype="application/json"
        )
        else:
            return func.HttpResponse(
            json.dumps({"status": status_code, "message": response_msg}),
            status_code=500,
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
 