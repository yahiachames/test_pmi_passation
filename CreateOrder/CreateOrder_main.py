import azure.functions as func
import json
import logging
import os
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
import math
from PMIStandard.PDF_Generator.PDFConfiguration import PDFConfiguration
from PMIStandard.Authentication import Authentication
from PMIStandard.AzureStorage.TableStorage import AzureDataTablesClient
from PMIStandard.AzureStorage.BlobStorage import AzureBlobProcessor
from PMIStandard.CegidWebService.CegidWebService import CegidWebService


class OrderProcessor:
    def __init__(self, req: func.HttpRequest):
        self.req = req
        self.data = req.get_json()
        self.file_link = None
        self.cegid_web_service = None
        self.blob_client = None

    def authenticate(self):
        auth = Authentication(self.req)
        logging.info(self.req)
        auth_result = auth.authenticate()
        return auth_result

    def transform_order_date(self):
        order_date_str = self.data["orderDate"]
        parsed_date = datetime.strptime(order_date_str, '%Y-%m-%d')
        formatted_date = parsed_date.strftime('%d-%m-%Y')
        self.data["orderDate"] = formatted_date[-4:] + '-' + formatted_date[3:5] + '-' + formatted_date[0:2]

    def determine_order_type(self):
        if self.data['orderNumber'].startswith("RET"):
            self.data["orderType"] = "return"
        self.data["orderType"] = self.data["orderType"].lower()

    def set_file_link(self):
        current_date = datetime.now().strftime("%d-%m-%Y")
        self.file_link = os.environ.get("fileLink") + self.data["orderType"] + "/" + current_date + "/" + self.data["orderNumber"] + ".pdf"

    def connect_to_table_storage(self):
        table_client = AzureDataTablesClient("OrderIds", os.environ.get("storage_account_connections_tring"))
        table_client.connect_table_service()
        entity_properties = {'PartitionKey': 'Invoices', 'RowKey': self.data['orderNumber']}
        table_client.create_entity(entity_properties)

    def create_cegid_connection(self):
        self.data["DatabaseId"] = os.environ.get("database_id")
        self.cegid_web_service = CegidWebService(
            os.environ.get("cegid_sale_wsdl"),
            os.environ.get("cegid_soap_username"),
            os.environ.get("cegid_soap_password"),
            self.data["DatabaseId"],
            self.file_link
        )
        self.cegid_web_service.create_soap_connector()

    def connect_to_blob_storage(self):
        self.blob_client = AzureBlobProcessor("$web", self.data["orderType"], os.environ.get("static_blob_connection_string"))

    def create_order_document(self):
        return self.cegid_web_service.create_order_document(self.data["orderType"], self.data)

    def handle_replacement_order(self):
        cegid_web_service_transfer = CegidWebService(
            os.environ.get("cegid_sale_transfert"),
            os.environ.get("cegid_soap_username"),
            os.environ.get("cegid_soap_password"),
            self.data["DatabaseId"]
        )
        cegid_web_service_transfer.create_soap_connector()
        response_create_transfer = cegid_web_service_transfer.transfer(self.data["items"], self.data["orderDate"], self.data["orderNumber"])
        return response_create_transfer

    def get_order_details(self, response_content):
        cegid_web_service_get_by_key = CegidWebService(
            os.environ.get("cegid_sale_wsdl"),
            os.environ.get("cegid_soap_username"),
            os.environ.get("cegid_soap_password"),
            self.data["DatabaseId"]
        )
        cegid_web_service_get_by_key.create_soap_connector()
        response_get_item = cegid_web_service_get_by_key.get_by_key_document(response_content)
        return response_get_item[2]

    def generate_pdf_with_barcode(self, formatted_lines, data_dict, customer_full_name, invoice_time, Numbera):
        BASE_DIR = Path(__file__).resolve().parent.parent
        FONT_PATH = os.path.join(BASE_DIR, "PMIStandard", "PDF_Generator", 'fonts')
        TEMPLATE_FILE = "template_pmi.html"
        OUTPUT_FILE = "output_jinja_new_font.pdf"
        BARCODE_CODE = data_dict['Header']['InternalReference']
        BARCODE_OUTPUT_DIR = '/tmp'
        SAMPLE_DATA = [
            ['7622100690917', 'IQOS ORIGINALS ONE MOBILITY KIT - SLATE', 1, '9.600', '12', '12', '9.600'],
            ['', 'أيقوص أوريجينالز وان موبايلتي كيت - arabic', '', '', '', '', ''],
            ['', 'AKXVA31C185P', '', '', '', '', ''],
            ['2222222222222', 'shipping cost', 1, '0.000', '12', '12', '0.000'],
        ]
        DATA = {
            "data": formatted_lines,
            "image_path": f"file://{os.path.join(BASE_DIR, 'PMIStandard', 'PDF_Generator', 'assets', 'ALAB_Logo.png')}",
            "customer_name": customer_full_name,
            "invoice_number": Numbera,
            "invoice_date": data_dict["Header"].get("Date", "").strftime("%B %d, %Y"),
            "invoice_time": invoice_time.strftime("%I:%M %p"),
            "invoice_code": self.data["orderNumber"],
            "order_type": self.data["orderType"],
            'method_of_payment': ["Ecom card payments"],
            'amount': [self.data["total_after_voucher_and_vat"]],
            'quantity': str(math.trunc(float(data_dict['Header']['TotalQuantity']))),
            'total_original_amount': self.data["Totale_orig"],
            'total_discount': self.data["total_discount"],
            'total_after_discount': self.data["total_after_discount"],
            'total_discount_before_vat': self.data["total_discount_before_vat"],
            'amount_excl_vat': self.data["Amount_excl"],
            'vat_amount': self.data["VAT"],
            'total_after_vat': self.data["total_after_vat"],
            "voucher_value": self.data["voucher_amount_str"],
            "voucher_number": self.data["voucher_number"],
            "total_after_voucher_and_vat": self.data["total_after_voucher_and_vat"]
        }

        pdf_service = PDFConfiguration(
            base_dir=BASE_DIR,
            template_file=TEMPLATE_FILE,
            font_path=FONT_PATH,
            output_file=OUTPUT_FILE
        )
        barcode_path = os.path.join(BARCODE_OUTPUT_DIR, BARCODE_CODE)
        modified_barcode_buffer, modified_barcode_path = pdf_service.generate_and_modify_barcode(BARCODE_CODE, barcode_path)
        DATA.update({"barcode_path": f"file://{modified_barcode_path}", "font_path": FONT_PATH})
        pdf_bytes = pdf_service.generate_pdf(DATA, FONT_PATH)
        self.blob_client.insertOneFile(self.data["orderNumber"] + ".pdf", pdf_bytes)

    def extract_item_details(self, item):
        reference = item['ItemReference']
        description = item['Label']
        tax_included_price = round(float(item['TaxIncludedUnitPrice']), 3)
        tax_unit_included_price = round(float(item['TaxIncludedUnitPrice']), 3)
        tax_included_net_unit_price = round(float(item['TaxIncludedNetUnitPrice']), 3)
        tax_excluded_net_unit_price = round(float(item['TaxExcludedNetUnitPrice']), 3)
        value_hors_tva = round(float(item['TaxExcludedUnitPrice']), 3)
        quantity = int(float(item['Quantity']))
        return reference, description, tax_included_price, tax_unit_included_price, tax_included_net_unit_price, tax_excluded_net_unit_price, value_hors_tva, quantity

    def process_line_item(self, item, data_iq, formatted_lines, totals):
        try:
            reference, description, tax_included_price, tax_unit_included_price, tax_included_net_unit_price, tax_excluded_net_unit_price, value_hors_tva, quantity = self.extract_item_details(item)

            totals['Totale_orig'] += tax_excluded_net_unit_price * quantity
            totals['total_after_discount'] += value_hors_tva * quantity
            tva_after_discount = (tax_included_price - value_hors_tva) * quantity

            orig_price = f'{tax_unit_included_price:.2f}'
            value_hors_tva_str = f'{value_hors_tva:.2f}'
            totals['tva_before_discount'] += (tax_included_net_unit_price - tax_excluded_net_unit_price) * quantity
            tva_str = f'{tva_after_discount:.2f}'
            total = round(tax_unit_included_price * quantity, 3)
            total_str = f'{total:.2f}'
            totals['Amount_excl'] += item["TaxExcludedUnitPrice"] * quantity
            totals['VAT_after_discount'] += tva_after_discount
            totals['total_after_vat'] += tax_included_price * quantity
            totals['total_after_voucher_and_vat'] += tax_unit_included_price * quantity

            if data_iq["orderType"] == "return":
                formatted_lines.append([reference, description, quantity, "-" + orig_price, "-" + value_hors_tva_str, "-" + tva_str, total_str])
            else:
                formatted_lines.append([reference, description, quantity, orig_price, value_hors_tva_str, tva_str, total_str])

            if 'ComplementaryDescription' in item and item['ComplementaryDescription']:
                arabic_text = item['ComplementaryDescription']
                formatted_lines.append(['', f'{arabic_text} arabic', '', '', '', '', ''])

            if 'SerialNumberId' in item and item['SerialNumberId']:
                serial_number = item['SerialNumberId']
                formatted_lines.append(['', serial_number, '', '', '', '', ''])

            if 'CatalogReference' in item and item['CatalogReference'] and data_iq["orderType"] == "sale":
                voucher_number = item['ExternalReference']
                if voucher_number is not None:
                    formatted_lines.append(['', f'{voucher_number} voucher applied', '', '', '', '', ''])

            if item['TaxIncludedNetUnitPrice'] != item['TaxIncludedUnitPrice']:
                discount_amount = abs(float(item['TaxIncludedUnitPrice'])) - abs(float(item['TaxIncludedNetUnitPrice']))
                discount_perc = round((discount_amount / float(item['TaxIncludedUnitPrice'])) * 100, 1)
                discount_total = round(discount_amount * quantity, 3)
                totals['total_discount'].append(discount_total)
                totals['total_discount_before_vat'] += round((abs(float(item['TaxExcludedUnitPrice'])) - abs(float(item['TaxExcludedNetUnitPrice']))) * quantity, 3)
                formatted_lines.append(['', f'DISCOUNT on {discount_perc}% -{discount_amount:.3f}', '', '', '', '', ''])
                formatted_lines.append(['', f'Orig. Amount: {float(item["TaxIncludedUnitPrice"]):.3f}', '', '', '', '', ''])

        except (KeyError, ValueError) as e:
            logging.info(f"Error processing item: {item}, error: {e}")

    def process_payment_item(self, item, totals):
        try:
            if 'Code' in item and item['Code'] == '10':
                totals['voucher_amount'] = round(float(item['Amount']), 3)
                totals['voucher_amount_str'] = f'{totals["voucher_amount"]:.2f}'
                totals['total_after_voucher_and_vat'] -= totals['voucher_amount']

            if 'Code' in item and item['Code'] != '10':
                totals['paid_amount'] = totals['total_after_voucher_and_vat']
                totals['paid_amount_str'] = f'{totals["paid_amount"]:.2f}'

        except (KeyError, ValueError) as e:
            logging.info(f"Error processing item: {item}, error: {e}")

    @staticmethod
    def rename_shipping_cost(formatted_lines):
        if formatted_lines and 'shipping cost' in formatted_lines[-1]:
            formatted_lines[-1][formatted_lines[-1].index('shipping cost')] = 'Shipment fees'

    @staticmethod
    def format_totals(totals):
        Totale_orig = f'{totals["Totale_orig"]:.2f}'
        total_discount = f'{sum(map(float, totals["total_discount"])):.2f}'
        total_after_discount = f'{totals["total_after_discount"]:.2f}'
        total_before_tva = f'{totals["total_before_tva"]:.2f}'
        total_tva = f'{totals["total_tva"]:.2f}'
        total_after_vat = f'{totals["total_after_vat"]:.2f}'
        VAT = f'{totals["VAT_after_discount"]:.2f}'
        Amount_excl = f'{totals["Amount_excl"]:.2f}'
        total_after_voucher_and_vat = f'{totals["total_after_voucher_and_vat"]:.2f}'

        return Totale_orig, total_discount, total_after_discount, total_before_tva, total_tva, total_after_vat, Amount_excl, VAT, totals['voucher_amount_str'], totals['paid_amount_str'], totals['voucher_number']

    def process_data(self, data_iq={}):
        formatted_lines = []
        totals = {
            'Totale_orig': 0,
            'total_discount': [],
            'total_after_discount': 0,
            'total_before_tva': 0,
            'total_tva': 0,
            'total_after_vat': 0,
            'Amount_excl': 0,
            'VAT_after_discount': 0,
            'total_after_voucher_and_vat': 0,
            'tva_before_discount': 0,
            'voucher_amount': 0,
            'voucher_amount_str': "0",
            'paid_amount_str': "0",
            'total_discount_before_vat': 0
        }

        for item in self.data['Lines']['Get_Line']:
            self.process_line_item(item, data_iq, formatted_lines, totals)

        for item in self.data['Payments']['Get_Payment']:
            self.process_payment_item(item, totals)

        self.rename_shipping_cost(formatted_lines)

        return formatted_lines, *self.format_totals(totals)


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        processor = OrderProcessor(req)
        
        auth_result = processor.authenticate()
        if auth_result:
            return auth_result
        
        processor.transform_order_date()
        processor.determine_order_type()
        processor.set_file_link()
        processor.connect_to_table_storage()
        processor.create_cegid_connection()
        processor.connect_to_blob_storage()
        
        response_create_rep = processor.create_order_document()
        logging.info(response_create_rep)
        status_code, response_msg, response_content = response_create_rep
        
        if processor.data["orderType"] == "replacement":
            response_create_transfer = processor.handle_replacement_order()
            status_code = (status_code + response_create_transfer[0]) / 2
            response_msg = f"replacement {response_msg} transfer {response_create_transfer[1]}"
            logging.info(response_msg)
        
        if status_code == 200:
            logging.info("response_content")
            logging.info(response_content)
            
            Numbera = response_content["Number"]
            data_dict = processor.get_order_details(response_content)
            formatted_lines, Totale_orig, total_discount, total_after_discount, total_before_tva, total_tva, total_after_vat, Amount_excl, VAT, voucher_amount_str, paid_amount_str, voucher_number = processor.process_data(data_dict)
            
            delivery_address = data_dict['DeliveryAddress']
            header = data_dict["Header"]
            last_name = delivery_address.get('LastName', "")
            last_name = "" if last_name == "WALK IN CUSTOMER" else last_name
            customer_full_name = f"{delivery_address.get('FirstName', '')} {last_name}".strip()
            invoice_time = datetime.now() + timedelta(hours=3)
            
            processor.data.update({
                "Totale_orig": Totale_orig,
                "total_discount": total_discount,
                "total_after_discount": total_after_discount,
                "total_before_tva": total_before_tva,
                "total_tva": total_tva,
                "total_after_vat": total_after_vat,
                "Amount_excl": Amount_excl,
                "VAT": VAT,
                "voucher_amount_str": voucher_amount_str,
                "paid_amount_str": paid_amount_str,
                "voucher_number": voucher_number,
                "total_after_voucher_and_vat": total_after_voucher_and_vat
            })
            
            processor.generate_pdf_with_barcode(formatted_lines, data_dict, customer_full_name, invoice_time, Numbera)
            logging.info("after insertion in azure")
            
            return func.HttpResponse(
                json.dumps({"status": status_code, "content": {"message": response_msg, "fileLink": processor.file_link, "orderDate": processor.data["orderDate"], "orderNumber": processor.data["orderNumber"], "invoiceNumber": Numbera}}),
                status_code=200,
                mimetype="application/json"
            )
        
        return func.HttpResponse(
            json.dumps({"status": 422, "message": response_msg}),
            status_code=422,
            mimetype="application/json"
        )
    
    except Exception as e:
        logging.exception("Error processing request")
        return func.HttpResponse(
            json.dumps({"status": 500, "message": f"internal error occurred: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )