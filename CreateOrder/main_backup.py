import azure.functions as func
import json
import logging
import pandas as pd
from PDF_Generator.PdfGenerator import PDFgenrator
import os
import barcode
from barcode.writer import ImageWriter
from PIL import Image
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
import io

def main(req: func.HttpRequest) -> func.HttpResponse:

    request_test = req.get_json()
    if not request_test:
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
    
    BASE_DIR = Path(__file__).resolve().parent.parent
    BASE_DIR = str(BASE_DIR)
    BASE_DIR_PDF_RESSOURCES = os.path.join(BASE_DIR, "PDF_Generator")
    BASE_DIR_FUNCTION = os.path.join(BASE_DIR, "CreateOrder")


    logging.info(BASE_DIR)

        # Generate barcode
    def generate_and_modify_barcode(barcode_code: str, barcode_path: str) -> io.BytesIO:
        # Create a BytesIO object to hold the barcode image in memory
        barcode_buffer = io.BytesIO()

        # Generate barcode
        ean = barcode.get('ean13', barcode_code, writer=ImageWriter())
        ean.write(barcode_buffer, {"module_width": 0.8, "module_height": 15, "font_size": 16, "text_distance": 8.5, "quiet_zone": 1})

        # Move the cursor to the beginning of the BytesIO object
        barcode_buffer.seek(0)

        # Open the barcode image from the BytesIO object
        barcode_image = Image.open(barcode_buffer)

        # Crop the barcode image to remove the text
        width, height = barcode_image.size
        cropped_image = barcode_image.crop((0, 0, width, height - 120))

        # Create a new BytesIO object to hold the modified image
        modified_barcode_buffer = io.BytesIO()

        # Save the modified image to the new BytesIO object
        cropped_image.save(modified_barcode_buffer, format='PNG')

        # Move the cursor to the beginning of the modified BytesIO object
        modified_barcode_buffer.seek(0)

        # Save the modified barcode image to disk
        modified_barcode_path = f"{barcode_path}_modified.png"
        cropped_image.save(modified_barcode_path)

        return modified_barcode_buffer, modified_barcode_path
    # Usage
    barcode_code = '123456789102'
    barcode_path = barcode_path = os.path.join( '/tmp', barcode_code)  # Provide the path where you want to save the barcode image

    # Generate and modify the barcode
    modified_barcode_buffer, modified_barcode_path = generate_and_modify_barcode(barcode_code, barcode_path)

    invoice_code = "RET123456789102"
    barcode_code = '123456789102'
    # barcode_path = os.path.join(BASE_DIR_FUNCTION, 'tmp', barcode_code)
    # print(barcode_path)
    # ean = barcode.get('ean13', barcode_code, writer=ImageWriter())
    # ean.save(barcode_path, {"module_width": 0.8, "module_height": 15, "font_size": 16, "text_distance": 8.5, "quiet_zone": 1})
    
    # # Open the barcode image
    # barcode_image = Image.open(barcode_path + '.png')

    # # Crop the barcode image to remove the text
    # cropped_image = barcode_image.crop((0, 0, barcode_image.width, barcode_image.height - 120))

    # # Save the modified barcode image without the text
    # modified_barcode_path = barcode_path + '_modified.png'
    # cropped_image.save(modified_barcode_path)

    template_barcode_path = f"file://{modified_barcode_path}"
    logging.info(template_barcode_path)

    # Sample data to be used in the PDF
    sample_data = [
        ['7622100690917', 'IQOS ORIGINALS ONE MOBILITY KIT - SLATE', 1, '9.600', '12', '12', '9.600'],
        ['', 'أيقوص أوريجينالز وان موبايلتي كيت - arabic', '', '', '', '', ''],
        ['', 'AKXVA31C185P', '', '', '', '', ''],
        ['2222222222222', 'shipping cost', 1, '0.000', '12', '12', '0.000'],
        ['7622100690917', 'IQOS ORIGINALS ONE MOBILITY KIT - SLATE', 1, '9.600', '12', '12', '9.600'],
        ['7622100690917', 'IQOS ORIGINALS ONE MOBILITY KIT - SLATE', 1, '9.600', '12', '12', '9.600'],
        ['7622100690917', 'IQOS ORIGINALS ONE MOBILITY KIT - SLATE', 1, '9.600', '12', '12', '9.600'],
        ['7622100690917', 'IQOS ORIGINALS ONE MOBILITY KIT - SLATE', 1, '9.600', '12', '12', '9.600'],
        ['7622100690917', 'IQOS ORIGINALS ONE MOBILITY KIT - SLATE', 1, '9.600', '12', '12', '9.600'],
        ['7622100690917', 'IQOS ORIGINALS ONE MOBILITY KIT - SLATE', 1, '9.600', '12', '12', '9.600'],
        ['7622100690917', 'IQOS ORIGINALS ONE MOBILITY KIT - SLATE', 1, '9.600', '12', '12', '9.600'],
        ['7622100690917', 'IQOS ORIGINALS ONE MOBILITY KIT - SLATE', 1, '9.600', '12', '12', '9.600'],
        ['7622100690917', 'IQOS ORIGINALS ONE MOBILITY KIT - SLATE', 1, '9.600', '12', '12', '9.600'],
        ['7622100690917', 'IQOS ORIGINALS ONE MOBILITY KIT - SLATE', 1, '9.600', '12', '12', '9.600']
    ]

    # Path to the logo image
    image_path = f"file://{BASE_DIR_PDF_RESSOURCES}/assets/ALAB_Logo.png"
    template_path = os.path.join(BASE_DIR_PDF_RESSOURCES, "templates", "template_pmi.html")
    font_path = os.path.join(BASE_DIR_PDF_RESSOURCES,'fonts')
    logging.info(image_path)
    logging.info(template_path)
    logging.info(font_path)

    # Verify if the template path exists
    if not os.path.exists(template_path):
        logging.error(f"Template file not found: {template_path}")
        return func.HttpResponse(
            json.dumps({
                "status": False,
                "error": {
                    "code": 500,
                    "message": "Template file not found."
                }
            }),
            status_code=500,
            mimetype="application/json"
        )

    # Configuration for PDF generation
    # Log the directories that Jinja2 is using to search for templates
    env = Environment(loader=FileSystemLoader(os.path.dirname(template_path)))
    logging.info(env.loader.list_templates())


    logging.info("test jinja loader")
    logging.info(os.path.basename(template_path))
    pdf_config = PDFgenrator(
        html_template_file=os.path.basename(template_path),
        data={
            "data": sample_data,
            "image_path": image_path,
            "customer_name": "MAHMOUDAhmed MahmoudAhmed",
            "invoice_number": 24,
            "invoice_date": "August 05, 2023",
            "invoice_time": "02:14 PM",
            "voucher_code": "AED60 00",
            "barcode_path": template_barcode_path,
            "invoice_code": invoice_code,
            "order_type": "return",
            "font_path" : font_path
        },
        output_file="output_jinja_new_font.pdf" ,
          env = env
    )

    # Generate the PDF
    pdf = pdf_config.generate_pdf(font_path)
    
    # return func.HttpResponse(
    #     json.dumps({"status": True, "payload": request_test}),
    #     status_code=500,
    #     mimetype="application/json"
    # )
    return func.HttpResponse(
        body=pdf,
        headers={
            "Content-Type": "application/pdf",
            "Content-Disposition": "attachment; filename=output_jinja_new_font.pdf"
        },
        status_code=200
    )