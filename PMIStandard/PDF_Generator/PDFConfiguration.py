import os
import io
import barcode
from barcode.writer import ImageWriter
from PIL import Image
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from weasyprint import HTML
from PMIStandard.PDF_Generator.PdfGenerator import PDFgenrator

class PDFConfiguration:
    def __init__(self, base_dir: str, template_file: str, font_path: str, output_file: str):
        self.base_dir = base_dir
        self.template_file = template_file
        self.font_path = font_path
        self.output_file = output_file
        self.template_path = os.path.join(self.base_dir, "PMIStandard","PDF_Generator", "templates", self.template_file)
        self.image_path = f"file://{os.path.join(self.base_dir, 'PMIStandard','PDF_Generator', 'assets', 'ALAB_Logo.png')}"
        self.env = Environment(loader=FileSystemLoader(os.path.dirname(self.template_path)))

    def generate_and_modify_barcode(self, barcode_code: str, barcode_path: str) -> io.BytesIO:
        barcode_buffer = io.BytesIO()
        ean = barcode.get('code128', barcode_code, writer=ImageWriter())
        ean.write(barcode_buffer, {"module_width": 0.8, "module_height": 15, "font_size": 16, "text_distance": 8.5, "quiet_zone": 1})
        barcode_buffer.seek(0)
        barcode_image = Image.open(barcode_buffer)
        width, height = barcode_image.size
        cropped_image = barcode_image.crop((0, 0, width, height - 120))
        modified_barcode_buffer = io.BytesIO()
        cropped_image.save(modified_barcode_buffer, format='PNG')
        modified_barcode_buffer.seek(0)
        modified_barcode_path = f"{barcode_path}_modified.png"
        cropped_image.save(modified_barcode_path)
        return modified_barcode_buffer, modified_barcode_path

    def generate_pdf(self, data: dict, font_path : str) -> bytes:
        pdf_config = PDFgenrator(
            html_template_file=os.path.basename(self.template_path),
            data=data,
            output_file=self.output_file,
            env=self.env
        )
        pdf = pdf_config.generate_pdf(font_path)
        return pdf
   