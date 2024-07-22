from jinja2 import Environment, FileSystemLoader
from weasyprint import HTML
import os

class PDFgenrator:
    """
    A class to generate PDFs from HTML templates using Jinja2 and WeasyPrint.

    Attributes:
        html_template_file (str): Path to the HTML template file.
        data (dict): Data to be rendered in the template.
        output_file (str): Name of the output PDF file.
        BASE_DIR (str): Base directory of the current file.
        output_path (str): Full path to the output PDF file.
        env (jinja2.Environment): Jinja2 environment for loading templates.
        template (jinja2.Template): Loaded Jinja2 template.
        rendered_html (str): Rendered HTML content.

    Methods:
        generate_pdf() -> bytes:
            Converts the rendered HTML to PDF and saves it.
    """

    def __init__(self, html_template_file="", data={}, output_file="",env = Environment(loader=FileSystemLoader("./"))):
        """
        Initializes the PDFgenrator with the HTML template, data, and output file.

        Args:
            html_template_file (str): Path to the HTML template file.
            data (dict): Data to be rendered in the template.
            output_file (str): Name of the output PDF file.
        """
        self.html_template_file = html_template_file
        self.data = data
        self.BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        self.output_file = output_file
        self.output_path = self.BASE_DIR + "/" + output_file
        self.env = env
        self.template = self.env.get_template(self.html_template_file)
        self.rendered_html = self.template.render(self.data)

    def generate_pdf(self,path_font) -> bytes:
        """
        Converts the rendered HTML to PDF and saves it.

        Returns:
            bytes: The generated PDF content.
        """
        # Convert HTML to PDF using WeasyPrint
        pdf_bytes = HTML(string=self.rendered_html,base_url=path_font).write_pdf()

        print(f"Conversion complete. PDF saved as '{self.output_file}'")
        return pdf_bytes