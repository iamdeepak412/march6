import boto3
from flask import Flask, request, jsonify
from tempfile import NamedTemporaryFile
from pyresparser import ResumeParser
import warnings
from urllib.parse import urlparse
import json
from flasgger import Swagger
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Get AWS credentials from environment variables
AWS_REGION = os.getenv('AWS_REGION')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

# Ignore warnings
warnings.filterwarnings("ignore", category=UserWarning)

app = Flask(__name__)
Swagger(app)

def parse_local_resume(file_path):
    # Parse the resume file
    data = ResumeParser(file_path).get_extracted_data()

    # Extract specific fields
    extracted_data = {
        "college_name": None,
        "company_names": None,
        "degree": None,
        "designation": None,
        "email": data.get("email", None),
        "experience": data.get("experience", []),
        "mobile_number": None,
        "name": data.get("name", "Top Skills"),  # Default name to "Top Skills" if not found
        "no_of_pages": data.get("no_of_pages", None),
        "skills": data.get("skills", []),
        "total_experience": data.get("total_experience", None)
    }

    return extracted_data

@app.route('/parse_local_resume', methods=['POST'])
def parse_local_resume_endpoint():
    """
    Parse Resume Endpoint
    ---
    parameters:
      - name: file
        in: formData
        type: file
        required: true
    responses:
      200:
        description: JSON object containing extracted resume data
    """
    resume_file = request.files['file']
    resume_file.save('uploaded_resume.pdf')
    extracted_data = parse_local_resume('uploaded_resume.pdf')
    return jsonify(extracted_data)



# Initialize S3 resource client
s3 = boto3.resource(
    service_name='s3',
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)


# Function to fetch PDF content from S3 URL
def fetch_pdf_content_from_s3(s3_url):
    parsed_url = urlparse(s3_url)
    bucket_name = parsed_url.netloc.split('.')[0]
    object_key = parsed_url.path[1:]  # Removing leading '/'
    try:
        obj = s3.Object(bucket_name, object_key)
        response = obj.get()
        return response['Body'].read()
    except Exception as e:
        print(f"Error fetching PDF content: {e}")
        return None

@app.route('/parse_resume', methods=['POST'])
def parse_resume():
    """
    Endpoint to parse resume from S3 URL
    ---
    parameters:
      - name: body
        in: body
        required: true
        schema:
          id: Resume
          properties:
            resume_s3_url:
              type: string
              description: The URL of the resume stored in S3 bucket
    responses:
      200:
        description: OK
      400:
        description: Bad Request
      500:
        description: Internal Server Error
    """
    data = request.get_json()
    resume_s3_url = data.get('resume_s3_url')

    if not resume_s3_url:
        return jsonify({"error": "resume_s3_url not provided"}), 400

    # Fetch PDF content from S3
    pdf_content = fetch_pdf_content_from_s3(resume_s3_url)

    if pdf_content:
        # Create a temporary file to store the PDF content
        with NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
            temp_file.write(pdf_content)
            temp_file_path = temp_file.name

        # Parse the resume content
        data = ResumeParser(temp_file_path).get_extracted_data()

        # Delete the temporary file
        temp_file.close()
        return jsonify(data), 200
    else:
        return jsonify({"error": "Failed to fetch PDF content. Parsing aborted."}), 500

if __name__ == '__main__':
    app.run(debug=True)
