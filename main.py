from flask import Flask, request, render_template, redirect, url_for, flash
from google.cloud import storage
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
app.secret_key = 'your-secret-key-here'  # Для flash messages

# Configure from environment variables
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')

print(f"Using Project ID: {PROJECT_ID}")
print(f"Using Bucket: {GCS_BUCKET_NAME}")

# Initialize the Google Cloud Storage client
try:
    storage_client = storage.Client(project=PROJECT_ID)
    print("Google Cloud Storage client initialized successfully")
except Exception as e:
    print(f"Error initializing storage client: {e}")


@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('No file part', 'error')
            return redirect(request.url)

        file = request.files['file']
        if file.filename == '':
            flash('No selected file', 'error')
            return redirect(request.url)

        # Check file extension
        allowed_extensions = {'.csv', '.xlsx', '.xls'}
        file_ext = os.path.splitext(file.filename)[1].lower()

        if file_ext not in allowed_extensions:
            flash('Invalid file type. Please upload CSV or Excel files only.', 'error')
            return redirect(request.url)

        if file:
            try:
                # Upload the file to GCS
                bucket = storage_client.bucket(GCS_BUCKET_NAME)
                blob = bucket.blob(file.filename)

                # Reset file pointer to beginning
                file.seek(0)
                blob.upload_from_file(file)

                flash(f'File {file.filename} uploaded successfully to {GCS_BUCKET_NAME}!', 'success')
                return redirect(url_for('upload_file'))

            except Exception as e:
                flash(f'Error uploading file: {str(e)}', 'error')
                return redirect(request.url)

    return render_template('index.html')


@app.route('/health')
def health_check():
    return {
        'status': 'healthy',
        'project_id': PROJECT_ID,
        'bucket_name': GCS_BUCKET_NAME
    }


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)