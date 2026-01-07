# Local Timesheet PDF Generator

A local web application to convert Excel timesheets into formatted PDF reports (PowerBI-like matrix style).

## Setup

1. **Prerequisites**: Python 3.9+ installed.
2. **Create Virtual Environment**:
   ```powershell
   python -m venv venv
   .\venv\Scripts\Activate
   ```
3. **Install Dependencies**:
   ```powershell
   pip install -r requirements.txt
   ```
4. **Install Playwright Browsers**:
   ```powershell
   playwright install chromium
   ```

## Usage

1. **Run the App**:
   ```powershell
   uvicorn app:app --reload
   ```
2. **Open Browser**:
   Go to `http://localhost:8000`
3. **Generate Reports**:
   - Upload your `.xlsx` timesheet file.
   - Click "Generate PDFs".
   - Download the `timesheet_pdfs.zip` file.

## Privacy Note
This application runs entirely on your local machine (`localhost`). No data is uploaded to the cloud/internet.
