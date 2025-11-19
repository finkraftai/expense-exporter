# 🧾 Expense Exporter

A comprehensive data processing application for hotel invoice management that supports both cloud storage ingestion and CSV/Excel fileprocessing. The application downloads PDFs, uploads them to cloud storage, generates secure links, and stores processed data in PostgreSQL and MongoDB databases.

---

## 🚀Features

### 📊CSV/Excel Processing Mode

- **PDF Download**: Downloads PDFs from external URLs in the spreadsheet.-[Wormhole]
- **Cloud Upload**: Uploads processed PDFs to **S3/Azure** with organized
  folder structures.
- **Database Integration**: Stores data in both **PostgreSQL** and
  **MongoDB**.
- **Link Generation**: Creates **pre-signed URLs** for secure file access.

### ☁️ Cloud

Ingestion Mode

- **Azure Blob Storage**: Downloads and processes PDFs from Azure
  containers.
- **AWS S3**: Downloads and processes PDFs from AWS S3 buckets.

---

## 🏗️Project Structure

```

├── 📁downloads
├── 📁 logs
├── 📁utils
│   ├── 🐍 cloud_helper.py
│   ├── 🐍 config.py
│   ├── 🐍 file_process.py
│   ├── 🐍 logger.py
│   ├── 🐍 mongodb_process.py
│   └── 🐍 postgres_process.py
└── 🐍
main.py

```

---

## ⚙️

Installation

### 1. Clone the Repository

```bash

git clone <repository-url>

cd expense-exporter

```

### 2. Create a Virtual Environment

```bash

python -m venv venv

source venv/bin/activate      # On
Windows: venv\Scripts\activate

```

### 3. Install Dependencies

```bash

pip install -r requirements.txt

```

### 4. Configure Environment Variables

Copy `.env.example` to `.env` and update the following values:

```env

# ---------------- Database Configuration ----------------
DB_NAME=your_db_name
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_HOST=your_db_host
DB_PORT=5432

# ---------------- Cloud Provider ----------------
CLOUD_PROVIDER=aws  # or azure

# ---------------- Azure Configuration ----------------
AZURE_CONNECTION_STRING=your_azure_connection_string
AZURE_CONTAINER_NAME=your_container_name
AZURE_PDF_PATH=Email/Hotel

# ---------------- AWS Configuration ----------------

AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_REGION=us-east-1
AWS_BUCKET_NAME=your_bucket_name
AWS_PREFIX=your_prefix

# ---------------- Client and Source ----------------

CLIENT=your_client_name
SOURCE=your_source_name

# ---------------- File Paths ----------------

INPUT_FILE_PATH=sample_expense.xlsx
OUTPUT_FILE_PATH=processed_expense.xlsx

# ---------------- MongoDB Configuration ----------------

MONGO_URI=mongodb://localhost:27017/
MONGO_DB_NAME=hotel_invoice_db
MONGO_COLLECTION_NAME=processed_invoices

# ---------------- S3 Upload Configuration ----------------
S3_UPLOAD_BUCKET=your_upload_bucket
CLIENT_FOLDER=your_client_folder

```

---

## 🧠Usage

### 🧾CSV/Excel Processing Mode

Run the application to process expense files with invoice links:

```bash

python main.py

```

This will:

- Read the input Excel/CSV file
- Download PDFs from the `external_file_link` column
- Upload PDFs to cloud storage with structured paths
- Insert data into **MongoDB** and **PostgreSQL**
- Update the output file with processing status and file links

---

## 🔄 Data Flow

### CSV/Excel Processing Pipeline

1. **File Reading** → Load Excel/CSV with invoice data.
2. **Link Expansion** → Handle multiple invoice links per row.
3. **PDF Download** → Download invoices from external URLs.
4. **Hash Calculation** → Generate MD5 hashes for duplicate prevention.
5. **Cloud Upload** → Upload PDFs to **S3/Azure** with structured paths.
6. **MongoDB Insert** → Store detailed invoice data with duplicate
   control.
7. **PostgreSQL Insert** → Store standardized metadata for reporting.
8. **File Update** → Update the output file with links, hashes, and
   statuses.

---

## 🗃️ Database Schema

### 🐘PostgreSQL (`hotel_invoice` table)

- `id`: Primary key
- `file_url`: Cloud storage URL
- `source`: Data source identifier
- `client_name`: Client identifier
- `file_hash`: MD5 hash for duplicate detection
- `status`: Processing status
- `source_id`: Reference to MongoDB document
- Additional fields such as `gstin`, `invoice_number`, etc.
- `updated_on`: Last update timestamp

### 🍃MongoDB (`processed_invoices` collection)

- All Excel columns plus additional metadata:

  - `external_file_link`: Original
    download URL
  - `s3_link`: Cloud storage URL
  - `file_hash`: MD5 hash for duplicate
    control
  - `corp_name`: Client name
  - `processed_at`: Timestamp of
    processing
  - `status`: Processing result

---

## 🧰Logging & Monitoring

- Logs are stored in the `/logs` directory with timestamps.
- Each run records:

  - Process start and end time
  - File download/upload results
  - Database insert statuses (MongoDB
    & PostgreSQL)

---

## 🧩Future Enhancements

- [ ] Retry logic for failed downloads/uploads
- [ ] Environment validation before runtime
- [ ] CLI support for mode selection (`--mode csv` / `--mode cloud`)
- [ ] Support for Google Cloud Storage (GCS)
- [ ] Cross-database duplicate detection

---

## 📜License

This project is proprietary and developed for **Finkraft’s internal expense automation system**.

Unauthorized distribution or modification without prior consent is prohibited.
