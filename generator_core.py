import os
import json
import time
import threading
from typing import Dict, Any, List
import random

import pandas as pd
from datetime import datetime, timedelta
from faker import Faker
from sqlalchemy import create_engine, text

# Prefer Pakistani locale for more realistic synthetic data
fake = Faker(["en_PK"])  # falls back to default where providers missing
STATUS_FILE = os.path.join(os.getcwd(), "amc_output", "status.json")

# --- Optional Azure Data Lake / Blob upload support ---
_adls_container_client = None
_adls_path_prefix = None

def _adls_enabled() -> bool:
    return str(os.getenv("ADLS_UPLOAD", "false")).lower() in ("1", "true", "yes", "on")

def _get_adls_container_client():
    """Create and cache a container client using env configuration.

    Supported env vars:
      - ADLS_CONNECTION_STRING (preferred)
      - ADLS_ACCOUNT_NAME (required if no connection string)
      - ADLS_CONTAINER (required)
      - ADLS_SAS_TOKEN (optional)
      - ADLS_PATH_PREFIX (optional, virtual folder prefix)
    Credentials are resolved via DefaultAzureCredential if no SAS/conn string.
    """
    global _adls_container_client, _adls_path_prefix
    if _adls_container_client is not None:
        return _adls_container_client
    if not _adls_enabled():
        return None
    try:
        from azure.storage.blob import BlobServiceClient
        try:
            # Lazy import only when needed to keep local runs lightweight
            from azure.identity import DefaultAzureCredential  # type: ignore
        except Exception:
            DefaultAzureCredential = None  # type: ignore

        connection_string = os.getenv("ADLS_CONNECTION_STRING")
        container = os.getenv("ADLS_CONTAINER")
        account_name = os.getenv("ADLS_ACCOUNT_NAME")
        sas_token = os.getenv("ADLS_SAS_TOKEN")
        _adls_path_prefix = os.getenv("ADLS_PATH_PREFIX", "").strip("/")

        if not container:
            _append_log("ADLS_UPLOAD is enabled but ADLS_CONTAINER is not set; skipping uploads")
            return None

        if connection_string:
            svc = BlobServiceClient.from_connection_string(connection_string)
        else:
            if not account_name:
                _append_log("ADLS_UPLOAD is enabled but ADLS_ACCOUNT_NAME is not set; skipping uploads")
                return None
            account_url = f"https://{account_name}.blob.core.windows.net"
            if sas_token:
                # Ensure token begins with '?'
                if not sas_token.startswith("?"):
                    sas_token = "?" + sas_token
                svc = BlobServiceClient(account_url=account_url + sas_token)
            else:
                if DefaultAzureCredential is None:
                    _append_log("azure-identity not available; cannot use DefaultAzureCredential")
                    return None
                cred = DefaultAzureCredential()
                svc = BlobServiceClient(account_url=account_url, credential=cred)

        container_client = svc.get_container_client(container)
        try:
            container_client.create_container()
        except Exception:
            pass
        _adls_container_client = container_client
        _append_log(f"ADLS uploads initialized for container '{container}'")
        return _adls_container_client
    except Exception as exc:
        _append_log(f"ADLS init failed: {exc}")
        return None

def _upload_path_to_adls(local_path: str, outdir: str) -> None:
    """Upload a file to ADLS under the same relative path beneath ADLS_PATH_PREFIX.
    Best-effort; logs errors but does not raise.
    """
    try:
        container_client = _get_adls_container_client()
        if container_client is None:
            return
        rel = os.path.relpath(local_path, outdir).replace("\\", "/")
        blob_path = f"{_adls_path_prefix}/{rel}" if _adls_path_prefix else rel
        blob_client = container_client.get_blob_client(blob_path)
        with open(local_path, "rb") as fh:
            blob_client.upload_blob(fh, overwrite=True)
        _append_log(f"ADLS uploaded: {blob_path}")
        # Track uploads in status
        status = _read_status()
        uploaded = status.get("adls_uploaded", [])
        uploaded.append(local_path)
        _write_status({"adls_uploaded": uploaded})
    except Exception as exc:
        _append_log(f"ADLS upload failed for {local_path}: {exc}")

_live_thread = None
_live_running = False
_status_lock = threading.Lock()


def _ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)


def _read_status() -> Dict[str, Any]:
    try:
        with open(STATUS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _write_status(update: Dict[str, Any]) -> None:
    with _status_lock:
        current = _read_status()
        current.update(update)
        current["updated_at"] = datetime.utcnow().isoformat() + "Z"
        _ensure_parent_dir(STATUS_FILE)
        with open(STATUS_FILE, "w", encoding="utf-8") as f:
            json.dump(current, f, indent=2, default=str)


def _append_log(message: str) -> None:
    with _status_lock:
        current = _read_status()
        logs: List[str] = current.get("logs", [])
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        logs.append(f"[{timestamp}] {message}")
        # Keep last 200 lines max
        logs = logs[-200:]
        current["logs"] = logs
        current["last_message"] = message
        current["updated_at"] = datetime.utcnow().isoformat() + "Z"
        _ensure_parent_dir(STATUS_FILE)
        with open(STATUS_FILE, "w", encoding="utf-8") as f:
            json.dump(current, f, indent=2, default=str)


def save_dataframe(df, outdir, name, date_prefix=None):
    os.makedirs(outdir, exist_ok=True)
    
    # Use date prefix if provided (format: yyyy-mm-)
    if date_prefix:
        prefix = f"{date_prefix}-"
    else:
        prefix = ""

    # Save CSV (gzip)
    csv_path = os.path.join(outdir, f"{prefix}{name}.csv.gz")
    df.to_csv(csv_path, index=False, compression="gzip")

    # Save JSON (gzip)
    json_path = os.path.join(outdir, f"{prefix}{name}.json.gz")
    df.to_json(json_path, orient="records", lines=True, compression="gzip")

    # Save Parquet
    parquet_path = os.path.join(outdir, f"{prefix}{name}.parquet")
    df.to_parquet(parquet_path, index=False)

    _append_log(f"Saved {name} → {os.path.basename(csv_path)}, {os.path.basename(json_path)}, {os.path.basename(parquet_path)}")
    files = [csv_path, json_path, parquet_path]
    _write_status({"files_written": (_read_status().get("files_written", []) + files)})
    # Optional: upload to ADLS/Blob if configured
    if _adls_enabled():
        for f in files:
            _upload_path_to_adls(f, outdir)


def _get_engine(db_url: str):
    try:
        engine = create_engine(db_url, pool_pre_ping=True)
        # Simple connectivity check
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        _append_log(f"Connected to database: {db_url}")
        _write_status({"db_url": db_url, "db_connected": True})
        return engine
    except Exception as exc:
        _append_log(f"Database connection failed: {exc}")
        _write_status({"db_url": db_url, "db_connected": False, "db_error": str(exc)})
        return None


def generate_historic_data(start_date, end_date, num_patients, outdir, db_url):
    """Generate full historic dataset and save to files + DB"""
    _write_status({
        "mode": "historic",
        "outdir": outdir,
        "step": "init",
        "files_written": [],
    })
    _append_log("Starting historic data generation")
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    # --- Localized constants for Pakistan / Rawalpindi ---
    _write_status({"step": "patients"})
    patients = []
    pk_cities = [
        "Karachi", "Lahore", "Islamabad", "Rawalpindi", "Faisalabad", "Multan",
        "Peshawar", "Quetta", "Sialkot", "Hyderabad", "Gujranwala", "Sukkur",
        "Bahawalpur", "Mardan", "Abbottabad", "Jhelum", "Murree", "Sahiwal",
        "Okara", "Sheikhupura", "Rahim Yar Khan", "Gujrat", "Kasur", "Mianwali",
        "Sargodha", "Chiniot", "Kot Addu", "Hafizabad", "Kohat", "Jacobabad",
        "Shikarpur", "Muzaffargarh", "Khanpur", "Hala", "Kandhkot", "Bhakkar",
        "Zhob", "Dera Ismail Khan", "Pakpattan", "Tando Allahyar", "Ahmadpur East",
        "Kamalia", "Khuzdar", "Vihari", "New Mirpur", "Kohat", "Dadu", "Gojra",
        "Mandi Bahauddin", "Hassan Abdal", "Muzaffarabad", "Eminabad", "Nankana Sahib",
        "Larkana", "Chakwal", "Toba Tek Singh", "Khanewal", "Hafizabad", "Attock",
        "Arifwala", "Shahkot", "Mian Channu", "Layyah", "Chishtian", "Hasilpur",
        "Ahmadpur Sial", "Burewala", "Jalalpur Jattan", "Haroonabad", "Kahror Pakka",
        "Tandlianwala", "Dera Ghazi Khan", "Shujaabad", "Kabirwala", "Mansehra",
        "Nowshera", "Charsadda", "Qila Abdullah", "Swabi", "Tank", "Dera Bugti",
        "Kohlu", "Mastung", "Kalat", "Nushki", "Panjgur", "Turbat", "Gwadar",
        "Pasni", "Ormara", "Jiwani", "Kech", "Awaran", "Kharan", "Washuk",
        "Lasbela", "Kech", "Killa Saifullah", "Sherani", "Barkhan", "Musakhel",
        "Zhob", "Killa Abdullah", "Pishin", "Quetta", "Mastung", "Kalat",
        "Nushki", "Panjgur", "Turbat", "Gwadar", "Pasni", "Ormara", "Jiwani"
    ]
    pk_departments = [
        "Internal Medicine", "General Surgery", "Pediatrics", "ICU", "Cardiology",
        "Orthopedics & Trauma", "Gynecology & Obstetrics", "ENT", "Neurology",
        "Dermatology", "Ophthalmology", "Urology", "Nephrology", "Gastroenterology",
        "Pulmonology", "Endocrinology", "Hematology", "Oncology", "Psychiatry",
        "Radiology", "Pathology", "Anesthesiology", "Emergency Medicine", "Plastic Surgery",
        "Neurosurgery", "Cardiothoracic Surgery", "Vascular Surgery", "Pediatric Surgery",
        "Maxillofacial Surgery", "Burn Unit", "NICU", "PICU", "CCU", "HDU",
        "Dialysis Unit", "Chemotherapy Unit", "Radiotherapy Unit", "Blood Bank",
        "Pharmacy", "Physiotherapy", "Occupational Therapy", "Speech Therapy",
        "Nutrition & Dietetics", "Social Services", "Medical Records", "Quality Assurance"
    ]
    ward_types = ["General", "Semi-private", "Private", "Executive", "ICU"]
    ward_rate_pkr = {  # simple PKR rate card per day
        "General": 4500,
        "Semi-private": 6000,
        "Private": 7500,
        "Executive": 10000,
        "ICU": 18000,
    }
    panels = [
        "IGI Life", "TPL Life", "EFU Allianz", "State Life", "Adabjee",
        "Abbott", "Qatar Takaful", "Government Org",
    ]
    surname_weights = [
        ("Raja", 30), ("Satti", 20), ("Abbasi", 15), ("Khan", 10),
        ("Bhatti", 10), ("Mughal", 10), ("Sardar", 5),
    ]
    lab_tests_catalog = [
        # Basic tests (high frequency)
        ("CBC", 0.15), ("LFT", 0.08), ("KFT", 0.08), ("CRP", 0.06), ("Glucose Fasting", 0.05),
        ("HbA1c", 0.04), ("Lipid Profile", 0.04), ("Urine R/E", 0.05), ("Thyroid Profile", 0.04),
        ("D-Dimer", 0.03), ("PT/INR", 0.03), ("COVID PCR", 0.02), ("Malaria ICT", 0.02),
        # Extended lab tests
        ("ESR", 0.03), ("Blood Group", 0.02), ("Hepatitis B Surface Ag", 0.02), ("Hepatitis C Antibody", 0.02),
        ("HIV Screening", 0.01), ("VDRL", 0.01), ("Stool R/E", 0.02), ("Sputum AFB", 0.01),
        ("Blood Culture", 0.02), ("Urine Culture", 0.02), ("Widal Test", 0.01), ("Dengue NS1", 0.01),
        ("Dengue IgG/IgM", 0.01), ("Chikungunya", 0.01), ("Troponin I", 0.02), ("CK-MB", 0.02),
        ("BNP", 0.01), ("Ferritin", 0.02), ("Vitamin B12", 0.02), ("Vitamin D", 0.02),
        ("PSA", 0.01), ("CA-125", 0.01), ("CEA", 0.01), ("AFP", 0.01),
        ("Hb Electrophoresis", 0.01), ("G6PD", 0.01), ("Coomb's Test", 0.01), ("Cross Match", 0.01),
        ("Bone Marrow Aspiration", 0.005), ("Lumbar Puncture", 0.005), ("Pleural Fluid Analysis", 0.005),
        ("Ascitic Fluid Analysis", 0.005), ("CSF Analysis", 0.005), ("Synovial Fluid Analysis", 0.005),
    ]
    lab_rate_pkr = {
        "CBC": 600, "LFT": 1200, "KFT": 1200, "CRP": 900, "Glucose Fasting": 350,
        "HbA1c": 1400, "Lipid Profile": 1800, "Urine R/E": 500, "Thyroid Profile": 2200,
        "D-Dimer": 2500, "PT/INR": 1000, "COVID PCR": 2500, "Malaria ICT": 700,
        "ESR": 400, "Blood Group": 300, "Hepatitis B Surface Ag": 800, "Hepatitis C Antibody": 800,
        "HIV Screening": 1200, "VDRL": 600, "Stool R/E": 400, "Sputum AFB": 500,
        "Blood Culture": 1500, "Urine Culture": 1000, "Widal Test": 600, "Dengue NS1": 1200,
        "Dengue IgG/IgM": 1000, "Chikungunya": 1000, "Troponin I": 2000, "CK-MB": 1500,
        "BNP": 3000, "Ferritin": 1000, "Vitamin B12": 1200, "Vitamin D": 1500,
        "PSA": 1000, "CA-125": 2000, "CEA": 1500, "AFP": 1500,
        "Hb Electrophoresis": 2000, "G6PD": 800, "Coomb's Test": 1000, "Cross Match": 800,
        "Bone Marrow Aspiration": 5000, "Lumbar Puncture": 3000, "Pleural Fluid Analysis": 1500,
        "Ascitic Fluid Analysis": 1500, "CSF Analysis": 2000, "Synovial Fluid Analysis": 1500,
    }
    diagnostics_catalog = [
        # Common imaging
        ("X-Ray", 0.25), ("Ultrasound", 0.20), ("CT Scan", 0.10), ("MRI", 0.06),
        ("Echo", 0.08), ("ECG", 0.08), ("Stress Test", 0.03), ("Holter Monitor", 0.02),
        # Specialized imaging
        ("Mammography", 0.02), ("Bone Densitometry", 0.01), ("PET Scan", 0.005),
        ("Nuclear Medicine Scan", 0.01), ("Angiography", 0.01), ("Colonoscopy", 0.01),
        ("Endoscopy", 0.02), ("Bronchoscopy", 0.01), ("Cystoscopy", 0.01),
        ("Laparoscopy", 0.01), ("Arthroscopy", 0.005), ("ERCP", 0.005),
        # Cardiac procedures
        ("Cardiac Catheterization", 0.005), ("Pacemaker Check", 0.01), ("Echocardiogram", 0.03),
        ("TEE", 0.01), ("Stress Echo", 0.01), ("Carotid Doppler", 0.01),
        # Other procedures
        ("Biopsy", 0.01), ("Fine Needle Aspiration", 0.01), ("Pleural Tap", 0.005),
        ("Ascitic Tap", 0.005), ("Lumbar Puncture", 0.005), ("Bone Marrow Biopsy", 0.005),
    ]
    diagnostics_rate_pkr = {
        "X-Ray": 1200, "Ultrasound": 2500, "CT Scan": 7000, "MRI": 12000, "Echo": 3500, "ECG": 800,
        "Stress Test": 5000, "Holter Monitor": 3000, "Mammography": 4000, "Bone Densitometry": 3000,
        "PET Scan": 25000, "Nuclear Medicine Scan": 8000, "Angiography": 15000, "Colonoscopy": 8000,
        "Endoscopy": 6000, "Bronchoscopy": 8000, "Cystoscopy": 5000, "Laparoscopy": 12000,
        "Arthroscopy": 15000, "ERCP": 10000, "Cardiac Catheterization": 20000, "Pacemaker Check": 2000,
        "Echocardiogram": 4000, "TEE": 6000, "Stress Echo": 6000, "Carotid Doppler": 3000,
        "Biopsy": 3000, "Fine Needle Aspiration": 2000, "Pleural Tap": 2000, "Ascitic Tap": 2000,
        "Lumbar Puncture": 3000, "Bone Marrow Biopsy": 5000,
    }
    medication_catalog = [
        # Common medications (Pakistani market)
        ("Paracetamol 500mg", 12), ("Amoxicillin 500mg", 35), ("Ceftriaxone 1g", 180),
        ("Omeprazole 20mg", 25), ("Metformin 500mg", 18), ("Losartan 50mg", 30),
        ("Atorvastatin 20mg", 45), ("Azithromycin 500mg", 90), ("Insulin 10ml", 550),
        # Antibiotics
        ("Ciprofloxacin 500mg", 40), ("Levofloxacin 500mg", 60), ("Clarithromycin 500mg", 80),
        ("Doxycycline 100mg", 25), ("Cefixime 200mg", 120), ("Cefuroxime 250mg", 100),
        ("Vancomycin 500mg", 300), ("Meropenem 1g", 800), ("Imipenem 500mg", 600),
        # Cardiovascular
        ("Amlodipine 5mg", 20), ("Enalapril 5mg", 15), ("Bisoprolol 5mg", 25),
        ("Digoxin 0.25mg", 8), ("Warfarin 5mg", 12), ("Clopidogrel 75mg", 35),
        ("Aspirin 75mg", 5), ("Nitroglycerin 0.5mg", 15), ("Furosemide 40mg", 10),
        # Diabetes
        ("Glibenclamide 5mg", 8), ("Gliclazide 80mg", 12), ("Pioglitazone 15mg", 25),
        ("Sitagliptin 50mg", 45), ("Empagliflozin 10mg", 60), ("Dapagliflozin 10mg", 55),
        # Respiratory
        ("Salbutamol Inhaler", 120), ("Budesonide Inhaler", 200), ("Theophylline 200mg", 20),
        ("Montelukast 10mg", 30), ("Prednisolone 5mg", 8), ("Dexamethasone 0.5mg", 5),
        # Gastrointestinal
        ("Ranitidine 150mg", 15), ("Pantoprazole 40mg", 35), ("Domperidone 10mg", 12),
        ("Ondansetron 4mg", 25), ("Loperamide 2mg", 8), ("Lactulose 10ml", 20),
        # Pain management
        ("Ibuprofen 400mg", 15), ("Diclofenac 50mg", 12), ("Tramadol 50mg", 20),
        ("Morphine 10mg", 25), ("Pethidine 50mg", 30), ("Ketorolac 10mg", 18),
        # Psychiatric
        ("Fluoxetine 20mg", 25), ("Sertraline 50mg", 30), ("Amitriptyline 25mg", 15),
        ("Lorazepam 1mg", 20), ("Diazepam 5mg", 18), ("Haloperidol 5mg", 12),
        # Other specialties
        ("Levothyroxine 50mcg", 20), ("Warfarin 5mg", 12), ("Allopurinol 100mg", 15),
        ("Colchicine 0.5mg", 25), ("Methotrexate 2.5mg", 30), ("Hydroxychloroquine 200mg", 35),
    ]  # price per unit (PKR)

    def generate_cnic() -> str:
        # xxxxx-xxxxxxx-x
        part1 = random.randint(10000, 99999)
        part2 = random.randint(1000000, 9999999)
        part3 = random.randint(0, 9)
        return f"{part1}-{part2}-{part3}"

    def generate_pk_phone() -> str:
        # Formats like 03xx-xxxxxxx or +92-3xx-xxxxxxx
        prefix = random.choice(["+92-3", "03"]) + str(random.randint(0, 9)) + str(random.randint(0, 9))
        rest = f"{random.randint(1000000, 9999999)}"
        if prefix.startswith("+92"):
            return f"{prefix}-{rest}"
        return f"{prefix}-{rest}"

    def weighted_choice(options_with_weights):
        options, weights = zip(*options_with_weights)
        total = sum(weights)
        r = random.uniform(0, total)
        upto = 0
        for option, weight in options_with_weights:
            if upto + weight >= r:
                return option
            upto += weight
        return options[-1]

    def generate_pk_name() -> str:
        gender = random.choice(["M", "F"])
        first = fake.first_name_male() if gender == "M" else fake.first_name_female()
        surname = weighted_choice(surname_weights)
        return f"{first} {surname}", gender
    for i in range(num_patients):
        full_name, gender = generate_pk_name()
        is_panel = random.random() < 0.28  # ~28% panel patients
        patients.append({
            "patient_id": f"P{i+1:06d}",
            "name": full_name,
            "gender": gender,
            "dob": fake.date_of_birth(minimum_age=1, maximum_age=90),
            "city": random.choice(pk_cities),
            "cnic": generate_cnic(),
            "phone": generate_pk_phone(),
            "email": fake.free_email(),
            "panel": is_panel,
            "panel_name": random.choice(panels) if is_panel else None,
        })
    # Generate date prefix for file naming (yyyy-mm format)
    date_prefix = start_date.strftime("%Y-%m")
    
    df_patients = pd.DataFrame(patients)
    save_dataframe(df_patients, outdir, "patients", date_prefix)

    # --- Admissions --- (approx ~3 per patient over period)
    _write_status({"step": "admissions"})
    num_admissions = num_patients * 3
    admissions = []
    for i in range(num_admissions):
        admit_date = fake.date_between(start_date=start_date, end_date=end_date)
        los_days = fake.random_int(1, 12)
        discharge_date = admit_date + timedelta(days=los_days)
        ward = random.choices(ward_types, weights=[40, 25, 20, 5, 10])[0]
        admissions.append({
            "admission_id": f"A{i+1:06d}",
            "patient_id": fake.random_element(df_patients["patient_id"].tolist()),
            "admit_dt": admit_date,
            "discharge_dt": discharge_date,
            "department": fake.random_element(pk_departments),
            "ward_type": ward,
            "outcome": fake.random_element(["Discharged", "Referred", "Expired"]),
        })
    df_admissions = pd.DataFrame(admissions)
    save_dataframe(df_admissions, outdir, "admissions", date_prefix)

    # --- Labs --- (~2 per admission)
    _write_status({"step": "labs"})
    labs = []
    for i, adm in enumerate(admissions):
        k = max(1, int(random.gauss(2.0, 0.8)))
        for j in range(k):
            test_name = weighted_choice(lab_tests_catalog)
            ordered_dt = adm["admit_dt"] + timedelta(days=random.randint(0, max(0, (adm["discharge_dt"] - adm["admit_dt"]).days)))
            labs.append({
                "lab_id": f"L{i+1:06d}_{j+1}",
                "admission_id": adm["admission_id"],
                "patient_id": adm["patient_id"],
                "test_name": test_name,
                "ordered_dt": ordered_dt,
                "result_value": round(random.uniform(0.2, 15.0), 2),
                "unit": "arb",
                "price_pkr": lab_rate_pkr.get(test_name, 800),
            })
    df_labs = pd.DataFrame(labs)
    save_dataframe(df_labs, outdir, "labs", date_prefix)

    # --- Diagnostics --- (~0.7 per admission)
    _write_status({"step": "diagnostics"})
    diagnostics = []
    for i, adm in enumerate(admissions):
        if random.random() < 0.70:
            diag_name = weighted_choice(diagnostics_catalog)
            when = adm["admit_dt"] + timedelta(days=random.randint(0, max(0, (adm["discharge_dt"] - adm["admit_dt"]).days)))
            diagnostics.append({
                "diagnostic_id": f"D{i+1:06d}",
                "admission_id": adm["admission_id"],
                "patient_id": adm["patient_id"],
                "modality": diag_name,
                "performed_dt": when,
                "price_pkr": diagnostics_rate_pkr.get(diag_name, 1500),
            })
    df_diags = pd.DataFrame(diagnostics)
    save_dataframe(df_diags, outdir, "diagnostics", date_prefix)

    # --- Medications --- (~1.5 per admission)
    _write_status({"step": "medications"})
    meds = []
    for i, adm in enumerate(admissions):
        k = 1 if random.random() < 0.5 else 2
        for j in range(k):
            drug, unit_price = random.choice(medication_catalog)
            qty = random.randint(1, 10)
            meds.append({
                "med_id": f"M{i+1:06d}_{j+1}",
                "admission_id": adm["admission_id"],
                "patient_id": adm["patient_id"],
                "drug_name": drug,
                "qty": qty,
                "unit_price_pkr": unit_price,
                "total_price_pkr": unit_price * qty,
            })
    df_meds = pd.DataFrame(meds)
    save_dataframe(df_meds, outdir, "medications", date_prefix)

    # --- Occupancy per day (by department/ward) ---
    _write_status({"step": "occupancy"})
    occ_rows = []
    for adm in admissions:
        current = adm["admit_dt"]
        while current <= adm["discharge_dt"]:
            occ_rows.append({
                "date": current,
                "department": adm["department"],
                "ward_type": adm.get("ward_type", "General"),
                "admission_id": adm["admission_id"],
                "patient_id": adm["patient_id"],
            })
            current += timedelta(days=1)
    df_occupancy = pd.DataFrame(occ_rows)
    if not df_occupancy.empty:
        df_occupancy_agg = (
            df_occupancy.groupby(["date", "department", "ward_type"]).size().reset_index(name="beds_occupied")
        )
    else:
        df_occupancy_agg = pd.DataFrame(columns=["date", "department", "ward_type", "beds_occupied"])
    save_dataframe(df_occupancy_agg, outdir, "occupancy", date_prefix)

    # --- Revenue summary per admission ---
    _write_status({"step": "revenue"})
    rev_rows = []
    labs_by_adm = df_labs.groupby("admission_id")["price_pkr"].sum() if not df_labs.empty else {}
    diags_by_adm = df_diags.groupby("admission_id")["price_pkr"].sum() if not df_diags.empty else {}
    meds_by_adm = df_meds.groupby("admission_id")["total_price_pkr"].sum() if not df_meds.empty else {}
    for adm in admissions:
        los = (adm["discharge_dt"] - adm["admit_dt"]).days or 1
        ward = adm.get("ward_type", "General")
        room_rev = ward_rate_pkr.get(ward, 4500) * los
        lab_rev = float(labs_by_adm.get(adm["admission_id"], 0))
        diag_rev = float(diags_by_adm.get(adm["admission_id"], 0))
        med_rev = float(meds_by_adm.get(adm["admission_id"], 0))
        total = room_rev + lab_rev + diag_rev + med_rev
        rev_rows.append({
            "admission_id": adm["admission_id"],
            "patient_id": adm["patient_id"],
            "ward_type": ward,
            "los_days": los,
            "room_rev_pkr": room_rev,
            "lab_rev_pkr": lab_rev,
            "diag_rev_pkr": diag_rev,
            "pharmacy_rev_pkr": med_rev,
            "total_rev_pkr": total,
        })
    df_revenue = pd.DataFrame(rev_rows)
    save_dataframe(df_revenue, outdir, "revenue", date_prefix)

    # Write to DB (best-effort)
    _write_status({"step": "db_write"})
    engine = _get_engine(db_url)
    if engine is not None:
        try:
            df_patients.to_sql("Patients", con=engine, if_exists="append", index=False)
            _append_log(f"Inserted {len(df_patients)} rows into table Patients")
            df_admissions.to_sql("Admissions", con=engine, if_exists="append", index=False)
            _append_log(f"Inserted {len(df_admissions)} rows into table Admissions")
            if not df_labs.empty:
                df_labs.to_sql("Labs", con=engine, if_exists="append", index=False)
                _append_log(f"Inserted {len(df_labs)} rows into table Labs")
            if not df_diags.empty:
                df_diags.to_sql("Diagnostics", con=engine, if_exists="append", index=False)
                _append_log(f"Inserted {len(df_diags)} rows into table Diagnostics")
            if not df_meds.empty:
                df_meds.to_sql("Medications", con=engine, if_exists="append", index=False)
                _append_log(f"Inserted {len(df_meds)} rows into table Medications")
            if not df_revenue.empty:
                df_revenue.to_sql("Revenue", con=engine, if_exists="append", index=False)
                _append_log(f"Inserted {len(df_revenue)} rows into table Revenue")
            _write_status({"db_write_ok": True})
        except Exception as exc:
            _append_log(f"DB write failed: {exc}")
            _write_status({"db_write_ok": False, "db_write_error": str(exc)})

    # Final status
    _write_status({
        "step": "done",
        "patients": len(df_patients),
        "admissions": len(df_admissions),
        "labs": int(len(df_labs)),
        "diagnostics": int(len(df_diags)),
        "medications": int(len(df_meds)),
        "revenue_rows": int(len(df_revenue)),
    })
    _append_log("Historic data generation completed")


def _live_loop(db_url, outdir):
    global _live_running
    _write_status({"mode": "live", "outdir": outdir, "step": "tick_init"})
    engine = _get_engine(db_url)
    batch = 0
    while _live_running:
        batch += 1
        _write_status({"step": "tick", "batch": batch})
        _append_log(f"Live simulation tick {batch}: generating data...")

        # Minimal live batch: 10 admissions referencing existing or synthetic patient ids
        try:
            num_new = 10
            pk_cities = [
                "Karachi", "Lahore", "Islamabad", "Rawalpindi", "Faisalabad", "Multan",
                "Peshawar", "Quetta", "Sialkot", "Hyderabad", "Gujranwala", "Sukkur",
            ]

            def generate_cnic() -> str:
                part1 = random.randint(10000, 99999)
                part2 = random.randint(1000000, 9999999)
                part3 = random.randint(0, 9)
                return f"{part1}-{part2}-{part3}"

            def generate_pk_phone() -> str:
                prefix = random.choice(["+92-3", "03"]) + str(random.randint(0, 9)) + str(random.randint(0, 9))
                rest = f"{random.randint(1000000, 9999999)}"
                if prefix.startswith("+92"):
                    return f"{prefix}-{rest}"
                return f"{prefix}-{rest}"

            patients = [
                {
                    "patient_id": f"LP{batch:04d}_{i+1:03d}",
                    "name": fake.name(),
                    "gender": fake.random_element(["M", "F"]),
                    "dob": fake.date_of_birth(minimum_age=1, maximum_age=90),
                    "city": random.choice(pk_cities),
                    "cnic": generate_cnic(),
                    "phone": generate_pk_phone(),
                    "email": fake.free_email(),
                }
                for i in range(3)
            ]
            df_live_patients = pd.DataFrame(patients)
            save_dataframe(df_live_patients, outdir, f"patients_live_{batch:04d}")

            admissions = []
            today = datetime.utcnow().date()
            pk_departments = [
                "Medicine", "Surgery", "Pediatrics", "ICU", "Cardiology", "Orthopedics",
                "Gynecology", "ENT", "Neurology"
            ]

            for i in range(num_new):
                admit_date = today
                discharge_date = admit_date + timedelta(days=fake.random_int(1, 10))
                admissions.append({
                    "admission_id": f"LA{batch:04d}_{i+1:03d}",
                    "patient_id": fake.random_element(df_live_patients["patient_id"].tolist()),
                    "admit_dt": admit_date,
                    "discharge_dt": discharge_date,
                    "department": fake.random_element(pk_departments),
                    "outcome": fake.random_element(["Discharged", "Referred", "Expired"]),
                })
            df_live_admissions = pd.DataFrame(admissions)
            save_dataframe(df_live_admissions, outdir, f"admissions_live_{batch:04d}")

            if engine is not None:
                try:
                    df_live_patients.to_sql("Patients", con=engine, if_exists="append", index=False)
                    df_live_admissions.to_sql("Admissions", con=engine, if_exists="append", index=False)
                    _append_log(f"Live batch {batch}: inserted patients={len(df_live_patients)}, admissions={len(df_live_admissions)}")
                except Exception as exc:
                    _append_log(f"Live DB write failed (batch {batch}): {exc}")

            _write_status({"last_live_counts": {"patients": len(df_live_patients), "admissions": len(df_live_admissions)}})
        except Exception as exc:
            _append_log(f"Live tick error: {exc}")

        time.sleep(5)


def start_live_simulation(db_url, outdir):
    global _live_thread, _live_running
    if _live_running:
        return
    _live_running = True
    _live_thread = threading.Thread(target=_live_loop, args=(db_url, outdir), daemon=True)
    _live_thread.start()
    _append_log("Live simulation started")


def stop_live_simulation():
    global _live_running
    _live_running = False
    _append_log("Live simulation stopping...")
