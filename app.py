import os
import json
from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for, flash
from flask_sqlalchemy import SQLAlchemy
from generator_core import generate_historic_data, start_live_simulation, stop_live_simulation, STATUS_FILE
from sqlalchemy import create_engine, text

# --- Config ---
app = Flask(__name__, template_folder=os.path.join(os.path.dirname(__file__), "templates"))
app.secret_key = os.getenv("SECRET_KEY", "secret123")

# --- Helper Functions ---
def get_existing_azure_config():
    """Extract configuration from existing Azure resource files"""
    config = {}
    
    # Check for existing storage account config
    storage_config_file = os.path.join(os.getcwd(), "adlsg2dataeng.json")
    if os.path.exists(storage_config_file):
        try:
            with open(storage_config_file, "r") as f:
                storage_config = json.load(f)
            config["storage_account"] = {
                "name": storage_config.get("name", "adslg2dataeng"),
                "location": storage_config.get("location", "eastus"),
                "resource_group": "rg_dataEng",  # From the ID path
                "blob_endpoint": storage_config.get("properties", {}).get("primaryEndpoints", {}).get("blob", ""),
                "subscription_id": "16a47c34-9089-4585-9ea3-31f7a2a197e0"  # From the ID
            }
        except Exception:
            pass
    
    # Check for existing resource group config
    rg_config_file = os.path.join(os.getcwd(), "resourcegroup.json")
    if os.path.exists(rg_config_file):
        try:
            with open(rg_config_file, "r") as f:
                rg_config = json.load(f)
            config["resource_group"] = {
                "name": rg_config.get("name", "rg_dataEng"),
                "location": rg_config.get("location", "eastus")
            }
        except Exception:
            pass
    
    return config

def generate_connection_string_template(account_name, blob_endpoint):
    """Generate a connection string template for the user"""
    return f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey=YOUR_ACCOUNT_KEY_HERE;EndpointSuffix=core.windows.net"

# Default to local SQL Server Express instance; can be changed in UI
DB_URL = os.getenv(
    "DB_URL",
    "mssql+pyodbc://@DESKTOP-A3PO7VB\\SQLEXPRESS/master?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes&TrustServerCertificate=yes",
)
OUTDIR = os.getenv("OUTDIR", os.path.join(os.getcwd(), "amc_output"))
os.makedirs(OUTDIR, exist_ok=True)

app.config["SQLALCHEMY_DATABASE_URI"] = DB_URL
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)


# --- Routes ---
@app.route("/")
def index():
    return redirect(url_for("controls"))


@app.route("/controls", methods=["GET", "POST"])
def controls():
    global DB_URL
    if request.method == "POST":
        action = request.form.get("action")

        if action == "generate":
            start_date = request.form.get("start_date", "2020-01-01")
            end_date = request.form.get("end_date", "2024-12-31")
            num_patients = int(request.form.get("num_patients", 5000))

            generate_historic_data(start_date, end_date, num_patients, OUTDIR, DB_URL)
            flash(f"Historic data generated. Files saved to {OUTDIR}.", "success")

        elif action == "start":
            start_live_simulation(DB_URL, OUTDIR)
            flash("✅ Live simulation started.", "success")

        elif action == "stop":
            stop_live_simulation()
            flash("⏹ Live simulation stopped.", "warning")

        elif action == "set_db":
            server = request.form.get("db_server", "DESKTOP-A3PO7VB\\SQLEXPRESS").strip()
            database = request.form.get("db_name", "master").strip()
            driver = request.form.get("db_driver", "ODBC Driver 17 for SQL Server").strip()
            auth = request.form.get("db_auth", "windows")
            username = request.form.get("db_user", "").strip()
            password = request.form.get("db_pass", "").strip()

            if auth == "windows":
                DB_URL = (
                    f"mssql+pyodbc://@{server}/{database}?driver={driver.replace(' ', '+')}"
                    f"&trusted_connection=yes&TrustServerCertificate=yes"
                )
            else:
                DB_URL = (
                    f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver.replace(' ', '+')}"
                    f"&TrustServerCertificate=yes"
                )

            app.config["SQLALCHEMY_DATABASE_URI"] = DB_URL
            flash("Database configuration updated.", "success")

        elif action == "test_db":
            try:
                engine = create_engine(DB_URL, pool_pre_ping=True)
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                flash("✅ Successfully connected to database.", "success")
            except Exception as exc:
                flash(f"❌ Database connection failed: {exc}", "warning")

        elif action == "configure_adls":
            try:
                
                # Get form data
                auth_method = request.form.get("auth_method", "connection_string")
                container_name = request.form.get("container_name", "amc-data")
                path_prefix = request.form.get("path_prefix", "")
                auto_upload = request.form.get("auto_upload") == "on"
                
                # Create ADLS configuration
                adls_config = {
                    "auth_method": auth_method,
                    "container_name": container_name,
                    "path_prefix": path_prefix,
                    "auto_upload": auto_upload,
                    "configured_at": datetime.utcnow().isoformat() + "Z"
                }
                
                # Add authentication details based on method
                if auth_method == "connection_string":
                    connection_string = request.form.get("connection_string", "")
                    if connection_string:
                        adls_config["connection_string"] = connection_string
                    else:
                        # Try to load from existing adlsg2dataeng.json file
                        existing_config_file = os.path.join(os.getcwd(), "adlsg2dataeng.json")
                        if os.path.exists(existing_config_file):
                            try:
                                with open(existing_config_file, "r") as f:
                                    existing_config = json.load(f)
                                # Extract account info from existing config
                                if "properties" in existing_config and "primaryEndpoints" in existing_config["properties"]:
                                    account_name = existing_config.get("name", "adslg2dataeng")
                                    blob_endpoint = existing_config["properties"]["primaryEndpoints"]["blob"]
                                    # Note: We can't get the key from the JSON, user needs to provide it
                                    flash(f"ℹ️ Found existing storage account '{account_name}'. Please provide the connection string or account key.", "info")
                            except Exception:
                                pass
                elif auth_method == "account_key":
                    account_name = request.form.get("account_name", "")
                    account_key = request.form.get("account_key", "")
                    if account_name:
                        adls_config["account_name"] = account_name
                    if account_key:
                        adls_config["account_key"] = account_key
                elif auth_method == "sas_token":
                    sas_token = request.form.get("sas_token", "")
                    if sas_token:
                        adls_config["sas_token"] = sas_token
                # azure_cli doesn't need additional config
                
                # Save configuration to file
                config_file = os.path.join(os.getcwd(), "adls_config.json")
                with open(config_file, "w") as f:
                    json.dump(adls_config, f, indent=2)
                
                # Set environment variables for immediate use
                os.environ["ADLS_UPLOAD"] = "true"
                os.environ["ADLS_CONTAINER"] = container_name
                if path_prefix:
                    os.environ["ADLS_PATH_PREFIX"] = path_prefix
                
                if auth_method == "connection_string" and adls_config.get("connection_string"):
                    os.environ["ADLS_CONNECTION_STRING"] = adls_config["connection_string"]
                elif auth_method == "account_key" and adls_config.get("account_name"):
                    os.environ["ADLS_ACCOUNT_NAME"] = adls_config["account_name"]
                    if adls_config.get("account_key"):
                        os.environ["ADLS_ACCOUNT_KEY"] = adls_config["account_key"]
                elif auth_method == "sas_token" and adls_config.get("sas_token"):
                    os.environ["ADLS_SAS_TOKEN"] = adls_config["sas_token"]
                
                flash("✅ ADLS configuration saved successfully!", "success")
                
            except Exception as exc:
                flash(f"❌ Failed to save ADLS configuration: {exc}", "warning")

        elif action == "upload_adls":
            try:
                from generator_core import _adls_enabled, _get_adls_container_client, _upload_path_to_adls
                import glob
                
                if not _adls_enabled():
                    flash("❌ ADLS upload not enabled. Configure ADLS settings first.", "warning")
                else:
                    container_client = _get_adls_container_client()
                    if container_client is None:
                        flash("❌ Failed to initialize ADLS connection. Check your configuration.", "warning")
                    else:
                        # Upload all existing files in output directory
                        uploaded_count = 0
                        for pattern in ["*.csv.gz", "*.json.gz", "*.parquet"]:
                            for filepath in glob.glob(os.path.join(OUTDIR, pattern)):
                                _upload_path_to_adls(filepath, OUTDIR)
                                uploaded_count += 1
                        
                        if uploaded_count > 0:
                            flash(f"✅ Successfully uploaded {uploaded_count} files to ADLS.", "success")
                        else:
                            flash("ℹ️ No files found to upload. Generate data first.", "info")
            except Exception as exc:
                flash(f"❌ ADLS upload failed: {exc}", "warning")

        elif action == "upload_infrastructure":
            try:
                from generator_core import _adls_enabled, _get_adls_container_client, _upload_path_to_adls
                import glob
                
                if not _adls_enabled():
                    flash("❌ ADLS upload not enabled. Configure ADLS settings first.", "warning")
                else:
                    container_client = _get_adls_container_client()
                    if container_client is None:
                        flash("❌ Failed to initialize ADLS connection. Check your configuration.", "warning")
                    else:
                        # Upload infrastructure files
                        uploaded_count = 0
                        infrastructure_patterns = [
                            "*.json",  # ARM templates, resource configs
                            "*.tf",    # Terraform files
                            "*.tfvars", # Terraform variables
                            "*.sh",    # Shell scripts
                            "*.ps1",   # PowerShell scripts
                            "*.bat",   # Batch files
                            "*.yml",   # YAML configs
                            "*.yaml"   # YAML configs
                        ]
                        
                        for pattern in infrastructure_patterns:
                            for filepath in glob.glob(os.path.join(os.getcwd(), pattern)):
                                # Skip virtual environment and cache directories
                                if any(skip in filepath for skip in ["venv", "__pycache__", ".git"]):
                                    continue
                                _upload_path_to_adls(filepath, os.getcwd())
                                uploaded_count += 1
                        
                        if uploaded_count > 0:
                            flash(f"✅ Successfully uploaded {uploaded_count} infrastructure files to ADLS.", "success")
                        else:
                            flash("ℹ️ No infrastructure files found to upload.", "info")
            except Exception as exc:
                flash(f"❌ Infrastructure upload failed: {exc}", "warning")

        return redirect(url_for("controls"))

    # Load status if available
    status = {}
    if os.path.exists(STATUS_FILE):
        with open(STATUS_FILE, "r", encoding="utf-8") as f:
            status = json.load(f)

    # Load ADLS configuration if available
    adls_config = {}
    config_file = os.path.join(os.getcwd(), "adls_config.json")
    if os.path.exists(config_file):
        try:
            with open(config_file, "r") as f:
                adls_config = json.load(f)
        except Exception:
            pass

    # Load existing Azure configuration
    azure_config = get_existing_azure_config()

    return render_template("controls.html", status=status, db_url=DB_URL, adls_config=adls_config, azure_config=azure_config)


@app.route("/status")
def status():
    if os.path.exists(STATUS_FILE):
        with open(STATUS_FILE, "r", encoding="utf-8") as f:
            status = json.load(f)
        return status
    return {"status": "idle"}


# --- Run ---
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
