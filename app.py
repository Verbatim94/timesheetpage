import io
import os
import socket
import datetime
import locale
import zipfile
import asyncio
import re
import uuid
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, UploadFile, File, Request, Form, BackgroundTasks, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import secrets
import glob
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates
import pandas as pd
import calendar
import uvicorn
from playwright.async_api import async_playwright


from fastapi.staticfiles import StaticFiles


# Setup templates
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")

app = FastAPI()

# Mount static files
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")

templates = Jinja2Templates(directory=TEMPLATES_DIR)

# Global Job Store
# Global Job Store
jobs: Dict[str, Any] = {}

# --- SECURITY CONFIGURATION ---
# Change this password!
APP_PASSWORD = os.environ.get("APP_PASSWORD", "SuperSecurePassword2025!")
security = HTTPBasic()

def get_current_username(credentials: HTTPBasicCredentials = Depends(security)):
    correct_password = secrets.compare_digest(credentials.password, APP_PASSWORD)
    if not correct_password:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username
# ------------------------------

# Italian month mapping
MONTH_MAP = {
    "gennaio": 1, "febbraio": 2, "marzo": 3, "aprile": 4, "maggio": 5, "giugno": 6,
    "luglio": 7, "agosto": 8, "settembre": 9, "ottobre": 10, "novembre": 11, "dicembre": 12
}

def parse_italian_date(date_str: str) -> Optional[datetime.date]:
    """
    Parses 'martedì 1 aprile 2025' or '1 aprile 2025' into a date object.
    Ignores weekday if present.
    """
    if not isinstance(date_str, str):
        return pd.to_datetime(date_str).date() if pd.notna(date_str) else None
        
    date_str = date_str.lower().strip()
    
    # Remove weekday if present (assumes simple structure: 'weekday d month y')
    # Use regex to find pattern: day (digits) month (letters) year (4 digits)
    match = re.search(r'(\d+)\s+([a-z]+)\s+(\d{4})', date_str)
    if not match:
        return None
        
    day_s, month_s, year_s = match.groups()
    try:
        day = int(day_s)
        year = int(year_s)
        month = MONTH_MAP.get(month_s)
        if not month:
            return None
        return datetime.date(year, month, day)
    except:
        return None

def get_easter_monday(year):
    """Calculates Easter Monday for a given year."""
    # Anonymous algorithm for Easter date
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    
    easter = datetime.date(year, month, day)
    return easter + datetime.timedelta(days=1)

def get_italian_holidays(year):
    """Returns a set of holiday dates for the year."""
    fixed = [
        (1, 1), (1, 6), (4, 25), (5, 1), (6, 2),
        (8, 15), (11, 1), (12, 8), (12, 25), (12, 26)
    ]
    holidays = {datetime.date(year, m, d) for m, d in fixed}
    holidays.add(get_easter_monday(year))
    return holidays

def preprocess_df(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans and prepares the DataFrame for storage/processing."""
    # Normalize columns
    df.columns = [c.strip() for c in df.columns]
    
    # Required columns check
    required = ["Date", "Name", "Plans", "Hour", "Payroll Number", "Cost Center"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {', '.join(missing)}")
    
    # Date processing
    df['DateObj'] = df['Date'].apply(parse_italian_date)
    df = df.dropna(subset=['DateObj']) # Drop invalid dates
    # Convert date objects to timestamp for Parquet compatibility
    df['DateObj'] = pd.to_datetime(df['DateObj'])
    
    # Hour processing
    def parse_hour(h):
        if isinstance(h, (int, float)):
            return float(h)
        if isinstance(h, str):
            try:
                return float(h.replace(',', '.'))
            except:
                return 0.0
        return 0.0
        
    df['HourVal'] = df['Hour'].apply(parse_hour)
    
    return df

def build_users_dict(df: pd.DataFrame) -> Dict[str, Any]:
    """Organizes the CLEAN dataframe into the report structure."""
    users_data = {}
    for name, user_df in df.groupby("Name"):
        # Payroll and Cost Center (take first valid)
        payroll = user_df["Payroll Number"].iloc[0] if not user_df["Payroll Number"].empty else ""
        try:
            if pd.notna(payroll):
                # Handle string numbers with commas
                payroll_str = str(payroll).replace(',', '.')
                payroll = str(int(float(payroll_str)))
        except:
            payroll = str(payroll)

        cost_center = user_df["Cost Center"].iloc[0] if not user_df["Cost Center"].empty else ""
        
        # Sort by date
        user_df = user_df.sort_values('DateObj')
        
        # Identify months needed
        unique_periods = user_df['DateObj'].apply(lambda d: (d.year, d.month)).drop_duplicates()
        
        month_blocks = []
        
        for (year, month) in unique_periods:
            # Filter for this month
            month_df = user_df[
                (user_df['DateObj'].dt.year == year) & 
                (user_df['DateObj'].dt.month == month)
            ]
            
            # Build matrix
            month_df['Day'] = month_df['DateObj'].dt.day
            
            pivot = month_df.pivot_table(
                index="Plans", 
                columns="Day", 
                values="HourVal", 
                aggfunc="sum",
                fill_value=0
            )
            
            # Ensure columns 1..31 exist
            for d in range(1, 32):
                if d not in pivot.columns:
                    pivot[d] = 0
            
            # Reorder columns 1..31
            pivot = pivot[sorted(pivot.columns)]
            
            # Rows processing
            rows = []
            
            # Calculate Totals
            pivot['Total'] = pivot.sum(axis=1)
            total_row_vals = pivot.sum(axis=0)
            
            # Identify month name
            # month_name_it = [k for k,v in MONTH_MAP.items() if v == month][0]
            # MAPPING FOR ENGLISH UPPERCASE
            MONTH_MAP_EN = {
                1: "JANUARY", 2: "FEBRUARY", 3: "MARCH", 4: "APRIL", 5: "MAY", 6: "JUNE",
                7: "JULY", 8: "AUGUST", 9: "SEPTEMBER", 10: "OCTOBER", 11: "NOVEMBER", 12: "DECEMBER"
            }
            month_name_it = MONTH_MAP_EN.get(month, "")
            
            # Holidays for this year
            holidays = get_italian_holidays(year)
            
            day_meta = {}
            for d in range(1, 32):
                date_obj = None
                try:
                    date_obj = datetime.date(year, month, d)
                except ValueError:
                    pass
                
                is_holiday = False
                is_weekend = False
                if date_obj:
                    if date_obj.weekday() >= 5: # 5=Sat, 6=Sun
                        is_weekend = True
                    if date_obj in holidays:
                        is_holiday = True
                
                day_meta[d] = {
                    "day": d,
                    "shaded": is_weekend or is_holiday,
                    "date_valid": date_obj is not None
                }
            
            # Format numbers: 2 decimals, comma separator
            def fmt(n, is_zero_target_empty=True):
                if n == 0 and is_zero_target_empty:
                    return ""
                s = "{:.2f}".format(n).replace('.', ',')
                return s
            
            # Total Row
            total_row_cells = []
            total_row_day_values = []
            total_row_day_shaded_status = []
            for d in range(1, 32):
                val = total_row_vals[d]
                meta = day_meta[d]
                total_row_day_values.append(val)
                total_row_day_shaded_status.append(meta["shaded"])
            
            grand_total = total_row_vals['Total']

            # Get days in month for header hiding logic
            days_in_month = calendar.monthrange(year, month)[1]

            # Plan Rows
            plans_list = []
            for plan_name, row_series in pivot.iterrows():
                if plan_name == "Total": continue
                
                row_cells = []
                for d in range(1, 32):
                    val = row_series[d]
                    meta = day_meta[d]
                    row_cells.append({
                        "value": fmt(val, is_zero_target_empty=True),
                        "shaded": meta["shaded"]
                    })
                
                plans_list.append({
                    "plan_name": plan_name,
                    "is_total": False,
                    "is_indent": str(plan_name).startswith("_"),
                    "days": row_cells,
                    "total": fmt(row_series['Total'], is_zero_target_empty=True)
                })

            month_blocks.append({
                "month_name": month_name_it,
                "days_in_month": days_in_month,
                "plans": plans_list,
                "total_row_cells": [fmt(x, is_zero_target_empty=True) for x in total_row_day_values],
                "total_row_day_shaded_status": total_row_day_shaded_status, # Add shaded status for total row
                "grand_total": fmt(grand_total, is_zero_target_empty=True)
            })
            
        users_data[name] = {
            "payroll": payroll,
            "cost_center": cost_center,
            "blocks": month_blocks
        }
        
    return users_data

@app.get("/", response_class=HTMLResponse)
async def index(request: Request, username: str = Depends(get_current_username)):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/robots.txt", response_class=PlainTextResponse)
async def robots():
    return "User-agent: *\\nDisallow: /"

def generate_user_excel(user_name: str, user_data: Dict[str, Any]) -> Optional[bytes]:
    """Generates an Excel file with one single sheet containing all tables stacked."""
    try:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            
            all_data_rows = []
            
            # Headers for the columns
            columns = ["Plan"] + [str(d) for d in range(1, 32)] + ["Total"]
            
            for block in user_data["blocks"]:
                month_name = block["month_name"]
                
                # Add a Header Row for the Month
                # We can simulate this by adding a row with "Plan" = Month Name and rest empty/None
                # But to make it distinct, let's put it in the 'Plan' column or a separate row structure
                
                # 1. Month Header Row
                month_header_row = {c: None for c in columns}
                month_header_row["Plan"] = f"--- {month_name} ---"
                all_data_rows.append(month_header_row)
                
                # 2. Column Headers Row (Plan, 1, 2 ... Total)
                # If we want the headers to repeat for each table:
                # We can just append the dict representation of headers, but DataFrame keys are fixed.
                # Actually, pandas to_excel writes headers once. 
                # To stack tables, we might need to be clever.
                
                # Simplest approach for "Stacked Tables" in one DataFrame:
                # Just treat headers as data rows.
                
                # Table Header Row
                # We need to explicitly add a row that LOOKS like the header
                # OR we rely on the first print.
                # Let's add the numeric headers as a row for every month for clarity
                headers_row = {c: c for c in columns}
                all_data_rows.append(headers_row)

                # Plan Rows
                for plan in block["plans"]:
                    row = {"Plan": plan["plan_name"]}
                    # Fill days
                    for i, day_cell in enumerate(plan["days"]):
                        val = day_cell["value"]
                        # We try to keep numbers as numbers, strings as strings
                        # But since we are mixing with header strings in the same column, 
                        # pandas might force object type (string). That's usually fine for a report.
                        try:
                            if val:
                                row[str(i+1)] = float(val.replace(',', '.'))
                            else:
                                row[str(i+1)] = None
                        except:
                            row[str(i+1)] = val
                            
                    # Total
                    try:
                        if plan["total"]:
                            row["Total"] = float(plan["total"].replace(',', '.'))
                        else:
                            row["Total"] = None
                    except:
                        row["Total"] = plan["total"]
                        
                    all_data_rows.append(row)
                
                # Total Row for the month
                total_row = {"Plan": "TOTALE"}
                for i, val in enumerate(block["total_row_cells"]):
                    try:
                        if val:
                             total_row[str(i+1)] = float(val.replace(',', '.'))
                        else:
                             total_row[str(i+1)] = None
                    except:
                        total_row[str(i+1)] = val
                
                if block["grand_total"]:
                     try:
                        total_row["Total"] = float(block["grand_total"].replace(',', '.'))
                     except:
                        total_row["Total"] = block["grand_total"]
                
                all_data_rows.append(total_row)
                
                # Empty Spacer Row
                all_data_rows.append({c: None for c in columns})
                
            # Create DataFrame
            df_sheet = pd.DataFrame(all_data_rows, columns=columns)
            
            # Write to Excel
            # header=False because we manually embedded headers for each block
            df_sheet.to_excel(writer, sheet_name="Report", index=False, header=False)
                
        return output.getvalue()
    except Exception as e:
        print(f"Error generating Excel for {user_name}: {e}")
        return None

import sqlite3
import json
import time # Added for time.time()

# --- DATABASE SETUP ---
DB_FILE = "jobs.db"

def init_db():
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    status TEXT,
                    progress INTEGER,
                    total INTEGER,
                    logs TEXT,
                    filename TEXT,
                    created_at REAL,
                    error TEXT
                )
            """)
    except Exception as e:
        print(f"DB Init Error: {e}")

init_db()

def create_job_in_db(job_id, total):
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute(
            "INSERT INTO jobs (job_id, status, progress, total, logs, filename, created_at, error) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (job_id, "queued", 0, total, "[]", None, time.time(), None)
        )

def update_job_progress(job_id, progress=None, status=None, log_msg=None, filename=None, error=None):
    with sqlite3.connect(DB_FILE) as conn:
        # Get current logs to append
        if log_msg:
            cur = conn.execute("SELECT logs FROM jobs WHERE job_id = ?", (job_id,))
            row = cur.fetchone()
            if row:
                current_logs = json.loads(row[0]) if row[0] else []
                current_logs.append(log_msg)
                logs_json = json.dumps(current_logs)
                conn.execute("UPDATE jobs SET logs = ? WHERE job_id = ?", (logs_json, job_id))

        if progress is not None:
             conn.execute("UPDATE jobs SET progress = ? WHERE job_id = ?", (progress, job_id))
        
        if status:
             conn.execute("UPDATE jobs SET status = ? WHERE job_id = ?", (status, job_id))
             
        if filename:
             conn.execute("UPDATE jobs SET filename = ? WHERE job_id = ?", (filename, job_id))
             
        if error:
             conn.execute("UPDATE jobs SET error = ? WHERE job_id = ?", (error, job_id))

def get_job_from_db(job_id):
    with sqlite3.connect(DB_FILE) as conn:
        # Use row factory for easier dict access
        conn.row_factory = sqlite3.Row
        cur = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,))
        row = cur.fetchone()
        if row:
            d = dict(row)
            d['logs'] = json.loads(d['logs']) if d['logs'] else []
            return d
        return None

async def process_generation_task(job_id: str, filtered_data: dict, generate_pdf_flag: bool = True, generate_excel_flag: bool = True):
    update_job_progress(job_id, status="processing", progress=0)
    
    try:
        zip_buffer = io.BytesIO()
        
        # We need a reusable function for core processing that can work with or without browser
        async def process_user_content(user_name, user_data, browser_page=None):
            results_dict = {"user_name": user_name, "cc": user_data.get("cost_center")}
            
            try:
                # 1. Excel Generation (Fast, CPU only)
                if generate_excel_flag:
                    # Sync call, but fast enough. If very slow, could offload to thread.
                    excel_bytes = generate_user_excel(user_name, user_data)
                    results_dict["excel_bytes"] = excel_bytes
                
                # 2. PDF Generation (Requires Browser)
                if generate_pdf_flag and browser_page:
                    update_job_progress(job_id, log_msg=f"[TASK] Generating PDF for {user_name}...")
                    
                    html_content = templates.get_template("timesheet_report.html").render(
                        user_name=user_name,
                        payroll_number=user_data["payroll"],
                        cost_center=user_data["cost_center"],
                        blocks=user_data["blocks"],
                        is_shaded_col=lambda d: False 
                    )
                    
                    footer_html = """
    <div style="font-size: 8px; font-family: sans-serif; width: 100%; display: flex; justify-content: space-between; padding: 0 20px; color: #555;">
        <span>Year: <span class="date"></span></span>
        <span>Page <span class="pageNumber"></span> / <span class="totalPages"></span></span>
        <span>Signature: __________________________</span>
    </div>
    """
                    await browser_page.set_content(html_content, wait_until="domcontentloaded", timeout=60000)
                    pdf_bytes = await browser_page.pdf(
                        format="A4",
                        print_background=True,
                        landscape=False,
                        display_header_footer=True,
                        header_template="<div></div>", 
                        footer_template=footer_html,
                        margin={"top": "1cm", "right": "0.5cm", "bottom": "1.5cm", "left": "0.5cm"},
                        timeout=60000
                    )
                    results_dict["pdf_bytes"] = pdf_bytes
                    update_job_progress(job_id, log_msg=f"[SUCCESS] {user_name} processed.")
                elif generate_excel_flag and not generate_pdf_flag:
                     update_job_progress(job_id, log_msg=f"[SUCCESS] {user_name} Excel generated.")
                
                return results_dict
                
            except Exception as e:
                update_job_progress(job_id, log_msg=f"[ERROR] Failed {user_name}: {str(e)}")
                return None

        results = []
        
        current_prog = 0
        
        if generate_pdf_flag:
            update_job_progress(job_id, log_msg=f"[SYSTEM] Initializing browser engine...")
            sem = asyncio.Semaphore(3) # Control concurrency (Lowered for stability on Render)
            async with async_playwright() as p:
                browser = await p.chromium.launch()
                
                async def runner(name, data):
                    nonlocal current_prog
                    async with sem:
                        page = await browser.new_page()
                        try:
                            res = await process_user_content(name, data, page)
                            current_prog += 1
                            update_job_progress(job_id, progress=current_prog)
                            return res
                        finally:
                            await page.close()

                tasks = [runner(name, data) for name, data in filtered_data.items()]
                results = await asyncio.gather(*tasks)
                await browser.close()
        else:
            # Excel Only - No Browser Overhead
            update_job_progress(job_id, log_msg=f"[SYSTEM] Starting Excel-only generation (Fast Mode)...")
            for i, (name, data) in enumerate(filtered_data.items()):
                # No semaphore needed really for CPU bound simple task, but let's just loop
                res = await process_user_content(name, data, None)
                results.append(res)
                current_prog += 1
                # Batch updates to db every 5 items to reduce I/O lock contention? 
                # For now simple update every item is safer for user feedback
                update_job_progress(job_id, progress=current_prog)
                if i % 10 == 0: await asyncio.sleep(0.01)

        update_job_progress(job_id, log_msg=f"[SYSTEM] Compressing files...")
        
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            for res in results:
                if not res: continue
                user_name = res["user_name"]
                cc = res["cc"]
                
                cc_folder = str(cc).strip()
                cc_folder = "".join([c for c in cc_folder if c.isalnum() or c in (' ', '_', '-')]).strip()
                if not cc_folder: cc_folder = "NoCostCenter"
                
                safe_name = "".join([c for c in user_name if c.isalnum() or c in (' ', '_', '-')]).strip()
                
                if "pdf_bytes" in res:
                    zf.writestr(f"{cc_folder}/PDF/{safe_name}.pdf", res["pdf_bytes"])
                
                if "excel_bytes" in res:
                     zf.writestr(f"{cc_folder}/Excel/{safe_name}.xlsx", res["excel_bytes"])
        
        zip_buffer.seek(0)
        
        # Save to temp file
        temp_filename = f"output_{job_id}.zip"
        with open(temp_filename, "wb") as f:
            f.write(zip_buffer.getvalue())
            
        update_job_progress(job_id, status="completed", filename=temp_filename, log_msg=f"[SYSTEM] Job Dispatched. Ready for download.")
        
    except Exception as e:
        update_job_progress(job_id, status="failed", error=str(e), log_msg=f"[CRITICAL] Job Failed: {str(e)}")

        if "Cost Center" in df.columns:
            # Group by Name to count users, not rows (assuming one cost center per user)
            # Use 'first' to get a representative cost center for the user
            users_cc = df.groupby("Name")["Cost Center"].first()
            # Handle potential None/NaN values in cost center
            users_cc = users_cc.fillna("Unknown")
            cost_centers = users_cc.value_counts().to_dict()
        else:
            cost_centers = {"Unknown": df["Name"].nunique()}

        return templates.TemplateResponse("select_cost_centers.html", {
            "request": request,
            "cost_centers": cost_centers,
            "file_id": file_id
        })
        
    except Exception as e:
         return HTMLResponse(f"<h3>Errore nella lettura del file:</h3><p>{str(e)}</p>", status_code=400)

@app.post("/generate")
async def generate_pdf(background_tasks: BackgroundTasks, 
                      request: Request, 
                      cost_centers: List[str] = Form(...), 
                      file_id: str = Form(...), 
                      output_pdf: Optional[str] = Form(None),
                      output_excel: Optional[str] = Form(None),
                      username: str = Depends(get_current_username)):
    
    # Checkbox logic: if checked, value is 'on'. If unchecked, None.
    generate_pdf_flag = True if output_pdf == 'on' else False
    generate_excel_flag = True if output_excel == 'on' else False
    
    # Fallback/Validation
    if not generate_pdf_flag and not generate_excel_flag:
         return JSONResponse({"error": "Seleziona almeno un formato di output"}, status_code=400)

    temp_file = f"temp_{file_id}.parquet"
    if not os.path.exists(temp_file):
        return JSONResponse({"error": "Sessione scaduta o file non trovato"}, status_code=400)
        
    try:
        # Load from Parquet
        df = pd.read_parquet(temp_file)
        
        # PRIVACY ERASE: Delete immediately after reading
        try:
            os.remove(temp_file)
        except:
            pass
            
        # --- DATE FILTERING REMOVED: Processing all data ---
        
        try:
            data = build_users_dict(df)
        except Exception as e:
             return JSONResponse({"error": f"Errore elaborazione: {str(e)}"}, status_code=400)
        
        # Filter users based on selected Cost Centers
        filtered_data = {
            name: u_data 
            for name, u_data in data.items() 
            if u_data.get("cost_center") in cost_centers or (not u_data.get("cost_center") and "Unknown" in cost_centers)
        }
        
        if not filtered_data:
             return JSONResponse({"error": "Nessun utente trovato"}, status_code=400)

        # Create Job in DB
        job_id = str(uuid.uuid4())
        create_job_in_db(job_id, len(filtered_data))
        
        # Start Background Task
        background_tasks.add_task(process_generation_task, job_id, filtered_data, generate_pdf_flag, generate_excel_flag)
        
        return JSONResponse({"job_id": job_id})
    
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/job/{job_id}")
async def get_job_status(job_id: str, username: str = Depends(get_current_username)):
    job = get_job_from_db(job_id)
    if not job:
        return JSONResponse({"error": "Job not found"}, status_code=404)
    return JSONResponse(job)

@app.get("/download/{job_id}")
async def download_job(job_id: str, background_tasks: BackgroundTasks, username: str = Depends(get_current_username)):
    job = get_job_from_db(job_id)
    if not job or job["status"] != "completed" or not job["filename"]:
         return JSONResponse({"error": "File not ready"}, status_code=404)
         
    path = jobs[job_id]["filename"]
    
    def cleanup():
        try:
            os.remove(path)
            del jobs[job_id]
        except:
            pass
            
    background_tasks.add_task(cleanup)
    
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"timesheet_pdfs_{timestamp}.zip"
    
    return StreamingResponse(
        open(path, "rb"), 
        media_type="application/zip", 
        headers={'Content-Disposition': f'attachment; filename="{filename}"'}
    )

@app.post("/analyze", response_class=HTMLResponse)
async def analyze_excel(request: Request, file: UploadFile = File(...), username: str = Depends(get_current_username)):
    content = await file.read()
    
    try:
        # Read Excel ONCE
        df = pd.read_excel(io.BytesIO(content), engine='openpyxl')
        
        # Preprocess NOW to ensure clean types for Parquet
        df = preprocess_df(df)
        
        # Save CLEAN data to Unique Parquet
        file_id = str(uuid.uuid4())
        temp_file = f"temp_{file_id}.parquet"
        df.to_parquet(temp_file, engine='pyarrow')
        
        # Cost Center counting
        if "Cost Center" in df.columns:
            # Group by Name to count users, not rows (assuming one cost center per user)
            # Use 'first' to get a representative cost center for the user
            users_cc = df.groupby("Name")["Cost Center"].first()
            # Handle potential None/NaN values in cost center
            users_cc = users_cc.fillna("Unknown")
            cost_centers = users_cc.value_counts().to_dict()
        else:
            cost_centers = {"Unknown": df["Name"].nunique()}

        return templates.TemplateResponse("select_cost_centers.html", {
            "request": request,
            "cost_centers": cost_centers,
            "file_id": file_id
        })
        
    except Exception as e:
         return HTMLResponse(f"<h3>Errore nella lettura del file:</h3><p>{str(e)}</p>", status_code=400)

if __name__ == "__main__":
    # Startup cleanup
    for f in glob.glob("temp*.parquet"):
        try:
            os.remove(f)
        except:
            pass
    for f in glob.glob("output_*.zip"):
        try:
            os.remove(f)
        except:
            pass
            
    def get_local_ip():
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            # Doesn't need to be reachable
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except:
            return "127.0.0.1"

    local_ip = get_local_ip()
    print(f"\\n--- APP PRONTA ---")
    print(f"Locale:  http://localhost:8107")
    print(f"Rete:    http://{local_ip}:8107")
    print(f"------------------\\n")

    uvicorn.run(app, host="0.0.0.0", port=8107)
