# etl_fixed.py
"""
Fixed ETL script (date bug resolved).
Key fixes:
 - Ensure dim_date['date'] is converted to appropriate types:
   * Before writing to SQL: convert to python date (so MySQL DATE works well).
   * After reading from SQL: convert to pandas datetime (Timestamp) for comparisons.
 - Use a normalized Timestamp cutoff for comparisons.
 - Small helper ensure_sql_types_before_load to make types SQL-friendly.
"""
import pandas as pd
import numpy as np
import random
import datetime
from sqlalchemy import create_engine
from sqlalchemy import text
import re

# --- Edit DB credentials ---
MYSQL_USER = "root"
MYSQL_PASS = "admin"
MYSQL_HOST = "localhost"
MYSQL_DB = "dwh_university"
# ----------------------------
engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}/{MYSQL_DB}")

def ensure_sql_types_before_load(df):
    """
    Convert DataFrame columns to types that map well to SQL schema:
    - date/datetime columns -> python date for DATE columns (or leave as datetime64 if preferred).
    - numeric columns keep numeric types.
    This function only converts columns it recognizes by name.
    """
    df = df.copy()
    # convert 'date' and any column that endswith '_date' to python date objects (for MySQL DATE)
    for col in df.columns:
        if col == 'date' or col.endswith('_date') or col.endswith('_at'):
            # ensure pandas datetime first, then convert to python date (no time)
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
    return df

def transform_dimensions():
    """Perform real ETL transformations on dimension data"""
    print("Performing ETL transformations on dimension data...")
    
    # Read raw CSV data
    df_students = pd.read_csv("dim_student.csv")
    df_employees = pd.read_csv("dim_employee.csv")
    df_courses = pd.read_csv("dim_course.csv")
    df_departments = pd.read_csv("dim_department.csv")
    df_accounts = pd.read_csv("dim_account.csv")
    df_vendors = pd.read_csv("dim_vendor.csv")
    df_dates = pd.read_csv("dim_date.csv")
    
    # ==================== DEPARTMENT TRANSFORMATIONS ====================
    print("Transforming department data...")
    valid_dept_ids = set(df_departments['department_id'].unique())
    valid_manager_ids = set(df_employees['employee_id'].unique()) | {0}
    df_departments['manager_id'] = df_departments['manager_id'].apply(
        lambda x: x if x in valid_manager_ids else 0
    )
    df_departments['department_name'] = df_departments['department_name'].str.strip()
    
    # ==================== STUDENT TRANSFORMATIONS ====================
    print("Transforming student data...")
    df_students['first_name'] = df_students['first_name'].astype(str).str.strip().str.title()
    df_students['last_name'] = df_students['last_name'].astype(str).str.strip().str.title()
    df_students['city'] = df_students['city'].astype(str).str.strip().str.title()
    df_students['country'] = df_students['country'].astype(str).str.strip().str.title()
    current_year = datetime.datetime.now().year
    df_students['admission_year'] = df_students['admission_year'].apply(
        lambda x: x if pd.notna(x) and 2000 <= int(x) <= current_year else random.randint(2019, current_year)
    )
    df_students['birth_date'] = pd.to_datetime(df_students['birth_date'], errors='coerce')
    df_students['age'] = ((pd.to_datetime('today') - df_students['birth_date']).dt.days / 365.25).fillna(0).astype(int)
    df_students = df_students[(df_students['age'] >= 16) & (df_students['age'] <= 60)]
    program_mapping = {
        'BS Software Eng': 'BS Software Engineering',
        'BS Electrical': 'BS Electrical Engineering'
    }
    df_students['program'] = df_students['program'].replace(program_mapping)
    
    # ==================== EMPLOYEE TRANSFORMATIONS ====================
    print("Transforming employee data...")
    df_employees['first_name'] = df_employees['first_name'].astype(str).str.strip().str.title()
    df_employees['last_name'] = df_employees['last_name'].astype(str).str.strip().str.title()
    df_employees['email'] = df_employees['email'].astype(str).str.lower().str.strip()
    df_employees['manager_id'] = df_employees['manager_id'].fillna(0).astype(int)
    df_employees['department_id'] = df_employees['department_id'].apply(
        lambda x: x if x in valid_dept_ids else random.choice(list(valid_dept_ids))
    )
    df_employees['hire_date'] = pd.to_datetime(df_employees['hire_date'], errors='coerce')
    df_employees['tenure_years'] = ((pd.to_datetime('today') - df_employees['hire_date']).dt.days / 365.25).round(1).fillna(0)
    
    salary_ranges = {
        'Professor': (70000, 120000),
        'Associate Professor': (60000, 90000),
        'Assistant Professor': (50000, 75000),
        'Lecturer': (40000, 60000),
        'HR Manager': (55000, 80000),
        'Finance Officer': (45000, 70000),
        'Department Head': (80000, 130000),
        'Administrative Assistant': (35000, 50000),
        'IT Support': (40000, 65000),
        'Research Assistant': (30000, 45000)
    }
    def adjust_salary(row):
        default_range = (30000, 120000)
        min_sal, max_sal = salary_ranges.get(row.get('job_title', None), default_range)
        try:
            sal = float(row.get('salary', 0))
        except:
            sal = random.uniform(*default_range)
        if sal < min_sal or sal > max_sal:
            return round(random.uniform(min_sal, max_sal), 2)
        return round(sal, 2)
    df_employees['salary'] = df_employees.apply(adjust_salary, axis=1)
    
    # ==================== COURSE TRANSFORMATIONS ====================
    print("Transforming course data...")
    df_courses['course_code'] = df_courses['course_code'].astype(str).str.upper().str.strip()
    def extract_course_level(code):
        match = re.search(r'(\d+)', str(code))
        if match:
            number = int(match.group(1))
            if number < 200:
                return 'Introductory'
            elif number < 400:
                return 'Intermediate'
            else:
                return 'Advanced'
        return 'Unknown'
    df_courses['course_level'] = df_courses['course_code'].apply(extract_course_level)
    df_courses['credit_hours'] = df_courses['credit_hours'].apply(
        lambda x: x if x in [2, 3, 4] else random.choice([2, 3, 4])
    )
    
    # ==================== FINANCE TRANSFORMATIONS ====================
    print("Transforming finance data...")
    valid_account_types = ['Asset', 'Liability', 'Equity', 'Revenue', 'Expense']
    df_accounts['account_type'] = df_accounts['account_type'].apply(
        lambda x: x if x in valid_account_types else 'Expense'
    )
    df_vendors['vendor_name'] = df_vendors['vendor_name'].astype(str).str.strip().str.title()
    df_vendors['contact_person'] = df_vendors['contact_person'].astype(str).str.strip().str.title()
    df_vendors['email'] = df_vendors['email'].astype(str).str.lower().str.strip()
    
    # ==================== DATE TRANSFORMATIONS ====================
    print("Transforming date data...")
    df_dates['date'] = pd.to_datetime(df_dates['date'], errors='coerce')
    df_dates['year'] = df_dates['date'].dt.year
    df_dates['month'] = df_dates['date'].dt.month
    df_dates['day'] = df_dates['date'].dt.day
    df_dates['weekday'] = df_dates['date'].dt.day_name()
    df_dates['quarter'] = df_dates['date'].dt.quarter
    df_dates['is_weekend'] = df_dates['date'].dt.dayofweek >= 5
    def calculate_semester(date):
        if pd.isna(date):
            return None
        month = date.month
        if month in [1, 2, 3, 4]:
            return f"Spring {date.year}"
        elif month in [5, 6, 7, 8]:
            return f"Summer {date.year}"
        else:
            return f"Fall {date.year}"
    df_dates['semester'] = df_dates['date'].apply(calculate_semester)
    
    # ==================== DATA QUALITY CHECKS ====================
    print("Performing data quality checks...")
    df_students = df_students.drop_duplicates(subset=['student_id'])
    df_employees = df_employees.drop_duplicates(subset=['employee_id'])
    df_courses = df_courses.drop_duplicates(subset=['course_id'])
    valid_student_ids = set(df_students['student_id'].unique())
    valid_course_ids = set(df_courses['course_id'].unique())
    valid_employee_ids = set(df_employees['employee_id'].unique())
    valid_department_ids = set(df_departments['department_id'].unique())
    
    print(f"Data quality summary:")
    print(f"- Students: {len(df_students)} valid records")
    print(f"- Employees: {len(df_employees)} valid records")
    print(f"- Courses: {len(df_courses)} valid records")
    print(f"- Departments: {len(df_departments)} valid records")
    print(f"- Accounts: {len(df_accounts)} valid records")
    print(f"- Vendors: {len(df_vendors)} valid records")
    print(f"- Dates: {len(df_dates)} valid records")
    
    return {
        'students': df_students,
        'employees': df_employees,
        'courses': df_courses,
        'departments': df_departments,
        'accounts': df_accounts,
        'vendors': df_vendors,
        'dates': df_dates
    }

def create_transformed_tables():
    """Create tables with proper constraints"""
    print("Creating transformed tables...")
    with engine.begin() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
        conn.execute(text("DROP TABLE IF EXISTS fact_academics, fact_hr_metrics, fact_finance;"))
        conn.execute(text("DROP TABLE IF EXISTS dim_student, dim_course, dim_employee, dim_department, dim_account, dim_vendor, dim_date;"))
        # (same CREATE statements as before — kept unchanged)
        conn.execute(text("""
        CREATE TABLE dim_date (
            date_key INT PRIMARY KEY,
            date DATE,
            year INT,
            quarter INT,
            month INT,
            week INT,
            day INT,
            weekday VARCHAR(10),
            is_weekend BOOLEAN,
            semester VARCHAR(20)
        );
        """))
        conn.execute(text("""
        CREATE TABLE dim_student (
            student_id INT PRIMARY KEY,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            gender VARCHAR(10),
            birth_date DATE,
            admission_year INT,
            program VARCHAR(100),
            city VARCHAR(100),
            country VARCHAR(100),
            created_at DATE,
            age INT
        );
        """))
        conn.execute(text("""
        CREATE TABLE dim_course (
            course_id INT PRIMARY KEY,
            course_code VARCHAR(20),
            course_name VARCHAR(200),
            department VARCHAR(100),
            credit_hours INT,
            course_level VARCHAR(20)
        );
        """))
        conn.execute(text("""
        CREATE TABLE dim_department (
            department_id INT PRIMARY KEY,
            department_name VARCHAR(100),
            manager_id INT,
            budget DECIMAL(15,2),
            location VARCHAR(100)
        );
        """))
        conn.execute(text("""
        CREATE TABLE dim_employee (
            employee_id INT PRIMARY KEY,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            email VARCHAR(150),
            phone VARCHAR(20),
            hire_date DATE,
            job_title VARCHAR(100),
            salary DECIMAL(10,2),
            department_id INT,
            manager_id INT,
            employment_type VARCHAR(50),
            benefits_eligible BOOLEAN,
            tenure_years DECIMAL(4,1),
            FOREIGN KEY (department_id) REFERENCES dim_department(department_id)
        );
        """))
        conn.execute(text("""
        CREATE TABLE dim_account (
            account_id INT PRIMARY KEY,
            account_code VARCHAR(20),
            account_name VARCHAR(100),
            account_type VARCHAR(50),
            category VARCHAR(50)
        );
        """))
        conn.execute(text("""
        CREATE TABLE dim_vendor (
            vendor_id INT PRIMARY KEY,
            vendor_name VARCHAR(100),
            vendor_type VARCHAR(50),
            contact_person VARCHAR(100),
            phone VARCHAR(20),
            email VARCHAR(150)
        );
        """))
        conn.execute(text("""
        CREATE TABLE fact_academics (
            record_id BIGINT AUTO_INCREMENT PRIMARY KEY,
            date_key INT,
            student_id INT,
            course_id INT,
            employee_id INT,
            grade DECIMAL(5,2),
            attendance_percent DECIMAL(5,2),
            fee_paid DECIMAL(10,2),
            created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
            FOREIGN KEY (student_id) REFERENCES dim_student(student_id),
            FOREIGN KEY (course_id) REFERENCES dim_course(course_id),
            FOREIGN KEY (employee_id) REFERENCES dim_employee(employee_id)
        );
        """))
        conn.execute(text("""
        CREATE TABLE fact_hr_metrics (
            record_id BIGINT AUTO_INCREMENT PRIMARY KEY,
            employee_id INT,
            department_id INT,
            date_key INT,
            salary_amount DECIMAL(10,2),
            bonus_amount DECIMAL(10,2),
            overtime_hours DECIMAL(5,2),
            leave_days_taken INT,
            performance_rating DECIMAL(3,2),
            FOREIGN KEY (employee_id) REFERENCES dim_employee(employee_id),
            FOREIGN KEY (department_id) REFERENCES dim_department(department_id),
            FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
        );
        """))
        conn.execute(text("""
        CREATE TABLE fact_finance (
            record_id BIGINT AUTO_INCREMENT PRIMARY KEY,
            date_key INT,
            account_id INT,
            department_id INT,
            vendor_id INT,
            transaction_type ENUM('Revenue', 'Expense'),
            amount DECIMAL(15,2),
            description VARCHAR(200),
            reference_number VARCHAR(50),
            FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
            FOREIGN KEY (account_id) REFERENCES dim_account(account_id),
            FOREIGN KEY (department_id) REFERENCES dim_department(department_id),
            FOREIGN KEY (vendor_id) REFERENCES dim_vendor(vendor_id)
        );
        """))
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
    print("All tables created successfully!")

def load_transformed_data(transformed_data):
    """Load transformed data into database in correct order"""
    print("Loading transformed data into database...")
    # Convert date columns to python date objects so MySQL DATE columns accept them cleanly
    dates_for_sql = ensure_sql_types_before_load(transformed_data['dates'])
    dates_for_sql.to_sql("dim_date", con=engine, if_exists="append", index=False)
    print("Loaded dim_date")
    transformed_data['departments'].to_sql("dim_department", con=engine, if_exists="append", index=False)
    print("Loaded dim_department")
    transformed_data['accounts'].to_sql("dim_account", con=engine, if_exists="append", index=False)
    transformed_data['vendors'].to_sql("dim_vendor", con=engine, if_exists="append", index=False)
    transformed_data['courses'].to_sql("dim_course", con=engine, if_exists="append", index=False)
    # For students, convert birth_date/created_at to date too
    students_for_sql = ensure_sql_types_before_load(transformed_data['students'])
    students_for_sql.to_sql("dim_student", con=engine, if_exists="append", index=False)
    print("Loaded independent dimensions")
    valid_dept_ids = set(transformed_data['departments']['department_id'].unique())
    employee_dept_ids = set(transformed_data['employees']['department_id'].unique())
    invalid_dept_ids = employee_dept_ids - valid_dept_ids
    if invalid_dept_ids:
        print(f"Warning: Found invalid department IDs in employees: {invalid_dept_ids}")
        transformed_data['employees']['department_id'] = transformed_data['employees']['department_id'].apply(
            lambda x: x if x in valid_dept_ids else random.choice(list(valid_dept_ids))
        )
    # convert hire_date to python date before load
    employees_for_sql = ensure_sql_types_before_load(transformed_data['employees'])
    employees_for_sql.to_sql("dim_employee", con=engine, if_exists="append", index=False)
    print("Loaded dim_employee")
    print("All transformed data loaded successfully!")

def generate_business_facts():
    """Generate fact tables with business logic"""
    print("Generating fact tables with business logic...")
    df_students = pd.read_sql("SELECT * FROM dim_student", engine)
    df_courses = pd.read_sql("SELECT * FROM dim_course", engine)
    df_employees = pd.read_sql("SELECT * FROM dim_employee", engine)
    df_departments = pd.read_sql("SELECT * FROM dim_department", engine)
    df_accounts = pd.read_sql("SELECT * FROM dim_account", engine)
    df_vendors = pd.read_sql("SELECT * FROM dim_vendor", engine)
    df_dates = pd.read_sql("SELECT * FROM dim_date", engine)
    
    # IMPORTANT: ensure df_dates['date'] is pandas datetime (Timestamp).
    # This avoids comparisons between Timestamp and python.date.
    df_dates['date'] = pd.to_datetime(df_dates['date'], errors='coerce')
    
    # Generate academic facts
    print("Generating academic facts...")
    academic_data = []
    student_ids = df_students['student_id'].sample(5000, replace=True).unique()
    for student_id in student_ids:
        student = df_students[df_students['student_id'] == student_id].iloc[0]
        course = df_courses.sample(1).iloc[0]
        # handle case where no instructor matches by fallback
        profs = df_employees[df_employees['job_title'].fillna('').str.contains('Professor|Lecturer', na=False)]
        if profs.empty:
            instructor = df_employees.sample(1).iloc[0]
        else:
            instructor = profs.sample(1).iloc[0]
        date = df_dates.sample(1).iloc[0]
        base_grade = random.gauss(70, 12)
        if course.get('course_level') == 'Advanced':
            base_grade -= 5
        elif course.get('course_level') == 'Introductory':
            base_grade += 5
        if student.get('age', 0) > 25:
            base_grade += 3
        grade = max(0, min(100, base_grade))
        attendance = max(60, min(100, grade + random.gauss(0, 8)))
        academic_data.append({
            'date_key': int(date['date_key']),
            'student_id': int(student_id),
            'course_id': int(course['course_id']),
            'employee_id': int(instructor['employee_id']),
            'grade': round(grade, 1),
            'attendance_percent': round(attendance, 1),
            'fee_paid': round(course.get('credit_hours', 3) * 250 * (0.8 if random.random() < 0.2 else 1.0), 2)
        })
    pd.DataFrame(academic_data).to_sql("fact_academics", con=engine, if_exists="append", index=False)
    print(f"Generated {len(academic_data)} academic records")
    
    # Generate HR facts
    print("Generating HR facts...")
    hr_data = []
    # Use a normalized cutoff Timestamp
    cutoff = pd.to_datetime('today').normalize() - pd.DateOffset(years=2)
    # Make sure df_dates['date'] is datetime (done above)
    recent_dates = df_dates[df_dates['date'] >= cutoff]
    quarterly_dates = recent_dates[recent_dates['month'].isin([3, 6, 9, 12])]
    for employee_id in df_employees['employee_id'].unique():
        employee = df_employees[df_employees['employee_id'] == employee_id].iloc[0]
        # sample up to 8 quarters present
        sample_size = min(8, len(quarterly_dates))
        if sample_size == 0:
            # fallback to any date
            selected_dates = df_dates.sample(min(4, len(df_dates)))
        else:
            selected_dates = quarterly_dates.sample(sample_size)
        for _, date_row in selected_dates.iterrows():
            base_performance = random.gauss(3.5, 0.5)
            if float(employee.get('tenure_years', 0) or 0) > 5:
                base_performance += 0.3
            if 'Professor' in str(employee.get('job_title', '')):
                base_performance += 0.2
            performance = max(1.0, min(5.0, base_performance))
            hr_data.append({
                'employee_id': int(employee_id),
                'department_id': int(employee.get('department_id', 0) or 0),
                'date_key': int(date_row['date_key']),
                'salary_amount': float(employee.get('salary', 0) or 0),
                'bonus_amount': round((float(employee.get('salary', 0) or 0) * (performance / 20)), 2),
                'overtime_hours': round(random.uniform(0, 10), 1),
                'leave_days_taken': random.randint(0, 8),
                'performance_rating': round(performance, 1)
            })
    if hr_data:
        pd.DataFrame(hr_data).to_sql("fact_hr_metrics", con=engine, if_exists="append", index=False)
    print(f"Generated {len(hr_data)} HR records")
    
    # Generate finance facts (unchanged)
    print("Generating finance facts...")
    finance_data = []
    for _ in range(2000):
        account = df_accounts.sample(1).iloc[0]
        department = df_departments.sample(1).iloc[0]
        vendor = df_vendors.sample(1).iloc[0]
        valid_dates = df_dates[df_dates['year'] >= 2020]
        if len(valid_dates) > 0:
            date = valid_dates.sample(1).iloc[0]
        else:
            date = df_dates.sample(1).iloc[0]
        transaction_type = "Expense" if account['account_type'] == 'Expense' else "Revenue"
        if transaction_type == 'Revenue':
            amount = round(random.uniform(1000, 50000), 2)
        else:
            amount = round(random.uniform(100, 20000), 2)
        if department['department_name'] in ['Finance', 'Administration']:
            amount *= 1.5
        finance_data.append({
            'date_key': int(date['date_key']),
            'account_id': int(account['account_id']),
            'department_id': int(department['department_id']),
            'vendor_id': int(vendor['vendor_id']),
            'transaction_type': transaction_type,
            'amount': amount,
            'description': f"{transaction_type} for {account.get('category', '')} - {vendor.get('vendor_name','')}",
            'reference_number': f"REF{random.randint(10000, 99999)}"
        })
    pd.DataFrame(finance_data).to_sql("fact_finance", con=engine, if_exists="append", index=False)
    print(f"Generated {len(finance_data)} finance records")

def main():
    print("Starting REAL ETL process with transformations...")
    try:
        transformed_data = transform_dimensions()
        create_transformed_tables()
        load_transformed_data(transformed_data)
        generate_business_facts()
        print("\n" + "="*50)
        print("REAL ETL process completed successfully!")
        print("="*50)
    except Exception as e:
        print(f"ETL process failed: {e}")
        raise

if __name__ == "__main__":
    main()
