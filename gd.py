"""
generate_dummy_data.py
Generates dummy dimension data only.
Usage:
  pip install -r requirements.txt
  python generate_dummy_data.py
"""
import pandas as pd
import random
import datetime
from faker import Faker
import os

fake = Faker()
F_STUDENTS = 1000
F_COURSES = 60
F_EMPLOYEES = 60
F_DEPARTMENTS = 8
F_ACCOUNTS = 20
F_VENDORS = 30

# Check if data already exists
data_files = [
    "dim_student.csv", "dim_employee.csv", "dim_course.csv", 
    "dim_department.csv", "dim_account.csv", "dim_vendor.csv", "dim_date.csv"
]

all_files_exist = all(os.path.exists(file) for file in data_files)

if all_files_exist:
    print("Data already generated. CSV files exist:")
    for file in data_files:
        print(f"  - {file}")
    exit()

# Departments
departments_data = [
    "Computer Science", "Software Engineering", "Business", "Electrical Engineering", 
    "Mathematics", "Human Resources", "Finance", "Administration"
]
departments = []
for i in range(1, F_DEPARTMENTS + 1):
    departments.append({
        "department_id": i,
        "department_name": departments_data[i-1],
        "manager_id": None,
        "budget": round(random.uniform(50000, 500000), 2),
        "location": fake.city()
    })

# Employees
job_titles = [
    "Professor", "Associate Professor", "Assistant Professor", "Lecturer",
    "HR Manager", "Finance Officer", "Administrative Assistant", "IT Support",
    "Department Head", "Research Assistant"
]
employment_types = ["Full-time", "Part-time", "Contract"]
employees = []
manager_ids = []

for i in range(1, F_EMPLOYEES + 1):
    dept_id = random.randint(1, F_DEPARTMENTS)
    hire_date = fake.date_between(start_date='-15y', end_date='today')
    is_manager = random.random() < 0.1
    
    employees.append({
        "employee_id": i,
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email(),
        "phone": fake.phone_number()[:20],
        "hire_date": hire_date,
        "job_title": random.choice(job_titles),
        "salary": round(random.uniform(30000, 120000), 2),
        "department_id": dept_id,
        "manager_id": None,
        "employment_type": random.choice(employment_types),
        "benefits_eligible": random.choice([True, False])
    })
    
    if is_manager:
        manager_ids.append(i)

# Set manager relationships
for employee in employees:
    if employee["employee_id"] not in manager_ids and manager_ids:
        employee["manager_id"] = random.choice(manager_ids)

# Students
programs = ["BS Computer Science", "BS Software Eng", "BS Business", "BS Electrical", "BS Mathematics"]
students = []
for i in range(1, F_STUDENTS + 1):
    gender = random.choice(["Male", "Female"])
    birth = fake.date_between(start_date='-30y', end_date='-17y')
    adm_year = random.choice([2019, 2020, 2021, 2022, 2023, 2024])
    students.append({
        "student_id": i,
        "first_name": fake.first_name_male() if gender=="Male" else fake.first_name_female(),
        "last_name": fake.last_name(),
        "gender": gender,
        "birth_date": birth,
        "admission_year": adm_year,
        "program": random.choice(programs),
        "city": fake.city(),
        "country": fake.country(),
        "created_at": fake.date_between(start_date=f"-{adm_year - 2018}y", end_date='today')
    })

# Courses
courses = []
for i in range(1, F_COURSES + 1):
    dept = random.choice(departments_data[:5])
    code = f"{dept[:3].upper()}{100 + i}"
    courses.append({
        "course_id": i,
        "course_code": code,
        "course_name": fake.sentence(nb_words=3).rstrip('.'),
        "department": dept,
        "credit_hours": random.choice([2,3,4])
    })

# Finance: Accounts
account_types = ["Asset", "Liability", "Equity", "Revenue", "Expense"]
account_categories = ["Tuition", "Salary", "Equipment", "Utilities", "Supplies", "Maintenance"]
accounts = []
for i in range(1, F_ACCOUNTS + 1):
    accounts.append({
        "account_id": i,
        "account_code": f"ACC{1000 + i}",
        "account_name": f"{random.choice(account_categories)} Account",
        "account_type": random.choice(account_types),
        "category": random.choice(account_categories)
    })

# Finance: Vendors
vendor_types = ["Supplier", "Service Provider", "Contractor", "Utility Company"]
vendors = []
for i in range(1, F_VENDORS + 1):
    vendors.append({
        "vendor_id": i,
        "vendor_name": fake.company(),
        "vendor_type": random.choice(vendor_types),
        "contact_person": fake.name(),
        "phone": fake.phone_number()[:20],
        "email": fake.email()
    })

# Date dimension
dates = []
start = datetime.date.today() - datetime.timedelta(days=365*3)
end = datetime.date.today()
d = start
while d <= end:
    month = d.month
    quarter = (month - 1) // 3 + 1
    week = d.isocalendar()[1]
    if month in [1,2,3,4]:
        sem = "Spring"
    elif month in [5,6,7,8]:
        sem = "Summer"
    else:
        sem = "Fall"
    dates.append({
        "date_key": int(d.strftime("%Y%m%d")),
        "date": d,
        "year": d.year,
        "quarter": quarter,
        "month": d.month,
        "week": week,
        "day": d.day,
        "weekday": d.strftime("%A"),
        "is_weekend": d.weekday() >= 5,
        "semester": f"{sem} {d.year}"
    })
    d = d + datetime.timedelta(days=1)

# Save to CSV files
pd.DataFrame(students).to_csv("dim_student.csv", index=False)
pd.DataFrame(employees).to_csv("dim_employee.csv", index=False)
pd.DataFrame(courses).to_csv("dim_course.csv", index=False)
pd.DataFrame(departments).to_csv("dim_department.csv", index=False)
pd.DataFrame(accounts).to_csv("dim_account.csv", index=False)
pd.DataFrame(vendors).to_csv("dim_vendor.csv", index=False)
pd.DataFrame(dates).to_csv("dim_date.csv", index=False)

print("Generated dummy data for all dimension tables")
print(f"Students: {len(students)}")
print(f"Employees: {len(employees)}")
print(f"Courses: {len(courses)}")
print(f"Departments: {len(departments)}")
print(f"Accounts: {len(accounts)}")
print(f"Vendors: {len(vendors)}")
print(f"Dates: {len(dates)}")
print("CSV files created successfully!")