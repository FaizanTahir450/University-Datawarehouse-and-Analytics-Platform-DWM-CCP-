import csv
import os
from sqlalchemy import create_engine, text

def create_connection():
    """Create SQLAlchemy database connection"""
    try:
        engine = create_engine("mysql+pymysql://root:admin@localhost/test_db")
        connection = engine.connect()
        print("Connected to MySQL database via SQLAlchemy")
        return connection
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def check_tables_exist(connection):
    """Check if the required tables exist in the database"""
    try:
        # Check if employee table exists
        result = connection.execute(text("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = 'test_db' AND table_name = 'dim_employee'
        """))
        employee_exists = result.scalar() > 0
        
        # Check if student table exists
        result = connection.execute(text("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = 'test_db' AND table_name = 'dim_student'
        """))
        student_exists = result.scalar() > 0
        
        print(f"dim_employee table exists: {'✓' if employee_exists else '✗'}")
        print(f"dim_student table exists: {'✓' if student_exists else '✗'}")
        
        return employee_exists, student_exists
        
    except Exception as e:
        print(f"Error checking tables: {e}")
        return False, False

def extract_employee_data(connection, csv_file):
    """Extract employee data from MySQL and save to CSV"""
    try:
        # Query all employee data
        query = text("SELECT * FROM dim_employee")
        result = connection.execute(query)
        
        # Get column names
        columns = result.keys()
        
        # Convert to list of dictionaries
        employees = []
        for row in result:
            employees.append(dict(zip(columns, row)))
        
        if not employees:
            print("✗ No employee data found in the database")
            return False
        
        # Write to CSV
        with open(csv_file, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=columns)
            writer.writeheader()
            writer.writerows(employees)
        
        print(f"✓ Extracted {len(employees)} employee records to {csv_file}")
        return True
        
    except Exception as e:
        print(f"✗ Error extracting employee data: {e}")
        return False

def extract_student_data(connection, csv_file):
    """Extract student data from MySQL and save to CSV"""
    try:
        # Query all student data
        query = text("SELECT * FROM dim_student")
        result = connection.execute(query)
        
        # Get column names
        columns = result.keys()
        
        # Convert to list of dictionaries
        students = []
        for row in result:
            students.append(dict(zip(columns, row)))
        
        if not students:
            print("✗ No student data found in the database")
            return False
        
        # Write to CSV
        with open(csv_file, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=columns)
            writer.writeheader()
            writer.writerows(students)
        
        print(f"✓ Extracted {len(students)} student records to {csv_file}")
        return True
        
    except Exception as e:
        print(f"✗ Error extracting student data: {e}")
        return False

def verify_extracted_data():
    """Verify that CSV files were created and contain data"""
    print("\n" + "="*50)
    print("EXTRACTION VERIFICATION")
    print("="*50)
    
    files = ['dim_employee.csv', 'dim_student.csv']
    
    for file in files:
        if os.path.exists(file):
            try:
                with open(file, 'r') as f:
                    reader = csv.reader(f)
                    rows = list(reader)
                    record_count = len(rows) - 1  # Subtract header
                    print(f"✓ {file}: {record_count} records extracted")
                    
                    # Show first few columns of header
                    if rows:
                        print(f"  Columns: {', '.join(rows[0][:3])}...")
            except Exception as e:
                print(f"✗ Error reading {file}: {e}")
        else:
            print(f"✗ {file}: File not created")

def get_table_counts(connection):
    """Get record counts from both tables"""
    try:
        # Count employees
        result = connection.execute(text("SELECT COUNT(*) FROM dim_employee"))
        employee_count = result.scalar()
        
        # Count students
        result = connection.execute(text("SELECT COUNT(*) FROM dim_student"))
        student_count = result.scalar()
        
        print(f"Database records - Employees: {employee_count}, Students: {student_count}")
        return employee_count, student_count
        
    except Exception as e:
        print(f"Error getting table counts: {e}")
        return 0, 0

def main():
    """Main function for data extraction from MySQL to CSV"""
    print("Starting Data Extraction Process (MySQL → CSV)")
    print("="*60)
    
    # Step 1: Create database connection
    connection = create_connection()
    if not connection:
        print("Failed to connect to database. Exiting.")
        return

    try:
        # Step 2: Check if tables exist
        employee_exists, student_exists = check_tables_exist(connection)
        
        if not employee_exists and not student_exists:
            print("No tables found in database. Exiting.")
            return
        
        # Step 3: Get initial counts
        print("\nCurrent database state:")
        get_table_counts(connection)
        
        # Step 4: Extract employee data if table exists
        employee_success = False
        if employee_exists:
            print("\nExtracting employee data...")
            employee_success = extract_employee_data(connection, 'dim_employee.csv')
        else:
            print("Skipping employee extraction - table not found")
        
        # Step 5: Extract student data if table exists
        student_success = False
        if student_exists:
            print("\nExtracting student data...")
            student_success = extract_student_data(connection, 'dim_student.csv')
        else:
            print("Skipping student extraction - table not found")
        
        # Step 6: Verify extraction
        if employee_success or student_success:
            print("\n✓ Data extraction completed!")
            verify_extracted_data()
        else:
            print("\n✗ No data was extracted")
            
    except Exception as e:
        print(f"\n✗ Critical error during extraction: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Close connection
        if connection:
            connection.close()
            print("\nDatabase connection closed.")

def preview_csv_data():
    """Optional: Preview the extracted CSV data"""
    print("\n" + "="*50)
    print("CSV DATA PREVIEW")
    print("="*50)
    
    files = ['dim_employee.csv', 'dim_student.csv']
    
    for file in files:
        if os.path.exists(file):
            print(f"\n{file}:")
            try:
                with open(file, 'r') as f:
                    reader = csv.reader(f)
                    # Show header and first 2 data rows
                    for i, row in enumerate(reader):
                        if i < 3:  # Header + 2 rows
                            print(f"  Row {i}: {row}")
                        if i == 3:
                            print("  ...")
                            break
            except Exception as e:
                print(f"  Error reading file: {e}")

if __name__ == "__main__":
    main()
    
    # Optional: Preview the extracted data
    preview_csv_data()