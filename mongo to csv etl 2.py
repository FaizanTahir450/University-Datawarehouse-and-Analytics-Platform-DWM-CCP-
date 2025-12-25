import pymongo
import csv
import sys

try:
    client = pymongo.MongoClient("")
except pymongo.errors.ConfigurationError:
    print("An Invalid URI host error was received. Is your Atlas host name correct in your connection string?")
    sys.exit(1)

# use a database named "myDatabase"
db = client.myDatabase

# use a collection named "departments"
my_collection = db["departments"]

try:
    # Find all documents in the collection
    documents = my_collection.find()
    
    # Convert to list to check if we have data
    documents_list = list(documents)
    
    if not documents_list:
        print("No data found in the departments collection.")
        sys.exit(1)
    
    # Define the CSV filename
    csv_filename = "dim_department.csv"
    
    # Get field names from the first document
    fieldnames = list(documents_list[0].keys())
    
    # Remove MongoDB's _id field if present
    if '_id' in fieldnames:
        fieldnames.remove('_id')
    
    # Write to CSV file
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        # Write header
        writer.writeheader()
        
        # Write data rows
        for doc in documents_list:
            # Remove _id field from document if present
            if '_id' in doc:
                del doc['_id']
            writer.writerow(doc)
    
    print(f"Successfully exported {len(documents_list)} records to {csv_filename}")
    print(f"Fields exported: {', '.join(fieldnames)}")

except pymongo.errors.OperationFailure:
    print("An authentication error was received. Are you sure your database user is authorized to perform read operations?")
    sys.exit(1)
except Exception as e:
    print(f"An error occurred: {e}")

    sys.exit(1)
