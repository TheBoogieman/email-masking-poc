"""
Email Masking Pipeline - Customer Data Transformation
-----------------------------------------------------
This script:
1. Reads nested JSON customer data
2. Flattens it into tabular format using DuckDB
3. Masks email addresses, showing only domains
4. Outputs results to CSV, Parquet, and creates a DuckDB database

Date: December 2025
"""

import duckdb
import json
import re
from pathlib import Path


def mask_email(email: str) -> str:
    """
    Mask email address, showing only the domain.
    
    Args:
        email: Email address to mask
        
    Returns:
        Masked email in format *******@domain.com
        
    Example:
        carlos91@gmail.com -> *******@gmail.com
    """
    if not email or '@' not in email:
        return email
    
    # Split email into local part and domain
    local_part, domain = email.split('@', 1)
    
    # Mask local part with asterisks (7 asterisks as per example)
    masked_local = '*' * 7
    
    return f"{masked_local}@{domain}"


def flatten_and_transform_customer_data(json_file_path: str, output_dir: str = "output"):
    """
    Process customer JSON data: flatten structure and mask emails.
    
    Args:
        json_file_path: Path to the Customers.json file
        output_dir: Directory for output files
    """
    # Create output directory if it doesn't exist
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Initialize DuckDB connection (in-memory database)
    con = duckdb.connect(database=':memory:')
    
    print("=" * 70)
    print("Email Masking Pipeline - Customer Data Transformation")
    print("=" * 70)
    
    # Step 1: Read and flatten JSON data
    print("\n[Step 1] Reading and flattening JSON data...")
    
    with open(json_file_path, 'r') as f:
        data = json.load(f)
    
    # Extract nested customer data
    company_id = data['CompanyID']
    company_info = data['CompanyInfo'][0]
    company_name = company_info['Name']
    customers = company_info['Customers']
    
    # Flatten the nested structure
    flattened_customers = []
    for customer in customers:
        flattened_customers.append({
            'CompanyID': company_id,
            'CompanyName': company_name,
            'CustomerName': customer['Name'],
            'Email': customer['Email'],
            'Phone': customer['Phone'],
            'DateOfBirth': customer['Birth'],
            'PlaceOfBirth': customer['Place of Birth'],
            'Role': customer['Role']
        })
    
    print(f"   [OK] Extracted {len(flattened_customers)} customer records")
    print(f"   [OK] Company: {company_name} (ID: {company_id})")
    
    # Step 2: Create DuckDB table from flattened data
    print("\n[Step 2] Creating DuckDB table from flattened data...")
    
    # Register the flattened data as a DuckDB table
    con.execute("""
        CREATE TABLE customers_raw AS 
        SELECT * FROM read_json_auto(?)
    """, [json_file_path])
    
    # Flatten the nested structure using DuckDB's JSON functions
    con.execute("""
        CREATE TABLE customers_flattened AS
        SELECT 
            CompanyID,
            UNNEST(CompanyInfo, recursive := true)
        FROM customers_raw
    """)
    
    # Create final structured table
    con.execute("""
        CREATE TABLE customers_tabular AS
        SELECT 
            CompanyID,
            Name as CompanyName,
            UNNEST(Customers, recursive := true)
        FROM customers_flattened
    """)
    
    print("   [OK] Created tabular structure in DuckDB")
    
    # Step 3: Apply email masking
    print("\n[Step 3] Applying email masking transformation...")
    
    # Fetch data for masking
    df = con.execute("SELECT * FROM customers_tabular").fetchdf()
    
    # Apply email masking using Python function
    df['Email_Masked'] = df['Email'].apply(mask_email)
    
    # Create final table with masked emails
    con.execute("""
        CREATE TABLE customers_secure AS
        SELECT 
            CompanyID,
            CompanyName,
            Name as CustomerName,
            Email as Email_Original,
            ? as Email_Masked,
            Phone,
            Birth as DateOfBirth,
            "Place of Birth" as PlaceOfBirth,
            Role
        FROM customers_tabular
    """, [df['Email_Masked'].tolist()])
    
    # Alternative: Use DuckDB's string functions for masking (pure SQL approach)
    con.execute("""
        CREATE OR REPLACE TABLE customers_secure_sql AS
        SELECT 
            CompanyID,
            CompanyName,
            Name as CustomerName,
            Email as Email_Original,
            CONCAT('*******@', SPLIT_PART(Email, '@', 2)) as Email_Masked,
            Phone,
            Birth as DateOfBirth,
            "Place of Birth" as PlaceOfBirth,
            Role
        FROM customers_tabular
    """)
    
    print("   [OK] Email masking applied successfully")
    
    # Step 4: Display results
    print("\n[Step 4] Displaying transformed data...")
    print("\n" + "-" * 70)
    print("SECURE CUSTOMER DATA (Email Masked)")
    print("-" * 70)
    
    result = con.execute("""
        SELECT 
            CompanyID,
            CompanyName,
            CustomerName,
            Email_Masked,
            Phone,
            DateOfBirth,
            PlaceOfBirth,
            Role
        FROM customers_secure_sql
    """).fetchdf()
    
    print(result.to_string(index=False))
    
    # Step 5: Export results
    print("\n[Step 5] Exporting results...")
    
    # Export to CSV
    csv_path = f"{output_dir}/customers_secure.csv"
    con.execute(f"""
        COPY customers_secure_sql 
        TO '{csv_path}' 
        (HEADER, DELIMITER ',')
    """)
    print(f"   [OK] Exported to CSV: {csv_path}")
    
    # Export to Parquet for efficient storage
    parquet_path = f"{output_dir}/customers_secure.parquet"
    con.execute(f"""
        COPY customers_secure_sql 
        TO '{parquet_path}' 
        (FORMAT PARQUET)
    """)
    print(f"   [OK] Exported to Parquet: {parquet_path}")
    
    # Optional: Create a persistent DuckDB database file
    db_path = f"{output_dir}/customers.duckdb"
    persistent_con = duckdb.connect(database=db_path)
    persistent_con.execute("""
        CREATE TABLE IF NOT EXISTS customers_secure AS 
        SELECT * FROM read_parquet(?)
    """, [parquet_path])
    persistent_con.close()
    print(f"   [OK] Created DuckDB database: {db_path}")
    
    # Step 6: Data quality summary
    print("\n[Step 6] Data Quality Summary...")
    print("-" * 70)
    
    summary = con.execute("""
        SELECT 
            COUNT(*) as TotalRecords,
            COUNT(DISTINCT CompanyID) as UniqueCompanies,
            COUNT(DISTINCT Email_Original) as UniqueEmails,
            COUNT(DISTINCT CustomerName) as UniqueCustomers,
            MIN(DateOfBirth) as OldestDOB,
            MAX(DateOfBirth) as YoungestDOB
        FROM customers_secure_sql
    """).fetchdf()
    
    print(summary.to_string(index=False))
    
    # Verify masking worked correctly
    print("\n[Step 7] Email Masking Verification...")
    print("-" * 70)
    
    verification = con.execute("""
        SELECT 
            CustomerName,
            Email_Original,
            Email_Masked,
            CASE
                WHEN Email_Masked LIKE '*******@%' THEN '[OK] Masked Correctly'
                ELSE '[FAIL] Masking Failed'
            END as MaskingStatus
        FROM customers_secure_sql
    """).fetchdf()
    
    print(verification.to_string(index=False))
    
    # Close connection
    con.close()
    
    print("\n" + "=" * 70)
    print("Transformation Complete!")
    print("=" * 70)
    print(f"\nOutput files created in '{output_dir}/' directory:")
    print(f"  - customers_secure.csv (for analysis)")
    print(f"  - customers_secure.parquet (for efficient storage)")
    print(f"  - customers.duckdb (persistent database)")


if __name__ == "__main__":
    # Configuration
    JSON_FILE = "../data/Customers.json"
    OUTPUT_DIR = "../data"

    # Execute transformation
    try:
        flatten_and_transform_customer_data(JSON_FILE, OUTPUT_DIR)
    except FileNotFoundError:
        print(f"Error: Could not find {JSON_FILE}")
        print("Please ensure the Customers.json file is in the data/ directory.")
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise
