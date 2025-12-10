"""
Analytics Queries - Customer Data Analytics
-------------------------------------------
This file demonstrates how to query the transformed customer data
stored in the DuckDB database.
"""

import duckdb

# Connect to the persistent database
con = duckdb.connect(database='../data/customers.duckdb', read_only=True)

print("=" * 80)
print("CUSTOMER DATA ANALYTICS - EXAMPLE QUERIES")
print("=" * 80)

# Query 1: View all secure customer data
print("\n1. All Customers (Masked Emails)")
print("-" * 80)
result = con.execute("""
    SELECT 
        CustomerName,
        Email_Masked,
        Role,
        PlaceOfBirth
    FROM customers_secure
    ORDER BY CustomerName
""").fetchdf()
print(result.to_string(index=False))

# Query 2: Count by role
print("\n2. Customer Distribution by Role")
print("-" * 80)
result = con.execute("""
    SELECT 
        Role,
        COUNT(*) as CustomerCount
    FROM customers_secure
    GROUP BY Role
    ORDER BY CustomerCount DESC
""").fetchdf()
print(result.to_string(index=False))

# Query 3: Email domain analysis
print("\n3. Email Domain Distribution")
print("-" * 80)
result = con.execute("""
    SELECT 
        SPLIT_PART(Email_Masked, '@', 2) as EmailDomain,
        COUNT(*) as Count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as Percentage
    FROM customers_secure
    GROUP BY EmailDomain
    ORDER BY Count DESC
""").fetchdf()
print(result.to_string(index=False))

# Query 4: Age calculation
print("\n4. Customer Age Analysis")
print("-" * 80)
result = con.execute("""
    SELECT 
        CustomerName,
        DateOfBirth,
        DATE_DIFF('year', DateOfBirth::DATE, CURRENT_DATE) as Age,
        PlaceOfBirth
    FROM customers_secure
    ORDER BY Age DESC
""").fetchdf()
print(result.to_string(index=False))

# Query 5: Customers by birth location
print("\n5. Customers by Birth Location")
print("-" * 80)
result = con.execute("""
    SELECT 
        PlaceOfBirth,
        COUNT(*) as CustomerCount,
        STRING_AGG(CustomerName, ', ') as Customers
    FROM customers_secure
    GROUP BY PlaceOfBirth
    ORDER BY CustomerCount DESC
""").fetchdf()
print(result.to_string(index=False))

# Query 6: Company summary
print("\n6. Company Summary Statistics")
print("-" * 80)
result = con.execute("""
    SELECT 
        CompanyID,
        CompanyName,
        COUNT(*) as TotalCustomers,
        COUNT(DISTINCT Role) as UniqueRoles,
        COUNT(DISTINCT PlaceOfBirth) as UniqueBirthPlaces,
        MIN(DateOfBirth) as OldestCustomerDOB,
        MAX(DateOfBirth) as YoungestCustomerDOB
    FROM customers_secure
    GROUP BY CompanyID, CompanyName
""").fetchdf()
print(result.to_string(index=False))

# Close connection
con.close()

print("\n" + "=" * 80)
print("Query Examples Complete!")
print("=" * 80)
print("\nNote: In production, the 'Email_Original' column would be:")
print("  - Stored in a separate secure table with restricted access")
print("  - Encrypted at rest")
print("  - Accessible only to authorized personnel with audit logging")
print("=" * 80)
