import duckdb

conn = duckdb.connect('data/processed/cybersec_health.duckdb')

print("DuckDB SQL Interface - Type 'exit' to quit")
print("Available tables: customer_health_scores, stg_customers, stg_security_incidents")
print("-" * 60)

while True:
    query = input("SQL> ")
    if query.lower() in ['exit', 'quit']:
        break
    
    try:
        result = conn.execute(query).fetchall()
        if result:
            for row in result:
                print(row)
        else:
            print("Query executed successfully (no results)")
    except Exception as e:
        print(f"Error: {e}")

conn.close()