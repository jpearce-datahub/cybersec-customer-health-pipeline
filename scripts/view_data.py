import duckdb

conn = duckdb.connect('data/processed/cybersec_health.duckdb')

# Show tables
print("=== TABLES ===")
tables = conn.execute("SHOW TABLES").fetchall()
for table in tables:
    print(table[0])

print("\n=== CUSTOMER HEALTH SCORES (First 10) ===")
result = conn.execute("SELECT * FROM customer_health_scores LIMIT 10").fetchall()
for row in result:
    print(row)

print("\n=== RISK SUMMARY ===")
risk_summary = conn.execute("""
    SELECT risk_level, COUNT(*) as count, AVG(mrr) as avg_mrr
    FROM customer_health_scores 
    GROUP BY risk_level
""").fetchall()
for row in risk_summary:
    print(f"{row[0]}: {row[1]} customers, Avg MRR: ${row[2]:.2f}")

conn.close()