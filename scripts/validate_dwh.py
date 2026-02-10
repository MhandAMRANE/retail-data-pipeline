import os
import psycopg2
import sys
import traceback

def check_table_not_empty(cursor, table_name):
    cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
    count = cursor.fetchone()[0]
    print(f"Table {table_name}: {count} rows found.")
    return count > 0

def main():
    db_user = os.getenv("DB_USER", "postgres")
    db_pass = os.getenv("DB_PASS", "postgres")
    db_host = os.getenv("DB_HOST", "postgres-server")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "retail_db")

    tables_to_check = [
        "dim_product",
        "dim_date",
        "dim_country",
        "fact_sales"
    ]

    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_pass,
            host=db_host,
            port=db_port
        )
        cur = conn.cursor()

        print("Starting Data Warehouse Validation...")
        
        all_ok = True
        for table in tables_to_check:
            if not check_table_not_empty(cur, table):
                print(f"FAILED: Table {table} is empty!")
                all_ok = False
        
        cur.close()
        conn.close()

        if not all_ok:
            print("Validation FAILED: One or more tables are empty.")
            sys.exit(1)
            
        print("Validation SUCCESS: All DWH tables are populated. Ready for Power BI refresh!")
        
    except Exception as e:
        print("Error during validation:")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
