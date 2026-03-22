"""
Run database migration
Creates the distributed_clustering database and all tables
"""
import mysql.connector
import os
import sys
from dotenv import load_dotenv

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
load_dotenv(os.path.join(project_root, '.env'))


def run_migration():
    """Execute SQL migration file"""
    
    script_dir = os.path.dirname(__file__)
    sql_file = os.path.join(script_dir, 'schema_distributed.sql')
    
    with open(sql_file, 'r', encoding='utf-8') as f:
        sql_script = f.read()
    
    print("Connecting to MySQL server...")
    
    try:
        conn = mysql.connector.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            port=int(os.getenv('DB_PORT', 3306)),
            user=os.getenv('DB_USER', 'root'),
            password=os.getenv('DB_PASSWORD', 'Bobita2@1'),
            charset='utf8mb4',
            collation='utf8mb4_unicode_ci'
        )
    except mysql.connector.Error as e:
        print(f"[ERROR] Cannot connect to MySQL: {e}")
        print()
        print("Make sure MySQL is running:")
        print("  Windows: net start MySQL80")
        print("  Linux:   sudo systemctl start mysql")
        sys.exit(1)
    
    cursor = conn.cursor()
    
    statements = [s.strip() for s in sql_script.split(';') if s.strip()]
    
    success_count = 0
    error_count = 0
    
    for statement in statements:
        lines = [l for l in statement.split('\n') if not l.strip().startswith('--')]
        clean = '\n'.join(lines).strip()
        if not clean:
            continue
        
        try:
            cursor.execute(statement)
            
            if cursor.with_rows:
                rows = cursor.fetchall()
                for row in rows:
                    print(f"  {row}")
            
            conn.commit()
            success_count += 1
            
        except mysql.connector.Error as e:
            error_count += 1
            short = statement[:60].replace('\n', ' ')
            print(f"  [WARN] {short}... -> {e}")
    
    cursor.close()
    conn.close()
    
    print()
    print(f"Migration completed: {success_count} statements OK, {error_count} warnings")
    print("[OK] Database 'distributed_clustering' is ready!")


if __name__ == '__main__':
    run_migration()