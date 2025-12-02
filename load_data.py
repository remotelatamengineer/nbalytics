import os
import tarfile
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = os.getenv("DB_NAME", "nbalytics_bronze")
DB_PORT = os.getenv("DB_PORT", "3306")

# Create SQLAlchemy engine
# Using pymysql as the driver for MariaDB compatibility
connection_string = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_string)

def load_datasets(datasets_dir="datasets"):
    """
    Iterates through .tar.xz files in the datasets directory,
    extracts CSVs, and loads them into the database.
    """
    if not os.path.exists(datasets_dir):
        print(f"Directory '{datasets_dir}' not found.")
        return

    files = [f for f in os.listdir(datasets_dir) if f.endswith(".tar.xz")]
    
    if not files:
        print("No .tar.xz files found in the datasets directory.")
        return

    print(f"Found {len(files)} datasets to process.")

    for filename in files:
        file_path = os.path.join(datasets_dir, filename)
        table_name = filename.replace(".tar.xz", "").replace(".", "_")
        
        print(f"Processing {filename} -> Table: {table_name}...")

        try:
            with tarfile.open(file_path, "r:xz") as tar:
                # Find the CSV file inside the archive
                csv_member = None
                for member in tar.getmembers():
                    if member.name.endswith(".csv"):
                        csv_member = member
                        break
                
                if csv_member:
                    print(f"  Extracting {csv_member.name}...")
                    f = tar.extractfile(csv_member)
                    
                    # Load into Pandas DataFrame
                    # using chunksize to handle large files if necessary, 
                    # but for simplicity loading all at once first. 
                    # If memory issues arise, we can switch to chunking.
                    df = pd.read_csv(f)
                    
                    print(f"  Loading {len(df)} rows into database...")
                    df.to_sql(table_name, engine, if_exists="replace", index=False)
                    print(f"  Successfully loaded {table_name}.")
                else:
                    print(f"  No CSV file found in {filename}.")

        except Exception as e:
            print(f"  Error processing {filename}: {e}")

if __name__ == "__main__":
    print("Starting data loading process...")
    try:
        # Test connection
        with engine.connect() as conn:
            print("Successfully connected to the database.")
        
        load_datasets()
        print("Data loading complete.")
    except Exception as e:
        print(f"Database connection failed: {e}")
        print("Please check your .env file and ensure the database exists.")
