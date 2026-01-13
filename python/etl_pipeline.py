"""
E-commerce Data Warehouse ETL Pipeline
Using pygramETL for dimension and fact table loading
"""

import os
import sys
import pandas as pd
import psycopg2
import pygrametl
from pygrametl.tables import CachedDimension, FactTable
from datetime import datetime
import json
from dotenv import load_dotenv

# Load environment variables from .env file with Windows encoding
load_dotenv(
    os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env'),
    encoding='cp1252'
)

# ====================
# 1. DATABASE CONNECTION
# ====================
def safe_print(message):
    """Print function that handles encoding issues on Windows"""
    try:
        print(message)
    except UnicodeEncodeError:
        # Fallback for Windows console with limited character set
        message = message.replace('✓', '[OK]').replace('', '[ERROR]')
        print(message)

def get_db_connection():
    """Create connection to PostgreSQL database using connection parameters"""
    try:
        # Get connection parameters with defaults
        db_params = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'database': os.getenv('DB_NAME', 'ecommerce_dwh'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': 'motdepasse123',  
            'port': os.getenv('DB_PORT', '5432')
        }
        
        # Create connection string
        conn_string = f"host={db_params['host']} dbname={db_params['database']} user={db_params['user']} password={db_params['password']} port={db_params['port']}"
        
        # Print connection info (without password) for debugging
        safe_print("\nConnecting to database:")
        safe_print(f"  Host: {db_params['host']}")
        safe_print(f"  Database: {db_params['database']}")
        safe_print(f"  User: {db_params['user']}")
        safe_print(f"  Port: {db_params['port']}")
        
        # Create connection using the connection string
        conn = psycopg2.connect(conn_string)
        
        # Test the connection
        with conn.cursor() as cur:
            cur.execute('SELECT 1')
            if cur.fetchone()[0] != 1:
                raise Exception("Test query failed")
                
        safe_print("[OK] Database connection successful!")
        return conn
        
    except Exception as e:
        safe_print(f"\n[ERROR] Database connection error: {str(e)}")
        safe_print("\nTroubleshooting steps:")
        safe_print("1. Make sure PostgreSQL is running")
        safe_print("2. Verify the database 'ecommerce_dwh' exists")
        safe_print("3. Check your .env file for correct database credentials")
        safe_print("4. Ensure PostgreSQL is configured to accept connections")
        safe_print("5. Check if the password is correct")
        sys.exit(1)

# ====================
# 2. DATA EXTRACTION
# ====================
def extract_sales_data():
    try:
        df = pd.read_csv('data/sales.csv')
        safe_print("Colonnes disponibles dans sales.csv: " + ", ".join(df.columns))
        if 'sale_id' in df.columns and 'order_id' not in df.columns:
            df = df.rename(columns={'sale_id': 'order_id'})
            safe_print("Remarque: La colonne 'sale_id' a ete renommee en 'order_id'")
        required_columns = ['order_id', 'product_id', 'customer_id', 'order_date', 
                          'quantity', 'unit_price', 'discount', 'region']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError("Colonnes requises manquantes dans les donnees de vente: " + ", ".join(missing_columns))
        df = df[required_columns].copy()
        safe_print(f"Donnees de vente extraites avec succes: {len(df)} enregistrements")
        return df
    except Exception as e:
        safe_print(f"[ERROR] Erreur lors de l'extraction des donnees de vente: {str(e)}")
        raise

def extract_customer_data():
    with open('data/customers.json', 'r') as f:
        return pd.json_normalize(json.load(f)).copy()

def extract_product_data():
    df = pd.read_csv('data/products.csv')
    required_columns = ['product_id', 'product_name', 'category', 'subcategory', 'unit_price']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns in products data: {', '.join(missing_columns)}")
    return df.copy()

def extract_region_data():
    try:
        # Read the regions data
        df = pd.read_excel('data/regions.xlsx')
        safe_print("Colonnes disponibles dans regions.xlsx: " + ", ".join(df.columns))
        
        # Standardize column names if needed
        if 'region' in df.columns and 'region_name' not in df.columns:
            df = df.rename(columns={'region': 'region_name'})
            safe_print("Remarque: La colonne 'region' a ete renommee en 'region_name'")
            
        # Verify required columns
        required_columns = ['region_name', 'country', 'continent']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError("Colonnes requises manquantes dans les donnees de region: " + 
                           ", ".join(missing_columns))
        
        # Ensure region names are properly formatted
        df['region_name'] = df['region_name'].str.strip()
        
        # Create region mapping with case-insensitive matching
        canonical_regions = {region.lower(): region for region in df['region_name'].unique()}
        
        # Add common aliases and variations
        region_aliases = {
            'europe central': 'Europe Central',
            'east': 'Europe East',
            'west': 'Europe West',
            'north': 'North America',
            'south': 'South America',
            'asia': 'Asia Pacific',
            'emea': 'Europe Middle East Africa',
            'europe': 'Europe Central',
            'america': 'North America',
            'apj': 'Asia Pacific',
            'north america': 'North America',
            'south america': 'South America',
            'europe west': 'Europe West',
            'europe east': 'Europe East',
            'europe central': 'Europe Central',
            'asia pacific': 'Asia Pacific',
            'europe middle east africa': 'Europe Middle East Africa'
        }
        
        # Combine mappings, giving priority to canonical names
        region_mapping = {**{k.lower(): v for k, v in region_aliases.items()}, **canonical_regions}
        
        # Store the mapping as a function attribute for later use
        extract_region_data.region_mapping = region_mapping
        
        safe_print(f"Donnees de region extraites avec succes: {len(df)} enregistrements")
        safe_print("Regions disponibles: " + ", ".join(df['region_name'].unique()))
        
        return df
        
    except Exception as e:
        safe_print(f"[ERROR] Erreur lors de l'extraction des donnees de region: {str(e)}")
        raise

def normalize_region_name(region_name, region_mapping):
    """Normalize region name using the provided mapping"""
    if not region_name or pd.isna(region_name):
        return None
        
    try:
        # Convert to string and clean
        region_str = str(region_name).strip().lower()
        
        # Try exact match first
        if region_str in region_mapping:
            return region_mapping[region_str]
            
        # Try partial matches
        for key, value in region_mapping.items():
            if key in region_str or region_str in key:
                return value
                
        # Try with title case
        title_case = region_str.title()
        if title_case in region_mapping.values():
            return title_case
            
        # If no match found, return the original string in title case
        print(f"Avertissement: Aucune correspondance trouvée pour la région: {region_name}")
        return region_str.title()
        
    except Exception as e:
        print(f"Erreur lors de la normalisation de la région '{region_name}': {str(e)}")
        return region_name

# ====================
# 3. DATA TRANSFORMATION
# ====================
def transform_sales_data(df):
    df = df.copy()
    df.loc[:, 'order_date'] = pd.to_datetime(df['order_date'])
    df.loc[:, 'total_amount'] = (df['quantity'] * df['unit_price']).round(2)
    df.loc[:, 'unit_price'] = df['unit_price'].round(2)
    safe_print(f"Cleaned sales data: {len(df)} valid records")
    return df

def transform_customer_data(df):
    # Create a copy to avoid SettingWithCopyWarning
    df = df.copy()
    # Remove duplicates based on customer_id
    df = df.drop_duplicates(subset=['customer_id'])
    safe_print(f"Cleaned customer data: {len(df)} unique customers")
    return df

def create_date_dimension(sales_df):
    dates = pd.DataFrame({'full_date': sales_df['order_date'].unique()})
    dates['full_date'] = pd.to_datetime(dates['full_date'])
    dates['day'] = dates['full_date'].dt.day
    dates['month'] = dates['full_date'].dt.month
    dates['year'] = dates['full_date'].dt.year
    dates['quarter'] = dates['full_date'].dt.quarter
    dates['day_of_week'] = dates['full_date'].dt.day_name()
    dates['is_weekend'] = dates['full_date'].dt.dayofweek.isin([5, 6])
    safe_print(f"Created {len(dates)} date records")
    return dates

# ====================
# 4. ETL PIPELINE
# ====================
def run_etl_pipeline():
    safe_print("\n" + "="*50)
    safe_print("STARTING ETL PIPELINE")
    safe_print("="*50 + "\n")

    conn = get_db_connection()
    dwh_conn = pygrametl.ConnectionWrapper(connection=conn)

    # Define dimensions
    dim_product = CachedDimension(
        name='dim_product', key='product_key',
        attributes=['product_id', 'product_name', 'category', 'subcategory', 'unit_price'],
        lookupatts=['product_id'], targetconnection=dwh_conn
    )
    dim_date = CachedDimension(
        name='dim_date', key='date_key',
        attributes=['full_date', 'day', 'month', 'year', 'quarter', 'day_of_week', 'is_weekend'],
        lookupatts=['full_date'], targetconnection=dwh_conn
    )
    dim_customer = CachedDimension(
        name='dim_customer', key='customer_key',
        attributes=['customer_id', 'customer_name', 'segment', 'country', 'city'],
        lookupatts=['customer_id'], targetconnection=dwh_conn
    )
    dim_region = CachedDimension(
        name='dim_region', key='region_key',
        attributes=['region_name', 'country', 'continent'],
        lookupatts=['region_name'], targetconnection=dwh_conn
    )

    # Define fact table
    fact_sales = FactTable(
        name='fact_sales',
        keyrefs=['product_key', 'date_key', 'customer_key', 'region_key'],
        measures=['quantity', 'unit_price', 'discount', 'total_amount'],
        targetconnection=dwh_conn
    )

    # Extraction
    sales_df = extract_sales_data()
    customer_df = extract_customer_data()
    product_df = extract_product_data()
    region_df = extract_region_data()

    # Transformation
    safe_print("\n" + "="*50)
    safe_print("TRANSFORMATION PHASE")
    safe_print("="*50 + "\n")
    sales_df = transform_sales_data(sales_df)
    customer_df = transform_customer_data(customer_df)
    date_df = create_date_dimension(sales_df)

    # Loading
    safe_print("\n" + "="*50)
    safe_print("LOADING PHASE")
    safe_print("="*50 + "\n")

    # Load products - only insert if not exists
    for _, row in product_df.iterrows():
        product_data = row.to_dict()
        if not dim_product.lookup({'product_id': product_data['product_id']}):
            dim_product.insert(product_data)
    safe_print(f"✓ Processed {len(product_df)} products")

    # Load dates - only insert if not exists
    for _, row in date_df.iterrows():
        date_data = row.to_dict()
        if not dim_date.lookup({'full_date': date_data['full_date']}):
            dim_date.insert(date_data)
    safe_print(f"✓ Processed {len(date_df)} dates")

    # Load customers - only insert if not exists
    for _, row in customer_df.iterrows():
        customer_data = row.to_dict()
        if not dim_customer.lookup({'customer_id': customer_data['customer_id']}):
            dim_customer.insert(customer_data)
    safe_print(f"✓ Processed {len(customer_df)} customers")

    # Load regions
    for _, row in region_df.iterrows():
        region_row = {'region_name': row['region_name'], 'country': row['country'], 'continent': row['continent']}
        if dim_region.lookup(region_row, {'region_name': 'region_name'}) is None:
            dim_region.insert(region_row)
    safe_print(f"✓ Loaded {len(region_df)} regions")

    # Fact loading
    valid_products = set(product_df['product_id'].unique())
    fact_count = 0
    skipped_records = 0

    for idx, row in sales_df.iterrows():
        try:
            if row['product_id'] not in valid_products:
                skipped_records += 1
                continue

            row['region'] = normalize_region_name(row['region'], extract_region_data.region_mapping)

            fact_row = {}
            fact_row['product_key'] = dim_product.lookup(row, {'product_id': 'product_id'})
            fact_row['date_key'] = dim_date.lookup(row, {'full_date': 'order_date'})
            fact_row['customer_key'] = dim_customer.lookup(row, {'customer_id': 'customer_id'})
            fact_row['region_key'] = dim_region.lookup({'region_name': row['region']}, {'region_name': 'region_name'})

            if None in fact_row.values():
                skipped_records += 1
                continue

            fact_row['quantity'] = row['quantity']
            fact_row['unit_price'] = row['unit_price']
            fact_row['discount'] = row['discount']
            fact_row['total_amount'] = row['total_amount']

            fact_sales.insert(fact_row)
            fact_count += 1
        except Exception as e:
            skipped_records += 1
            continue

    safe_print(f"✓ Loaded {fact_count} sales facts")
    if skipped_records > 0:
        safe_print(f"⚠️  Skipped {skipped_records} records due to missing references")

    dwh_conn.commit()
    dwh_conn.close()
    safe_print("\n" + "="*50)
    safe_print("ETL PIPELINE COMPLETED SUCCESSFULLY!")
    safe_print("="*50)

# ====================
# 5. MAIN EXECUTION
# ====================
if __name__ == "__main__":
    try:
        run_etl_pipeline()
    except Exception as e:
        safe_print(f"\n ERROR: {str(e)}")
        raise
