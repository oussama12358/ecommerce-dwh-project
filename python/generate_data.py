"""
Generate sample e-commerce data for the DWH project
Creates CSV, JSON, and Excel files
"""

import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
import random

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

# ====================
# CONFIGURATION
# ====================
NUM_CUSTOMERS = 1000
NUM_PRODUCTS = 200
NUM_SALES = 10000
NUM_REGIONS = 8

# ====================
# 1. GENERATE CUSTOMERS (JSON)
# ====================
def generate_customers():
    """Generate customer data"""
    print("Generating customer data...")
    
    segments = ['Consumer', 'Corporate', 'Home Office']
    countries = ['USA', 'Canada', 'UK', 'Germany', 'France', 'Spain', 'Italy', 'Australia']
    cities = {
        'USA': ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'],
        'Canada': ['Toronto', 'Vancouver', 'Montreal', 'Calgary'],
        'UK': ['London', 'Manchester', 'Birmingham', 'Leeds'],
        'Germany': ['Berlin', 'Munich', 'Hamburg', 'Frankfurt'],
        'France': ['Paris', 'Lyon', 'Marseille', 'Toulouse'],
        'Spain': ['Madrid', 'Barcelona', 'Valencia', 'Seville'],
        'Italy': ['Rome', 'Milan', 'Naples', 'Turin'],
        'Australia': ['Sydney', 'Melbourne', 'Brisbane', 'Perth']
    }
    
    customers = []
    for i in range(NUM_CUSTOMERS):
        country = random.choice(countries)
        customer = {
            'customer_id': f'CUST{i+1:05d}',
            'customer_name': f'Customer {i+1}',
            'segment': random.choice(segments),
            'country': country,
            'city': random.choice(cities[country])
        }
        customers.append(customer)
    
    # Save as JSON
    with open('data/customers.json', 'w') as f:
        json.dump(customers, f, indent=2)
    
    print(f"✓ Generated {len(customers)} customers (saved to customers.json)")
    return customers

# ====================
# 2. GENERATE PRODUCTS (CSV)
# ====================
def generate_products():
    """Generate product catalog"""
    print("Generating product data...")
    
    categories = {
        'Technology': ['Smartphones', 'Laptops', 'Tablets', 'Accessories'],
        'Furniture': ['Chairs', 'Tables', 'Bookcases', 'Furnishings'],
        'Office Supplies': ['Paper', 'Binders', 'Art', 'Storage']
    }
    
    products = []
    product_id = 1
    
    for category, subcategories in categories.items():
        for subcategory in subcategories:
            num_products = random.randint(10, 20)
            for i in range(num_products):
                product = {
                    'product_id': f'PROD{product_id:05d}',
                    'product_name': f'{subcategory} Product {i+1}',
                    'category': category,
                    'subcategory': subcategory,
                    'unit_price': round(random.uniform(10, 2000), 2)
                }
                products.append(product)
                product_id += 1
    
    # Create DataFrame and save as CSV
    df = pd.DataFrame(products)
    df.to_csv('data/products.csv', index=False)
    
    print(f"✓ Generated {len(products)} products (saved to products.csv)")
    return df

# ====================
# 3. GENERATE REGIONS (Excel)
# ====================
def generate_regions():
    """Generate region data"""
    print("Generating region data...")
    
    regions = [
        {'region_name': 'North America', 'country': 'USA', 'continent': 'North America'},
        {'region_name': 'North America', 'country': 'Canada', 'continent': 'North America'},
        {'region_name': 'Europe West', 'country': 'UK', 'continent': 'Europe'},
        {'region_name': 'Europe West', 'country': 'France', 'continent': 'Europe'},
        {'region_name': 'Europe Central', 'country': 'Germany', 'continent': 'Europe'},
        {'region_name': 'Europe South', 'country': 'Spain', 'continent': 'Europe'},
        {'region_name': 'Europe South', 'country': 'Italy', 'continent': 'Europe'},
        {'region_name': 'Asia Pacific', 'country': 'Australia', 'continent': 'Oceania'}
    ]
    
    df = pd.DataFrame(regions)
    df.to_excel('data/regions.xlsx', index=False)
    
    print(f"✓ Generated {len(regions)} regions (saved to regions.xlsx)")
    return df

# ====================
# 4. GENERATE SALES TRANSACTIONS (CSV)
# ====================
def generate_sales(customers, products_df, regions_df):
    """Generate sales transaction data"""
    print("Generating sales transactions...")
    
    # Date range: last 2 years
    start_date = datetime.now() - timedelta(days=730)
    end_date = datetime.now()
    
    sales = []
    
    for i in range(NUM_SALES):
        # Random date in range
        days_diff = (end_date - start_date).days
        random_days = random.randint(0, days_diff)
        order_date = start_date + timedelta(days=random_days)
        
        # Random customer
        customer = random.choice(customers)
        
        # Random product
        product = products_df.sample(n=1).iloc[0]
        
        # Find matching region for customer's country
        region_row = regions_df[regions_df['country'] == customer['country']]
        if len(region_row) > 0:
            region = region_row.iloc[0]['region_name']
        else:
            region = 'Other'
        
        # Generate transaction details
        quantity = random.randint(1, 10)
        discount = round(random.choice([0, 0, 0, 0.05, 0.1, 0.15, 0.2]), 2)
        
        sale = {
            'sale_id': f'SALE{i+1:07d}',
            'order_date': order_date.strftime('%Y-%m-%d'),
            'customer_id': customer['customer_id'],
            'product_id': product['product_id'],
            'region': region,
            'quantity': quantity,
            'unit_price': product['unit_price'],
            'discount': discount
        }
        sales.append(sale)
        
        if (i + 1) % 1000 == 0:
            print(f"  Generated {i+1} sales transactions...")
    
    # Create DataFrame and save as CSV
    df = pd.DataFrame(sales)
    df.to_csv('data/sales.csv', index=False)
    
    print(f"✓ Generated {len(sales)} sales transactions (saved to sales.csv)")
    return df

# ====================
# 5. DATA QUALITY ISSUES (Realistic)
# ====================
def introduce_data_quality_issues():
    """Introduce realistic data quality issues"""
    print("\nIntroducing realistic data quality issues...")
    
    # Add some nulls to sales data
    sales_df = pd.read_csv('data/sales.csv')
    
    # Randomly set 2% of discounts to null
    null_indices = np.random.choice(sales_df.index, size=int(len(sales_df) * 0.02), replace=False)
    sales_df.loc[null_indices, 'discount'] = np.nan
    
    sales_df.to_csv('data/sales.csv', index=False)
    print("✓ Added missing values to demonstrate data cleaning")

# ====================
# 6. GENERATE DATA SUMMARY
# ====================
def generate_summary():
    """Print summary of generated data"""
    print("\n" + "="*60)
    print("DATA GENERATION SUMMARY")
    print("="*60)
    
    # Load data
    customers_count = len(json.load(open('data/customers.json')))
    products_df = pd.read_csv('data/products.csv')
    sales_df = pd.read_csv('data/sales.csv')
    regions_df = pd.read_excel('data/regions.xlsx')
    
    print(f"\n Generated Files:")
    print(f"  • customers.json: {customers_count} customers")
    print(f"  • products.csv: {len(products_df)} products")
    print(f"  • regions.xlsx: {len(regions_df)} regions")
    print(f"  • sales.csv: {len(sales_df)} transactions")
    
    print(f"\n Sales Summary:")
    total_revenue = (sales_df['quantity'] * sales_df['unit_price'] * 
                    (1 - sales_df['discount'].fillna(0))).sum()
    print(f"  • Total Revenue: ${total_revenue:,.2f}")
    print(f"  • Date Range: {sales_df['order_date'].min()} to {sales_df['order_date'].max()}")
    print(f"  • Average Order Value: ${total_revenue/len(sales_df):,.2f}")
    
    print(f"\n Product Categories:")
    for category in products_df['category'].unique():
        count = len(products_df[products_df['category'] == category])
        print(f"  • {category}: {count} products")
    
    print(f"\n Customer Segments:")
    customers = json.load(open('data/customers.json'))
    segments = {}
    for c in customers:
        seg = c['segment']
        segments[seg] = segments.get(seg, 0) + 1
    for seg, count in segments.items():
        print(f"  • {seg}: {count} customers")
    
    print("\n" + "="*60)
    print("✓ All data generated successfully!")
    print("="*60)

# ====================
# MAIN EXECUTION
# ====================
if __name__ == "__main__":
    print("="*60)
    print("E-COMMERCE DATA WAREHOUSE - DATA GENERATION")
    print("="*60 + "\n")
    
    # Create data directory
    import os
    os.makedirs('data', exist_ok=True)
    
    try:
        # Generate all data
        customers = generate_customers()
        products_df = generate_products()
        regions_df = generate_regions()
        sales_df = generate_sales(customers, products_df, regions_df)
        
        # Introduce realistic issues for ETL demonstration
        introduce_data_quality_issues()
        
        # Print summary
        generate_summary()
        
    except Exception as e:
        print(f"\n ERROR: {str(e)}")
        raise