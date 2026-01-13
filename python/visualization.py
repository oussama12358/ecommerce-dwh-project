"""
E-commerce Business Intelligence Dashboard
Using Matplotlib for data visualization
"""

import os
import matplotlib.pyplot as plt
import pandas as pd
import psycopg2
import numpy as np
from matplotlib.gridspec import GridSpec
import seaborn as sns
from dotenv import load_dotenv

# Load environment variables with Windows encoding
load_dotenv(
    os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env'),
    encoding='cp1252'
)

# Set style
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

# ====================
# DATABASE CONNECTION
# ====================
def get_db_connection():
    """Create connection to PostgreSQL DWH using environment variables"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            database=os.getenv('DB_NAME', 'ecommerce_dwh'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', ''),
            port=os.getenv('DB_PORT', '5432')
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {str(e)}")
        raise

# ====================
# DATA EXTRACTION FOR VISUALIZATION
# ====================
def get_revenue_by_category(conn):
    """Get revenue grouped by product category"""
    query = """
        SELECT 
            p.category,
            SUM(f.total_amount) as revenue,
            COUNT(f.sale_key) as order_count
        FROM fact_sales f
        JOIN dim_product p ON f.product_key = p.product_key
        GROUP BY p.category
        ORDER BY revenue DESC
    """
    return pd.read_sql(query, conn)

def get_monthly_sales_trend(conn):
    """Get monthly sales trends"""
    query = """
        SELECT 
            d.year,
            d.month,
            SUM(f.total_amount) as revenue,
            COUNT(DISTINCT f.customer_key) as unique_customers
        FROM fact_sales f
        JOIN dim_date d ON f.date_key = d.date_key
        GROUP BY d.year, d.month
        ORDER BY d.year, d.month
    """
    df = pd.read_sql(query, conn)
    df['year_month'] = df['year'].astype(str) + '-' + df['month'].astype(str).str.zfill(2)
    return df

def get_sales_by_region(conn):
    """Get sales distribution by region"""
    query = """
        SELECT 
            r.region_name,
            SUM(f.total_amount) as revenue,
            COUNT(f.sale_key) as orders
        FROM fact_sales f
        JOIN dim_region r ON f.region_key = r.region_key
        GROUP BY r.region_name
        ORDER BY revenue DESC
    """
    return pd.read_sql(query, conn)

def get_top_products(conn, limit=10):
    """Get top N products by revenue"""
    query = f"""
        SELECT 
            p.product_name,
            SUM(f.total_amount) as revenue,
            SUM(f.quantity) as units_sold
        FROM fact_sales f
        JOIN dim_product p ON f.product_key = p.product_key
        GROUP BY p.product_name
        ORDER BY revenue DESC
        LIMIT {limit}
    """
    return pd.read_sql(query, conn)

def get_customer_segments(conn):
    """Get customer segment analysis"""
    query = """
        SELECT 
            c.segment,
            COUNT(DISTINCT c.customer_key) as customer_count,
            SUM(f.total_amount) as total_revenue
        FROM fact_sales f
        JOIN dim_customer c ON f.customer_key = c.customer_key
        GROUP BY c.segment
        ORDER BY total_revenue DESC
    """
    return pd.read_sql(query, conn)

def get_quarterly_performance(conn):
    """Get quarterly sales performance"""
    query = """
        SELECT 
            d.year,
            d.quarter,
            SUM(f.total_amount) as revenue
        FROM fact_sales f
        JOIN dim_date d ON f.date_key = d.date_key
        GROUP BY d.year, d.quarter
        ORDER BY d.year, d.quarter
    """
    df = pd.read_sql(query, conn)
    df['year_quarter'] = df['year'].astype(str) + '-Q' + df['quarter'].astype(str)
    return df

# ====================
# VISUALIZATION FUNCTIONS
# ====================
def create_comprehensive_dashboard():
    """Create a comprehensive BI dashboard with multiple visualizations"""
    
    # Connect to database
    conn = get_db_connection()
    
    # Fetch all data
    print("Fetching data from data warehouse...")
    category_data = get_revenue_by_category(conn)
    monthly_data = get_monthly_sales_trend(conn)
    region_data = get_sales_by_region(conn)
    product_data = get_top_products(conn, 10)
    segment_data = get_customer_segments(conn)
    quarterly_data = get_quarterly_performance(conn)
    
    # Close connection
    conn.close()
    print("Data fetched successfully!")
    
    # Create figure with custom layout
    fig = plt.figure(figsize=(20, 12))
    gs = GridSpec(3, 3, figure=fig, hspace=0.3, wspace=0.3)
    
    # Main title
    fig.suptitle('E-COMMERCE BUSINESS INTELLIGENCE DASHBOARD', 
                 fontsize=20, fontweight='bold', y=0.98)
    
    # ====================
    # 1. REVENUE BY CATEGORY (Bar Chart)
    # ====================
    ax1 = fig.add_subplot(gs[0, :2])
    colors = plt.cm.viridis(np.linspace(0, 1, len(category_data)))
    bars = ax1.bar(category_data['category'], category_data['revenue'], 
                   color=colors, edgecolor='black', linewidth=1.5)
    ax1.set_title('Revenue by Product Category', fontsize=14, fontweight='bold', pad=10)
    ax1.set_xlabel('Category', fontsize=11)
    ax1.set_ylabel('Revenue ($)', fontsize=11)
    ax1.tick_params(axis='x', rotation=45)
    ax1.grid(axis='y', alpha=0.3)
    
    
    # Add value labels on bars
    for bar in bars:
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                f'${height:,.0f}',
                ha='center', va='bottom', fontsize=9)
    
    # ====================
    # 2. MONTHLY SALES TREND (Line Chart)
    # ====================
    ax2 = fig.add_subplot(gs[0, 2])
    ax2.plot(monthly_data['year_month'], monthly_data['revenue'], 
            marker='o', linewidth=2.5, markersize=6, color='#2E86AB')
    ax2.set_title('Monthly Sales Trend', fontsize=14, fontweight='bold', pad=10)
    ax2.set_xlabel('Month', fontsize=11)
    ax2.set_ylabel('Revenue ($)', fontsize=11)
    ax2.tick_params(axis='x', rotation=90)
    ax2.grid(True, alpha=0.3)
    ax2.fill_between(monthly_data['year_month'], monthly_data['revenue'], 
                     alpha=0.3, color='#2E86AB')
    
    # ====================
    # 3. SALES BY REGION (Pie Chart)
    # ====================
    ax3 = fig.add_subplot(gs[1, 0])
    colors_pie = plt.cm.Set3(np.linspace(0, 1, len(region_data)))
    wedges, texts, autotexts = ax3.pie(region_data['revenue'], 
                                        labels=region_data['region_name'],
                                        autopct='%1.1f%%',
                                        startangle=90,
                                        colors=colors_pie,
                                        explode=[0.05] * len(region_data))
    ax3.set_title('Sales Distribution by Region', fontsize=14, fontweight='bold', pad=10)
    
    # Improve text visibility
    for autotext in autotexts:
        autotext.set_color('white')
        autotext.set_fontsize(10)
        autotext.set_fontweight('bold')
    
    # ====================
    # 4. TOP 10 PRODUCTS (Horizontal Bar Chart)
    # ====================
    ax4 = fig.add_subplot(gs[1, 1:])
    colors_products = plt.cm.plasma(np.linspace(0, 1, len(product_data)))
    y_pos = np.arange(len(product_data))
    bars = ax4.barh(y_pos, product_data['revenue'], color=colors_products, 
                    edgecolor='black', linewidth=1)
    ax4.set_yticks(y_pos)
    ax4.set_yticklabels(product_data['product_name'], fontsize=9)
    ax4.set_xlabel('Revenue ($)', fontsize=11)
    ax4.set_title('Top 10 Products by Revenue', fontsize=14, fontweight='bold', pad=10)
    ax4.invert_yaxis()
    ax4.grid(axis='x', alpha=0.3)
    
    # Add value labels
    for i, (bar, revenue) in enumerate(zip(bars, product_data['revenue'])):
        ax4.text(revenue, i, f' ${revenue:,.0f}', 
                va='center', fontsize=9, fontweight='bold')
    
    # ====================
    # 5. CUSTOMER SEGMENTS (Donut Chart)
    # ====================
    ax5 = fig.add_subplot(gs[2, 0])
    colors_segment = plt.cm.Pastel1(np.linspace(0, 1, len(segment_data)))
    wedges, texts, autotexts = ax5.pie(segment_data['total_revenue'],
                                        labels=segment_data['segment'],
                                        autopct='%1.1f%%',
                                        startangle=45,
                                        colors=colors_segment,
                                        wedgeprops=dict(width=0.5, edgecolor='white'))
    ax5.set_title('Revenue by Customer Segment', fontsize=14, fontweight='bold', pad=10)
    
    for autotext in autotexts:
        autotext.set_color('black')
        autotext.set_fontsize(10)
        autotext.set_fontweight('bold')
    
    # ====================
    # 6. QUARTERLY PERFORMANCE (Grouped Bar Chart)
    # ====================
    ax6 = fig.add_subplot(gs[2, 1:])
    quarters = quarterly_data['year_quarter']
    revenues = quarterly_data['revenue']
    colors_quarter = plt.cm.coolwarm(np.linspace(0, 1, len(quarterly_data)))
    bars = ax6.bar(quarters, revenues, color=colors_quarter, 
                   edgecolor='black', linewidth=1.5)
    ax6.set_title('Quarterly Sales Performance', fontsize=14, fontweight='bold', pad=10)
    ax6.set_xlabel('Quarter', fontsize=11)
    ax6.set_ylabel('Revenue ($)', fontsize=11)
    ax6.tick_params(axis='x', rotation=45)
    ax6.grid(axis='y', alpha=0.3)
    
    # Add value labels
    for bar in bars:
        height = bar.get_height()
        ax6.text(bar.get_x() + bar.get_width()/2., height,
                f'${height:,.0f}',
                ha='center', va='bottom', fontsize=9, rotation=0)
    
    # Add summary statistics
    total_revenue = monthly_data['revenue'].sum()
    avg_order = total_revenue / len(monthly_data)
    
    fig.text(0.5, 0.02, 
             f'Total Revenue: ${total_revenue:,.2f} | Average Monthly Revenue: ${avg_order:,.2f}',
             ha='center', fontsize=12, fontweight='bold',
             bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.5))
    
    # Save the dashboard
    plt.savefig('ecommerce_dashboard.png', dpi=300, bbox_inches='tight')
    print("\n✓ Dashboard saved as 'ecommerce_dashboard.png'")
    
    # Show the dashboard
    plt.show()

# ====================
# INDIVIDUAL VISUALIZATION FUNCTIONS
# ====================
def create_simple_kpi_dashboard():
    """Create a simple 2x2 KPI dashboard"""
    
    conn = get_db_connection()
    
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle('E-commerce Key Performance Indicators', 
                 fontsize=16, fontweight='bold')
    
    # Chart 1: Revenue by Category
    df1 = get_revenue_by_category(conn)
    axes[0, 0].bar(df1['category'], df1['revenue'], color='steelblue')
    axes[0, 0].set_title('Revenue by Category', fontweight='bold')
    axes[0, 0].set_ylabel('Revenue ($)')
    axes[0, 0].tick_params(axis='x', rotation=45)
    axes[0, 0].grid(axis='y', alpha=0.3)
    
    # Chart 2: Monthly Trend
    df2 = get_monthly_sales_trend(conn)
    axes[0, 1].plot(df2['year_month'], df2['revenue'], 
                   marker='o', linewidth=2, color='green')
    axes[0, 1].set_title('Monthly Sales Trend', fontweight='bold')
    axes[0, 1].set_ylabel('Revenue ($)')
    axes[0, 1].tick_params(axis='x', rotation=90)
    axes[0, 1].grid(True, alpha=0.3)
    
    # Chart 3: Regional Distribution
    df3 = get_sales_by_region(conn)
    axes[1, 0].pie(df3['revenue'], labels=df3['region_name'], 
                  autopct='%1.1f%%', startangle=90)
    axes[1, 0].set_title('Sales by Region', fontweight='bold')
    
    # Chart 4: Top Products
    df4 = get_top_products(conn, 5)
    axes[1, 1].barh(df4['product_name'], df4['revenue'], color='coral')
    axes[1, 1].set_title('Top 5 Products', fontweight='bold')
    axes[1, 1].set_xlabel('Revenue ($)')
    axes[1, 1].invert_yaxis()
    
    conn.close()
    
    plt.tight_layout()
    plt.savefig('kpi_dashboard_simple.png', dpi=300, bbox_inches='tight')
    print("✓ Simple dashboard saved as 'kpi_dashboard_simple.png'")
    plt.show()

# ====================
# MAIN EXECUTION
# ====================
if __name__ == "__main__":
    print("="*60)
    print("E-COMMERCE BUSINESS INTELLIGENCE DASHBOARD")
    print("="*60)
    
    try:
        # Create comprehensive dashboard
        create_comprehensive_dashboard()
        
        # Optionally create simple dashboard
        # create_simple_kpi_dashboard()
        
    except Exception as e:
        print(f"\n ERROR: {str(e)}")
        raise