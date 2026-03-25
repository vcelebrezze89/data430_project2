CREATE TABLE IF NOT EXISTS clean_customers (
  customer_id INTEGER PRIMARY KEY,
  first_name TEXT,
  last_name TEXT,
  email TEXT,
  phone TEXT,
  address TEXT,
  city TEXT,
  state TEXT,
  zip_code TEXT,
  date_of_birth DATE,
  registration_date DATE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS clean_suppliers (
  supplier_id INTEGER PRIMARY KEY,
  company_name TEXT,
  contact_name TEXT,
  email TEXT,
  phone TEXT,
  address TEXT,
  city TEXT,
  state TEXT,
  country TEXT,
  industry TEXT,
  rating NUMERIC(2,1),
  contract_start_date DATE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS clean_products (
  product_id INTEGER PRIMARY KEY,
  product_name TEXT,
  category TEXT,
  price NUMERIC(10,2),
  cost NUMERIC(10,2),
  sku TEXT,
  weight_kg NUMERIC(10,3),
  stock_quantity INTEGER,
  supplier_id INTEGER REFERENCES clean_suppliers(supplier_id),
  created_date DATE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS clean_daily_sales (
  sale_id INTEGER PRIMARY KEY,
  sale_date DATE,
  customer_id INTEGER REFERENCES clean_customers(customer_id),
  product_id INTEGER REFERENCES clean_products(product_id),
  quantity INTEGER,
  unit_price NUMERIC(10,2),
  total_amount NUMERIC(10,2),
  discount_pct NUMERIC(5,2),
  payment_method TEXT,
  channel TEXT,
  region TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS customer_lifetime_value (
  customer_id INTEGER PRIMARY KEY,
  total_orders INTEGER,
  lifetime_value NUMERIC(12,2),
  avg_order_value NUMERIC(12,2),
  last_order_date DATE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sales_trend (
  sale_date DATE PRIMARY KEY,
  total_transactions INTEGER,
  total_sales NUMERIC(12,2),
  total_discount NUMERIC(12,2),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS category_performance (
  category TEXT PRIMARY KEY,
  total_products INTEGER,
  total_units_sold INTEGER,
  total_revenue NUMERIC(12,2),
  avg_price NUMERIC(10,2),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS supplier_performance (
  supplier_id INTEGER PRIMARY KEY REFERENCES clean_suppliers(supplier_id),
  company_name TEXT,
  total_products INTEGER,
  avg_product_price NUMERIC(10,2),
  total_stock INTEGER,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS etl_audit (
  run_id SERIAL PRIMARY KEY,
  dag_name TEXT,
  task_name TEXT,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  status TEXT,
  records_processed INTEGER,
  message TEXT
);