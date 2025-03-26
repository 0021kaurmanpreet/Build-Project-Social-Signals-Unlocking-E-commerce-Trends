import pandas as pd
import logging
from sqlalchemy import create_engine, DateTime
from sqlalchemy.exc import SQLAlchemyError

# Configure logging
logging.basicConfig(filename="C:/Users/kaur6/Downloads/BuildProject-ECommerce/transformation_log.log", level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# Database connection details
USERNAME = "root"
PASSWORD = "password"
HOST = "localhost"
DATABASE = "ecommerce"

# Create an SQLAlchemy engine for database connection
try:
    engine = create_engine(f"mysql+mysqlconnector://{USERNAME}:{PASSWORD}@{HOST}/{DATABASE}")
    logging.info("Database connection established.")
except SQLAlchemyError as e:
    logging.error(f"Database connection error: {e}")
    raise

def load_tables(tables, engine):
    """Load tables from the database into Pandas DataFrames."""
    try:
        logging.info(f"Loading tables: {tables}")
        return {table: pd.read_sql(f"SELECT * FROM {table}", con=engine) for table in tables}
    except SQLAlchemyError as e:
        logging.error(f"Error loading tables: {e}")
        return {}

def transform_payments(df):
    """Transform the payments table by aggregating payment information for each order."""
    try:
        df['OrderID'] = df['OrderID'].astype(str)
        df['PaymentSequential'] = df['PaymentSequential'].astype(int)
        df = df.groupby('OrderID').agg({
            'PaymentSequential': lambda x: ', '.join(map(str, sorted(x))),
            'PaymentType': lambda x: ', '.join(x.iloc[x.argsort()].tolist()),
            'PaymentInstallments': lambda x: ', '.join(map(str, x.iloc[x.argsort()])),
            'PaymentValue': 'sum'
        }).reset_index()
        df.rename(columns={'OrderID': 'PaymentID'}, inplace=True)
        df['PaymentType'] = df['PaymentType'].str.replace('_', ' ').str.title()
        logging.info("Payments table transformed successfully.")
        return df
    except Exception as e:
        logging.error(f"Error transforming payments: {e}")
        return pd.DataFrame()

def transform_feedbacks(df):
    """Transform the feedbacks table by formatting dates and modifying FeedbackID."""
    try:
        df['FeedbackID'] = df['FeedbackID'].astype(str)
        df['FeedbackScore'] = df['FeedbackScore'].astype(int)
        df['FeedbackID'] = df.groupby('FeedbackID').cumcount().astype(str) + "_" + df['FeedbackID']
        df['FeedbackFormSentDate'] = pd.to_datetime(df['FeedbackFormSentDate'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
        df['FeedbackAnswerDate'] = pd.to_datetime(df['FeedbackAnswerDate'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
        logging.info("Feedbacks table transformed successfully.")
        return df
    except Exception as e:
        logging.error(f"Error transforming feedbacks: {e}")
        return pd.DataFrame()

def transform_products(df):
    """Transform the products table."""
    try:
        df['ProductID'] = df['ProductID'].astype(str)
        df['ProductCategory'] = df['ProductCategory'].astype(str)
        df['ProductCategory'] = df['ProductCategory'].str.replace('_', ' ').str.title()
        logging.info("Products table transformed successfully.")
        return df
    except Exception as e:
        logging.error(f"Error transforming products: {e}")
        return pd.DataFrame()

def transform_sellers(df):
    """Transform the sellers table."""
    try:
        df['SellerID'] = df['SellerID'].astype(str)
        df['SellerZIPCode'] = df['SellerZIPCode'].astype(str)
        df['SellerCity'] = df['SellerCity'].str.title()
        df['SellerState'] = df['SellerState'].str.title()
        logging.info("Sellers table transformed successfully.")
        return df
    except Exception as e:
        logging.error(f"Error transforming sellers: {e}")
        return pd.DataFrame()

def transform_order_items(df):
    """Transform the order_items table."""
    try:
        if 'PickupLimitDate' in df.columns:
            df.drop(columns=['PickupLimitDate'], inplace=True)
        df['ProductSellerPair'] = df['ProductID'].astype(str) + '-' + df['SellerID'].astype(str)
        df = df.groupby(['OrderID', 'ProductSellerPair']).agg({
            'OrderItemID': lambda x: ', '.join(map(str, sorted(x))),
            'Price': 'sum'
        }).reset_index()
        df[['ProductID', 'SellerID']] = df['ProductSellerPair'].str.split('-', expand=True)
        df.drop(columns=['ProductSellerPair'], inplace=True)
        df['Quantity'] = df['OrderItemID'].apply(lambda x: len(x.split(', ')))
        logging.info("Order items table transformed successfully.")
        return df
    except Exception as e:
        logging.error(f"Error transforming order items: {e}")
        return pd.DataFrame()

def transform_users(df):
    """Transform the users table."""
    try:
        df['UserZIPCode'] = df['UserZIPCode'].astype(str)
        df['UserID'] = df['UserID'].astype(str)
        df['UserCity'] = df['UserCity'].str.title()
        df['UserState'] = df['UserState'].str.title()
        df = df.groupby('UserID').agg({
            'UserZIPCode': lambda x: ', '.join(sorted(x.unique())),
            'UserCity': lambda x: ', '.join(sorted(x.unique())),
            'UserState': lambda x: ', '.join(sorted(x.unique()))
        }).reset_index()
        logging.info("Users table transformed successfully.")
        return df
    except Exception as e:
        logging.error(f"Error transforming users: {e}")
        return pd.DataFrame()

def transform_orders(df, df_feedbacks):
    """Transform the orders table."""
    try:
        date_columns = ['OrderDate', 'OrderApprovedDate', 'PickupDate', 'DeliveredDate', 'EstimatedDeliveryDate']
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
        df = df.merge(df_feedbacks[['OrderID', 'FeedbackID']], on='OrderID', how='left')
        logging.info("Orders table transformed successfully.")
        return df
    except Exception as e:
        logging.error(f"Error transforming orders: {e}")
        return pd.DataFrame()

def save_transformed_tables(dataframes, engine):
    """Save transformed tables to the database."""
    try:
        date_columns = {
            'transformed_feedbacks': ['FeedbackFormSentDate', 'FeedbackAnswerDate'],
            'transformed_orders': ['OrderDate', 'OrderApprovedDate', 'PickupDate', 'DeliveredDate', 'EstimatedDeliveryDate'],
        }
        for table, df in dataframes.items():
            transformed_table_name = "transformed_" + table
            dtype_mapping = {col: DateTime() for col in date_columns.get(transformed_table_name, [])}
            df.to_sql(name=transformed_table_name, con=engine, if_exists='replace', index=False, dtype=dtype_mapping)
            logging.info(f"Saved {transformed_table_name} back to the database.")
    except SQLAlchemyError as e:
        logging.error(f"Error saving transformed tables: {e}")

# Main execution
try:
    tables = ["feedbacks", "orders", "order_items", "payments", "products", "sellers", "users"]
    dataframes = load_tables(tables, engine)
    logging.info("Loaded all tables.")

    dataframes['payments'] = transform_payments(dataframes['payments'])
    dataframes['feedbacks'] = transform_feedbacks(dataframes['feedbacks'])
    dataframes['products'] = transform_products(dataframes['products'])
    dataframes['sellers'] = transform_sellers(dataframes['sellers'])
    dataframes['order_items'] = transform_order_items(dataframes['order_items'])
    dataframes['users'] = transform_users(dataframes['users'])
    dataframes['orders'] = transform_orders(dataframes['orders'], dataframes['feedbacks'])

    save_transformed_tables(dataframes, engine)
    logging.info("All transformations completed successfully.")
except Exception as e:
    logging.error(f"Unexpected error in script execution: {e}")
