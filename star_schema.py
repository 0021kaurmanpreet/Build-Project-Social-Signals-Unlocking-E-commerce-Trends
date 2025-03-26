import subprocess  # Importing subprocess to run external scripts
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import logging
import config

# Configure logging
logging.basicConfig(filename="C:/Users/kaur6/Downloads/BuildProject-ECommerce/Ecommerce/transformation_log.log", level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# Function to call the existing transformation script
def run_transformation_script():
    """
    Runs the existing transformation script using subprocess.
    Ensure that the script 'transforming_tables.py' exists in the specified location.
    The script will be executed as a separate process.
    """
    subprocess.run(["python", "C:\\Users\\kaur6\\Downloads\\BuildProject-ECommerce\\Ecommerce\\transforming_tables.py"], check=True)

# Database connection details
USERNAME = config.USERNAME       # Username for the database
PASSWORD = config.PASSWORD  # Password for the database
HOST = config.HOST       # Database host (localhost for local development)
DATABASE = config.DATABASE   # Name of the database being accessed

# Function to create a user dimension table with a primary key
def create_dim_users(engine):
    """
    This function creates a dimension table called 'dim_users' with a primary key on the 'UserID' column.
    It first drops any existing 'dim_users' table, then creates a new table by selecting relevant 
    columns from the 'transformed_users' table. 
    """
    with engine.begin() as connection:  # Ensures auto-commit and transaction management
        # Drop the 'fact_order_items' table if it exists (though it's not part of the 'dim_users' creation, its for for cleanup)
        connection.execute(text("DROP TABLE IF EXISTS fact_order_items;"))
        
        # Drop the existing 'dim_users' table if it exists
        connection.execute(text(""" 
            DROP TABLE IF EXISTS dim_users;
        """))

        # Create the 'dim_users' table by selecting columns from the 'transformed_users' table
        # UserID, UserZIPCode, UserCity, and UserState are selected from 'transformed_users'
        connection.execute(text(""" 
            CREATE TABLE dim_users 
            AS 
            SELECT UserID, UserZIPCode, UserCity, UserState 
            FROM transformed_users;
        """))

        # Modify the UserID column to VARCHAR(50), ensuring it is of the correct type
        connection.execute(text(""" 
            ALTER TABLE dim_users MODIFY COLUMN UserID VARCHAR(50);
        """))

        # Add a primary key constraint to the 'UserID' column
        connection.execute(text(""" 
            ALTER TABLE dim_users ADD PRIMARY KEY (UserID);
        """))

        # Print confirmation message
        print("Dimension Table 'dim_users' created.")


# Function to create a feedbacks dimension table with a primary key
def create_dim_feedbacks(engine):
    """
    This function creates a dimension table called 'dim_feedbacks' with a primary key on the 'FeedbackID' column.
    It first drops any existing 'dim_feedbacks' table, then creates a new table by selecting relevant 
    columns from the 'transformed_feedbacks' table.
    """
    with engine.begin() as connection:  # Ensures auto-commit and transaction management
        # Drop the existing 'dim_feedbacks' table if it exists to avoid conflicts
        connection.execute(text(""" 
            DROP TABLE IF EXISTS dim_feedbacks;
        """))

        # Create the 'dim_feedbacks' table by selecting columns from the 'transformed_feedbacks' table
        # The selected columns include FeedbackID, FeedbackScore, FeedbackFormSentDate, and FeedbackAnswerDate
        connection.execute(text(""" 
            CREATE TABLE dim_feedbacks 
            AS 
            SELECT FeedbackID, FeedbackScore, FeedbackFormSentDate, FeedbackAnswerDate 
            FROM transformed_feedbacks;
        """))

        # Modify the FeedbackID column to ensure it is of type VARCHAR(50)
        connection.execute(text(""" 
            ALTER TABLE dim_feedbacks MODIFY COLUMN FeedbackID VARCHAR(50);
        """))

        # Add a primary key constraint to the 'FeedbackID' column
        connection.execute(text(""" 
            ALTER TABLE dim_feedbacks ADD PRIMARY KEY (FeedbackID);
        """))

        # Print confirmation message once the table is created successfully
        print("Dimension Table 'dim_feedbacks' created.")


# Function to create a payments dimension table with a primary key
def create_dim_payments(engine):
    """
    This function creates a dimension table called 'dim_payments' with a primary key on the 'PaymentID' column.
    It first drops any existing 'dim_payments' table, then creates a new table by selecting relevant 
    columns from the 'transformed_payments' table.
    """
    with engine.begin() as connection:  # Ensures auto-commit and transaction management
        # Drop the existing 'dim_payments' table if it exists to avoid conflicts
        connection.execute(text(""" 
            DROP TABLE IF EXISTS dim_payments;
        """))

        # Create the 'dim_payments' table by selecting columns from the 'transformed_payments' table
        # The selected columns include PaymentID, PaymentValue, PaymentInstallments, PaymentSequential, and PaymentType
        connection.execute(text(""" 
            CREATE TABLE dim_payments 
            AS 
            SELECT PaymentID, PaymentValue, PaymentInstallments, PaymentSequential, PaymentType 
            FROM transformed_payments;
        """))

        # Modify the PaymentID column to ensure it is of type VARCHAR(50)
        connection.execute(text(""" 
            ALTER TABLE dim_payments MODIFY COLUMN PaymentID VARCHAR(50);
        """))

        # Add a primary key constraint to the 'PaymentID' column
        connection.execute(text(""" 
            ALTER TABLE dim_payments ADD PRIMARY KEY (PaymentID);
        """))

        # Print confirmation message once the table is created successfully
        print("Dimension Table 'dim_payments' created.")


# Function to create a products dimension table with a primary key
def create_dim_products(engine):
    """
    This function creates a dimension table called 'dim_products' with a primary key on the 'ProductID' column.
    It first drops any existing 'dim_products' table, then creates a new table by selecting relevant 
    columns from the 'transformed_products' table.
    """
    with engine.begin() as connection:  # Ensures auto-commit and transaction management
        # Drop the existing 'dim_products' table if it exists to avoid conflicts
        connection.execute(text(""" 
            DROP TABLE IF EXISTS dim_products;
        """))

        # Create the 'dim_products' table by selecting columns from the 'transformed_products' table
        # The selected columns include ProductID, ProductCategory, ProductNameLength, ProductDescriptionLength, 
        # ProductPhotosQuantity, ProductWeightInGrams, ProductLengthInCm, ProductHeightInCm, and ProductWidthInCm
        connection.execute(text(""" 
            CREATE TABLE dim_products 
            AS 
            SELECT ProductID, ProductCategory, ProductNameLength, ProductDescriptionLength, ProductPhotosQuantity, 
                   ProductWeightInGrams, ProductLengthInCm, ProductHeightInCm, ProductWidthInCm
            FROM transformed_products;
        """))

        # Modify the ProductID column to ensure it is of type VARCHAR(50)
        connection.execute(text(""" 
            ALTER TABLE dim_products MODIFY COLUMN ProductID VARCHAR(50);
        """))

        # Add a primary key constraint to the 'ProductID' column
        connection.execute(text(""" 
            ALTER TABLE dim_products ADD PRIMARY KEY (ProductID);
        """))

        # Print confirmation message once the table is created successfully
        print("Dimension Table 'dim_products' created.")


# Function to create a sellers dimension table with a primary key
def create_dim_sellers(engine):
    """
    This function creates a dimension table called 'dim_sellers' with a primary key on the 'SellerID' column.
    It first drops any existing 'dim_sellers' table, then creates a new table by selecting relevant 
    columns from the 'transformed_sellers' table.
    """
    with engine.begin() as connection:  # Ensures auto-commit and transaction management
        # Drop the existing 'dim_sellers' table if it exists to avoid conflicts
        connection.execute(text(""" 
            DROP TABLE IF EXISTS dim_sellers;
        """))

        # Create the 'dim_sellers' table by selecting columns from the 'transformed_sellers' table
        # The selected columns include SellerID, SellerCity, SellerState, and SellerZIPCode
        connection.execute(text(""" 
            CREATE TABLE dim_sellers 
            AS 
            SELECT SellerID, SellerCity, SellerState, SellerZIPCode
            FROM transformed_sellers;
        """))

        # Modify the SellerID column to ensure it is of type VARCHAR(50)
        connection.execute(text(""" 
            ALTER TABLE dim_sellers MODIFY COLUMN SellerID VARCHAR(50);
        """))

        # Add a primary key constraint to the 'SellerID' column
        connection.execute(text(""" 
            ALTER TABLE dim_sellers ADD PRIMARY KEY (SellerID);
        """))

        # Print confirmation message once the table is created successfully
        print("Dimension Table 'dim_sellers' created.")


# Function to create and populate dim_date table
def create_dim_date(engine):
    """
    This function creates and populates a 'dim_date' table with columns for date-related information.
    It first drops the existing table, then creates a new schema and populates it with date data.
    The date range is from '2016-04-09' to '2018-10-17' (modifiable).
    """
    with engine.begin() as connection:  # Ensures auto-commit and transaction management
        # Drop the 'dim_date' table if it already exists to avoid conflicts
        connection.execute(text("DROP TABLE IF EXISTS dim_date;"))

        # Create the 'dim_date' table with various columns related to date information
        connection.execute(text(""" 
            CREATE TABLE dim_date (
                DateKey INT PRIMARY KEY,         
                Date DATETIME,                   
                Day VARCHAR(2),                 
                DayName VARCHAR(9),             
                Month VARCHAR(2),                
                MonthName VARCHAR(9),            
                Quarter CHAR(1),                 
                Season VARCHAR(6),               
                Year CHAR(4)                     
            );
        """))

    # Print confirmation message that the 'dim_date' table schema has been created
    print("Dimension Table 'dim_date' created.")

    # Define the start and end dates for the date range to populate the dim_date table
    start_date = pd.to_datetime("2016-04-09")  # Starting date for the date range
    end_date = pd.to_datetime("2018-10-17")    # Ending date for the date range

    # Generate a date range from the start date to the end date with daily frequency
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')

    # Create a DataFrame with columns derived from the date range
    df = pd.DataFrame({
        "DateKey": date_range.strftime('%Y%m%d').astype(int),  # DateKey as YYYYMMDD integer
        "Date": date_range,                                    # Full date
        "Day": date_range.day.astype(str),                     # Day of the month as a string
        "DayName": date_range.strftime('%A'),                  # Full name of the day (e.g., 'Monday')
        "Month": date_range.month.astype(str),                 # Month as a 2-digit string (e.g., '01')
        "MonthName": date_range.strftime('%B'),                # Full month name (e.g., 'January')
        "Quarter": date_range.quarter.astype(str),             # Quarter (1, 2, 3, or 4)
        "Season": date_range.month.map(lambda m: 
            'Winter' if m in [12, 1, 2] else                    # Season based on month
            'Spring' if m in [3, 4, 5] else
            'Summer' if m in [6, 7, 8] else
            'Fall'),
        "Year": date_range.year.astype(str)                    # Year in string format (e.g., '2016')
    })

    # Insert the populated DataFrame into the 'dim_date' table in MySQL
    # The 'if_exists="append"' option ensures that the data is appended to the existing table
    # The 'method="multi"' option speeds up the insertion by using batch insertions
    df.to_sql("dim_date", con=engine, if_exists="append", index=False, method="multi")

    # Print confirmation message once the data is successfully inserted
    print("Dimension table 'dim_date' populated successfully.")


# Function to create and populate dim_time table
def create_dim_time(engine):
    with engine.begin() as connection:
        # Drop table if it exists
        connection.execute(text("DROP TABLE IF EXISTS dim_time;"))

        # Create table schema
        connection.execute(text(""" 
            CREATE TABLE dim_time (
                TimeKey INT PRIMARY KEY, 
                AM_PM VARCHAR(2),          
                Hour INT,                  
                Minute INT,                
                Second INT,                
                Time TIME,                
                TimeOfDay VARCHAR(10)     
            );
        """))

    print("Dimension Table 'dim_time' created.")

    # Generate the time range from 00:00:00 to 23:59:59 with 1-minute intervals
    time_range = pd.date_range("00:00:00", "23:59:59", freq="min")

    # Create a DataFrame to populate the table
    df = pd.DataFrame({
        "TimeKey": [int(t.strftime('%H%M%S')) for t in time_range],  # Time as HHMMSS
        "AM_PM": [t.strftime('%p') for t in time_range],            # AM or PM
        "Hour": [t.hour for t in time_range],                        # Hour part
        "Minute": [t.minute for t in time_range],                    # Minute part
        "Second": [t.second for t in time_range],                    # Second part
        "Time": time_range,                                          # Full time
        "TimeOfDay": [    # Time of day classification
            'Morning' if t.hour < 12 else
            'Afternoon' if 12 <= t.hour < 18 else
            'Evening' if 18 <= t.hour < 21 else
            'Night' 
            for t in time_range]
    })

    # Insert into MySQL
    df.to_sql("dim_time", con=engine, if_exists="append", index=False, method="multi")

    print("Dimension table 'dim_time' populated successfully.")


def create_fact_order_items(engine):
    with engine.begin() as connection:
        
        # Creating the fact_order_items table with necessary fields and foreign key constraints
        connection.execute(text("""
            CREATE TABLE fact_order_items (
                OrderID VARCHAR(50),
                UserID VARCHAR(50),
                ProductID VARCHAR(50),
                SellerID VARCHAR(50),
                PaymentID VARCHAR(50),
                FeedbackID VARCHAR(50),
                OrderDateKey INT,
                OrderTimeKey INT,
                PaymentValue DOUBLE,
                UserState VARCHAR(70),
                DeliveredDateKey INT,
                DeliveredTimeKey INT,
                DeliveryDelayCheck VARCHAR(6),
                DeliveryDelayDays INT,
                EstimatedDeliveryDateKey INT,
                EstimatedDeliveryTimeKey INT,
                OrderApprovedDateKey INT,
                OrderApprovedTimeKey INT,
                OrderStatus VARCHAR(20),
                PickupDateKey INT,
                PickupTimeKey INT,
                Quantity INT,
                ShippingDays INT,
                FOREIGN KEY (UserID) REFERENCES dim_users(UserID),
                FOREIGN KEY (ProductID) REFERENCES dim_products(ProductID),
                FOREIGN KEY (SellerID) REFERENCES dim_sellers(SellerID),
                FOREIGN KEY (PaymentID) REFERENCES dim_payments(PaymentID),
                FOREIGN KEY (FeedbackID) REFERENCES dim_feedbacks(FeedbackID),
                FOREIGN KEY (OrderDateKey) REFERENCES dim_date(DateKey),
                FOREIGN KEY (OrderTimeKey) REFERENCES dim_time(TimeKey),
                FOREIGN KEY (DeliveredDateKey) REFERENCES dim_date(DateKey),
                FOREIGN KEY (DeliveredTimeKey) REFERENCES dim_time(TimeKey),
                FOREIGN KEY (EstimatedDeliveryDateKey) REFERENCES dim_date(DateKey),
                FOREIGN KEY (EstimatedDeliveryTimeKey) REFERENCES dim_time(TimeKey),
                FOREIGN KEY (OrderApprovedDateKey) REFERENCES dim_date(DateKey),
                FOREIGN KEY (OrderApprovedTimeKey) REFERENCES dim_time(TimeKey),
                FOREIGN KEY (PickupDateKey) REFERENCES dim_date(DateKey),
                FOREIGN KEY (PickupTimeKey) REFERENCES dim_time(TimeKey)
            );
        """))
        
        # Fact table 'fact_order_items' created successfully with all constraints.
        print("Fact Table 'fact_sales' created.")



def insert_into_fact_order_items(engine):
    if engine is None:
        print("Cannot proceed: No database connection.")
        return
    
    try:
        with engine.begin() as connection:
        
            # Inserting data into fact_order_items table from transformed data sources
            connection.execute(text("""
                INSERT INTO fact_order_items (
                    OrderID, UserID, ProductID, SellerID, PaymentID, FeedbackID, 
                    OrderDateKey, OrderTimeKey, PaymentValue, UserState, 
                    DeliveredDateKey, DeliveredTimeKey, DeliveryDelayCheck, DeliveryDelayDays,
                    EstimatedDeliveryDateKey, EstimatedDeliveryTimeKey,
                    OrderApprovedDateKey, OrderApprovedTimeKey, 
                    OrderStatus, PickupDateKey, PickupTimeKey, 
                    Quantity, ShippingDays
                )
                SELECT
                    o.OrderID,
                    o.UserID,
                    oi.ProductID,
                    oi.SellerID,
                    p.PaymentID,
                    f.FeedbackID,
                    d1.DateKey AS OrderDateKey,
                    t1.TimeKey AS OrderTimeKey,
                    p.PaymentValue,
                    u.UserState,
                    d2.DateKey AS DeliveredDateKey,
                    t2.TimeKey AS DeliveredTimeKey,
                    CASE 
                        WHEN d2.DateKey > d3.DateKey THEN 'TRUE' 
                        ELSE 'FALSE' 
                    END AS DeliveryDelayCheck,
                    CASE 
                        WHEN DATEDIFF(d2.DateKey, d3.DateKey) < 0 THEN 0 
                        ELSE DATEDIFF(d2.DateKey, d3.DateKey) 
                    END AS DeliveryDelayDays,
                    d3.DateKey AS EstimatedDeliveryDateKey,
                    t3.TimeKey AS EstimatedDeliveryTimeKey,
                    d4.DateKey AS OrderApprovedDateKey,
                    t4.TimeKey AS OrderApprovedTimeKey,
                    o.OrderStatus,
                    d5.DateKey AS PickupDateKey,
                    t5.TimeKey AS PickupTimeKey,
                    oi.Quantity,
                    DATEDIFF(o.DeliveredDate, o.PickupDate) AS ShippingDays
                FROM transformed_orders o
                LEFT JOIN transformed_order_items oi ON o.OrderID = oi.OrderID
                LEFT JOIN dim_users u ON o.UserID = u.UserID
                LEFT JOIN dim_sellers s ON oi.SellerID = s.SellerID
                LEFT JOIN dim_products pd ON oi.ProductID = pd.productID
                LEFT JOIN dim_payments p ON o.OrderID = p.PaymentID
                LEFT JOIN dim_feedbacks f ON o.FeedbackID = f.FeedbackID
                LEFT JOIN dim_date d1 ON DATE(o.OrderDate) = DATE(d1.Date)
                LEFT JOIN dim_time t1 ON HOUR(o.OrderDate) = t1.Hour AND MINUTE(o.OrderDate) = t1.Minute
                LEFT JOIN dim_date d2 ON DATE(o.DeliveredDate) = DATE(d2.Date)
                LEFT JOIN dim_time t2 ON HOUR(o.DeliveredDate) = t2.Hour AND MINUTE(o.DeliveredDate) = t2.Minute
                LEFT JOIN dim_date d3 ON DATE(o.EstimatedDeliveryDate) = DATE(d3.Date)
                LEFT JOIN dim_time t3 ON HOUR(o.EstimatedDeliveryDate) = t3.Hour AND MINUTE(o.EstimatedDeliveryDate) = t3.Minute
                LEFT JOIN dim_date d4 ON DATE(o.OrderApprovedDate) = DATE(d4.Date)
                LEFT JOIN dim_time t4 ON HOUR(o.OrderApprovedDate) = t4.Hour AND MINUTE(o.OrderApprovedDate) = t4.Minute
                LEFT JOIN dim_date d5 ON DATE(o.PickupDate) = DATE(d5.Date)
                LEFT JOIN dim_time t5 ON HOUR(o.PickupDate) = t5.Hour AND MINUTE(o.PickupDate) = t5.Minute;
            """))
            
            # Fact table 'fact_order_items' populated successfully
            print("Fact table 'fact_order_items' populated successfully.")
    
    except SQLAlchemyError as e:
        # Error handling if insertion fails
        print(f"Error inserting into 'fact_order_items': {e}")


def main():
    try:
        # Log the start of the main process
        logging.info('Starting the data transformation process...')
        
        # Create an SQLAlchemy engine
        logging.info(f"Connecting to the MySQL database at {HOST}...")
        engine = create_engine(f"mysql+mysqlconnector://{USERNAME}:{PASSWORD}@{HOST}/{DATABASE}")
        logging.info("Database connection established.")
        
        print("Running the transformation script...")
        run_transformation_script()  # Call the existing transformation script
        
        # Log the creation of each dimension and fact table
        logging.info("Creating dimension tables...")

        create_dim_users(engine)
        logging.info("dim_users table created.")
        
        create_dim_feedbacks(engine)
        logging.info("dim_feedbacks table created.")
        
        create_dim_payments(engine)
        logging.info("dim_payments table created.")
        
        create_dim_products(engine)
        logging.info("dim_products table created.")
        
        create_dim_sellers(engine)
        logging.info("dim_sellers table created.")
        
        create_dim_date(engine)
        logging.info("dim_date table created.")
        
        create_dim_time(engine)
        logging.info("dim_time table created.")
        
        # Log the creation of the fact table
        logging.info("Creating fact_order_items table...")
        create_fact_order_items(engine)
        logging.info("fact_order_items table created.")
        
        # Log the insertion into fact table
        logging.info("Inserting data into fact_order_items...")
        insert_into_fact_order_items(engine)
        logging.info("Data inserted into fact_order_items table.")
        
        # Log completion of the entire process
        logging.info('Data transformation process completed successfully.')

    except Exception as e:
        logging.error(f"An error occurred during the data transformation process: {e}")
        raise  # Re-raise the error to ensure the failure is captured

if __name__ == "__main__":
    main()