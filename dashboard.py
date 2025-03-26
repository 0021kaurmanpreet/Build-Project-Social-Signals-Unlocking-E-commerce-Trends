from math import e
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import altair as alt
import plotly.express as px
import seaborn as sns

# Database connection details
USERNAME = "root"        # Username for the database
PASSWORD = "password"  # Password for the database
HOST = "localhost"       # Database host (localhost for local development)
DATABASE = "ecommerce"   # Name of the database being accessed

print("Connecting to db...................")
engine = create_engine(f"mysql+mysqlconnector://{USERNAME}:{PASSWORD}@{HOST}/{DATABASE}")

st.write("# E-Commerce Dashboard")

col1, col2 = st.columns(2)


with col1:
    query9="""
    SELECT COUNT(DISTINCT OrderID)/1000 AS TotalDistinctOrders
    FROM fact_order_items;
    """
    total_orders=pd.read_sql(query9, engine)
    st.metric(label="Total Orders", value=f"{total_orders['TotalDistinctOrders'][0]:.2f} K")

    query10 = """
    SELECT 
        SUM(distinct_orders.PaymentValue) / 1e9 AS Total_Revenue
    FROM (
        SELECT DISTINCT foi.OrderID, foi.PaymentValue
        FROM fact_order_items foi
    ) AS distinct_orders;
    """
    distinct_orders_payment = pd.read_sql(query10, engine)
    st.metric(label="Total Revenue", value=f"{distinct_orders_payment['Total_Revenue'][0]:.2f} Billions")


with col2:
    query5 = """
    WITH RECURSIVE numbers AS (
        SELECT 1 AS n
        UNION ALL
        SELECT n + 1 FROM numbers WHERE n <= (
            SELECT MAX(LENGTH(PaymentInstallments) - LENGTH(REPLACE(PaymentInstallments, ',', '')) + 1)
            FROM dim_payments
        )
    )
    SELECT AVG(CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(PaymentInstallments, ',', n.n), ',', -1) AS DECIMAL)) AS avg_installments
    FROM dim_payments
    JOIN numbers n 
    ON CHAR_LENGTH(PaymentInstallments) - CHAR_LENGTH(REPLACE(PaymentInstallments, ',', '')) >= n.n - 1
    WHERE PaymentInstallments IS NOT NULL AND TRIM(PaymentInstallments) != '';
    """

    df_avg_installments = pd.read_sql(query5, engine)
    st.metric(label="Average Installments", value=f"{df_avg_installments['avg_installments'][0]:.2f}")

    query10 = """
    SELECT COUNT(DISTINCT OrderID) AS DelayedOrders
    FROM fact_order_items
    WHERE DeliveryDelayDays <> 0;
    """
    delayed_orders = pd.read_sql(query10, engine)

    st.metric(label="Delayed Orders", value=f"{delayed_orders['DelayedOrders'][0]}")


query1 = """
--  Peak period for our e-commerce platform (by Season, Month)
SELECT 
    dd.Season AS Season,
    SUM(distinct_orders.PaymentValue) / 1e9 AS Total_Revenue_In_Billions
FROM (
    SELECT DISTINCT foi.OrderID, foi.OrderDateKey, foi.PaymentValue
    FROM fact_order_items foi
) AS distinct_orders
JOIN dim_date dd ON distinct_orders.OrderDateKey = dd.DateKey
GROUP BY dd.Season
ORDER BY Total_Revenue_In_Billions DESC;
"""
df1 = pd.read_sql(query1, engine)

st.write("### Peak Period Analysis (Season & Month)")

st.write("#### Total Revenue by Season:")
st.dataframe(df1, hide_index=True)

query2 = """
WITH Season_Revenue AS (
    SELECT 
        dd.Season AS Season,
        SUM(distinct_orders.PaymentValue) / 1e9 AS Total_Revenue_In_Billions
    FROM (
        SELECT DISTINCT foi.OrderID, foi.OrderDateKey, foi.PaymentValue
        FROM fact_order_items foi
    ) AS distinct_orders
    JOIN dim_date dd ON distinct_orders.OrderDateKey = dd.DateKey
    GROUP BY dd.Season
)
SELECT 
    dd.Season AS Season,
    dd.MonthName AS MonthName,
    SUM(distinct_orders.PaymentValue) / 1e9 AS Total_Revenue_In_Billions
FROM (
    SELECT DISTINCT foi.OrderID, foi.OrderDateKey, foi.PaymentValue
    FROM fact_order_items foi
) AS distinct_orders
JOIN dim_date dd ON distinct_orders.OrderDateKey = dd.DateKey
JOIN Season_Revenue sr ON dd.Season = sr.Season
GROUP BY dd.Season, dd.MonthName
ORDER BY sr.Total_Revenue_In_Billions DESC, Total_Revenue_In_Billions DESC;
"""
df2 = pd.read_sql(query2, engine)

st.write("#### Total Revenue by Season & Month:")
st.dataframe(df2, hide_index=True)

peak_season = df2.groupby("Season")["Total_Revenue_In_Billions"].sum().idxmax()

peak_season_data = df2[df2["Season"] == peak_season]

peak_month = peak_season_data.loc[peak_season_data["Total_Revenue_In_Billions"].idxmax(), "MonthName"]

st.write(f"The busiest season for our e-commerce platform is **{peak_season}**.")
st.write(f"The peak month in **{peak_season}** is **{peak_month}**.")


st.write("### Orders by Season")

df_season_sorted = df1.sort_values(by="Total_Revenue_In_Billions", ascending=False)

chart1 = alt.Chart(df_season_sorted).mark_bar().encode(
    x=alt.X("Season:N", title="Season", sort=df_season_sorted["Season"].tolist()),
    y=alt.Y("Total_Revenue_In_Billions:Q", title="Total Revenue (in Billions)"),
    color=alt.Color("Season:N", legend=None), 
    tooltip=["Season", "Total_Revenue_In_Billions"]
).properties(
    width=800,
    height=400
)

st.altair_chart(chart1, use_container_width=True)


df_peak_season = df2[df2['Season'] == peak_season]
df_peak_season['Total_Revenue_In_Billions'] = df_peak_season['Total_Revenue_In_Billions'].astype(float)
df_peak_season_sorted = df_peak_season.sort_values(by="Total_Revenue_In_Billions", ascending=False)

month_order = df_peak_season_sorted['MonthName'].tolist()
df_peak_season_sorted['MonthName'] = pd.Categorical(df_peak_season_sorted['MonthName'], categories=month_order, ordered=True)

chart2 = alt.Chart(df_peak_season_sorted).mark_bar().encode(
    x=alt.X("MonthName:N", title="Month", sort=month_order),
    y=alt.Y("Total_Revenue_In_Billions:Q", title="Total Revenue (in Billions)"),
    color=alt.Color("MonthName:N", legend=None), 
    tooltip=["MonthName", "Total_Revenue_In_Billions"]
).properties(
    width=800,
    height=400
)

st.write(f"### Orders by Month for the **{peak_season}** Season")
st.altair_chart(chart2, use_container_width=True)


query3 = """
SELECT 
    dt.Hour AS Hour, 
    dt.TimeOfDay AS TimeOfDay,
    COUNT(DISTINCT foi.OrderID)/1000 AS Total_Orders_in_K
FROM fact_order_items foi
JOIN dim_time dt ON foi.OrderTimeKey = dt.TimeKey
GROUP BY dt.Hour, dt.TimeOfDay
ORDER BY Total_Orders_in_K DESC;
"""
df3 = pd.read_sql(query3, engine)

st.write("### Peak Hours (Time of Day) Analysis")

st.write("#### Total Orders by Hour and Time of Day")
st.dataframe(df3, hide_index=True)
st.write("#### Distribution using Bar Chart")

df3["Hour_Label"] = df3["Hour"].astype(str) + " " + df3["TimeOfDay"]


df3 = df3.sort_values(by="Hour")


chart = alt.Chart(df3).mark_bar().encode(
    x=alt.X("Hour_Label:N", title="Hour (Time of Day)", sort=df3["Hour_Label"].tolist()),
    y=alt.Y("Total_Orders_in_K:Q", title="Total Orders (in K)"),
    tooltip=["Hour", "TimeOfDay", "Total_Orders_in_K"]
).properties(
    width=800,
    height=400
)

st.altair_chart(chart, use_container_width=True)


threshold = 0.9 * df3["Total_Orders_in_K"].max()
peak_hours_df = df3[df3["Total_Orders_in_K"] >= threshold]
min_hour = peak_hours_df["Hour"].min()
max_hour = peak_hours_df["Hour"].max()
st.write(f"The peak order period is from **{min_hour}:00 to {max_hour}:00**. Specifically, the hours 10:00 to 5:00 i.e. Late Morning to Late Afternoon.")


query4 = """
WITH RECURSIVE payment_split AS (
    -- Initial extraction: first payment type and remaining part
    SELECT 
        PaymentID,
        TRIM(SUBSTRING_INDEX(PaymentType, ',', 1)) AS PaymentType,
        CASE 
            WHEN LOCATE(',', PaymentType) > 0 THEN SUBSTRING(PaymentType, LOCATE(',', PaymentType) + 1)
            ELSE NULL
        END AS Remaining
    FROM dim_payments

    UNION ALL

    -- Recursively extract next payment type
    SELECT 
        PaymentID,
        TRIM(SUBSTRING_INDEX(Remaining, ',', 1)) AS PaymentType,
        CASE 
            WHEN LOCATE(',', Remaining) > 0 THEN SUBSTRING(Remaining, LOCATE(',', Remaining) + 1)
            ELSE NULL
        END AS Remaining
    FROM payment_split
    WHERE Remaining IS NOT NULL AND Remaining <> ''
)

SELECT 
    PaymentType,
    COUNT(*) AS PaymentCount  
FROM payment_split
WHERE PaymentType IS NOT NULL AND PaymentType <> ''  
GROUP BY PaymentType
ORDER BY PaymentCount DESC;
"""

df_payments = pd.read_sql(query4, engine)

st.write("### Most Popular Payment Methods")

df_payments['Percentage'] = (df_payments['PaymentCount'] / df_payments['PaymentCount'].sum()) * 100

pie_chart = alt.Chart(df_payments).mark_arc().encode(
    theta="PaymentCount:Q",
    color="PaymentType:N",
    tooltip=["PaymentType:N", "PaymentCount:Q", "Percentage:Q"], 
    text=alt.Text("PaymentType:N") 
).properties(title="Payment Methods Distribution")


pie_chart = pie_chart.mark_arc().encode(
    text=alt.Text("PaymentCount:Q")  
).properties(title="Distribution of Payment Methods")


pie_chart = pie_chart.mark_arc().encode(
    text=alt.Text("Percentage:Q", format=".2f")
)

st.altair_chart(pie_chart, use_container_width=True)

most_popular_method = df_payments.iloc[0]['PaymentType']
most_popular_count = df_payments.iloc[0]['PaymentCount']
most_popular_percentage = df_payments.iloc[0]['Percentage']

st.write(f"**Most Popular Payment Method:** {most_popular_method} ({most_popular_count} transactions, {most_popular_percentage:.2f}%)")


query6 = """
SELECT 
    SUBSTRING_INDEX(du.UserState, ',', 1) AS State, 
    COUNT(DISTINCT foi.OrderID) AS TotalOrders
FROM fact_order_items foi
JOIN dim_users du ON foi.UserID = du.UserID
GROUP BY State
ORDER BY TotalOrders DESC;
"""

df_geo = pd.read_sql(query6, engine)

latitude_longitude = {
    "Jawa Timur": (-7.7152, 112.7509),
    "Jawa Barat": (-6.8894, 107.6100),
    "Banten": (-6.1375, 106.1837),
    "Dki Jakarta": (-6.2088, 106.8456),
    "Jawa Tengah": (-7.1500, 110.3000),
    "Lampung": (-5.2593, 105.3436),
    "Sumatera Barat": (-0.9073, 100.4173),
    "Sulawesi Selatan": (-5.1470, 119.4238),
    "Jambi": (-1.6158, 103.6057),
    "Kepulauan Riau": (0.8871, 104.2194),
    "Kalimantan Timur": (0.7333, 117.2500),
    "Nusa Tenggara Timur": (-9.4412, 120.9997),
    "Aceh": (4.4952, 96.8324),
    "Sumatera Selatan": (-3.3188, 104.6956),
    "Sumatera Utara": (3.5952, 98.5523),
    "Papua": (-4.6999, 140.7417),
    "Riau": (0.4380, 101.4471),
    "Sulawesi Tenggara": (-3.5281, 122.7104),
    "Kalimantan Tengah": (-2.2758, 113.9783),
    "Sulawesi Tengah": (-0.6672, 119.8525),
    "Nusa Tenggara Barat": (-8.5833, 116.4533),
    "Bengkulu": (-3.8000, 102.2650),
    "Bali": (-8.3405, 115.0919),
    "Sulawesi Utara": (1.0804, 124.8476),
    "Di Yogyakarta": (-7.7956, 110.3695),
    "Maluku Utara": (1.3637, 127.7475),
    "Kalimantan Utara": (3.4065, 116.4891),
    "Maluku": (-3.4126, 127.2084),
    "Papua Barat": (-2.1895, 134.0714),
    "Kalimantan Barat": (0.0125, 109.2964),
    "Sulawesi Barat": (-2.8182, 119.4100),
    "Kalimantan Selatan": (-2.9367, 115.2365),
    "Gorontalo": (-0.5613, 123.0574),
    "Kepulauan Bangka Belitung": (-2.2400, 106.4633)
}

df_geo['Latitude'] = df_geo['State'].map(lambda x: latitude_longitude.get(x, (None, None))[0])
df_geo['Longitude'] = df_geo['State'].map(lambda x: latitude_longitude.get(x, (None, None))[1])

fig = px.scatter_geo(df_geo, 
                     lat="Latitude", 
                     lon="Longitude", 
                     size="TotalOrders", 
                     color_discrete_sequence=["blue"], 
                     scope="asia",
                     projection="mercator",  
                     center={"lat": -5.0, "lon": 120.0},  
                     hover_data={"TotalOrders": True, "State": True, "Latitude": False, "Longitude": False}, 
                     )

fig.update_geos(
    visible=True,
    projection_type="mercator",
    center={"lat": -5.0, "lon": 120.0},
    projection_scale=5, 
    coastlinecolor="Black",
)

st.write("### Purchase Frequency by States in Indonesia")
st.plotly_chart(fig)

top_3_states = df_geo.nlargest(3, 'TotalOrders')

state1, count1 = top_3_states.iloc[0]['State'], top_3_states.iloc[0]['TotalOrders']
state2, count2 = top_3_states.iloc[1]['State'], top_3_states.iloc[1]['TotalOrders']
state3, count3 = top_3_states.iloc[2]['State'], top_3_states.iloc[2]['TotalOrders']

st.write(f"**Top 3 States with the Most Orders:**")
st.write(f"🥇 **{state1}** - {count1} orders")
st.write(f"🥈 **{state2}** - {count2} orders")
st.write(f"🥉 **{state3}** - {count3} orders")


query7 = """
SELECT 
    SUBSTRING_INDEX(du.UserCity, ',', 1) AS UserCity,  
    ds.SellerCity,
    COUNT(DISTINCT d.OrderID) AS TotalOrders,  -- Use COUNT(DISTINCT) to avoid counting duplicates
    SUM(CASE WHEN d.DeliveryDelayCheck = 'TRUE' THEN 1 ELSE 0 END) AS TotalOrdersDelayed
FROM fact_order_items d
JOIN dim_users du ON d.UserID = du.UserID
JOIN dim_sellers ds ON d.SellerID = ds.SellerID
GROUP BY UserCity, ds.SellerCity
ORDER BY TotalOrders DESC;
"""

df_logistics = pd.read_sql(query7, engine)

st.write("### Logistics Analysis")
st.dataframe(df_logistics, hide_index=True)

heaviest_traffic = df_logistics[['UserCity', 'SellerCity', 'TotalOrders']].sort_values(by='TotalOrders', ascending=False).head(5)
st.write("#### Top 5 Routes with Heaviest Traffic")
st.dataframe(heaviest_traffic, hide_index=True)

st.write("#### Top 5 Routes with Heaviest Traffic (Bar Chart)")

top_traffic_routes = df_logistics[['UserCity', 'SellerCity', 'TotalOrders']].sort_values(by='TotalOrders', ascending=False).head(5)

plt.figure(figsize=(10, 6))
sns.barplot(x='TotalOrders', y='UserCity', data=top_traffic_routes, hue='SellerCity', dodge=False)
plt.xlabel('Total Orders')
plt.ylabel('User City')
plt.tight_layout()

st.pyplot(plt)

longest_delays = df_logistics[['UserCity', 'SellerCity', 'TotalOrdersDelayed']].sort_values(by='TotalOrdersDelayed', ascending=False).head(5)
st.write("#### Top 5 Routes with Longest Delivery Delays")
st.dataframe(longest_delays, hide_index=True)

st.write("#### Top 5 Routes with Longest Delivery Delays (Bar Chart)")

top_delayed_routes = df_logistics[['UserCity', 'SellerCity', 'TotalOrdersDelayed']].sort_values(by='TotalOrdersDelayed', ascending=False).head(5)

plt.figure(figsize=(10, 6))
sns.barplot(x='TotalOrdersDelayed', y='UserCity', data=top_delayed_routes, hue='SellerCity', dodge=False)
plt.xlabel('Total Delayed Orders')
plt.ylabel('User City')
plt.tight_layout()

st.pyplot(plt)


query8 = """
SELECT 
    d.OrderID,
    d.DeliveryDelayDays,
    f.FeedbackScore
FROM fact_order_items d
JOIN dim_feedbacks f ON d.FeedbackID = f.FeedbackID;
"""
df = pd.read_sql(query8, engine)


average_delaydays_by_score = df.groupby('FeedbackScore')['DeliveryDelayDays'].mean()

st.write("### Delivery Performance")

st.write("Average Delivery Delay Days by Feedback Score:")
st.dataframe(average_delaydays_by_score)

plt.figure(figsize=(8, 5))
plt.plot(average_delaydays_by_score.index, average_delaydays_by_score.values, marker='o', linestyle='-', color='b')


plt.xlabel("Feedback Score")
plt.ylabel("Average Delivery Delay Days")


plt.grid(True, linestyle="--", alpha=0.6)


st.pyplot(plt)

average_delaydays_by_score_df = average_delaydays_by_score.reset_index()

correlation = average_delaydays_by_score_df['FeedbackScore'].corr(average_delaydays_by_score_df['DeliveryDelayDays'])

st.write(f"Correlation between Feedback Score and Average Delivery Delay Days: {correlation}")

st.write("### Average Shipping Days by State")

query11="""
SELECT 
    SUBSTRING_INDEX(UserState, ',', 1) AS UserState,  -- Extract the first state
    AVG(ShippingDays) AS ShippingDays  -- Calculate average shipping days for each state
FROM 
    fact_order_items foi
WHERE 
    foi.PickupDateKey <= foi.DeliveredDateKey
GROUP BY 
    SUBSTRING_INDEX(UserState, ',', 1)  -- Group by the extracted first state
ORDER BY 
    ShippingDays DESC;
"""

df11 = pd.read_sql(query11, engine)
st.dataframe(df11, hide_index=True)

query12 = """
SELECT 
    SUBSTRING_INDEX(du.UserCity, ',', 1) AS UserCity,  
    ds.SellerCity,
    AVG(ShippingDays) AS ShippingDays
FROM fact_order_items d
JOIN dim_users du ON d.UserID = du.UserID
JOIN dim_sellers ds ON d.SellerID = ds.SellerID
WHERE 
    d.PickupDateKey <= d.DeliveredDateKey
GROUP BY du.UserCity, ds.SellerCity
ORDER BY ShippingDays DESC;
"""

df12 = pd.read_sql(query12, engine)

st.write("### Average Shipping Days by Logistics")
st.dataframe(df12, hide_index=True)


query13="""
SELECT 
    AVG(DATEDIFF(
        dd.Date,  -- Actual Delivered Date
        ed.Date   -- Estimated Delivery Date
    )) AS AvgDeliveryDifference
FROM 
    fact_order_items foi
JOIN 
    dim_date ed ON foi.EstimatedDeliveryDateKey = ed.DateKey  -- Join for Estimated Delivery Date
JOIN 
    dim_date dd ON foi.DeliveredDateKey = dd.DateKey  -- Join for Delivered Date
WHERE 
    foi.PickupDateKey <= foi.DeliveredDateKey  -- Ignore rows where PickupDate > DeliveredDate
;
"""

df13 = pd.read_sql(query13, engine)
avg_delivery_diff = df13.iloc[0]['AvgDeliveryDifference']
st.write(f"##### Average Delivery Time Difference (Estimated vs Actual): {avg_delivery_diff:.2f} days")


query14="""
SELECT 
    DATE(dd.Date) AS OrderDate, 
    COUNT(DISTINCT foi.OrderID) AS DistinctOrderCount
FROM 
    fact_order_items foi
JOIN 
    dim_date dd ON foi.OrderDateKey = dd.DateKey
GROUP BY 
    dd.Date
ORDER BY 
    dd.Date;
"""
df14 = pd.read_sql(query14, engine)
st.write("### Sales spikes on 24/11/2017 Friday")
df14['OrderDate'] = pd.to_datetime(df14['OrderDate'])

plt.figure(figsize=(10, 6))
plt.plot(df14['OrderDate'], df14['DistinctOrderCount'], marker='o', color='b', linestyle='-', linewidth=2)

plt.xlabel("Order Date", fontsize=12)
plt.ylabel("Order Count", fontsize=12)
plt.xticks(rotation=45)

plt.tight_layout()
st.pyplot(plt)

st.write("##### November 24, 2017 was a Friday in Indonesia.")
st.write("##### It was a public holiday in Indonesia to celebrate the 72nd Anniversary of the country's independence from the Netherlands.")

def main():
    st.title("Conclusion")
    
    st.header("Summary of Key Findings")
    
    st.subheader("Overall Performance")
    st.write("- **Total Orders:** 99.44K")
    st.write("- **Total Revenue:** $16.01 Billion")
    st.write("- **Average Installments per Purchase:** 2.85")
    st.write("- **Total Delayed Orders:** 6548")
    
    st.subheader("Peak Period Analysis")
    st.write("- **The busiest season is Spring, with May being the peak month.**")
    st.write("- **Peak order hours are from 10:00 AM to 9:00 PM, with the highest activity from 10:00 AM to 5:00 PM.**")
    
    st.subheader("Payment Trends")
    st.write("- **Credit Card is the most preferred payment method, accounting for 73.92% of transactions (76,795 orders).**")
    
    st.subheader("Geographical Insights")
    st.write("- **The top three states with the highest orders are Banten (21,236 orders), Jawa Barat (12,845 orders), and DKI Jakarta (12,549 orders).**")
    
    st.subheader("Logistics and Delivery Performance")
    st.write("- **The top 5 busiest delivery routes have the heaviest traffic.**")
    st.write("- **Significant delivery delays observed in specific routes.**")
    st.write("- **Strong negative correlation (-0.93) between feedback score and average delivery delay, indicating that longer delays lead to poor customer ratings.**")
    st.write("- **The average estimated vs. actual delivery time difference is -12.32 days, meaning deliveries are consistently earlier than expected.**")
    
    st.subheader("Special Events Impact")
    st.write("- **Sales spiked significantly on November 24, 2017 (Friday), which coincided with a public holiday in Indonesia.**")
    
    st.header("Business Recommendations")
    
    st.subheader("1. Optimize Inventory & Marketing During Peak Periods")
    st.write("- Increase stock levels and marketing during Spring, especially in May.")
    st.write("- Launch promotional offers from 10:00 AM - 9:00 PM.")
    
    st.subheader("2. Improve Delivery Efficiency & Reduce Delays")
    st.write("- Focus on optimizing top 5 routes with the longest delays.")
    st.write("- Strengthen logistics partnerships for better on-time performance.")
    st.write("- Implement real-time tracking and proactive delay communication.")
    
    st.subheader("3. Enhance Payment Experience")
    st.write("- Offer cashback, discounts, or rewards for Credit Card payments.")
    st.write("- Expand installment-based payment options.")
    
    st.subheader("4. Leverage Geographical Insights for Expansion & Targeted Ads")
    st.write("- Focus marketing on top states (Banten, Jawa Barat, DKI Jakarta).")
    st.write("- Study and improve engagement in low-performing regions.")
    
    st.subheader("5. Capitalize on Sales Trends Around Public Holidays")
    st.write("- Plan special discounts for national holidays and high-traffic events.")
    st.write("- Analyze other potential peak seasons to replicate sales spikes.")
    
    st.success("By implementing these recommendations, the platform can enhance customer satisfaction, operational efficiency, and revenue growth.")
    
if __name__ == "__main__":
    main()
