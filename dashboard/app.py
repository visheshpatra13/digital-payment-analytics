import streamlit as st
import pandas as pd
import os

st.set_page_config(page_title="Digital Payment Analytics", layout="wide")

st.title("💳 Digital Payment Analytics Dashboard")

data_path = "data/warehouse/fact_payment/data.parquet"

if not os.path.exists(data_path):
    st.error("Data file not found. Please generate data first.")
    st.stop()

df = pd.read_parquet(data_path)

# KPIs
total_transactions = len(df)
total_revenue = df["amount"].sum()
avg_transaction = df["amount"].mean()

col1, col2, col3 = st.columns(3)

col1.metric("Total Transactions", total_transactions)
col2.metric("Total Revenue", f"₹ {total_revenue:,.2f}")
col3.metric("Avg Transaction Value", f"₹ {avg_transaction:,.2f}")

st.divider()

st.subheader("📈 Daily Revenue Trend")

df["payment_date"] = pd.to_datetime(df["payment_date"])
daily_revenue = df.groupby(df["payment_date"].dt.date)["amount"].sum()

st.line_chart(daily_revenue)

st.subheader("🏪 Revenue by Merchant")

merchant_revenue = df.groupby("merchant_id")["amount"].sum()
st.bar_chart(merchant_revenue)