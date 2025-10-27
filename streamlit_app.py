import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import snowflake.connector
from datetime import datetime

# Page configuration
st.set_page_config(
    page_title="Avalanche Product Intelligence",
    page_icon="🏔️",
    layout="wide"
)

# Sidebar - Snowflake Connection
st.sidebar.header("🔐 Snowflake Connection")

# Initialize connection state
if 'connected' not in st.session_state:
    st.session_state.connected = False

# Connection form
with st.sidebar.form("connection_form"):
    account = st.text_input("Account", placeholder="abc12345.us-east-1")
    user = st.text_input("User", placeholder="your_username")
    password = st.text_input("Password", type="password")
    warehouse = st.text_input("Warehouse", value="COMPUTE_WH")
    database = st.text_input("Database", placeholder="your_database")
    schema = st.text_input("Schema", placeholder="your_schema")
    
    connect_button = st.form_submit_button("Connect to Snowflake")
    
    if connect_button:
        if not all([account, user, password, warehouse, database, schema]):
            st.error("❌ Please fill in all fields")
        else:
            try:
                conn = snowflake.connector.connect(
                    user=user,
                    password=password,
                    account=account,
                    warehouse=warehouse,
                    database=database,
                    schema=schema
                )
                st.session_state.conn = conn
                st.session_state.connected = True
                st.success("✅ Connected successfully!")
                st.rerun()
            except Exception as e:
                st.error(f"❌ Connection failed: {str(e)}")

# Disconnect button
if st.session_state.connected:
    if st.sidebar.button("Disconnect"):
        if 'conn' in st.session_state:
            st.session_state.conn.close()
        st.session_state.connected = False
        st.rerun()

# Main app - only show if connected
if st.session_state.connected and 'conn' in st.session_state:
    conn = st.session_state.conn
    
    # App header
    st.title("🏔️ Avalanche Product Intelligence Dashboard")
    st.markdown("### Customer Sentiment & Delivery Analysis")
    
    # Load data with caching
    @st.cache_data(ttl=600)  # Cache for 10 minutes
    def load_data(_conn):
        query = """
        SELECT 
            cr.product,
            cr.date,
            cr.summary,
            cr.sentiment_score,
            cr.order_id,
            sl.shipping_date,
            sl.carrier,
            sl.delivery_days,
            sl.late,
            sl.region,
            sl.status
        FROM customer_reviews cr
        LEFT JOIN shipping_logs sl ON cr.order_id = sl.order_id
        """
        cursor = _conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)
        cursor.close()
        return df
    
    try:
        df = load_data(conn)
        df['DATE'] = pd.to_datetime(df['DATE'])
        
        # Filters in sidebar
        st.sidebar.markdown("---")
        st.sidebar.header("📊 Filters")
        
        products = ['All'] + sorted(df['PRODUCT'].unique().tolist())
        selected_product = st.sidebar.selectbox("Select Product", products)
        
        min_date = df['DATE'].min().date()
        max_date = df['DATE'].max().date()
        date_range = st.sidebar.date_input(
            "Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )
        
        regions = ['All'] + sorted(df['REGION'].dropna().unique().tolist())
        selected_region = st.sidebar.selectbox("Select Region", regions)
        
        # Apply filters
        filtered_df = df.copy()
        
        if selected_product != 'All':
            filtered_df = filtered_df[filtered_df['PRODUCT'] == selected_product]
        
        if len(date_range) == 2:
            start_date = pd.Timestamp(date_range[0])
            end_date = pd.Timestamp(date_range[1])
            filtered_df = filtered_df[
                (filtered_df['DATE'] >= start_date) &
                (filtered_df['DATE'] <= end_date)
            ]
        
        if selected_region != 'All':
            filtered_df = filtered_df[filtered_df['REGION'] == selected_region]
        
        # Key metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Reviews", len(filtered_df))
        with col2:
            avg_sentiment = filtered_df['SENTIMENT_SCORE'].mean()
            st.metric("Avg Sentiment", f"{avg_sentiment:.3f}")
        with col3:
            late_count = (filtered_df['LATE'] == True).sum()
            st.metric("Late Deliveries", f"{late_count}")
        with col4:
            late_pct = (late_count / len(filtered_df) * 100) if len(filtered_df) > 0 else 0
            st.metric("Late %", f"{late_pct:.1f}%")
        
        # Regional sentiment visualization
        st.subheader("🗺️ Average Sentiment Score by Region")
        regional_sentiment = filtered_df.groupby('REGION').agg({
            'SENTIMENT_SCORE': 'mean',
            'ORDER_ID': 'count'
        }).reset_index()
        regional_sentiment.columns = ['REGION', 'AVG_SENTIMENT', 'ORDER_COUNT']
        regional_sentiment = regional_sentiment.sort_values('AVG_SENTIMENT', ascending=True)
        
        fig = go.Figure()
        fig.add_trace(go.Bar(
            y=regional_sentiment['REGION'],
            x=regional_sentiment['AVG_SENTIMENT'],
            orientation='h',
            text=regional_sentiment['AVG_SENTIMENT'].round(2),
            textposition='outside',
            marker=dict(
                color=regional_sentiment['AVG_SENTIMENT'],
                colorscale='RdYlGn',
                showscale=True,
                colorbar=dict(title="Sentiment")
            ),
            hovertemplate='<b>%{y}</b><br>' +
                          'Avg Sentiment: %{x:.3f}<br>' +
                          'Orders: %{customdata}<extra></extra>',
            customdata=regional_sentiment['ORDER_COUNT']
        ))
        fig.update_layout(
            xaxis_title='Average Sentiment Score',
            yaxis_title='Region',
            height=400
        )
        fig.add_vline(x=0, line_dash="dash", line_color="gray", opacity=0.5)
        st.plotly_chart(fig, use_container_width=True)
        
        # Negative regions
        st.markdown("### 📍 Regions with Most Negative Feedback")
        negative_regions = regional_sentiment[regional_sentiment['AVG_SENTIMENT'] < 0]
        if len(negative_regions) > 0:
            for _, row in negative_regions.iterrows():
                st.error(f"**{row['REGION']}**: {row['AVG_SENTIMENT']:.3f} ({row['ORDER_COUNT']} orders)")
        else:
            st.success("✅ No regions with negative sentiment!")
        
        # Delivery issues table
        st.subheader("🚨 Delivery Issues Analysis")
        delivery_issues = filtered_df[
            (filtered_df['SENTIMENT_SCORE'] < 0) | (filtered_df['LATE'] == True)
        ].copy()
        
        if len(delivery_issues) > 0:
            issues_summary = delivery_issues.groupby(['REGION', 'PRODUCT']).agg({
                'ORDER_ID': 'count',
                'SENTIMENT_SCORE': 'mean',
                'LATE': lambda x: (x == True).sum()
            }).reset_index()
            
            issues_summary.columns = ['REGION', 'PRODUCT', 'TOTAL_ISSUES', 'AVG_SENTIMENT', 'LATE_COUNT']
            issues_summary['LATE_PCT'] = (issues_summary['LATE_COUNT'] / issues_summary['TOTAL_ISSUES'] * 100).round(1)
            issues_summary = issues_summary.sort_values('AVG_SENTIMENT', ascending=True)
            
            st.dataframe(issues_summary, use_container_width=True)
            
            # Top 3 problem areas
            st.markdown("### 🔴 Top 3 Problem Areas")
            top_issues = issues_summary.head(3)
            
            cols = st.columns(min(3, len(top_issues)))
            for idx, (col, (_, row)) in enumerate(zip(cols, top_issues.iterrows())):
                with col:
                    st.metric(
                        label=f"#{idx+1}: {row['REGION']}",
                        value=f"{row['AVG_SENTIMENT']:.2f}",
                        delta=f"{row['LATE_PCT']:.0f}% late",
                        delta_color="inverse"
                    )
                    st.caption(f"📦 {row['PRODUCT']}: {row['TOTAL_ISSUES']} orders")
        else:
            st.success("✅ No delivery issues found in the filtered data!")
        
        # Data preview
        with st.expander("📋 View Filtered Data"):
            st.dataframe(filtered_df, use_container_width=True)
        
        # Chatbot
        st.divider()
        st.subheader("💬 Ask Questions About Your Data")
        
        if "messages" not in st.session_state:
            st.session_state.messages = []
        
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])
        
        if prompt := st.chat_input("Ask about sentiment and delivery data..."):
            st.session_state.messages.append({"role": "user", "content": prompt})
            with st.chat_message("user"):
                st.markdown(prompt)
            
            with st.chat_message("assistant"):
                # Prepare context
                context = f"""You are analyzing customer review and shipping data for Avalanche products.

Current filtered dataset summary:
- Total reviews: {len(filtered_df)}
- Average sentiment: {filtered_df['SENTIMENT_SCORE'].mean():.3f}
- Late deliveries: {(filtered_df['LATE'] == True).sum()}
- Products analyzed: {filtered_df['PRODUCT'].nunique()}

Regional sentiment (top 5):
{regional_sentiment.head(5).to_string(index=False)}

User question: {prompt}

Provide a helpful, concise answer based on this data. Include specific numbers and insights."""
                
                # Escape single quotes for SQL
                escaped_context = context.replace("'", "''")
                
                # Call Cortex Complete
                cortex_query = f"""
                SELECT SNOWFLAKE.CORTEX.COMPLETE(
                    'mistral-large',
                    '{escaped_context}'
                ) as response
                """
                
                try:
                    cursor = conn.cursor()
                    cursor.execute(cortex_query)
                    response = cursor.fetchone()[0]
                    cursor.close()
                    st.markdown(response)
                    st.session_state.messages.append({"role": "assistant", "content": response})
                except Exception as e:
                    error_msg = f"Sorry, I encountered an error: {str(e)}"
                    st.error(error_msg)
                    st.session_state.messages.append({"role": "assistant", "content": error_msg})
        
    except Exception as e:
        st.error(f"❌ Error loading data: {str(e)}")
        st.info("Please check your table names and permissions.")

else:
    # Welcome screen when not connected
    st.title("🏔️ Avalanche Product Intelligence Dashboard")
    st.markdown("""
    ### Welcome!
    
    This dashboard analyzes customer sentiment and delivery performance for Avalanche products.
    
    **To get started:**
    1. 👈 Enter your Snowflake credentials in the sidebar
    2. Click "Connect to Snowflake"
    3. Explore your data!
    
    **Features:**
    - 📊 Real-time sentiment analysis by region
    - 🚚 Delivery performance tracking
    - 🗺️ Geographic insights
    - 💬 AI-powered chatbot for data queries
    """)
    
    st.info("👈 Please enter your Snowflake connection details in the sidebar to begin.")