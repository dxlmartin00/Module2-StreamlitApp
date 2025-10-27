import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import snowflake.connector

# Page configuration
st.set_page_config(
    page_title="Avalanche Product Intelligence",
    page_icon="🏔️",
    layout="wide",
    initial_sidebar_state="auto"  # Auto-collapse sidebar on mobile
)

# Mobile-friendly CSS + Floating Chatbot Widget
st.markdown("""
    <style>
    /* Improve mobile readability */
    @media (max-width: 768px) {
        .main .block-container {
            padding-left: 1rem;
            padding-right: 1rem;
            max-width: 100%;
        }

        /* Make metrics more readable on mobile */
        [data-testid="stMetricValue"] {
            font-size: 1.5rem;
        }

        /* Improve button sizing on mobile */
        .stButton button {
            font-size: 0.9rem;
            padding: 0.5rem 1rem;
        }

        /* Better spacing for mobile */
        h1 {
            font-size: 1.8rem !important;
        }

        h2 {
            font-size: 1.4rem !important;
        }

        h3 {
            font-size: 1.2rem !important;
        }
    }

    /* Ensure tables are scrollable on mobile */
    [data-testid="stDataFrame"] {
        overflow-x: auto;
    }

    /* Hide chatbot sections by default */
    .chatbot-hidden {
        display: none !important;
    }

    /* Floating Chatbot Toggle Button */
    .floating-chat-button {
        position: relative;
        width: 64px;
        height: 64px;
        border-radius: 50%;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        box-shadow: 0 4px 16px rgba(102, 126, 234, 0.4);
        display: flex;
        align-items: center;
        justify-content: center;
        cursor: pointer;
        transition: all 0.3s ease;
        border: none;
        animation: pulse 2s infinite;
    }

    @keyframes pulse {
        0%, 100% {
            box-shadow: 0 4px 16px rgba(102, 126, 234, 0.4);
        }
        50% {
            box-shadow: 0 4px 20px rgba(102, 126, 234, 0.6), 0 0 0 10px rgba(102, 126, 234, 0.1);
        }
    }

    .floating-chat-button:hover {
        transform: scale(1.1);
        box-shadow: 0 6px 24px rgba(102, 126, 234, 0.6);
        animation: none;
    }

    .floating-chat-button span {
        font-size: 32px;
    }

    /* Notification badge */
    .chat-badge {
        position: absolute;
        top: -4px;
        right: -4px;
        background: #ff4757;
        color: white;
        border-radius: 50%;
        width: 24px;
        height: 24px;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 12px;
        font-weight: bold;
        border: 3px solid white;
        animation: bounce 0.5s ease-in-out;
    }

    @keyframes bounce {
        0%, 100% { transform: scale(1); }
        50% { transform: scale(1.2); }
    }

    /* Hide the Streamlit toggle button container */
    .hidden-toggle-container {
        position: absolute !important;
        opacity: 0 !important;
        pointer-events: none !important;
        width: 0 !important;
        height: 0 !important;
        overflow: hidden !important;
        z-index: -1 !important;
    }

    /* Floating Chatbot Container */
    .floating-chatbot {
        position: fixed !important;
        bottom: 100px;
        right: 24px;
        width: 420px;
        max-width: calc(100vw - 48px);
        max-height: 650px;
        background: white;
        border-radius: 20px;
        box-shadow: 0 10px 40px rgba(0, 0, 0, 0.2);
        z-index: 999;
        overflow: hidden;
        animation: slideUpFade 0.3s ease-out;
    }

    @keyframes slideUpFade {
        from {
            opacity: 0;
            transform: translateY(30px) scale(0.95);
        }
        to {
            opacity: 1;
            transform: translateY(0) scale(1);
        }
    }

    /* Chatbot Header */
    .floating-chatbot .chatbot-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 24px;
        border-radius: 20px 20px 0 0;
    }

    .floating-chatbot .chatbot-title {
        font-size: 20px;
        font-weight: 700;
        margin: 0 0 4px 0;
        display: flex;
        align-items: center;
        gap: 8px;
    }

    .floating-chatbot .chatbot-subtitle {
        font-size: 13px;
        opacity: 0.95;
        margin: 0;
        font-weight: 400;
    }

    /* Chatbot Body */
    .floating-chatbot .chatbot-content {
        max-height: 520px;
        overflow-y: auto;
        padding: 16px;
        background: #f8f9fa;
    }

    /* Style Streamlit elements inside chatbot */
    .floating-chatbot .stChatMessage {
        background: white;
        border-radius: 12px;
        padding: 12px;
        margin-bottom: 12px;
        box-shadow: 0 2px 6px rgba(0, 0, 0, 0.05);
    }

    .floating-chatbot .stChatInput {
        border-radius: 0 0 20px 20px;
    }

    .floating-chatbot .stButton button {
        border-radius: 10px;
        transition: all 0.2s;
    }

    .floating-chatbot .stButton button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(102, 126, 234, 0.3);
    }

    @media (max-width: 768px) {
        .floating-chatbot {
            width: calc(100vw - 32px);
            right: 16px;
            bottom: 80px;
            max-height: calc(100vh - 120px);
        }

        .floating-chat-button {
            bottom: 16px;
            right: 16px;
            width: 56px;
            height: 56px;
        }

        .floating-chat-button span {
            font-size: 28px;
        }
    }
    </style>
    """, unsafe_allow_html=True)

# Auto-connect function using secrets
@st.cache_resource
def init_connection():
    """Initialize Snowflake connection using secrets"""
    try:
        # Try to use Streamlit secrets first (for deployed app)
        if "snowflake" in st.secrets:
            return snowflake.connector.connect(
                user=st.secrets["snowflake"]["user"],
                password=st.secrets["snowflake"]["password"],
                account=st.secrets["snowflake"]["account"],
                warehouse=st.secrets["snowflake"]["warehouse"],
                database=st.secrets["snowflake"]["database"],
                schema=st.secrets["snowflake"]["schema"]
            )
        else:
            # Fallback for local testing - hardcoded credentials
            # IMPORTANT: Remove these before deploying publicly!
            return snowflake.connector.connect(
                user="dxlmartin00",
                password="YOUR_PASSWORD_HERE",  # Replace with your password
                account="FUB92942",
                warehouse="COMPUTE_WH",
                database="AVALANCHE_DB",
                schema="AVALANCHE_SCHEMA"
            )
    except Exception as e:
        st.error(f"❌ Could not connect to Snowflake: {str(e)}")
        st.stop()

# Initialize connection
conn = init_connection()

# Load data with caching
@st.cache_data(ttl=600)
def load_data():
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
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)
    cursor.close()
    return df

# App header
st.title("🏔️ Avalanche Product Intelligence Dashboard")
st.markdown("### Customer Sentiment & Delivery Analysis")

# Show connection status
st.sidebar.success("✅ Connected to Snowflake")
st.sidebar.caption("Auto-connected as: dxlmartin00")

try:
    # Load data
    with st.spinner("Loading data from Snowflake..."):
        df = load_data()
    
    # Convert data types
    df['DATE'] = pd.to_datetime(df['DATE'])
    df['SENTIMENT_SCORE'] = pd.to_numeric(df['SENTIMENT_SCORE'], errors='coerce')
    df['DELIVERY_DAYS'] = pd.to_numeric(df['DELIVERY_DAYS'], errors='coerce')
    
    if df['LATE'].dtype == 'object':
        df['LATE'] = df['LATE'].astype(str).str.upper().isin(['TRUE', '1', 'YES', 'T'])
    else:
        df['LATE'] = df['LATE'].astype(bool)
    
    # Filters in sidebar
    st.sidebar.markdown("---")
    st.sidebar.header("📊 Filters")
    
    products = ['All'] + sorted(df['PRODUCT'].dropna().unique().tolist())
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
    
    # Key metrics - 2x2 grid for better mobile responsiveness
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Total Reviews", len(filtered_df))
    with col2:
        avg_sentiment = filtered_df['SENTIMENT_SCORE'].mean()
        st.metric("Avg Sentiment", f"{avg_sentiment:.3f}")

    col3, col4 = st.columns(2)
    with col3:
        late_count = filtered_df['LATE'].sum()
        st.metric("Late Deliveries", f"{int(late_count)}")
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
        height=max(300, min(600, len(regional_sentiment) * 50)),  # Dynamic height based on data
        margin=dict(l=20, r=20, t=20, b=20)  # Smaller margins for mobile
    )
    fig.add_vline(x=0, line_dash="dash", line_color="gray", opacity=0.5)
    st.plotly_chart(fig, use_container_width=True)
    
    # Negative regions
    st.markdown("### 📍 Regions with Most Negative Feedback")
    negative_regions = regional_sentiment[regional_sentiment['AVG_SENTIMENT'] < 0]
    if len(negative_regions) > 0:
        for _, row in negative_regions.iterrows():
            st.error(f"**{row['REGION']}**: {row['AVG_SENTIMENT']:.3f} ({int(row['ORDER_COUNT'])} orders)")
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
            'LATE': 'sum'
        }).reset_index()
        
        issues_summary.columns = ['REGION', 'PRODUCT', 'TOTAL_ISSUES', 'AVG_SENTIMENT', 'LATE_COUNT']
        issues_summary['LATE_PCT'] = (issues_summary['LATE_COUNT'] / issues_summary['TOTAL_ISSUES'] * 100).round(1)
        issues_summary = issues_summary.sort_values('AVG_SENTIMENT', ascending=True)
        
        st.dataframe(issues_summary, use_container_width=True)
        
        # Top 3 problem areas
        st.markdown("### 🔴 Top 3 Problem Areas")
        top_issues = issues_summary.head(3)

        # Stack vertically for better mobile readability
        for idx, (_, row) in enumerate(top_issues.iterrows()):
            col1, col2 = st.columns([2, 1])
            with col1:
                st.metric(
                    label=f"#{idx+1}: {row['REGION']}",
                    value=f"{row['AVG_SENTIMENT']:.2f}",
                    delta=f"{row['LATE_PCT']:.0f}% late",
                    delta_color="inverse"
                )
            with col2:
                st.caption(f"📦 {row['PRODUCT']}")
                st.caption(f"🔢 {int(row['TOTAL_ISSUES'])} orders")
    else:
        st.success("✅ No delivery issues found in the filtered data!")
    
    # Data preview
    with st.expander("📋 View Filtered Data"):
        st.dataframe(filtered_df, use_container_width=True)

    # Initialize chatbot state
    if "chatbot_open" not in st.session_state:
        st.session_state.chatbot_open = False
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Render the floating chat button using HTML + JavaScript for proper positioning
    button_emoji = '✕' if st.session_state.chatbot_open else '💬'
    message_count = len(st.session_state.messages)
    show_badge = not st.session_state.chatbot_open and message_count > 0

    button_html = f"""
    <div style="position: fixed; bottom: 24px; right: 24px; z-index: 1001;">
        <div class="floating-chat-button" id="chat-button-widget">
            <span>{button_emoji}</span>
            {f'<div class="chat-badge">{message_count // 2}</div>' if show_badge else ''}
        </div>
    </div>
    <script>
        // Add click handler to floating button
        const chatButton = document.getElementById('chat-button-widget');
        if (chatButton && !chatButton.hasAttribute('data-listener')) {{
            chatButton.setAttribute('data-listener', 'true');
            chatButton.addEventListener('click', function() {{
                // Find and click the hidden Streamlit button
                const buttons = window.parent.document.querySelectorAll('button');
                buttons.forEach(btn => {{
                    if (btn.textContent.includes('Toggle Chat') || btn.getAttribute('kind') === 'secondary') {{
                        btn.click();
                    }}
                }});
            }});
        }}
    </script>
    """
    st.markdown(button_html, unsafe_allow_html=True)

    # Hidden button for toggle functionality (triggered by floating button click)
    st.markdown('<div class="hidden-toggle-container">', unsafe_allow_html=True)
    if st.button("Toggle Chat", key="toggle_chat_btn", help="Open/Close AI Assistant"):
        st.session_state.chatbot_open = not st.session_state.chatbot_open
        st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)

    # Render floating chatbot when open
    if st.session_state.chatbot_open:
        # Inject JavaScript to apply floating-chatbot class to container
        st.markdown('<div class="floating-chatbot">', unsafe_allow_html=True)

        # Chatbot Header
        st.markdown("""
            <div class="chatbot-header">
                <div class="chatbot-title">💬 AI Data Assistant</div>
                <div class="chatbot-subtitle">Ask questions about your customer sentiment and delivery data</div>
            </div>
        """, unsafe_allow_html=True)

        # Chatbot Content
        st.markdown('<div class="chatbot-content">', unsafe_allow_html=True)

        # Close and Clear buttons row
        btn_col1, btn_col2, btn_col3 = st.columns([1, 1, 3])
        with btn_col1:
            if st.button("✕ Close", key="close_chat", use_container_width=True):
                st.session_state.chatbot_open = False
                st.rerun()
        with btn_col2:
            if st.button("🗑️ Clear", key="clear_chat", use_container_width=True):
                st.session_state.messages = []
                st.rerun()

        # Display chat history
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])

        # Suggested questions
        if len(st.session_state.messages) == 0:
            st.markdown("**💡 Suggested questions:**")
            suggestions = [
                "Which region has the worst sentiment?",
                "Show me the top 3 problem areas",
                "How many late deliveries do we have?",
                "Which product has the best reviews?"
            ]

            # Display suggestions as buttons
            for i, suggestion in enumerate(suggestions):
                if st.button(f"💭 {suggestion}", key=f"suggest_{i}", use_container_width=True):
                    st.session_state.temp_prompt = suggestion
                    st.rerun()

        st.markdown('</div>', unsafe_allow_html=True)  # Close chatbot-content

        # Handle suggested question click
        if "temp_prompt" in st.session_state:
            prompt = st.session_state.temp_prompt
            del st.session_state.temp_prompt
        else:
            prompt = st.chat_input("Ask me anything about the data...")

        st.markdown('</div>', unsafe_allow_html=True)  # Close floating-chatbot
    else:
        prompt = None

    # Process user input
    if prompt and st.session_state.chatbot_open:
        # Add user message
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
        
        # Generate AI response with streaming
        with st.chat_message("assistant"):
            message_placeholder = st.empty()
            full_response = ""
            
            # Show thinking animation
            thinking_placeholder = st.empty()
            thinking_placeholder.markdown("🤔 *Analyzing data...*")
            
            # Prepare comprehensive context with formatting instructions
            context = f"""You are an intelligent data analyst assistant for Avalanche's product team. 

Current Data Insights:
━━━━━━━━━━━━━━━━━━
📊 Overview:
- Total Reviews: {len(filtered_df)}
- Average Sentiment: {filtered_df['SENTIMENT_SCORE'].mean():.3f}
- Sentiment Range: {filtered_df['SENTIMENT_SCORE'].min():.2f} to {filtered_df['SENTIMENT_SCORE'].max():.2f}
- Late Deliveries: {int(filtered_df['LATE'].sum())} ({late_pct:.1f}%)
- On-Time Deliveries: {int((~filtered_df['LATE']).sum())}

📦 Products: {', '.join(filtered_df['PRODUCT'].unique()[:5].tolist())}
🗺️ Regions: {', '.join(filtered_df['REGION'].dropna().unique()[:8].tolist())}
📅 Date Range: {filtered_df['DATE'].min().strftime('%Y-%m-%d')} to {filtered_df['DATE'].max().strftime('%Y-%m-%d')}

🎯 Regional Performance (sorted by sentiment):
{regional_sentiment.to_string(index=False, max_rows=10)}

⚠️ Problem Areas:
{issues_summary[['REGION', 'PRODUCT', 'TOTAL_ISSUES', 'AVG_SENTIMENT', 'LATE_PCT']].head(5).to_string(index=False) if len(delivery_issues) > 0 else 'No major issues detected'}

User's Question: "{prompt}"

CRITICAL FORMATTING INSTRUCTIONS - You MUST follow these rules:

1. **For LIST questions** (top, best, worst, which, show me):
   - Use bullet points with **bold headings**
   - Format: **Region/Product Name**: Description with numbers
   - Example:
     • **Northeast**: Excellent performance with 0.52 sentiment (2.1 days avg delivery, 2% late)
     • **Southwest**: Needs attention with -0.18 sentiment (5.8 days avg, 28% late)

2. **For COMPARISON questions** (compare, versus, difference):
   - Use markdown table format
   - Include key metrics in columns
   - Example:
     | Region | Sentiment | Late % | Avg Days |
     |--------|-----------|--------|----------|
     | Northeast | 0.52 | 2% | 2.1 |
     | Southwest | -0.18 | 28% | 5.8 |

3. **For COUNT/NUMBER questions** (how many, total, count):
   - Start with the direct answer in **bold**
   - Add 1-2 sentence explanation
   - Example: **27 late deliveries** out of 100 total orders (27.0%). This represents a significant issue affecting more than a quarter of shipments.

4. **For ANALYSIS questions** (why, explain, what's causing):
   - Use short paragraphs (2-3 sentences max)
   - Bold key findings
   - Include bullet points for supporting evidence
   - Example structure:
     The main issue is **late deliveries in Southwest region**. Analysis shows:
     • 28% late delivery rate (vs 2% in Northeast)
     • Average 5.8 days delivery time
     • Negative sentiment correlation of -0.18

5. **For TREND questions** (over time, trending, changes):
   - Start with summary sentence
   - Use numbered list for timeline if applicable
   - Include specific dates and numbers

6. **For RECOMMENDATION questions** (what should we do, suggest, recommend):
   - Use numbered action items
   - Format: **1. Action**: Brief explanation with expected impact
   - Keep it actionable and specific

7. **General formatting rules**:
   - Always use **bold** for region names, product names, and key metrics
   - Use numbers with proper formatting (0.52 not .52, 27.0% not 27%)
   - Add relevant emojis for visual appeal (📊 📈 📉 ⚠️ ✅ ❌)
   - Keep response under 200 words
   - Use line breaks between sections for readability

Answer the user's question now, following the appropriate format based on the question type."""
            
            escaped_context = context.replace("'", "''")
            
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
                
                # Clear thinking indicator
                thinking_placeholder.empty()
                
                # Stream the response word by word
                import time
                words = response.split()
                
                for i, word in enumerate(words):
                    full_response += word + " "
                    
                    # Add typing cursor effect
                    message_placeholder.markdown(full_response + "▌")
                    
                    # Variable speed: faster for common words, slower for numbers/formatting
                    if any(char.isdigit() for char in word) or word in ['|', '**', '-', '•']:
                        time.sleep(0.04)  # Slower for numbers and formatting
                    else:
                        time.sleep(0.02)  # Normal speed
                
                # Display final response
                message_placeholder.markdown(full_response.strip())
                
                # Save to chat history
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": full_response.strip()
                })
                
            except Exception as e:
                thinking_placeholder.empty()
                error_msg = f"❌ Sorry, I encountered an error: {str(e)}\n\nPlease try rephrasing your question."
                message_placeholder.markdown(error_msg)
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": error_msg
                })
    
except Exception as e:
    st.error(f"❌ Error loading data: {str(e)}")
    st.info("Please contact the administrator if this issue persists.")