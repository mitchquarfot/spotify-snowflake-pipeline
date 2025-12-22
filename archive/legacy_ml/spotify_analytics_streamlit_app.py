# Spotify Analytics Dashboard - Snowflake Native Streamlit App
# Deploy this in Snowflake to create an interactive analytics dashboard

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import numpy as np
from snowflake.snowpark.context import get_active_session

# Get the current session (Snowflake native Streamlit)
session = get_active_session()

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def apply_spotify_theme(fig, title=None):
    """Apply Spotify color theme to Plotly figures"""
    fig.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='#191414',
        font_color='#FFFFFF',
        title_font_color='#1DB954',
        title_font_size=16,
        title_font_family='Arial, sans-serif'
    )
    
    if title:
        fig.update_layout(title=title)
    
    # Update axes
    fig.update_xaxes(
        gridcolor='#535353',
        linecolor='#535353',
        tickcolor='#B3B3B3',
        tickfont_color='#B3B3B3'
    )
    fig.update_yaxes(
        gridcolor='#535353', 
        linecolor='#535353',
        tickcolor='#B3B3B3',
        tickfont_color='#B3B3B3'
    )
    
    return fig

def filter_data_by_sidebar(data, genre_filters, time_filters, weekend_filter):
    """Apply sidebar filters to any dataset"""
    filtered = data.copy()
    
    # Apply genre filters (multiselect)
    if genre_filters and 'PRIMARY_GENRE' in filtered.columns:
        filtered = filtered[filtered['PRIMARY_GENRE'].isin(genre_filters)]
    
    # Apply time filters (multiselect)
    if time_filters and 'TIME_OF_DAY_CATEGORY' in filtered.columns:
        filtered = filtered[filtered['TIME_OF_DAY_CATEGORY'].isin(time_filters)]
    
    # Apply weekend filter (single select)
    if 'IS_WEEKEND' in filtered.columns:
        if weekend_filter == 'Weekends Only':
            filtered = filtered[filtered['IS_WEEKEND'] == True]
        elif weekend_filter == 'Weekdays Only':
            filtered = filtered[filtered['IS_WEEKEND'] == False]
    
    return filtered

# ============================================================================
# PAGE CONFIG
# ============================================================================

st.set_page_config(
    page_title="🎵 Spotify Analytics Dashboard",
    page_icon="🎵",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# SPOTIFY COLOR SCHEME AND STYLING
# ============================================================================

# Spotify brand colors
SPOTIFY_GREEN = '#1DB954'
SPOTIFY_BLACK = '#191414'
SPOTIFY_WHITE = '#FFFFFF'
SPOTIFY_GRAY = '#535353'
SPOTIFY_LIGHT_GRAY = '#B3B3B3'
SPOTIFY_DARK_GREEN = '#1ED760'
SPOTIFY_PALE_GREEN = '#1db95440'  # Green with transparency

# Custom CSS for Spotify theme - STRATEGIC TEXT COLORS BY BACKGROUND
st.markdown("""
<style>
    /* =================================================================
       CORE APP STYLING - Dark Background (#191414)
    ================================================================= */
    
    /* Main app background - DARK */
    .stApp {
        background-color: #191414 !important;
        color: #FFFFFF !important;
    }
    
    /* Main content area - DARK */
    .main .block-container {
        background-color: #191414 !important;
        color: #FFFFFF !important;
    }
    
    /* Headers on dark background - GREEN */
    .main h1, .main h2, .main h3, .main h4, .main h5, .main h6 {
        color: #1DB954 !important;
        font-weight: bold !important;
    }
    
    /* Text on dark main area - WHITE */
    .main p, .main div:not([data-testid]), .main span {
        color: #FFFFFF !important;
    }
    
    /* =================================================================
       SIDEBAR STYLING - Mixed Backgrounds
    ================================================================= */
    
    /* Sidebar background - DARK */
    .css-1d391kg, section[data-testid="stSidebar"] {
        background-color: #0F0F0F !important;
    }
    
    /* Sidebar headers and titles - GREEN on dark */
    .css-1d391kg h1, .css-1d391kg h2, .css-1d391kg h3,
    section[data-testid="stSidebar"] h1, 
    section[data-testid="stSidebar"] h2, 
    section[data-testid="stSidebar"] h3 {
        color: #1DB954 !important;
        font-weight: bold !important;
    }
    
    /* Sidebar labels - GREEN on dark - COMPREHENSIVE TARGETING */
    section[data-testid="stSidebar"] label,
    .css-1d391kg label,
    section[data-testid="stSidebar"] .stSelectbox label,
    section[data-testid="stSidebar"] .stDateInput label,
    section[data-testid="stSidebar"] .stMultiSelect label,
    section[data-testid="stSidebar"] .stSelectbox > label,
    section[data-testid="stSidebar"] .stMultiSelect > label,
    section[data-testid="stSidebar"] .stDateInput > label,
    section[data-testid="stSidebar"] .stSelectbox div[data-testid="stSelectboxLabel"],
    section[data-testid="stSidebar"] .stMultiSelect div[data-testid="stMultiSelectLabel"],
    section[data-testid="stSidebar"] [data-testid*="label"],
    section[data-testid="stSidebar"] [class*="label"],
    .css-1d391kg .stSelectbox label,
    .css-1d391kg .stMultiSelect label {
        color: #1DB954 !important;
        font-weight: bold !important;
        font-size: 0.9rem !important;
    }
    
    /* Additional selectors for any text elements in sidebar that should be visible */
    section[data-testid="stSidebar"] > div label,
    section[data-testid="stSidebar"] > div > div label,
    section[data-testid="stSidebar"] .element-container label,
    section[data-testid="stSidebar"] .stFormHelperText {
        color: #1DB954 !important;
        font-weight: bold !important;
    }
    
    /* ULTIMATE NUCLEAR OPTION - Force ALL sidebar text to be visible */
    section[data-testid="stSidebar"] * {
        color: #FFFFFF !important;
    }
    
    /* Then specifically make labels green */
    section[data-testid="stSidebar"] label,
    section[data-testid="stSidebar"] .stSelectbox label,
    section[data-testid="stSidebar"] .stMultiSelect label,
    section[data-testid="stSidebar"] .stDateInput label {
        color: #1DB954 !important;
        font-weight: bold !important;
    }
    
    /* Input field text should be black on white background */
    section[data-testid="stSidebar"] input,
    section[data-testid="stSidebar"] .stSelectbox input,
    section[data-testid="stSidebar"] .stSelectbox div[role="combobox"],
    section[data-testid="stSidebar"] .stSelectbox div[data-baseweb="select"] {
        color: #000000 !important;
        background-color: #FFFFFF !important;
    }
    
    /* Input fields in sidebar - COMPREHENSIVE TARGETING for LIGHT backgrounds */
    
    /* All input elements in sidebar */
    section[data-testid="stSidebar"] input,
    section[data-testid="stSidebar"] .stSelectbox input,
    section[data-testid="stSidebar"] .stDateInput input,
    .css-1d391kg input {
        background-color: #FFFFFF !important;
        color: #000000 !important;
        border: 1px solid #535353 !important;
    }
    
    /* Selectbox containers and text */
    section[data-testid="stSidebar"] .stSelectbox > div,
    section[data-testid="stSidebar"] .stSelectbox > div > div,
    section[data-testid="stSidebar"] .stSelectbox div[data-baseweb="select"],
    section[data-testid="stSidebar"] .stSelectbox div[data-baseweb="select"] > div,
    .css-1d391kg .stSelectbox > div,
    .css-1d391kg .stSelectbox > div > div {
        background-color: #FFFFFF !important;
        color: #000000 !important;
        border: 1px solid #535353 !important;
    }
    
    /* Date input containers */
    section[data-testid="stSidebar"] .stDateInput > div,
    section[data-testid="stSidebar"] .stDateInput > div > div,
    section[data-testid="stSidebar"] .stDateInput > div > div > div,
    .css-1d391kg .stDateInput > div > div > div {
        background-color: #FFFFFF !important;
        color: #000000 !important;
        border: 1px solid #535353 !important;
    }
    
    /* Dropdown menu items when opened */
    section[data-testid="stSidebar"] .stSelectbox [role="listbox"],
    section[data-testid="stSidebar"] .stSelectbox [role="option"],
    section[data-testid="stSidebar"] .stSelectbox ul,
    section[data-testid="stSidebar"] .stSelectbox li {
        background-color: #FFFFFF !important;
        color: #000000 !important;
    }
    
    /* Additional fallback selectors for select elements */
    section[data-testid="stSidebar"] select,
    section[data-testid="stSidebar"] option,
    .css-1d391kg select,
    .css-1d391kg option {
        background-color: #FFFFFF !important;
        color: #000000 !important;
    }
    
    /* Force all text in sidebar inputs to be black */
    section[data-testid="stSidebar"] .stSelectbox *,
    section[data-testid="stSidebar"] .stDateInput *,
    section[data-testid="stSidebar"] input *,
    section[data-testid="stSidebar"] .stMultiSelect * {
        color: #000000 !important;
    }
    
    /* Additional nuclear option for dropdown content */
    .css-1d391kg [data-baseweb] *,
    section[data-testid="stSidebar"] [data-baseweb] *,
    section[data-testid="stSidebar"] div[role="listbox"] *,
    section[data-testid="stSidebar"] div[role="option"] *,
    section[data-testid="stSidebar"] .Select-menu *,
    section[data-testid="stSidebar"] .Select-option * {
        color: #000000 !important;
        background-color: #FFFFFF !important;
    }
    
    /* Override any remaining white text */
    section[data-testid="stSidebar"] [style*="color: white"],
    section[data-testid="stSidebar"] [style*="color: #FFFFFF"],
    section[data-testid="stSidebar"] [style*="color: #ffffff"] {
        color: #000000 !important;
    }
    
    /* =================================================================
       METRIC CARDS - Dark Background with Styled Values
    ================================================================= */
    
    [data-testid="metric-container"] {
        background: linear-gradient(135deg, #0F0F0F 0%, #191414 100%) !important;
        border: 2px solid #1DB954 !important;
        padding: 1.2rem !important;
        border-radius: 0.8rem !important;
        box-shadow: 0 4px 8px rgba(29, 185, 84, 0.2) !important;
    }
    
    /* Metric labels - WHITE on dark */
    [data-testid="metric-container"] [data-testid="metric-label"] {
        color: #FFFFFF !important;
        font-weight: 600 !important;
        font-size: 1rem !important;
    }
    
    /* Metric values - COLORED on dark */
    [data-testid="metric-container"] [data-testid="metric-value"] {
        font-size: 2.2rem !important;
        font-weight: 700 !important;
    }
    
    /* Different colors for each metric value */
    [data-testid="metric-container"]:nth-child(1) [data-testid="metric-value"] {
        color: #1DB954 !important; /* Spotify Green */
    }

    [data-testid="metric-container"]:nth-child(2) [data-testid="metric-value"] {
        color: #1ED760 !important; /* Light Green */
    }

    [data-testid="metric-container"]:nth-child(3) [data-testid="metric-value"] {
        color: #4ECDC4 !important; /* Teal */
    }

    [data-testid="metric-container"]:nth-child(4) [data-testid="metric-value"] {
        color: #45B7D1 !important; /* Blue */
    }

    [data-testid="metric-container"]:nth-child(5) [data-testid="metric-value"] {
        color: #96CEB4 !important; /* Pale Green */
    }
    
    /* =================================================================
       TAB STYLING - Mixed Backgrounds
    ================================================================= */
    
    /* Tab container - DARK */
    .stTabs [data-baseweb="tab-list"] {
        background-color: #0F0F0F !important;
    }
    
    /* Inactive tabs - DARK background, LIGHT text */
    .stTabs [data-baseweb="tab"] {
        background-color: #0F0F0F !important;
        color: #B3B3B3 !important;
        border-color: #535353 !important;
    }
    
    /* Active tab - LIGHT background (green), DARK text */
    .stTabs [aria-selected="true"] {
        background-color: #1DB954 !important;
        color: #000000 !important;
        font-weight: bold !important;
    }
    
    /* Tab content - DARK background, WHITE text */
    .stTabs [data-baseweb="tab-panel"] {
        background-color: #191414 !important;
        color: #FFFFFF !important;
    }
    
    /* =================================================================
       DATAFRAMES AND TABLES - Dark Background
    ================================================================= */
    
    /* Table container - DARK */
    .stDataFrame {
        background-color: #0F0F0F !important;
    }
    
    /* Table cells - DARK background, WHITE text */
    .stDataFrame td {
        background-color: #0F0F0F !important;
        color: #FFFFFF !important;
        border-color: #535353 !important;
    }
    
    /* Table headers - GREEN background, BLACK text */
    .stDataFrame th {
        background-color: #1DB954 !important;
        color: #000000 !important;
        font-weight: bold !important;
        border-color: #535353 !important;
    }
    
    /* =================================================================
       PLOTLY CHARTS - Background Handling
    ================================================================= */
    
    /* Chart containers - DARK */
    .stPlotlyChart {
        background-color: #191414 !important;
    }
    
    /* =================================================================
       ALERT MESSAGES - Various Backgrounds
    ================================================================= */
    
    /* Info alerts - DARK background, WHITE text */
    .stAlert[data-baseweb="notification"] {
        background-color: #0F0F0F !important;
        color: #FFFFFF !important;
        border: 1px solid #1DB954 !important;
    }
    
    /* Success messages - DARK green, GREEN text */
    .stSuccess {
        background-color: #0F2F0F !important;
        color: #1DB954 !important;
        border: 1px solid #1DB954 !important;
    }
    
    /* Warning messages - DARK yellow, YELLOW text */
    .stWarning {
        background-color: #2F2F0F !important;
        color: #FFEAA7 !important;
        border: 1px solid #FFEAA7 !important;
    }
    
    /* Error messages - DARK red, RED text */
    .stError {
        background-color: #2F0F0F !important;
        color: #FF6B6B !important;
        border: 1px solid #FF6B6B !important;
    }
    
    /* =================================================================
       FORM ELEMENTS - Light Background Inputs Need Dark Text
    ================================================================= */
    
    /* Text inputs - LIGHT background, DARK text */
    .stTextInput input {
        background-color: #FFFFFF !important;
        color: #000000 !important;
        border: 1px solid #535353 !important;
    }
    
    /* Number inputs - LIGHT background, DARK text */
    .stNumberInput input {
        background-color: #FFFFFF !important;
        color: #000000 !important;
        border: 1px solid #535353 !important;
    }
    
    /* Textarea - LIGHT background, DARK text */
    .stTextArea textarea {
        background-color: #FFFFFF !important;
        color: #000000 !important;
        border: 1px solid #535353 !important;
    }
    
    /* =================================================================
       SPECIFIC OVERRIDES for Common Issues
    ================================================================= */
    
    /* Make sure metric labels are always visible */
    [data-testid="metric-container"] > div:first-child {
        color: #FFFFFF !important;
    }
    
    /* Make sure metric values are always colored */
    [data-testid="metric-container"] > div:last-child {
        color: #1DB954 !important;
    }
    
    /* Ensure sidebar text is readable */
    section[data-testid="stSidebar"] p,
    section[data-testid="stSidebar"] div:not([class*="baseweb"]):not([data-testid]) {
        color: #FFFFFF !important;
    }
    
</style>
""", unsafe_allow_html=True)

# Spotify color palette for charts
SPOTIFY_COLORS = [
    '#1DB954',  # Spotify Green
    '#1ED760',  # Light Green  
    '#535353',  # Gray
    '#B3B3B3',  # Light Gray
    '#FFFFFF',  # White
    '#FF6B6B',  # Accent Red
    '#4ECDC4',  # Accent Teal
    '#45B7D1',  # Accent Blue
    '#96CEB4',  # Pale Green
    '#FFEAA7'   # Accent Yellow
]

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

@st.cache_data
def load_daily_summary():
    """Load daily listening summary data"""
    try:
        return session.sql("""
            SELECT *
            FROM spotify_analytics.medallion_arch.gold_daily_listening_summary
            ORDER BY denver_date DESC
        """).to_pandas()
    except Exception as e:
        st.error(f"Error loading daily summary data: {e}")
        return pd.DataFrame()

@st.cache_data
def load_genre_analysis():
    """Load genre analysis data"""
    try:
        return session.sql("""
            SELECT *
            FROM spotify_analytics.medallion_arch.gold_genre_analysis
            ORDER BY total_plays DESC
        """).to_pandas()
    except Exception as e:
        st.error(f"Error loading genre analysis data: {e}")
        return pd.DataFrame()

@st.cache_data
def load_monthly_insights():
    """Load monthly insights data"""
    try:
        return session.sql("""
            SELECT *
            FROM spotify_analytics.medallion_arch.gold_monthly_insights
            ORDER BY year DESC, month DESC
        """).to_pandas()
    except Exception as e:
        st.error(f"Error loading monthly insights data: {e}")
        return pd.DataFrame()

@st.cache_data
def load_artist_summary():
    """Load artist summary data"""
    try:
        return session.sql("""
            SELECT *
            FROM spotify_analytics.medallion_arch.silver_artist_summary
            WHERE total_plays >= 5
            ORDER BY total_plays DESC
            LIMIT 200
        """).to_pandas()
    except Exception as e:
        st.error(f"Error loading artist summary data: {e}")
        return pd.DataFrame()

@st.cache_data
def load_listening_data(start_date, end_date):
    """Load detailed listening data for date range"""
    try:
        # Convert dates to string format for SQL query
        start_str = pd.to_datetime(start_date).strftime('%Y-%m-%d')
        end_str = pd.to_datetime(end_date).strftime('%Y-%m-%d')
        
        return session.sql(f"""
            SELECT 
                denver_date,
                denver_timestamp,
                denver_hour,
                time_of_day_category,
                is_weekend,
                track_name,
                primary_artist_name,
                primary_genre,
                album_name,
                track_duration_minutes,
                track_popularity,
                artist_popularity,
                listening_source
            FROM spotify_analytics.medallion_arch.silver_listening_enriched
            WHERE denver_date BETWEEN '{start_str}' AND '{end_str}'
            ORDER BY denver_timestamp DESC
        """).to_pandas()
    except Exception as e:
        st.error(f"Error loading listening data: {e}")
        return pd.DataFrame()

# ============================================================================
# SIDEBAR FILTERS
# ============================================================================

st.sidebar.title("🎛️ Filters")

# Load data for filter options
daily_data = load_daily_summary()
genre_data = load_genre_analysis()

# Date range filter
if not daily_data.empty:
    try:
        # Ensure DENVER_DATE is datetime type and extract date
        daily_data['DENVER_DATE'] = pd.to_datetime(daily_data['DENVER_DATE'])
        min_date = daily_data['DENVER_DATE'].dt.date.min()
        max_date = daily_data['DENVER_DATE'].dt.date.max()
        
        date_range = st.sidebar.date_input(
            "📅 Date Range",
            value=(max_date - timedelta(days=30), max_date),
            min_value=min_date,
            max_value=max_date
        )
        
        if len(date_range) == 2:
            start_date, end_date = date_range
        else:
            start_date = end_date = date_range[0]
    except Exception as e:
        st.warning(f"Date filter issue: {e}. Using default date range.")
        start_date = end_date = datetime.now().date()
else:
    start_date = end_date = datetime.now().date()

# Genre filter - multiselect
if not genre_data.empty:
    available_genres = list(genre_data['PRIMARY_GENRE'].dropna().unique())
    selected_genres = st.sidebar.multiselect(
        "🎨 Genre", 
        options=available_genres,
        default=[],
        help="Select one or more genres (leave empty for all)"
    )
else:
    selected_genres = []

# Time of day filter - multiselect
time_periods = ['Morning', 'Afternoon', 'Evening', 'Night']
selected_times = st.sidebar.multiselect(
    "⏰ Time of Day",
    options=time_periods,
    default=[],
    help="Select one or more time periods (leave empty for all)"
)

# Weekend filter
weekend_filter = st.sidebar.selectbox("📅 Weekend/Weekday", ['All', 'Weekends Only', 'Weekdays Only'])

# ============================================================================
# MAIN DASHBOARD
# ============================================================================

st.title("🎵 Spotify Analytics Dashboard")
st.markdown("### Explore your personal music listening patterns and discoveries")

# ============================================================================
# KEY METRICS ROW
# ============================================================================

# Filter daily data based on selections
# Convert dates to ensure proper comparison
if not daily_data.empty:
    # Ensure DENVER_DATE is datetime type
    daily_data['DENVER_DATE'] = pd.to_datetime(daily_data['DENVER_DATE'])
    
    # Convert start_date and end_date to datetime for comparison
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)
    
    filtered_daily = daily_data[
        (daily_data['DENVER_DATE'] >= start_dt) &
        (daily_data['DENVER_DATE'] <= end_dt)
    ]
else:
    filtered_daily = daily_data

if weekend_filter == 'Weekends Only':
    filtered_daily = filtered_daily[filtered_daily['IS_WEEKEND'] == True]
elif weekend_filter == 'Weekdays Only':
    filtered_daily = filtered_daily[filtered_daily['IS_WEEKEND'] == False]

# Calculate key metrics
if not filtered_daily.empty:
    total_plays = filtered_daily['TOTAL_PLAYS'].sum()
    unique_tracks = filtered_daily['UNIQUE_TRACKS'].sum()
    unique_artists = filtered_daily['UNIQUE_ARTISTS'].sum()
    total_hours = filtered_daily['TOTAL_LISTENING_MINUTES'].sum() / 60
    avg_daily_plays = filtered_daily['TOTAL_PLAYS'].mean()
    
    # Replace this entire section:
    # col1, col2, col3, col4, col5 = st.columns(5)
    # with col1: st.metric(...)

    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, #0F0F0F 0%, #191414 100%);
            border: 2px solid #1DB954;
            padding: 1.2rem;
            border-radius: 0.8rem;
            box-shadow: 0 4px 8px rgba(29, 185, 84, 0.2);
            text-align: center;
            margin-bottom: 1rem;
        ">
            <div style="color: #FFFFFF; font-size: 0.9rem; font-weight: 600; margin-bottom: 0.5rem;">
                🎵 Total Plays
            </div>
            <div style="color: #1DB954; font-size: 2.2rem; font-weight: 700; text-shadow: 0 2px 4px rgba(29, 185, 84, 0.3);">
                {total_plays:,}
            </div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, #0F0F0F 0%, #191414 100%);
            border: 2px solid #1ED760;
            padding: 1.2rem;
            border-radius: 0.8rem;
            box-shadow: 0 4px 8px rgba(30, 215, 96, 0.2);
            text-align: center;
            margin-bottom: 1rem;
        ">
            <div style="color: #FFFFFF; font-size: 0.9rem; font-weight: 600; margin-bottom: 0.5rem;">
                🎤 Unique Tracks
            </div>
            <div style="color: #1ED760; font-size: 2.2rem; font-weight: 700; text-shadow: 0 2px 4px rgba(30, 215, 96, 0.3);">
                {unique_tracks:,}
            </div>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, #0F0F0F 0%, #191414 100%);
            border: 2px solid #4ECDC4;
            padding: 1.2rem;
            border-radius: 0.8rem;
            box-shadow: 0 4px 8px rgba(78, 205, 196, 0.2);
            text-align: center;
            margin-bottom: 1rem;
        ">
            <div style="color: #FFFFFF; font-size: 0.9rem; font-weight: 600; margin-bottom: 0.5rem;">
                👨‍🎤 Unique Artists
            </div>
            <div style="color: #4ECDC4; font-size: 2.2rem; font-weight: 700; text-shadow: 0 2px 4px rgba(78, 205, 196, 0.3);">
                {unique_artists:,}
            </div>
        </div>
        """, unsafe_allow_html=True)

    with col4:
        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, #0F0F0F 0%, #191414 100%);
            border: 2px solid #45B7D1;
            padding: 1.2rem;
            border-radius: 0.8rem;
            box-shadow: 0 4px 8px rgba(69, 183, 209, 0.2);
            text-align: center;
            margin-bottom: 1rem;
        ">
            <div style="color: #FFFFFF; font-size: 0.9rem; font-weight: 600; margin-bottom: 0.5rem;">
                ⏱️ Hours Listened
            </div>
            <div style="color: #45B7D1; font-size: 2.2rem; font-weight: 700; text-shadow: 0 2px 4px rgba(69, 183, 209, 0.3);">
                {total_hours:.1f}
            </div>
        </div>
        """, unsafe_allow_html=True)

    with col5:
        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, #0F0F0F 0%, #191414 100%);
            border: 2px solid #96CEB4;
            padding: 1.2rem;
            border-radius: 0.8rem;
            box-shadow: 0 4px 8px rgba(150, 206, 180, 0.2);
            text-align: center;
            margin-bottom: 1rem;
        ">
            <div style="color: #FFFFFF; font-size: 0.9rem; font-weight: 600; margin-bottom: 0.5rem;">
                📊 Avg Daily Plays
            </div>
            <div style="color: #96CEB4; font-size: 2.2rem; font-weight: 700; text-shadow: 0 2px 4px rgba(150, 206, 180, 0.3);">
                {avg_daily_plays:.1f}
            </div>
        </div>
        """, unsafe_allow_html=True)

# ============================================================================
# TABS FOR DIFFERENT ANALYSES
# ============================================================================

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["📈 Trends", "🎨 Genres", "👨‍🎤 Artists", "⏰ Time Patterns", "🔍 Detailed View", "🤖 ML Recommendations"])

# ============================================================================
# TAB 1: LISTENING TRENDS
# ============================================================================

with tab1:
    st.header("📈 Listening Trends Over Time")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Daily listening trend
        if not filtered_daily.empty:
            fig_daily = px.line(
                filtered_daily,
                x='DENVER_DATE',
                y='TOTAL_PLAYS',
                title='Daily Listening Activity',
                labels={'TOTAL_PLAYS': 'Number of Plays', 'DENVER_DATE': 'Date'},
                color_discrete_sequence=[SPOTIFY_GREEN]
            )
            fig_daily.update_layout(height=400)
            fig_daily = apply_spotify_theme(fig_daily)
            st.plotly_chart(fig_daily, use_container_width=True)
        
        # Genre diversity over time
        if not filtered_daily.empty:
            fig_diversity = px.line(
                filtered_daily,
                x='DENVER_DATE',
                y='GENRE_DIVERSITY_SCORE',
                title='Genre Diversity Score Over Time',
                labels={'GENRE_DIVERSITY_SCORE': 'Genre Diversity %', 'DENVER_DATE': 'Date'},
                color_discrete_sequence=[SPOTIFY_DARK_GREEN]
            )
            fig_diversity.update_layout(height=400)
            fig_diversity = apply_spotify_theme(fig_diversity)
            st.plotly_chart(fig_diversity, use_container_width=True)
    
    with col2:
        # Weekly pattern
        if not filtered_daily.empty:
            weekly_avg = filtered_daily.groupby('DAY_OF_WEEK')['TOTAL_PLAYS'].mean().reset_index()
            
            if not weekly_avg.empty:
                # Map abbreviated day names to full names and set correct order (Sunday first)
                day_mapping = {
                    'Sun': 'Sunday',
                    'Mon': 'Monday', 
                    'Tue': 'Tuesday',
                    'Wed': 'Wednesday',
                    'Thu': 'Thursday',
                    'Fri': 'Friday',
                    'Sat': 'Saturday'
                }
                
                # Map to full day names
                weekly_avg['DAY_FULL'] = weekly_avg['DAY_OF_WEEK'].map(day_mapping)
                
                # Set correct order (Sunday through Saturday)
                day_order = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
                weekly_avg['DAY_FULL'] = pd.Categorical(weekly_avg['DAY_FULL'], categories=day_order, ordered=True)
                weekly_avg = weekly_avg.sort_values('DAY_FULL')
                
                fig_weekly = px.line(
                    weekly_avg,
                    x='DAY_FULL',
                    y='TOTAL_PLAYS',
                    title='Average Plays by Day of Week',
                    labels={'TOTAL_PLAYS': 'Average Plays', 'DAY_FULL': 'Day'},
                    color_discrete_sequence=[SPOTIFY_GREEN],
                    markers=True
                )
                fig_weekly.update_traces(
                    line=dict(color=SPOTIFY_GREEN, width=3),
                    marker=dict(color=SPOTIFY_GREEN, size=8)
                )
                fig_weekly.update_layout(height=400)
                fig_weekly = apply_spotify_theme(fig_weekly)
                st.plotly_chart(fig_weekly, use_container_width=True)
        
        # Monthly trend
        monthly_data = load_monthly_insights()
        if not monthly_data.empty:
            monthly_data['date'] = pd.to_datetime(monthly_data[['YEAR', 'MONTH']].assign(day=1))
            
            # Convert start_date and end_date to datetime for comparison
            start_dt = pd.to_datetime(start_date)
            end_dt = pd.to_datetime(end_date)
            
            monthly_filtered = monthly_data[
                (monthly_data['date'] >= start_dt) &
                (monthly_data['date'] <= end_dt)
            ]
            
            if not monthly_filtered.empty:
                fig_monthly = px.bar(
                    monthly_filtered,
                    x='MONTH_NAME',
                    y='TOTAL_PLAYS',
                    title='Monthly Listening Activity',
                    labels={'TOTAL_PLAYS': 'Total Plays', 'MONTH_NAME': 'Month'},
                    color_discrete_sequence=[SPOTIFY_DARK_GREEN]
                )
                fig_monthly.update_layout(height=400)
                fig_monthly = apply_spotify_theme(fig_monthly)
                st.plotly_chart(fig_monthly, use_container_width=True)

# ============================================================================
# TAB 2: GENRE ANALYSIS
# ============================================================================

with tab2:
    st.header("🎨 Genre Analysis")
    
    # Apply filters to genre data based on time and weekend selections
    if not genre_data.empty:
        # Load detailed listening data to apply filters
        listening_detail = load_listening_data(start_date, end_date)
        if not listening_detail.empty:
            # Apply sidebar filters to the detailed data
            filtered_listening = filter_data_by_sidebar(listening_detail, selected_genres, selected_times, weekend_filter)
            
            # Recalculate genre stats from filtered data
            if not filtered_listening.empty:
                genre_stats = filtered_listening.groupby('PRIMARY_GENRE').agg({
                    'TRACK_NAME': 'count',  # Total plays
                    'PRIMARY_ARTIST_NAME': 'nunique',  # Unique artists
                    'TRACK_DURATION_MINUTES': 'sum'  # Total listening time
                }).round(2)
                genre_stats.columns = ['TOTAL_PLAYS', 'UNIQUE_ARTISTS', 'TOTAL_LISTENING_MINUTES']
                genre_stats = genre_stats.reset_index().sort_values('TOTAL_PLAYS', ascending=False)
                genre_stats['PERCENTAGE_OF_TOTAL_LISTENING'] = (
                    100 * genre_stats['TOTAL_PLAYS'] / genre_stats['TOTAL_PLAYS'].sum()
                ).round(2)
            else:
                genre_stats = pd.DataFrame()
        else:
            genre_stats = genre_data  # Fallback to original data
    else:
        genre_stats = genre_data
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Top genres pie chart
        if not genre_stats.empty:
            top_genres = genre_stats.head(10)
            fig_pie = px.pie(
                top_genres,
                values='TOTAL_PLAYS',
                names='PRIMARY_GENRE',
                title='Top 10 Genres by Plays',
                color_discrete_sequence=SPOTIFY_COLORS
            )
            fig_pie.update_layout(height=500)
            fig_pie = apply_spotify_theme(fig_pie)
            st.plotly_chart(fig_pie, use_container_width=True)
    
    with col2:
        # Genre metrics bar chart
        if not genre_stats.empty:
            fig_bar = px.bar(
                genre_stats.head(15),
                x='TOTAL_PLAYS',
                y='PRIMARY_GENRE',
                orientation='h',
                title='Top 15 Genres by Total Plays',
                labels={'TOTAL_PLAYS': 'Total Plays', 'PRIMARY_GENRE': 'Genre'},
                color_discrete_sequence=[SPOTIFY_GREEN]
            )
            fig_bar.update_layout(height=500)
            fig_bar = apply_spotify_theme(fig_bar)
            st.plotly_chart(fig_bar, use_container_width=True)
    
    # Genre details table
    st.subheader("🎵 Genre Details")
    if not genre_stats.empty:
        display_genres = genre_stats[['PRIMARY_GENRE', 'TOTAL_PLAYS', 'UNIQUE_ARTISTS', 
                                    'TOTAL_LISTENING_MINUTES', 'PERCENTAGE_OF_TOTAL_LISTENING']].head(20)
        
        # Format the dataframe for better display
        display_genres['TOTAL_LISTENING_HOURS'] = (display_genres['TOTAL_LISTENING_MINUTES'] / 60).round(1)
        display_genres = display_genres.drop('TOTAL_LISTENING_MINUTES', axis=1)
        
        st.dataframe(
            display_genres,
            use_container_width=True,
            column_config={
                'PRIMARY_GENRE': 'Genre',
                'TOTAL_PLAYS': st.column_config.NumberColumn('Total Plays', format='%d'),
                'UNIQUE_ARTISTS': st.column_config.NumberColumn('Unique Artists', format='%d'),
                'UNIQUE_TRACKS': st.column_config.NumberColumn('Unique Tracks', format='%d'),
                'TOTAL_LISTENING_HOURS': st.column_config.NumberColumn('Hours Listened', format='%.1f'),
                'PERCENTAGE_OF_TOTAL_LISTENING': st.column_config.NumberColumn('% of Total', format='%.1f%%')
            }
        )

# ============================================================================
# TAB 3: ARTIST ANALYSIS
# ============================================================================

with tab3:
    st.header("👨‍🎤 Artist Analysis")
    
    # Apply filters to artist data
    artist_data = load_artist_summary()
    if not artist_data.empty:
        # Apply genre filters if selected
        if selected_genres:
            artist_data = artist_data[artist_data['PRIMARY_GENRE'].isin(selected_genres)]
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Top artists
        if not artist_data.empty:
            top_artists = artist_data.head(15)
            fig_artists = px.bar(
                top_artists,
                x='TOTAL_PLAYS',
                y='ARTIST_NAME',
                orientation='h',
                title='Top 15 Most Played Artists',
                labels={'TOTAL_PLAYS': 'Total Plays', 'ARTIST_NAME': 'Artist'},
                color_discrete_sequence=[SPOTIFY_GREEN]
            )
            fig_artists.update_layout(height=600)
            fig_artists = apply_spotify_theme(fig_artists)
            st.plotly_chart(fig_artists, use_container_width=True)
    
    with col2:
        # Artist discovery scatter plot
        if not artist_data.empty:
            fig_scatter = px.scatter(
                artist_data.head(50),
                x='TOTAL_LISTENING_MINUTES',
                y='UNIQUE_TRACKS_PLAYED',
                size='TOTAL_PLAYS',
                color='PRIMARY_GENRE',
                hover_name='ARTIST_NAME',
                title='Artist Discovery: Listening Time vs Track Diversity',
                labels={
                    'TOTAL_LISTENING_MINUTES': 'Total Listening Minutes',
                    'UNIQUE_TRACKS_PLAYED': 'Unique Tracks Played'
                },
                color_discrete_sequence=SPOTIFY_COLORS
            )
            fig_scatter.update_layout(height=600)
            fig_scatter = apply_spotify_theme(fig_scatter)
            st.plotly_chart(fig_scatter, use_container_width=True)
    
    # Artist details table
    st.subheader("🎤 Artist Details")
    if not artist_data.empty:
        display_artists = artist_data[['ARTIST_NAME', 'PRIMARY_GENRE', 'TOTAL_PLAYS', 
                                     'UNIQUE_TRACKS_PLAYED', 'TOTAL_LISTENING_MINUTES',
                                     'ARTIST_POPULARITY', 'WEEKEND_PLAY_PERCENTAGE']].head(25)
        
        # Format the dataframe
        display_artists['TOTAL_LISTENING_HOURS'] = (display_artists['TOTAL_LISTENING_MINUTES'] / 60).round(1)
        display_artists = display_artists.drop('TOTAL_LISTENING_MINUTES', axis=1)
        
        st.dataframe(
            display_artists,
            use_container_width=True,
            column_config={
                'ARTIST_NAME': 'Artist',
                'PRIMARY_GENRE': 'Genre',
                'TOTAL_PLAYS': st.column_config.NumberColumn('Total Plays', format='%d'),
                'UNIQUE_TRACKS_PLAYED': st.column_config.NumberColumn('Unique Tracks', format='%d'),
                'TOTAL_LISTENING_HOURS': st.column_config.NumberColumn('Hours Listened', format='%.1f'),
                'ARTIST_POPULARITY': st.column_config.NumberColumn('Popularity', format='%d'),
                'WEEKEND_PLAY_PERCENTAGE': st.column_config.NumberColumn('Weekend %', format='%.1f%%')
            }
        )

# ============================================================================
# TAB 4: TIME PATTERNS
# ============================================================================

with tab4:
    st.header("⏰ Listening Time Patterns")
    
    # Load detailed data for time analysis
    listening_data = load_listening_data(start_date, end_date)
    
    # Apply sidebar filters to listening data
    if not listening_data.empty:
        listening_data = filter_data_by_sidebar(listening_data, selected_genres, selected_times, weekend_filter)
    
    if not listening_data.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            # Hourly listening pattern
            hourly_data = listening_data.groupby('DENVER_HOUR').size().reset_index(name='PLAYS')
            fig_hourly = px.line(
                hourly_data,
                x='DENVER_HOUR',
                y='PLAYS',
                title='Listening Activity by Hour of Day',
                labels={'DENVER_HOUR': 'Hour of Day', 'PLAYS': 'Number of Plays'},
                color_discrete_sequence=[SPOTIFY_GREEN]
            )
            fig_hourly.update_layout(
                height=400,
                xaxis=dict(tickmode='array', tickvals=list(range(0, 24, 2)))
            )
            fig_hourly = apply_spotify_theme(fig_hourly)
            st.plotly_chart(fig_hourly, use_container_width=True)
            
            # Time of day distribution
            time_dist = listening_data.groupby('TIME_OF_DAY_CATEGORY').size().reset_index(name='PLAYS')
            fig_time = px.pie(
                time_dist,
                values='PLAYS',
                names='TIME_OF_DAY_CATEGORY',
                title='Distribution by Time of Day',
                color_discrete_sequence=SPOTIFY_COLORS
            )
            fig_time.update_layout(height=400)
            fig_time = apply_spotify_theme(fig_time)
            st.plotly_chart(fig_time, use_container_width=True)
        
        with col2:
            # Weekend vs weekday patterns
            weekend_data = listening_data.groupby(['IS_WEEKEND', 'DENVER_HOUR']).size().reset_index(name='PLAYS')
            weekend_data['DAY_TYPE'] = weekend_data['IS_WEEKEND'].map({True: 'Weekend', False: 'Weekday'})
            
            fig_weekend = px.line(
                weekend_data,
                x='DENVER_HOUR',
                y='PLAYS',
                color='DAY_TYPE',
                title='Hourly Patterns: Weekdays vs Weekends',
                labels={'DENVER_HOUR': 'Hour of Day', 'PLAYS': 'Average Plays'},
                color_discrete_sequence=[SPOTIFY_GREEN, SPOTIFY_DARK_GREEN]
            )
            fig_weekend.update_layout(
                height=400,
                xaxis=dict(tickmode='array', tickvals=list(range(0, 24, 2)))
            )
            fig_weekend = apply_spotify_theme(fig_weekend)
            st.plotly_chart(fig_weekend, use_container_width=True)
            
            # Listening source distribution
            if 'LISTENING_SOURCE' in listening_data.columns:
                source_dist = listening_data.groupby('LISTENING_SOURCE').size().reset_index(name='PLAYS')
                fig_source = px.bar(
                    source_dist,
                    x='LISTENING_SOURCE',
                    y='PLAYS',
                    title='Listening Source Distribution',
                    labels={'LISTENING_SOURCE': 'Source', 'PLAYS': 'Number of Plays'},
                    color_discrete_sequence=[SPOTIFY_DARK_GREEN]
                )
                fig_source.update_layout(height=400)
                fig_source = apply_spotify_theme(fig_source)
                st.plotly_chart(fig_source, use_container_width=True)

# ============================================================================
# TAB 5: DETAILED VIEW
# ============================================================================

with tab5:
    st.header("🔍 Detailed Track Data")
    
    # Filters for detailed view
    col1, col2, col3 = st.columns(3)
    
    with col1:
        limit = st.selectbox("Number of records", [50, 100, 200, 500, 1000], index=1)
    
    with col2:
        sort_by = st.selectbox("Sort by", 
                              ['Latest First', 'Track Popularity', 'Artist Popularity', 'Duration'])
    
    with col3:
        if selected_genres:
            st.info(f"Filtered by genres: {', '.join(selected_genres)}")
    
    # Load and filter detailed data
    listening_data = load_listening_data(start_date, end_date)
    
    if not listening_data.empty:
        # Apply sidebar filters using our helper function
        listening_data = filter_data_by_sidebar(listening_data, selected_genres, selected_times, weekend_filter)
        
        # Sort data
        if sort_by == 'Latest First':
            listening_data = listening_data.sort_values('DENVER_TIMESTAMP', ascending=False)
        elif sort_by == 'Track Popularity':
            listening_data = listening_data.sort_values('TRACK_POPULARITY', ascending=False)
        elif sort_by == 'Artist Popularity':
            listening_data = listening_data.sort_values('ARTIST_POPULARITY', ascending=False)
        elif sort_by == 'Duration':
            listening_data = listening_data.sort_values('TRACK_DURATION_MINUTES', ascending=False)
        
        # Display data
        display_data = listening_data[['DENVER_TIMESTAMP', 'TRACK_NAME', 'PRIMARY_ARTIST_NAME', 
                                     'PRIMARY_GENRE', 'ALBUM_NAME', 'TRACK_DURATION_MINUTES',
                                     'TRACK_POPULARITY', 'TIME_OF_DAY_CATEGORY']].head(limit)
        
        st.dataframe(
            display_data,
            use_container_width=True,
            column_config={
                'DENVER_TIMESTAMP': st.column_config.DatetimeColumn('Date/Time'),
                'TRACK_NAME': 'Track',
                'PRIMARY_ARTIST_NAME': 'Artist',
                'PRIMARY_GENRE': 'Genre',
                'ALBUM_NAME': 'Album',
                'TRACK_DURATION_MINUTES': st.column_config.NumberColumn('Duration (min)', format='%.2f'),
                'TRACK_POPULARITY': st.column_config.NumberColumn('Popularity', format='%d'),
                'TIME_OF_DAY_CATEGORY': 'Time of Day'
            }
        )
        
        # Summary stats for filtered data
        st.subheader("📊 Filtered Data Summary")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Records", len(listening_data))
        with col2:
            st.metric("Unique Tracks", listening_data['TRACK_NAME'].nunique())
        with col3:
            st.metric("Unique Artists", listening_data['PRIMARY_ARTIST_NAME'].nunique())
        with col4:
            st.metric("Time Period", f"{(listening_data['DENVER_TIMESTAMP'].max() - listening_data['DENVER_TIMESTAMP'].min()).days} days")

# ============================================================================
# FOOTER
# ============================================================================

# ============================================================================
# TAB 6: ML RECOMMENDATIONS
# ============================================================================

with tab6:
    st.header("🤖 AI-Powered Music Recommendations")
    st.markdown("*Discover new music tailored to your taste using machine learning*")
    
    # Create two columns for controls and info
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("🎯 Generate Recommendations")
        
        # Recommendation controls
        num_recs = st.slider("Number of recommendations", 10, 50, 30)
        
        # Recommendation strategy selection
        strategy_type = st.selectbox(
            "Recommendation Strategy",
            ["Hybrid (All)", "Collaborative Filtering", "Content-Based", "Temporal Patterns", "Discovery"],
            help="Choose which AI algorithm to use for recommendations"
        )
        
        # Additional controls
        col_a, col_b = st.columns(2)
        with col_a:
            include_current_time = st.checkbox("Consider current time", value=True)
            mood_filter = st.selectbox("Mood", ["Any", "Energetic", "Relaxed", "Focused", "Nostalgic"])
        
        with col_b:
            max_popularity = st.slider("Max popularity", 10, 100, 80, 
                                     help="Higher = more mainstream, Lower = more underground")
            min_score = st.slider("Minimum recommendation score", 0.1, 1.0, 0.3, 0.1)
    
    with col2:
        st.subheader("ℹ️ How It Works")
        st.markdown("""
        **🧠 Collaborative Filtering**: Finds music similar to your taste patterns
        
        **📊 Content-Based**: Recommends based on track characteristics
        
        **⏰ Temporal Patterns**: Considers time-of-day preferences
        
        **🔍 Discovery**: Explores new genres and artists
        
        **🎯 Hybrid**: Combines all approaches for best results
        """)
    
    # Generate recommendations button
    if st.button("🎵 Generate Recommendations", type="primary"):
        with st.spinner("🤖 AI is analyzing your music taste..."):
            try:
                # Determine which strategy to use
                if strategy_type == "Hybrid (All)":
                    # Use hybrid recommendations with current context
                    current_hour = datetime.now().hour if include_current_time else None
                    is_weekend = datetime.now().weekday() >= 5 if include_current_time else None
                    
                    recs_query = f"""
                    SELECT * FROM TABLE(get_spotify_recommendations(
                        {num_recs}, 
                        {current_hour if current_hour else 'NULL'}, 
                        {is_weekend if is_weekend is not None else 'NULL'},
                        NULL,
                        {min_score}
                    ))
                    """
                
                elif strategy_type == "Collaborative Filtering":
                    recs_query = f"""
                    SELECT 
                        track_id,
                        track_name,
                        primary_artist_name as artist_name,
                        primary_genre as genre,
                        album_name,
                        track_popularity,
                        recommendation_score,
                        'https://open.spotify.com/track/' || track_id as spotify_url,
                        'Collaborative Filtering' as recommendation_reason
                    FROM ml_collaborative_recommendations 
                    WHERE recommendation_score >= {min_score}
                    ORDER BY recommendation_score DESC 
                    LIMIT {num_recs}
                    """
                
                elif strategy_type == "Content-Based":
                    recs_query = f"""
                    SELECT 
                        track_id,
                        track_name,
                        primary_artist_name as artist_name,
                        primary_genre as genre,
                        album_name,
                        track_popularity,
                        recommendation_score,
                        'https://open.spotify.com/track/' || track_id as spotify_url,
                        'Content-Based Filtering' as recommendation_reason
                    FROM ml_content_based_recommendations 
                    WHERE recommendation_score >= {min_score}
                    ORDER BY recommendation_score DESC 
                    LIMIT {num_recs}
                    """
                
                elif strategy_type == "Temporal Patterns":
                    current_hour = datetime.now().hour
                    is_weekend = datetime.now().weekday() >= 5
                    
                    recs_query = f"""
                    SELECT * FROM TABLE(get_time_based_recommendations(
                        {current_hour}, 
                        {is_weekend}, 
                        {num_recs}
                    ))
                    """
                
                elif strategy_type == "Discovery":
                    discovery_type = "hidden_gems" if max_popularity < 60 else "balanced"
                    
                    recs_query = f"""
                    SELECT * FROM TABLE(get_discovery_recommendations(
                        '{discovery_type}', 
                        {num_recs}, 
                        {max_popularity}
                    ))
                    """
                
                # Execute query
                recommendations_df = session.sql(recs_query).to_pandas()
                
                if not recommendations_df.empty:
                    st.success(f"🎵 Generated {len(recommendations_df)} personalized recommendations!")
                    
                    # Display recommendations in an attractive format
                    st.subheader("🎧 Your Personalized Playlist")
                    
                    # Create tabs for different views
                    rec_tab1, rec_tab2, rec_tab3 = st.tabs(["🎵 Playlist View", "📊 Analytics", "🔗 Quick Actions"])
                    
                    with rec_tab1:
                        # Display each recommendation as a card
                        for idx, track in recommendations_df.iterrows():
                            with st.container():
                                col1, col2, col3 = st.columns([3, 2, 1])
                                
                                with col1:
                                    # Track info with custom styling
                                    track_name = track.get('TRACK_NAME', 'Unknown Track')
                                    artist_name = track.get('ARTIST_NAME', track.get('PRIMARY_ARTIST_NAME', 'Unknown Artist'))
                                    album_name = track.get('ALBUM_NAME', 'Unknown Album')
                                    genre = track.get('GENRE', track.get('PRIMARY_GENRE', 'Unknown'))
                                    
                                    st.markdown(f"""
                                    <div style="background: linear-gradient(135deg, #1DB954 0%, #1aa34a 100%); 
                                                padding: 15px; border-radius: 10px; margin: 10px 0; 
                                                box-shadow: 0 4px 8px rgba(0,0,0,0.3);">
                                        <h4 style="color: white; margin: 0 0 5px 0;">{track_name}</h4>
                                        <p style="color: #f0f0f0; margin: 0 0 5px 0;"><strong>by {artist_name}</strong></p>
                                        <p style="color: #d0d0d0; margin: 0; font-size: 0.9em;">{album_name} • {genre}</p>
                                    </div>
                                    """, unsafe_allow_html=True)
                                
                                with col2:
                                    # Recommendation metrics
                                    score = track.get('RECOMMENDATION_SCORE', track.get('TEMPORAL_SCORE', track.get('SIMILARITY_SCORE', 0)))
                                    popularity = track.get('TRACK_POPULARITY', 0)
                                    
                                    st.metric("🎯 AI Score", f"{float(score):.3f}")
                                    st.metric("📈 Popularity", f"{int(popularity)}/100")
                                
                                with col3:
                                    # Quick actions
                                    spotify_url = track.get('SPOTIFY_URL', 
                                        f"https://open.spotify.com/track/{track.get('TRACK_ID', '')}")
                                    
                                    if spotify_url and 'open.spotify.com' in spotify_url:
                                        st.markdown(f"""
                                        <a href="{spotify_url}" target="_blank" 
                                           style="background-color: #1DB954; color: white; padding: 10px 15px; 
                                                  border-radius: 25px; text-decoration: none; display: inline-block;
                                                  font-weight: bold; text-align: center; width: 80px;">
                                            🎵 Play
                                        </a>
                                        """, unsafe_allow_html=True)
                                    
                                    # Recommendation reason
                                    reason = track.get('RECOMMENDATION_REASON', 
                                            track.get('SIMILARITY_REASON', 
                                            track.get('DISCOVERY_REASON', 'AI recommended')))
                                    st.caption(f"💡 {reason}")
                                
                                st.divider()
                    
                    with rec_tab2:
                        # Analytics about the recommendations
                        st.subheader("📊 Recommendation Analytics")
                        
                        # Metrics
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            avg_score = recommendations_df['RECOMMENDATION_SCORE'].mean() if 'RECOMMENDATION_SCORE' in recommendations_df.columns else 0
                            st.metric("Avg AI Score", f"{avg_score:.3f}")
                        
                        with col2:
                            unique_genres = recommendations_df['GENRE'].nunique() if 'GENRE' in recommendations_df.columns else 0
                            st.metric("Unique Genres", unique_genres)
                        
                        with col3:
                            unique_artists = recommendations_df['ARTIST_NAME'].nunique() if 'ARTIST_NAME' in recommendations_df.columns else 0
                            st.metric("Unique Artists", unique_artists)
                        
                        with col4:
                            avg_popularity = recommendations_df['TRACK_POPULARITY'].mean() if 'TRACK_POPULARITY' in recommendations_df.columns else 0
                            st.metric("Avg Popularity", f"{avg_popularity:.0f}")
                        
                        # Charts
                        if 'GENRE' in recommendations_df.columns:
                            # Genre distribution
                            genre_counts = recommendations_df['GENRE'].value_counts().head(8)
                            if not genre_counts.empty:
                                fig_genres = px.bar(
                                    x=genre_counts.values,
                                    y=genre_counts.index,
                                    orientation='h',
                                    title='Recommended Genres',
                                    labels={'x': 'Number of Tracks', 'y': 'Genre'},
                                    color_discrete_sequence=[SPOTIFY_GREEN]
                                )
                                fig_genres = apply_spotify_theme(fig_genres)
                                st.plotly_chart(fig_genres, use_container_width=True)
                        
                        # Score distribution
                        if 'RECOMMENDATION_SCORE' in recommendations_df.columns:
                            fig_scores = px.histogram(
                                recommendations_df,
                                x='RECOMMENDATION_SCORE',
                                title='AI Recommendation Score Distribution',
                                nbins=10,
                                color_discrete_sequence=[SPOTIFY_GREEN]
                            )
                            fig_scores = apply_spotify_theme(fig_scores)
                            st.plotly_chart(fig_scores, use_container_width=True)
                    
                    with rec_tab3:
                        # Export and sharing options
                        st.subheader("🔗 Export Your Playlist")
                        
                        # Create playlist text
                        playlist_text = "🎵 AI-Generated Spotify Recommendations\n\n"
                        for idx, track in recommendations_df.iterrows():
                            track_name = track.get('TRACK_NAME', 'Unknown')
                            artist_name = track.get('ARTIST_NAME', track.get('PRIMARY_ARTIST_NAME', 'Unknown'))
                            spotify_url = track.get('SPOTIFY_URL', '')
                            playlist_text += f"{idx+1}. {track_name} by {artist_name}\n   {spotify_url}\n\n"
                        
                        # Export options
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            # Download as text
                            st.download_button(
                                label="📄 Download as Text",
                                data=playlist_text,
                                file_name=f"spotify_recommendations_{datetime.now().strftime('%Y%m%d_%H%M')}.txt",
                                mime="text/plain"
                            )
                        
                        with col2:
                            # Copy to clipboard (via text area)
                            st.text_area(
                                "📋 Copy Playlist",
                                playlist_text,
                                height=200,
                                help="Select all and copy to share your playlist"
                            )
                        
                        # Quick stats
                        st.markdown("""
                        **📊 Quick Stats:**
                        - Total tracks: """ + str(len(recommendations_df)) + """
                        - Strategy used: """ + strategy_type + """
                        - Generated at: """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + """
                        """)
                        
                        # Feedback section
                        st.markdown("---")
                        st.subheader("💬 Feedback")
                        
                        feedback_col1, feedback_col2 = st.columns(2)
                        with feedback_col1:
                            if st.button("👍 Great recommendations!"):
                                st.success("Thanks! The AI will learn from your positive feedback.")
                        
                        with feedback_col2:
                            if st.button("👎 Not quite right"):
                                st.info("Feedback noted! Try adjusting the filters or strategy.")
                
                else:
                    st.warning("No recommendations found. Try adjusting your filters or ensure you have enough listening history.")
                    
            except Exception as e:
                st.error(f"❌ Failed to generate recommendations: {str(e)}")
                st.info("💡 Make sure you have:")
                st.info("• Sufficient listening history (at least 50 tracks)")
                st.info("• The ML recommendation views are set up in Snowflake")
                st.info("• Run the ML training scripts first")
    
    # Quick recommendations section
    st.markdown("---")
    st.subheader("⚡ Quick Recommendations")
    
    # Pre-built quick recommendation buttons
    quick_col1, quick_col2, quick_col3 = st.columns(3)
    
    with quick_col1:
        if st.button("🔥 Trending for Me"):
            with st.spinner("Finding trending tracks..."):
                try:
                    trending_query = """
                    SELECT 
                        track_id,
                        track_name,
                        primary_artist_name as artist_name,
                        primary_genre as genre,
                        track_popularity,
                        'https://open.spotify.com/track/' || track_id as spotify_url
                    FROM ml_track_content_features 
                    WHERE track_popularity BETWEEN 60 AND 85
                    AND user_play_count = 0
                    AND primary_genre IN (
                        SELECT primary_genre FROM ml_user_genre_interactions 
                        ORDER BY weighted_preference DESC LIMIT 3
                    )
                    ORDER BY track_popularity DESC, RANDOM()
                    LIMIT 5
                    """
                    
                    trending_df = session.sql(trending_query).to_pandas()
                    
                    if not trending_df.empty:
                        st.success(f"🔥 Found {len(trending_df)} trending tracks!")
                        for _, track in trending_df.iterrows():
                            st.markdown(f"🎵 **{track['TRACK_NAME']}** by {track['ARTIST_NAME']}")
                            st.markdown(f"   [Listen on Spotify]({track['SPOTIFY_URL']})")
                    else:
                        st.info("No trending tracks found. Try the full recommendation engine above!")
                        
                except Exception as e:
                    st.error(f"Error: {e}")
    
    with quick_col2:
        if st.button("🎯 Perfect for Now"):
            with st.spinner("Finding perfect tracks for this moment..."):
                try:
                    current_hour = datetime.now().hour
                    is_weekend = datetime.now().weekday() >= 5
                    
                    now_query = f"""
                    SELECT * FROM TABLE(get_time_based_recommendations(
                        {current_hour}, {is_weekend}, 5
                    ))
                    """
                    
                    now_df = session.sql(now_query).to_pandas()
                    
                    if not now_df.empty:
                        st.success(f"🎯 Found {len(now_df)} perfect tracks for now!")
                        for _, track in now_df.iterrows():
                            st.markdown(f"🎵 **{track['TRACK_NAME']}** by {track['ARTIST_NAME']}")
                            st.caption(f"Genre: {track['GENRE']} • {track['HOUR_RELEVANCE']}")
                    else:
                        st.info("No temporal recommendations found.")
                        
                except Exception as e:
                    st.error(f"Error: {e}")
    
    with quick_col3:
        if st.button("🔍 Discover Hidden Gems"):
            with st.spinner("Discovering hidden gems..."):
                try:
                    gems_query = """
                    SELECT * FROM TABLE(get_discovery_recommendations('hidden_gems', 5, 60))
                    """
                    
                    gems_df = session.sql(gems_query).to_pandas()
                    
                    if not gems_df.empty:
                        st.success(f"💎 Found {len(gems_df)} hidden gems!")
                        for _, track in gems_df.iterrows():
                            st.markdown(f"🎵 **{track['TRACK_NAME']}** by {track['ARTIST_NAME']}")
                            st.caption(f"Genre: {track['GENRE']} • {track['DISCOVERY_REASON']}")
                    else:
                        st.info("No hidden gems found.")
                        
                except Exception as e:
                    st.error(f"Error: {e}")

st.markdown("---")
st.markdown("### 🎵 Data powered by your Spotify listening history")
st.markdown("Built with ❤️ using Snowflake Native Streamlit")
st.markdown("""
<div style="color: #1DB954; font-size: 0.8em; text-align: center; margin-top: 20px;">
    🎵 Featuring Spotify's signature green & black theme 🎵<br>
    🚀 Interactive filtering across all tabs 🚀<br>
    📊 Real-time data from your medallion architecture 📊
</div>
""", unsafe_allow_html=True)
