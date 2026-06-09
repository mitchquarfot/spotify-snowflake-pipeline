# Spotify Analytics Dashboard - Snowflake Native Streamlit App
# Deploy this in Snowflake (SiS) to create an interactive analytics dashboard
#
# Changes from archived version:
#   - Removed track_popularity, artist_popularity, artist_followers columns
#     (removed from Spotify API Feb 2026). Replaced with Last.fm listeners proxy.
#   - Added ISRC column awareness.
#   - Replaced ML Recommendations tab with embedding-based Discovery (VECTOR_COSINE_SIMILARITY).
#   - Added Pipeline Health tab (PIPELINE_MONITORING, PIPELINE_ERRORS, DT refresh).
#   - Added Ask (Cortex Analyst) tab for natural language Q&A.
#   - New tables: ANALYTICS.ARTIST_AI_GENRES, ANALYTICS.ARTIST_EMBEDDINGS

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import json
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
    if genre_filters and 'PRIMARY_GENRE' in filtered.columns:
        filtered = filtered[filtered['PRIMARY_GENRE'].isin(genre_filters)]
    if time_filters and 'TIME_OF_DAY_CATEGORY' in filtered.columns:
        filtered = filtered[filtered['TIME_OF_DAY_CATEGORY'].isin(time_filters)]
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
    page_title="Spotify Analytics Dashboard",
    page_icon="🎵",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# SPOTIFY COLOR SCHEME AND STYLING
# ============================================================================

SPOTIFY_GREEN = '#1DB954'
SPOTIFY_BLACK = '#191414'
SPOTIFY_WHITE = '#FFFFFF'
SPOTIFY_GRAY = '#535353'
SPOTIFY_LIGHT_GRAY = '#B3B3B3'
SPOTIFY_DARK_GREEN = '#1ED760'
SPOTIFY_PALE_GREEN = '#1db95440'

st.markdown("""
<style>
    .stApp {
        background-color: #191414 !important;
        color: #FFFFFF !important;
    }
    .main .block-container {
        background-color: #191414 !important;
        color: #FFFFFF !important;
    }
    .main h1, .main h2, .main h3, .main h4, .main h5, .main h6 {
        color: #1DB954 !important;
        font-weight: bold !important;
    }
    .main p, .main div:not([data-testid]), .main span {
        color: #FFFFFF !important;
    }
    .css-1d391kg, section[data-testid="stSidebar"] {
        background-color: #0F0F0F !important;
    }
    section[data-testid="stSidebar"] h1,
    section[data-testid="stSidebar"] h2,
    section[data-testid="stSidebar"] h3 {
        color: #1DB954 !important;
        font-weight: bold !important;
    }
    section[data-testid="stSidebar"] * {
        color: #FFFFFF !important;
    }
    section[data-testid="stSidebar"] label {
        color: #1DB954 !important;
        font-weight: bold !important;
    }
    section[data-testid="stSidebar"] input,
    section[data-testid="stSidebar"] .stSelectbox div[data-baseweb="select"] {
        color: #000000 !important;
        background-color: #FFFFFF !important;
    }
    [data-testid="metric-container"] {
        background: linear-gradient(135deg, #0F0F0F 0%, #191414 100%) !important;
        border: 2px solid #1DB954 !important;
        padding: 1.2rem !important;
        border-radius: 0.8rem !important;
        box-shadow: 0 4px 8px rgba(29, 185, 84, 0.2) !important;
    }
    [data-testid="metric-container"] [data-testid="metric-label"] {
        color: #FFFFFF !important;
        font-weight: 600 !important;
    }
    [data-testid="metric-container"] [data-testid="metric-value"] {
        color: #1DB954 !important;
        font-size: 2.2rem !important;
        font-weight: 700 !important;
    }
    .stTabs [data-baseweb="tab-list"] {
        background-color: #0F0F0F !important;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: #0F0F0F !important;
        color: #B3B3B3 !important;
    }
    .stTabs [aria-selected="true"] {
        background-color: #1DB954 !important;
        color: #000000 !important;
        font-weight: bold !important;
    }
    .stTabs [data-baseweb="tab-panel"] {
        background-color: #191414 !important;
        color: #FFFFFF !important;
    }
    .stDataFrame td {
        background-color: #0F0F0F !important;
        color: #FFFFFF !important;
    }
    .stDataFrame th {
        background-color: #1DB954 !important;
        color: #000000 !important;
        font-weight: bold !important;
    }
    .stPlotlyChart {
        background-color: #191414 !important;
    }
    .stTextInput input, .stTextArea textarea {
        background-color: #FFFFFF !important;
        color: #000000 !important;
        border: 1px solid #535353 !important;
    }
</style>
""", unsafe_allow_html=True)

SPOTIFY_COLORS = [
    '#1DB954', '#1ED760', '#535353', '#B3B3B3', '#FFFFFF',
    '#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7'
]

# ============================================================================
# HELPER / DATA LOADING FUNCTIONS
# ============================================================================

@st.cache_data
def load_daily_summary():
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
    try:
        return session.sql("""
            SELECT *
            FROM spotify_analytics.medallion_arch.gold_genre_analysis_complete
            ORDER BY total_plays DESC
        """).to_pandas()
    except Exception as e:
        st.error(f"Error loading genre analysis data: {e}")
        return pd.DataFrame()


@st.cache_data
def load_monthly_insights():
    try:
        return session.sql("""
            SELECT *
            FROM spotify_analytics.medallion_arch.gold_monthly_insights_complete
            ORDER BY year DESC, month DESC
        """).to_pandas()
    except Exception as e:
        st.error(f"Error loading monthly insights data: {e}")
        return pd.DataFrame()


@st.cache_data
def load_artist_summary():
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
    try:
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

st.sidebar.title("Filters")

daily_data = load_daily_summary()
genre_data = load_genre_analysis()

if not daily_data.empty:
    try:
        daily_data['DENVER_DATE'] = pd.to_datetime(daily_data['DENVER_DATE'])
        min_date = daily_data['DENVER_DATE'].dt.date.min()
        max_date = daily_data['DENVER_DATE'].dt.date.max()
        date_range = st.sidebar.date_input(
            "Date Range",
            value=(max_date - timedelta(days=30), max_date),
            min_value=min_date,
            max_value=max_date
        )
        if len(date_range) == 2:
            start_date, end_date = date_range
        else:
            start_date = end_date = date_range[0]
    except Exception:
        start_date = end_date = datetime.now().date()
else:
    start_date = end_date = datetime.now().date()

if not genre_data.empty:
    available_genres = list(genre_data['PRIMARY_GENRE'].dropna().unique())
    selected_genres = st.sidebar.multiselect("Genre", options=available_genres, default=[])
else:
    selected_genres = []

time_periods = ['Morning', 'Afternoon', 'Evening', 'Night']
selected_times = st.sidebar.multiselect("Time of Day", options=time_periods, default=[])
weekend_filter = st.sidebar.selectbox("Weekend/Weekday", ['All', 'Weekends Only', 'Weekdays Only'])

# ============================================================================
# MAIN DASHBOARD
# ============================================================================

st.title("Spotify Analytics Dashboard")
st.markdown("### Explore your personal music listening patterns and discoveries")

# ============================================================================
# KEY METRICS ROW
# ============================================================================

if not daily_data.empty:
    daily_data['DENVER_DATE'] = pd.to_datetime(daily_data['DENVER_DATE'])
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

if not filtered_daily.empty:
    total_plays = filtered_daily['TOTAL_PLAYS'].sum()
    unique_tracks = filtered_daily['UNIQUE_TRACKS'].sum()
    unique_artists = filtered_daily['UNIQUE_ARTISTS'].sum()
    total_hours = filtered_daily['TOTAL_LISTENING_MINUTES'].sum() / 60
    avg_daily_plays = filtered_daily['TOTAL_PLAYS'].mean()

    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Total Plays", f"{total_plays:,}")
    with col2:
        st.metric("Unique Tracks", f"{unique_tracks:,}")
    with col3:
        st.metric("Unique Artists", f"{unique_artists:,}")
    with col4:
        st.metric("Hours Listened", f"{total_hours:.1f}")
    with col5:
        st.metric("Avg Daily Plays", f"{avg_daily_plays:.1f}")

# ============================================================================
# TABS
# ============================================================================

tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
    "Trends", "Genres", "Artists", "Time Patterns",
    "Detailed View", "Discovery", "Pipeline Health", "Ask (Cortex Analyst)"
])

# ============================================================================
# TAB 1: LISTENING TRENDS
# ============================================================================

with tab1:
    st.header("Listening Trends Over Time")
    col1, col2 = st.columns(2)

    with col1:
        if not filtered_daily.empty:
            fig_daily = px.line(
                filtered_daily, x='DENVER_DATE', y='TOTAL_PLAYS',
                title='Daily Listening Activity',
                labels={'TOTAL_PLAYS': 'Number of Plays', 'DENVER_DATE': 'Date'},
                color_discrete_sequence=[SPOTIFY_GREEN]
            )
            fig_daily.update_layout(height=400)
            fig_daily = apply_spotify_theme(fig_daily)
            st.plotly_chart(fig_daily, use_container_width=True)

        if not filtered_daily.empty and 'GENRE_DIVERSITY_SCORE' in filtered_daily.columns:
            fig_diversity = px.line(
                filtered_daily, x='DENVER_DATE', y='GENRE_DIVERSITY_SCORE',
                title='Genre Diversity Score Over Time',
                labels={'GENRE_DIVERSITY_SCORE': 'Genre Diversity %', 'DENVER_DATE': 'Date'},
                color_discrete_sequence=[SPOTIFY_DARK_GREEN]
            )
            fig_diversity.update_layout(height=400)
            fig_diversity = apply_spotify_theme(fig_diversity)
            st.plotly_chart(fig_diversity, use_container_width=True)

    with col2:
        if not filtered_daily.empty and 'DAY_OF_WEEK' in filtered_daily.columns:
            weekly_avg = filtered_daily.groupby('DAY_OF_WEEK')['TOTAL_PLAYS'].mean().reset_index()
            if not weekly_avg.empty:
                day_mapping = {
                    'Sun': 'Sunday', 'Mon': 'Monday', 'Tue': 'Tuesday',
                    'Wed': 'Wednesday', 'Thu': 'Thursday', 'Fri': 'Friday', 'Sat': 'Saturday'
                }
                weekly_avg['DAY_FULL'] = weekly_avg['DAY_OF_WEEK'].map(day_mapping)
                day_order = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
                weekly_avg['DAY_FULL'] = pd.Categorical(weekly_avg['DAY_FULL'], categories=day_order, ordered=True)
                weekly_avg = weekly_avg.sort_values('DAY_FULL')
                fig_weekly = px.line(
                    weekly_avg, x='DAY_FULL', y='TOTAL_PLAYS',
                    title='Average Plays by Day of Week',
                    labels={'TOTAL_PLAYS': 'Average Plays', 'DAY_FULL': 'Day'},
                    color_discrete_sequence=[SPOTIFY_GREEN], markers=True
                )
                fig_weekly.update_traces(line=dict(color=SPOTIFY_GREEN, width=3), marker=dict(color=SPOTIFY_GREEN, size=8))
                fig_weekly.update_layout(height=400)
                fig_weekly = apply_spotify_theme(fig_weekly)
                st.plotly_chart(fig_weekly, use_container_width=True)

        monthly_data = load_monthly_insights()
        if not monthly_data.empty:
            monthly_data['date'] = pd.to_datetime(monthly_data[['YEAR', 'MONTH']].assign(day=1))
            monthly_filtered = monthly_data[
                (monthly_data['date'] >= pd.to_datetime(start_date)) &
                (monthly_data['date'] <= pd.to_datetime(end_date))
            ]
            if not monthly_filtered.empty:
                fig_monthly = px.bar(
                    monthly_filtered, x='MONTH_NAME', y='TOTAL_PLAYS',
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
    st.header("Genre Analysis")
    if not genre_data.empty:
        listening_detail = load_listening_data(start_date, end_date)
        if not listening_detail.empty:
            filtered_listening = filter_data_by_sidebar(listening_detail, selected_genres, selected_times, weekend_filter)
            if not filtered_listening.empty:
                genre_stats = filtered_listening.groupby('PRIMARY_GENRE').agg({
                    'TRACK_NAME': 'count',
                    'PRIMARY_ARTIST_NAME': 'nunique',
                    'TRACK_DURATION_MINUTES': 'sum'
                }).round(2)
                genre_stats.columns = ['TOTAL_PLAYS', 'UNIQUE_ARTISTS', 'TOTAL_LISTENING_MINUTES']
                genre_stats = genre_stats.reset_index().sort_values('TOTAL_PLAYS', ascending=False)
                genre_stats['PERCENTAGE_OF_TOTAL_LISTENING'] = (
                    100 * genre_stats['TOTAL_PLAYS'] / genre_stats['TOTAL_PLAYS'].sum()
                ).round(2)
            else:
                genre_stats = pd.DataFrame()
        else:
            genre_stats = genre_data
    else:
        genre_stats = genre_data

    col1, col2 = st.columns(2)
    with col1:
        if not genre_stats.empty:
            top_genres = genre_stats.head(10)
            fig_pie = px.pie(
                top_genres, values='TOTAL_PLAYS', names='PRIMARY_GENRE',
                title='Top 10 Genres by Plays', color_discrete_sequence=SPOTIFY_COLORS
            )
            fig_pie.update_layout(height=500)
            fig_pie = apply_spotify_theme(fig_pie)
            st.plotly_chart(fig_pie, use_container_width=True)

    with col2:
        if not genre_stats.empty:
            fig_bar = px.bar(
                genre_stats.head(15), x='TOTAL_PLAYS', y='PRIMARY_GENRE', orientation='h',
                title='Top 15 Genres by Total Plays',
                labels={'TOTAL_PLAYS': 'Total Plays', 'PRIMARY_GENRE': 'Genre'},
                color_discrete_sequence=[SPOTIFY_GREEN]
            )
            fig_bar.update_layout(height=500)
            fig_bar = apply_spotify_theme(fig_bar)
            st.plotly_chart(fig_bar, use_container_width=True)

    st.subheader("Genre Details")
    if not genre_stats.empty:
        display_genres = genre_stats[['PRIMARY_GENRE', 'TOTAL_PLAYS', 'UNIQUE_ARTISTS',
                                      'TOTAL_LISTENING_MINUTES', 'PERCENTAGE_OF_TOTAL_LISTENING']].head(20)
        display_genres['TOTAL_LISTENING_HOURS'] = (display_genres['TOTAL_LISTENING_MINUTES'] / 60).round(1)
        display_genres = display_genres.drop('TOTAL_LISTENING_MINUTES', axis=1)
        st.dataframe(display_genres, use_container_width=True)

# ============================================================================
# TAB 3: ARTIST ANALYSIS
# ============================================================================

with tab3:
    st.header("Artist Analysis")
    artist_data = load_artist_summary()
    if not artist_data.empty:
        if selected_genres:
            artist_data = artist_data[artist_data['PRIMARY_GENRE'].isin(selected_genres)]

    col1, col2 = st.columns(2)
    with col1:
        if not artist_data.empty:
            top_artists = artist_data.head(15)
            fig_artists = px.bar(
                top_artists, x='TOTAL_PLAYS', y='ARTIST_NAME', orientation='h',
                title='Top 15 Most Played Artists',
                labels={'TOTAL_PLAYS': 'Total Plays', 'ARTIST_NAME': 'Artist'},
                color_discrete_sequence=[SPOTIFY_GREEN]
            )
            fig_artists.update_layout(height=600)
            fig_artists = apply_spotify_theme(fig_artists)
            st.plotly_chart(fig_artists, use_container_width=True)

    with col2:
        if not artist_data.empty:
            fig_scatter = px.scatter(
                artist_data.head(50),
                x='TOTAL_LISTENING_MINUTES', y='UNIQUE_TRACKS_PLAYED',
                size='TOTAL_PLAYS', color='PRIMARY_GENRE',
                hover_name='ARTIST_NAME',
                title='Artist Discovery: Listening Time vs Track Diversity',
                labels={'TOTAL_LISTENING_MINUTES': 'Total Listening Minutes',
                        'UNIQUE_TRACKS_PLAYED': 'Unique Tracks Played'},
                color_discrete_sequence=SPOTIFY_COLORS
            )
            fig_scatter.update_layout(height=600)
            fig_scatter = apply_spotify_theme(fig_scatter)
            st.plotly_chart(fig_scatter, use_container_width=True)

    st.subheader("Artist Details")
    if not artist_data.empty:
        # Columns available in silver_artist_summary (no artist_popularity/artist_followers)
        display_cols = ['ARTIST_NAME', 'PRIMARY_GENRE', 'TOTAL_PLAYS',
                        'UNIQUE_TRACKS_PLAYED', 'TOTAL_LISTENING_MINUTES']
        if 'WEEKEND_PLAY_PERCENTAGE' in artist_data.columns:
            display_cols.append('WEEKEND_PLAY_PERCENTAGE')
        display_artists = artist_data[[c for c in display_cols if c in artist_data.columns]].head(25)
        if 'TOTAL_LISTENING_MINUTES' in display_artists.columns:
            display_artists['TOTAL_LISTENING_HOURS'] = (display_artists['TOTAL_LISTENING_MINUTES'] / 60).round(1)
            display_artists = display_artists.drop('TOTAL_LISTENING_MINUTES', axis=1)
        st.dataframe(display_artists, use_container_width=True)

# ============================================================================
# TAB 4: TIME PATTERNS
# ============================================================================

with tab4:
    st.header("Listening Time Patterns")
    listening_data = load_listening_data(start_date, end_date)
    if not listening_data.empty:
        listening_data = filter_data_by_sidebar(listening_data, selected_genres, selected_times, weekend_filter)

    if not listening_data.empty:
        col1, col2 = st.columns(2)
        with col1:
            hourly_data = listening_data.groupby('DENVER_HOUR').size().reset_index(name='PLAYS')
            fig_hourly = px.line(
                hourly_data, x='DENVER_HOUR', y='PLAYS',
                title='Listening Activity by Hour of Day',
                labels={'DENVER_HOUR': 'Hour of Day', 'PLAYS': 'Number of Plays'},
                color_discrete_sequence=[SPOTIFY_GREEN]
            )
            fig_hourly.update_layout(height=400, xaxis=dict(tickmode='array', tickvals=list(range(0, 24, 2))))
            fig_hourly = apply_spotify_theme(fig_hourly)
            st.plotly_chart(fig_hourly, use_container_width=True)

            time_dist = listening_data.groupby('TIME_OF_DAY_CATEGORY').size().reset_index(name='PLAYS')
            fig_time = px.pie(
                time_dist, values='PLAYS', names='TIME_OF_DAY_CATEGORY',
                title='Distribution by Time of Day', color_discrete_sequence=SPOTIFY_COLORS
            )
            fig_time.update_layout(height=400)
            fig_time = apply_spotify_theme(fig_time)
            st.plotly_chart(fig_time, use_container_width=True)

        with col2:
            weekend_data = listening_data.groupby(['IS_WEEKEND', 'DENVER_HOUR']).size().reset_index(name='PLAYS')
            weekend_data['DAY_TYPE'] = weekend_data['IS_WEEKEND'].map({True: 'Weekend', False: 'Weekday'})
            fig_weekend = px.line(
                weekend_data, x='DENVER_HOUR', y='PLAYS', color='DAY_TYPE',
                title='Hourly Patterns: Weekdays vs Weekends',
                labels={'DENVER_HOUR': 'Hour of Day', 'PLAYS': 'Plays'},
                color_discrete_sequence=[SPOTIFY_GREEN, SPOTIFY_DARK_GREEN]
            )
            fig_weekend.update_layout(height=400, xaxis=dict(tickmode='array', tickvals=list(range(0, 24, 2))))
            fig_weekend = apply_spotify_theme(fig_weekend)
            st.plotly_chart(fig_weekend, use_container_width=True)

            if 'LISTENING_SOURCE' in listening_data.columns:
                source_dist = listening_data.groupby('LISTENING_SOURCE').size().reset_index(name='PLAYS')
                fig_source = px.bar(
                    source_dist, x='LISTENING_SOURCE', y='PLAYS',
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
    st.header("Detailed Track Data")
    col1, col2, col3 = st.columns(3)
    with col1:
        limit = st.selectbox("Number of records", [50, 100, 200, 500, 1000], index=1)
    with col2:
        sort_by = st.selectbox("Sort by", ['Latest First', 'Duration'])
    with col3:
        if selected_genres:
            st.info(f"Filtered by genres: {', '.join(selected_genres)}")

    listening_data = load_listening_data(start_date, end_date)
    if not listening_data.empty:
        listening_data = filter_data_by_sidebar(listening_data, selected_genres, selected_times, weekend_filter)
        if sort_by == 'Latest First':
            listening_data = listening_data.sort_values('DENVER_TIMESTAMP', ascending=False)
        elif sort_by == 'Duration':
            listening_data = listening_data.sort_values('TRACK_DURATION_MINUTES', ascending=False)

        display_cols = ['DENVER_TIMESTAMP', 'TRACK_NAME', 'PRIMARY_ARTIST_NAME',
                        'PRIMARY_GENRE', 'ALBUM_NAME', 'TRACK_DURATION_MINUTES',
                        'TIME_OF_DAY_CATEGORY']
        display_data = listening_data[[c for c in display_cols if c in listening_data.columns]].head(limit)
        st.dataframe(display_data, use_container_width=True)

        st.subheader("Filtered Data Summary")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Records", len(listening_data))
        with col2:
            st.metric("Unique Tracks", listening_data['TRACK_NAME'].nunique())
        with col3:
            st.metric("Unique Artists", listening_data['PRIMARY_ARTIST_NAME'].nunique())
        with col4:
            if 'DENVER_TIMESTAMP' in listening_data.columns:
                span = (pd.to_datetime(listening_data['DENVER_TIMESTAMP']).max() -
                        pd.to_datetime(listening_data['DENVER_TIMESTAMP']).min()).days
                st.metric("Time Period", f"{span} days")

# ============================================================================
# TAB 6: DISCOVERY (Embedding-based recommendations)
# ============================================================================

with tab6:
    st.header("Artist Discovery")
    st.markdown("*Find similar artists using Cortex AI embeddings (VECTOR_COSINE_SIMILARITY)*")

    # Check if embeddings table exists
    @st.cache_data
    def check_embeddings_available():
        try:
            result = session.sql("""
                SELECT COUNT(*) AS cnt
                FROM SPOTIFY_ANALYTICS.ANALYTICS.ARTIST_EMBEDDINGS
            """).to_pandas()
            return result['CNT'].iloc[0] > 0
        except Exception:
            return False

    embeddings_available = check_embeddings_available()

    if embeddings_available:
        # Load artist list for selection
        @st.cache_data
        def load_embedded_artists():
            return session.sql("""
                SELECT artist_name, primary_genre
                FROM SPOTIFY_ANALYTICS.ANALYTICS.ARTIST_EMBEDDINGS
                ORDER BY artist_name
            """).to_pandas()

        embedded_artists = load_embedded_artists()
        artist_list = embedded_artists['ARTIST_NAME'].tolist()

        selected_artist = st.selectbox("Select an artist to find similar artists:", artist_list)
        num_similar = st.slider("Number of similar artists", 5, 25, 10)

        if st.button("Find Similar Artists", type="primary"):
            with st.spinner("Computing embedding similarity..."):
                try:
                    # Single-quote escaping ('' doubling) to safely interpolate user input
                    similar_query = f"""
                    SELECT
                        target.artist_name   AS source_artist,
                        similar.artist_name  AS recommended_artist,
                        similar.primary_genre,
                        VECTOR_COSINE_SIMILARITY(target.embedding, similar.embedding) AS similarity_score
                    FROM SPOTIFY_ANALYTICS.ANALYTICS.ARTIST_EMBEDDINGS target
                    CROSS JOIN SPOTIFY_ANALYTICS.ANALYTICS.ARTIST_EMBEDDINGS similar
                    WHERE target.artist_name = '{selected_artist.replace("'", "''")}'
                      AND similar.artist_id != target.artist_id
                    ORDER BY similarity_score DESC
                    LIMIT {num_similar}
                    """
                    similar_df = session.sql(similar_query).to_pandas()

                    if not similar_df.empty:
                        st.success(f"Found {len(similar_df)} artists similar to **{selected_artist}**")

                        # Display as styled cards
                        for idx, row in similar_df.iterrows():
                            score_pct = row['SIMILARITY_SCORE'] * 100
                            st.markdown(f"""
                            <div style="background: linear-gradient(135deg, #0F0F0F, #191414);
                                        border-left: 4px solid #1DB954; padding: 12px; margin: 8px 0;
                                        border-radius: 6px;">
                                <strong style="color: #1DB954; font-size: 1.1em;">{row['RECOMMENDED_ARTIST']}</strong>
                                <span style="color: #B3B3B3; margin-left: 12px;">{row['PRIMARY_GENRE']}</span>
                                <span style="color: #FFFFFF; float: right; font-weight: bold;">{score_pct:.1f}% match</span>
                            </div>
                            """, unsafe_allow_html=True)

                        # Score distribution chart
                        fig_scores = px.bar(
                            similar_df, x='RECOMMENDED_ARTIST', y='SIMILARITY_SCORE',
                            title='Similarity Scores', color_discrete_sequence=[SPOTIFY_GREEN]
                        )
                        fig_scores.update_layout(height=350)
                        fig_scores = apply_spotify_theme(fig_scores)
                        st.plotly_chart(fig_scores, use_container_width=True)
                    else:
                        st.warning("No similar artists found.")
                except Exception as e:
                    st.error(f"Error computing similarity: {e}")

        # Also show AI-classified genres if available
        st.markdown("---")
        st.subheader("AI-Classified Genres")
        try:
            ai_genres_df = session.sql("""
                SELECT artist_name, ai_primary_genre, ai_secondary_genre, ai_mood,
                       ROUND(ai_confidence, 2) AS confidence
                FROM SPOTIFY_ANALYTICS.ANALYTICS.ARTIST_AI_GENRES
                ORDER BY classified_at DESC
                LIMIT 50
            """).to_pandas()
            if not ai_genres_df.empty:
                st.dataframe(ai_genres_df, use_container_width=True)
            else:
                st.info("No AI genre classifications yet. Run the classification job first.")
        except Exception:
            st.info("ARTIST_AI_GENRES table not yet populated.")
    else:
        st.warning(
            "Artist embeddings not available. Run `sql/create_ai_enrichment.sql` to generate "
            "ARTIST_EMBEDDINGS via EMBED_TEXT_768, then revisit this tab."
        )
        st.info("Once embeddings are populated, you can discover artists similar to your favorites "
                "using cosine similarity over Cortex AI vectors.")

# ============================================================================
# TAB 7: PIPELINE HEALTH
# ============================================================================

with tab7:
    st.header("Pipeline Health")
    st.markdown("*Monitor pipeline runs, errors, and dynamic table freshness*")

    # --- Pipeline Monitoring ---
    st.subheader("Recent Pipeline Runs")
    try:
        monitoring_df = session.sql("""
            SELECT *
            FROM SPOTIFY_ANALYTICS.RAW_DATA.PIPELINE_MONITORING
            ORDER BY run_timestamp DESC
            LIMIT 30
        """).to_pandas()
        if not monitoring_df.empty:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Runs (shown)", len(monitoring_df))
            with col2:
                if 'STATUS' in monitoring_df.columns:
                    success_count = (monitoring_df['STATUS'] == 'SUCCESS').sum()
                    st.metric("Successful", int(success_count))
            with col3:
                if 'STATUS' in monitoring_df.columns:
                    fail_count = (monitoring_df['STATUS'] != 'SUCCESS').sum()
                    st.metric("Failed/Other", int(fail_count))
            st.dataframe(monitoring_df.head(15), use_container_width=True)
        else:
            st.info("No pipeline monitoring records found.")
    except Exception as e:
        st.warning(f"Could not load PIPELINE_MONITORING: {e}")

    # --- Pipeline Errors (dead-letter queue) ---
    st.subheader("Recent Pipeline Errors")
    try:
        errors_df = session.sql("""
            SELECT error_id, run_id, error_timestamp, error_type, error_message
            FROM SPOTIFY_ANALYTICS.RAW_DATA.PIPELINE_ERRORS
            ORDER BY error_timestamp DESC
            LIMIT 20
        """).to_pandas()
        if not errors_df.empty:
            st.metric("Errors (last 20)", len(errors_df))
            st.dataframe(errors_df, use_container_width=True)
        else:
            st.success("No pipeline errors recorded.")
    except Exception as e:
        st.warning(f"Could not load PIPELINE_ERRORS: {e}")

    # --- Dynamic Table Refresh History ---
    st.subheader("Dynamic Table Refresh Status")
    try:
        dt_status_df = session.sql("""
            SELECT
                name,
                scheduling_state,
                last_completed_refresh_state,
                last_completed_refresh_state_message,
                data_timestamp
            FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
                NAME_PREFIX => 'SPOTIFY_ANALYTICS.MEDALLION_ARCH.'
            ))
            QUALIFY ROW_NUMBER() OVER (PARTITION BY name ORDER BY data_timestamp DESC) = 1
            ORDER BY data_timestamp DESC
            LIMIT 20
        """).to_pandas()
        if not dt_status_df.empty:
            # Colour-code status
            st.dataframe(dt_status_df, use_container_width=True)
        else:
            st.info("No dynamic table refresh history available.")
    except Exception as e:
        st.info(f"Dynamic table refresh history unavailable: {e}")
        st.markdown("*Requires ACCOUNTADMIN or MONITOR privilege on the dynamic tables.*")

# ============================================================================
# TAB 8: ASK (CORTEX ANALYST)
# ============================================================================

with tab8:
    st.header("Ask Your Data (Cortex Analyst)")
    st.markdown(
        "*Ask natural language questions about your Spotify listening data. "
        "Powered by Snowflake Cortex Analyst and the semantic model.*"
    )

    # --- Integration point for Cortex Analyst ---
    # The semantic model is deployed alongside this app as spotify_semantic_model.yml.
    # Cortex Analyst is invoked via the Snowpark session's REST helper or the
    # snowflake.cortex module depending on Snowflake release.

    SEMANTIC_MODEL_STAGE_PATH = "@SPOTIFY_ANALYTICS.STREAMLIT_APPS.STREAMLIT_STAGE/spotify_semantic_model.yml"

    user_question = st.text_input(
        "Ask a question about your listening data:",
        placeholder="e.g. What was my most played genre last month?"
    )

    if st.button("Ask", type="primary") and user_question.strip():
        with st.spinner("Cortex Analyst is thinking..."):
            try:
                # Use the _rest_request approach available in SiS sessions
                import _snowflake  # noqa: available in SiS runtime

                resp = _snowflake.send_snow_api_request(
                    "POST",
                    "/api/v2/cortex/analyst/message",
                    {},  # headers
                    {},  # params
                    json.dumps({
                        "messages": [{"role": "user", "content": [{"type": "text", "text": user_question}]}],
                        "semantic_model_file": SEMANTIC_MODEL_STAGE_PATH
                    }),
                    {},  # all_headers
                    30000  # timeout_ms
                )

                resp_json = json.loads(resp.get("body", "{}") if isinstance(resp, dict) else resp)
                # Extract SQL and text from response
                message_content = resp_json.get("message", {}).get("content", [])

                generated_sql = None
                analyst_text = ""
                for block in message_content:
                    if block.get("type") == "sql":
                        generated_sql = block.get("statement", "")
                    elif block.get("type") == "text":
                        analyst_text += block.get("text", "") + "\n"

                if analyst_text:
                    st.markdown(analyst_text)

                if generated_sql:
                    st.code(generated_sql, language="sql")
                    # Execute the generated SQL and show results
                    try:
                        result_df = session.sql(generated_sql).to_pandas()
                        if not result_df.empty:
                            st.dataframe(result_df, use_container_width=True)
                        else:
                            st.info("Query returned no results.")
                    except Exception as exec_err:
                        st.warning(f"Could not execute generated SQL: {exec_err}")
                elif not analyst_text:
                    st.warning("Cortex Analyst returned an empty response. Try rephrasing your question.")

            except ImportError:
                # Fallback: _snowflake not available (running locally or older SiS)
                st.warning(
                    "Cortex Analyst integration requires the SiS runtime (`_snowflake` module). "
                    "Deploy this app to Streamlit-in-Snowflake to enable NL Q&A."
                )
            except Exception as e:
                st.error(f"Cortex Analyst error: {e}")
                st.info("Ensure the semantic model is uploaded to the stage and Cortex Analyst is enabled on your account.")

    # Show example questions
    st.markdown("---")
    st.subheader("Example Questions")
    examples = [
        "How many songs did I listen to yesterday?",
        "What's my most played genre?",
        "Who is my top artist in country music?",
        "How has my listening changed over the past 3 months?",
        "What are my top 5 artists by total listening time?",
        "Which day of the week do I listen to the most music?",
    ]
    for ex in examples:
        st.markdown(f"- {ex}")

# ============================================================================
# FOOTER
# ============================================================================

st.markdown("---")
st.markdown("### Data powered by your Spotify listening history")
st.markdown("Built with Snowflake Native Streamlit | Cortex AI | Medallion Architecture")
