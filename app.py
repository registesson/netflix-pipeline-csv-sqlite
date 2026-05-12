import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
from pathlib import Path

DB_PATH = Path(__file__).parent / "data" / "netflix.duckdb"

st.set_page_config(
    page_title="Netflix Pipeline Dashboard",
    page_icon="🎬",
    layout="wide",
)

st.title("🎬 Netflix Pipeline Dashboard")
st.caption("Données issues des marts dbt — DuckDB")


@st.cache_data
def load_mart(query: str) -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    df = con.execute(query).fetchdf()
    con.close()
    return df


genre_df = load_mart("SELECT * FROM mart_titles_by_genre ORDER BY title_count DESC")
decade_df = load_mart("SELECT * FROM mart_titles_by_decade ORDER BY decade")
country_df = load_mart(
    "SELECT * FROM mart_titles_by_country WHERE country != 'Unknown' ORDER BY title_count DESC"
)

# ── KPIs ──────────────────────────────────────────────────────────────────────
total_titles = genre_df["title_count"].sum() // 2  # approximate unique (genres are exploded)
total_genres = len(genre_df)
total_countries = len(country_df)
latest_decade = int(decade_df["decade"].max())

col1, col2, col3, col4 = st.columns(4)
col1.metric("Titres indexés", f"{country_df['title_count'].sum():,}")
col2.metric("Genres distincts", total_genres)
col3.metric("Pays distincts", total_countries)
col4.metric("Décennie la + récente", latest_decade)

st.divider()

# ── Row 1 : Genres + Décennies ────────────────────────────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Top 15 genres")
    top_genres = genre_df.head(15).copy()
    fig_genre = px.bar(
        top_genres,
        x="title_count",
        y="genre",
        orientation="h",
        color="title_count",
        color_continuous_scale="Teal",
        labels={"title_count": "Titres", "genre": "Genre"},
    )
    fig_genre.update_layout(
        yaxis={"categoryorder": "total ascending"},
        coloraxis_showscale=False,
        margin={"t": 10, "b": 10},
        height=450,
    )
    st.plotly_chart(fig_genre, width="stretch")

with col_right:
    st.subheader("Titres par décennie")
    fig_decade = px.bar(
        decade_df,
        x="decade",
        y=["movie_count", "tv_show_count"],
        barmode="stack",
        labels={"decade": "Décennie", "value": "Titres", "variable": "Type"},
        color_discrete_map={"movie_count": "#636EFA", "tv_show_count": "#EF553B"},
    )
    fig_decade.for_each_trace(
        lambda t: t.update(name="Films" if t.name == "movie_count" else "Séries TV")
    )
    fig_decade.update_layout(margin={"t": 10, "b": 10}, height=450)
    st.plotly_chart(fig_decade, width="stretch")

st.divider()

# ── Row 2 : Top pays ──────────────────────────────────────────────────────────
st.subheader("Top 20 pays producteurs")
top_countries = country_df.head(20).copy()

fig_country = px.bar(
    top_countries,
    x="country",
    y=["movie_count", "tv_show_count"],
    barmode="group",
    labels={"country": "Pays", "value": "Titres", "variable": "Type"},
    color_discrete_map={"movie_count": "#636EFA", "tv_show_count": "#EF553B"},
)
fig_country.for_each_trace(
    lambda t: t.update(name="Films" if t.name == "movie_count" else "Séries TV")
)
fig_country.update_layout(margin={"t": 10, "b": 10}, height=380)
st.plotly_chart(fig_country, width="stretch")

st.divider()

# ── Raw data explorer ─────────────────────────────────────────────────────────
with st.expander("Explorer les données brutes"):
    mart = st.selectbox(
        "Choisir un mart",
        ["mart_titles_by_genre", "mart_titles_by_decade", "mart_titles_by_country"],
    )
    df_raw = load_mart(f"SELECT * FROM {mart}")
    st.dataframe(df_raw, width="stretch")
    st.caption(f"{len(df_raw)} lignes")