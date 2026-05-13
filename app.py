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

# ── Sidebar filters ───────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Filtres")
    top_n_genres = st.slider("Top N genres", 5, 30, 15)
    top_n_countries = st.slider("Top N pays", 5, 50, 20)
    type_filter = st.radio("Type de contenu", ["Tous", "Films", "Séries TV"])

type_col = {"Tous": "title_count", "Films": "movie_count", "Séries TV": "tv_show_count"}[type_filter]

genre_filtered = genre_df.copy()
genre_filtered["count"] = genre_filtered[type_col]
genre_filtered = (
    genre_filtered[genre_filtered["count"] > 0]
    .sort_values("count", ascending=False)
    .head(top_n_genres)
)

country_filtered = country_df.copy()
country_filtered["count"] = country_filtered[type_col]
country_filtered = country_filtered[country_filtered["count"] > 0].sort_values(
    "count", ascending=False
)

# ── KPIs ──────────────────────────────────────────────────────────────────────
total_titles = int(country_df["title_count"].sum())
total_recent = int(country_df["recent_titles_count"].sum())
total_movies = int(country_df["movie_count"].sum())
total_tv = int(country_df["tv_show_count"].sum())

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Titres indexés", f"{total_titles:,}")
col2.metric("Films / Séries", f"{total_movies:,} / {total_tv:,}")
col3.metric(
    "Titres récents (≥ 2015)",
    f"{total_recent:,}",
    f"{total_recent / total_titles * 100:.0f}% du total",
)
col4.metric("Genres distincts", len(genre_df))
col5.metric("Pays distincts", len(country_df))

st.divider()

# ── Row 1 : Genres + Décennies ────────────────────────────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    st.subheader(f"Top {top_n_genres} genres")
    fig_genre = px.bar(
        genre_filtered,
        x="count",
        y="genre",
        orientation="h",
        color="count",
        color_continuous_scale="Teal",
        labels={"count": "Titres", "genre": "Genre"},
    )
    fig_genre.update_layout(
        yaxis={"categoryorder": "total ascending"},
        coloraxis_showscale=False,
        margin={"t": 10, "b": 10},
        height=450,
    )
    st.plotly_chart(fig_genre, use_container_width=True)

with col_right:
    st.subheader("Évolution par décennie")
    if type_filter == "Tous":
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
    else:
        color = "#636EFA" if type_filter == "Films" else "#EF553B"
        fig_decade = px.line(
            decade_df,
            x="decade",
            y=type_col,
            markers=True,
            labels={"decade": "Décennie", type_col: "Titres"},
            color_discrete_sequence=[color],
        )
    fig_decade.update_layout(margin={"t": 10, "b": 10}, height=450)
    st.plotly_chart(fig_decade, use_container_width=True)

st.divider()

# ── Row 2 : Top pays (bar) ────────────────────────────────────────────────────
st.subheader(f"Top {top_n_countries} pays producteurs")
top_countries = country_filtered.head(top_n_countries).copy()

if type_filter == "Tous":
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
else:
    color = "#636EFA" if type_filter == "Films" else "#EF553B"
    fig_country = px.bar(
        top_countries,
        x="country",
        y="count",
        labels={"country": "Pays", "count": "Titres"},
        color_discrete_sequence=[color],
    )
fig_country.update_layout(margin={"t": 10, "b": 10}, height=380)
st.plotly_chart(fig_country, use_container_width=True)

st.divider()

# ── Row 3 : Carte choroplèthe ─────────────────────────────────────────────────
st.subheader("Carte mondiale des productions")
fig_map = px.choropleth(
    country_df,
    locations="country",
    locationmode="country names",
    color=type_col,
    hover_name="country",
    hover_data={
        "title_count": True,
        "movie_count": True,
        "tv_show_count": True,
        "recent_titles_count": True,
    },
    color_continuous_scale="Blues",
    labels={type_col: "Titres"},
)
fig_map.update_layout(
    margin={"t": 10, "b": 10, "l": 0, "r": 0},
    height=450,
    geo={"showframe": False, "showcoastlines": True},
)
st.plotly_chart(fig_map, use_container_width=True)

st.divider()

# ── Row 4 : Scatter pays + % titres récents ───────────────────────────────────
col_left2, col_right2 = st.columns(2)

with col_left2:
    st.subheader(f"Diversité vs volume — Top {top_n_countries} pays")
    scatter_df = country_filtered.head(top_n_countries).copy()
    scatter_df["recent_pct"] = (
        scatter_df["recent_titles_count"] / scatter_df["title_count"] * 100
    )
    fig_scatter = px.scatter(
        scatter_df,
        x="count",
        y="avg_genre_count",
        size="recent_titles_count",
        size_max=50,
        hover_name="country",
        color="recent_pct",
        color_continuous_scale="RdYlGn",
        labels={
            "count": "Titres",
            "avg_genre_count": "Genres moyens / titre",
            "recent_pct": "% récents",
            "recent_titles_count": "Titres récents",
        },
    )
    fig_scatter.update_layout(margin={"t": 10, "b": 10}, height=400)
    st.plotly_chart(fig_scatter, use_container_width=True)

with col_right2:
    st.subheader(f"% titres récents (≥ 2015) — Top {top_n_countries} pays")
    recent_df = country_df.head(top_n_countries).copy()
    recent_df["recent_pct"] = recent_df["recent_titles_count"] / recent_df["title_count"] * 100
    recent_df = recent_df.sort_values("recent_pct", ascending=True)
    fig_recent = px.bar(
        recent_df,
        x="recent_pct",
        y="country",
        orientation="h",
        color="recent_pct",
        color_continuous_scale="RdYlGn",
        labels={"recent_pct": "% récents", "country": "Pays"},
    )
    fig_recent.update_layout(
        coloraxis_showscale=False,
        margin={"t": 10, "b": 10},
        height=400,
    )
    st.plotly_chart(fig_recent, use_container_width=True)

st.divider()

# ── Raw data explorer ─────────────────────────────────────────────────────────
with st.expander("Explorer les données brutes"):
    mart = st.selectbox(
        "Choisir un mart",
        ["mart_titles_by_genre", "mart_titles_by_decade", "mart_titles_by_country"],
    )
    df_raw = load_mart(f"SELECT * FROM {mart}")
    st.dataframe(df_raw, use_container_width=True)
    st.caption(f"{len(df_raw)} lignes")