"""Prefect flow: raw Parquet -> DuckDB (two kinds of duplicates removed) -> one chart.

  python quakes_flow.py
"""
import os
import sys

import duckdb
import matplotlib


matplotlib.use("Agg")  # draw to a file, no window
import matplotlib.pyplot as plt
from prefect import flow, get_run_logger, task


SRC = sys.argv[1] if len(sys.argv) > 1 else "data/raw"
RAW = SRC.rstrip("/\\") + "/*.parquet"
NAME = os.path.basename(SRC.rstrip("/\\"))       # "raw" or "sample"
TAG = "" if NAME == "raw" else "_" + NAME
DB = f"bluesky{TAG}.duckdb"                     
CHART = f"charts/language_heatmap{TAG}.png"        # the sample never overwrites your chart


@task(retries=2, retry_delay_seconds=5)
def load_raw():
    with duckdb.connect(DB) as con:  # the flow is the ONLY writer of bluesky.duckdb
        con.execute(f"CREATE OR REPLACE TABLE raw_posts AS SELECT * FROM read_parquet('{RAW}')")
        return con.execute("SELECT count(*) FROM raw_posts").fetchone()[0]


@task
def stage():
    """Kind 1: the same Kafka record landed twice. DISTINCT keeps one copy."""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE stg_records AS
            SELECT DISTINCT * EXCLUDE (landed_at)
            FROM raw_posts
            WHERE action <> 'malformed'
              AND kind = 'commit'                   -- not identity or account events
              AND did IS NOT NULL AND rkey IS NOT NULL
        """)
        q = lambda sql: con.execute(sql).fetchone()[0]
        return {"malformed": q("SELECT count(*) FROM raw_posts WHERE action = 'malformed'"),
                "records": q("SELECT count(*) FROM stg_records")}

@task
def entities():
    """Kind 2: one row per post (did, rkey), with its latest operation."""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE posts AS
            SELECT did, rkey, operation, lang, text,
                   make_timestamp(time_us) AS event_time,  -- microseconds -> UTC
                   (raw::JSON ->> '$.data.commit.record.reply.parent.uri')
                       IS NOT NULL AS is_reply  -- parsed from raw
            FROM stg_records
            QUALIFY row_number() OVER (PARTITION BY did, rkey
                                       ORDER BY time_us DESC, kafka_offset DESC) = 1
        """)
        return con.execute("SELECT count(*) FROM posts").fetchone()[0]



@task
def marts():
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE mart_lengths AS
            SELECT lang,
                CASE
                    WHEN length(text) <= 50 THEN '0-50'
                    WHEN length(text) <= 100 THEN '51-100'
                    WHEN length(text) <= 150 THEN '101-150'
                    WHEN length(text) <= 200 THEN '151-200'
                    ELSE '201+'
                END AS length_group,
                COUNT(*) AS posts
            FROM posts
            WHERE operation = 'create'
                AND lang IS NOT NULL
                AND text IS NOT NULL
            GROUP BY lang, length_group
            ORDER BY lang, length_group
        """)


#Research Question: How does the distribution of post lengths differ across Bluesky’s major language communities?'''

@task
def chart():
    with duckdb.connect(DB, read_only=True) as con:
        df = con.execute("""
            SELECT lang, length_group, posts,
                100.0 * posts / SUM(posts) OVER (PARTITION BY lang) AS pct_posts
            FROM mart_lengths
        """).df()

    # Top 10 languages
    top_langs = df.groupby("lang")["posts"].sum().nlargest(10).index
    df = df[df["lang"].isin(top_langs)]

    # Reshape for heatmap
    groups = ["0-50", "51-100", "101-150", "151-200", "201+"]
    heatmap = df.pivot(index="lang", columns="length_group", values="pct_posts")
    heatmap = heatmap.reindex(index=top_langs, columns=groups).fillna(0)

    # Convert language codes to full names
    languages = {
        "en": "English", "ja": "Japanese", "pt": "Portuguese", "es": "Spanish",
        "de": "German", "fr": "French", "nl": "Dutch", "ne": "Nepali",
        "ko": "Korean", "tr": "Turkish"}

    lang_labels = [languages.get(lang, lang) for lang in top_langs]

    os.makedirs("charts", exist_ok=True)
    plt.figure(figsize=(10, 5))
    im = plt.imshow(heatmap, aspect="auto", cmap="YlOrRd")
    plt.colorbar(im, label="% of language's total posts")
    
    plt.xticks(range(len(groups)), groups)
    plt.yticks(range(len(top_langs)), lang_labels)
    plt.xlabel("Post length (characters)")
    plt.ylabel("Language")
    plt.title("Distribution of Bluesky post lengths by language")

    # Add percentages inside each square
    for i in range(len(top_langs)):
        for j in range(len(groups)):
            value = heatmap.iloc[i, j]
            color = "white" if im.norm(value) > 0.6 else "black"
            plt.text(j, i, f"{value:.1f}%", ha="center", va="center",
                     color=color, fontsize=10)


    plt.tight_layout()
    plt.savefig(CHART, dpi=120)
    plt.close()

    return CHART



@flow(name="bluesky-pipeline")
def pipeline():
    log = get_run_logger()
    raw = load_raw()
    n = stage()
    things = entities()
    marts()
    path = chart()
    log.info("Raw records landed:        %d", raw)
    log.info("Malformed (kept in raw):   %d", n["malformed"])
    log.info("Unique Kafka records:      %d", n["records"])
    log.info("Unique posts:             %d", things)
    log.info("Chart: %s", path)



if __name__ == "__main__":
    pipeline()
