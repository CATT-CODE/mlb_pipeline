# MLB Data Pipeline

## Overview

This project implements an ETL pipeline for MLB data. It pulls MLB stats (batters, pitchers, and game data), processes and structures the data, and runs analytical queries. The pipeline is designed to be efficient, prevent duplicate processing, and allow easy scaling.

The main focus of this project is on ETL (Extract, Transform, Load), database schema design, and query performance. One of the main queries I’ve optimized for is:

> **Player Combination Home Runs** – Finding the top 10 pairs of players who have hit home runs on the same day (including and excluding Aaron Judge).

---

## How It Works

The pipeline consists of three main scripts:

### 1. Data Extraction (`extract_mlb_data.py`)

- Pulls **team, roster, game, and boxscore** data from an **MLB API**.
- Stores everything in **JSON format** inside the `raw/` folder.
- Accepts **start and end dates** as command-line arguments.

#### **Command to run:**
```sh
python3 extract_mlb_data.py 2024-04-01 2024-04-30
```
*(I’ve already included four month’s worth of JSON data in `raw/` in case you don’t want to wait for API calls.)*

---

### 2. Data Transformation & Loading (`transform_load_mlb_data.py`)

- Reads the extracted **JSON** and loads it into an **SQLite database**.
- Uses a **structured relational schema** for efficient querying.
- **Batches inserts** to improve performance.
- Moves processed files to `historical/` to avoid reprocessing.

#### **Command to run:**
```sh
python3 transform_load_mlb_data.py
```
*(Automatically handles new files and prevents duplicate date ranges from being loaded.)*

---

### 3. Querying the Data (`query_mlb_data.py`)

- Runs **SQL queries** to analyze player performance.

#### **Command to run:**
```sh
python3 query_mlb_data.py
```
*(Prints out the top 10 player combinations who hit home runs on the same day, both with and without Aaron Judge.)*

---

## Database Design

### **Schema**
Here’s a quick breakdown of how the data is structured:

| Table | Description |
|--------|-------------|
| `teams` | Stores team info (team name, venue, city). |
| `players` | Stores player info (name, team, position). |
| `games` | Stores game data (date, teams, location, scores). |
| `batter_stats` | Stores batting stats (home runs, hits, strikeouts, etc.). |
| `pitcher_stats` | Stores pitching stats (innings, runs allowed, strikeouts, etc.). |
| `processed_files` | Tracks which files have already been processed to prevent duplicate loads. |

*(An ERD diagram is provided in the repository (see mlb_db_erd.png) for a visual representation of the schema.)*

### **Why a Relational Database?**
I chose **SQLite** because:

- **Structured data**: MLB stats fit well into a relational model.
- **Easy to query**: SQL is great for joins and aggregations.
- **Lightweight**: No complex setup or external packages needed for this demo.

*(For production, something like **PostgreSQL** would handle concurrent writes better.)*

---

## Setup & Running the Pipeline

### **1. Clone the Repo**
```sh
git clone https://github.com/CATT-CODE/mlb_pipeline.git
cd mlb_pipeline
```

### **2. Run Extraction**
```sh
python3 extract_mlb_data.py 2024-04-01 2024-04-30
```
*(Fetches game data for the given date range and saves it as JSON. I’ve already included four month’s worth of JSON data in `raw/` in case you don’t want to wait for API calls when running this script.)*

### **3. Run Transformation & Load**
```sh
python3 transform_load_mlb_data.py
```
*(Processes the JSON and loads it into SQLite.)*

### **4. Run Queries**
```sh
python3 query_mlb_data.py
```
*(Prints the top player combos for home runs on the same day.)*

---

## Challenges & What Could Be Better

### **Challenges**
- **Large JSON files** (~250k lines for a month of data).
  - Right now, I load everything into memory at once using `json.load()`, which is fine for this scale but wouldn’t be efficient file I/O and parsing for much bigger datasets.
- **SQLite’s write constraints**: Since SQLite locks the database during writes, concurrent processing is tricky. If this were a high-scale production system, I’d switch to a DB like **PostgreSQL**.

### **Potential Improvements**
- **Better scheduling & automation**
  - Using cron jobs or **Apache Airflow** to auto-run extraction & processing.
  - Detect new JSON files landing in storage and trigger processing.
- **Parallelization for performance**
  - Use multiprocessing or multithreading to handle multiple JSON files concurrently for parsing. (A more scalable DBMS would be needed due to SQLite’s write-lock limitations.)
  - Offload writes to a queue to avoid SQLite write contention.
- **Streaming JSON processing**
  - Instead of `json.load()`, use **ijson** to process large files in chunks.
- **Enhanced Error Handling** 
  - Add robust validation, logging, and exception management to the ETL process.
- **Performance Optimization** 
  - Implement indexing on critical fields (e.g., game_date, player_id) as the data volume grows.
- **Web interface for analysis**
  - **API Layer:** Expose the analytical queries via RESTful endpoints for real-time data retrieval.
  - **Interactive Dashboards:** Develop front-end dashboards to visualize player profiles, game statistics, and analytical insights.

---

## AI & Future Additions

If I had to integrate AI into this, I’d consider:

- **Natural Language to SQL**
  - A chatbot or web interface where users type questions like:  
    *"Which two players hit home runs the most on the same day?"*  
    and AI translates it into an SQL query.
- **Query Validation with AI**
  - Use a model to verify if auto-generated SQL queries are valid based on historical query patterns. Iteratively refine the AI’s translation capability based on user feedback.
- **Predictive Analytics for MLB stats**
  - Train a model to predict which players are most likely to hit home runs in the next game, etc.

---

## Final Thoughts

This project showcases an end-to-end ETL pipeline for MLB stats with a focus on:

- **Efficient data extraction & processing**
- **Query performance & structured schema design**
- **Preventing duplicate processing**
- **Potential for scalability & automation**

---

## 2024 MLB Season Query Results

### Top 10 Player Home Run Combinations (including all players):
- Aaron Judge / Anthony Santander - 16 times
- Aaron Judge / Giancarlo Stanton - 16 times
- Aaron Judge / Juan Soto - 15 times
- Aaron Judge / José Ramírez - 14 times
- Aaron Judge / Shohei Ohtani - 14 times
- José Ramírez / Shohei Ohtani - 14 times
- Marcell Ozuna / Aaron Judge - 14 times
- Marcell Ozuna / Anthony Santander - 14 times
- Shohei Ohtani / Pete Alonso - 14 times
- Brent Rooker / Aaron Judge - 13 times

### Top 10 Player Home Run Combinations (excluding Aaron Judge):
- José Ramírez / Shohei Ohtani - 14 times
- Marcell Ozuna / Anthony Santander - 14 times
- Shohei Ohtani / Pete Alonso - 14 times
- Brent Rooker / Shohei Ohtani - 13 times
- Juan Soto / Gunnar Henderson - 13 times
- Marcell Ozuna / Juan Soto - 13 times
- Shohei Ohtani / Francisco Lindor - 13 times
- Teoscar Hernández / Shohei Ohtani - 13 times
- Alex Bregman / Shohei Ohtani - 12 times
- Jake Burger / Shohei Ohtani - 12 times

---

## Quick Commands Summary

| **Action** | **Command** |
|------------|-------------|
| Extract Data | `python3 extract_mlb_data.py <start-date (YYYY-MM-DD)> <end-date (YYYY-MM-DD)>` |
| Transform & Load Data | `python3 transform_load_mlb_data.py` |
| Run Queries | `python3 query_mlb_data.py` |

---
