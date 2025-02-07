import sqlite3
import json
import logging
import os
import shutil
import re
from datetime import datetime

DATABASE_FILE = "mlb.db"
RAW_DIR = "raw"
HISTORICAL_DIR = "historical"

os.makedirs(HISTORICAL_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Database connection and table creation

def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        logging.info("Database connection established.")
        return conn
    except sqlite3.Error as e:
        logging.error(f"Error connecting to database: {e}")
        raise

def create_tables(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("PRAGMA foreign_keys = ON;")
        cursor.executescript('''
            CREATE TABLE IF NOT EXISTS teams (
                team_id INTEGER PRIMARY KEY AUTOINCREMENT,
                api_team_id INTEGER UNIQUE,
                name TEXT NOT NULL,
                venue TEXT,
                city TEXT
            );
            
            CREATE TABLE IF NOT EXISTS players (
                player_id INTEGER PRIMARY KEY AUTOINCREMENT,
                api_player_id INTEGER UNIQUE,
                name TEXT NOT NULL,
                team_id INTEGER,
                position TEXT,
                FOREIGN KEY (team_id) REFERENCES teams(team_id)
            );
            
            CREATE TABLE IF NOT EXISTS games (
                game_id INTEGER PRIMARY KEY AUTOINCREMENT,
                api_game_id INTEGER UNIQUE,
                game_date TEXT,
                location TEXT,
                home_team_id INTEGER,
                away_team_id INTEGER,
                home_team_score INTEGER,
                away_team_score INTEGER,
                FOREIGN KEY (home_team_id) REFERENCES teams(team_id),
                FOREIGN KEY (away_team_id) REFERENCES teams(team_id)
            );
            
            CREATE TABLE IF NOT EXISTS batter_stats (
                stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id INTEGER,
                player_id INTEGER,
                at_bats INTEGER,
                runs INTEGER,
                hits INTEGER,
                doubles INTEGER,
                triples INTEGER,
                home_runs INTEGER,
                rbi INTEGER,
                walks INTEGER,
                hit_by_pitch INTEGER,
                strikeouts INTEGER,
                stolen_bases INTEGER,
                caught_stealing INTEGER,
                avg REAL,
                obp REAL,
                slg REAL,
                ops REAL,
                FOREIGN KEY (game_id) REFERENCES games(game_id),
                FOREIGN KEY (player_id) REFERENCES players(player_id)
            );
            
            CREATE TABLE IF NOT EXISTS pitcher_stats (
                stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id INTEGER,
                player_id INTEGER,
                innings_pitched REAL,
                hits_allowed INTEGER,
                runs_allowed INTEGER,
                earned_runs INTEGER,
                home_runs_allowed INTEGER,
                walks_allowed INTEGER,
                strikeouts INTEGER,
                FOREIGN KEY (game_id) REFERENCES games(game_id),
                FOREIGN KEY (player_id) REFERENCES players(player_id)
            );
            
            CREATE TABLE IF NOT EXISTS processed_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_name TEXT UNIQUE,
                start_date TEXT,
                end_date TEXT,
                processed_at TEXT
            );
        ''')
        conn.commit()
        logging.info("Tables created successfully.")
    except sqlite3.Error as e:
        logging.error(f"Error creating tables: {e}")
        conn.rollback()
        raise

# Functions for upserting teams, players, and games

def get_or_create_team(conn, api_team_id, name, venue, city=None):
    try:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO teams (api_team_id, name, venue, city) VALUES (?, ?, ?, ?)",
            (api_team_id, name, venue, city)
        )
        conn.commit()
        cursor.execute("SELECT team_id FROM teams WHERE api_team_id = ?", (api_team_id,))
        row = cursor.fetchone()
        if row:
            return row[0]
        else:
            raise Exception(f"Error retrieving team with api_team_id {api_team_id}")
    except sqlite3.Error as e:
        logging.error(f"Error in get_or_create_team for {name}: {e}")
        raise

def get_or_create_player(conn, api_player_id, name, team_id, position):
    try:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO players (api_player_id, name, team_id, position) VALUES (?, ?, ?, ?)",
            (api_player_id, name, team_id, position)
        )
        conn.commit()
        cursor.execute("SELECT player_id FROM players WHERE api_player_id = ?", (api_player_id,))
        row = cursor.fetchone()
        if row:
            return row[0]
        else:
            raise Exception(f"Error retrieving player with api_player_id {api_player_id}")
    except sqlite3.Error as e:
        logging.error(f"Error in get_or_create_player for {name}: {e}")
        raise

def get_or_create_game(conn, api_game_id, game_date, location, home_team_id, away_team_id, home_team_score, away_team_score):
    try:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO games (api_game_id, game_date, location, home_team_id, away_team_id, home_team_score, away_team_score) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (api_game_id, game_date, location, home_team_id, away_team_id, home_team_score, away_team_score)
        )
        conn.commit()
        cursor.execute("SELECT game_id FROM games WHERE api_game_id = ?", (api_game_id,))
        row = cursor.fetchone()
        if row:
            return row[0]
        else:
            raise Exception(f"Error retrieving game with api_game_id {api_game_id}")
    except sqlite3.Error as e:
        logging.error(f"Error in get_or_create_game for API game ID {api_game_id}: {e}")
        raise

def bulk_insert_stats(conn, table, columns, data):
    try:
        cursor = conn.cursor()
        placeholders = ','.join(['?'] * len(columns))
        sql = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders})"
        cursor.executemany(sql, data)
        conn.commit()
        logging.info(f"Bulk inserted {len(data)} rows into {table}.")
    except sqlite3.Error as e:
        logging.error(f"Error bulk inserting into {table}: {e}")
        raise

# The main transform and load function

def transform_and_load(file_path):
    # Load the raw JSON
    with open(file_path, "r") as f:
        data = json.load(f)
    
    conn = create_connection(DATABASE_FILE)

    try:
        with conn:
            # Process teams
            teams = data.get("teams", [])
            team_mapping_by_api = {}
            team_mapping_by_name = {}
            for team in teams:
                api_team_id = team.get("id")
                name = team.get("name")
                venue = team.get("venue", {}).get("name")
                city = team.get("locationName")
                db_team_id = get_or_create_team(conn, api_team_id, name, venue, city)
                team_mapping_by_api[api_team_id] = db_team_id
                team_mapping_by_name[name] = db_team_id

            # Process rosters and players
            rosters = data.get("rosters", {})
            player_mapping = {}
            for team_key, roster in rosters.items():
                if team_key.isdigit():
                    db_team_id = team_mapping_by_api.get(int(team_key))
                else:
                    db_team_id = team_mapping_by_name.get(team_key)
                if db_team_id is None:
                    logging.warning(f"Team {team_key} not found in team mapping.")
                    continue
                for player in roster:
                    person = player.get("person", {})
                    api_player_id = person.get("id")
                    full_name = person.get("fullName")
                    position = player.get("position", {}).get("abbreviation", "NA")
                    db_player_id = get_or_create_player(conn, api_player_id, full_name, db_team_id, position)
                    player_mapping[api_player_id] = db_player_id
            
            # Process games
            games = data.get("games", [])
            game_mapping = {}
            for game in games:
                api_game_id = game.get("game_id")
                game_date = game.get("game_date")
                location = game.get("location")
                home_api_team_id = game.get("home_team_id")
                away_api_team_id = game.get("away_team_id")
                home_team_score = game.get("home_team_score")
                away_team_score = game.get("away_team_score")
                home_team_db_id = team_mapping_by_api.get(home_api_team_id)
                away_team_db_id = team_mapping_by_api.get(away_api_team_id)
                if home_team_db_id is None or away_team_db_id is None:
                    logging.warning(f"Game {api_game_id}: Missing team mapping for home or away team.")
                    continue
                db_game_id = get_or_create_game(conn, api_game_id, game_date, location, home_team_db_id, away_team_db_id, home_team_score, away_team_score)
                game_mapping[api_game_id] = db_game_id
            
        # Process batter stats
        batter_stats = data.get("batter_stats", [])
        if batter_stats:
            batter_columns = (
                'game_id', 'player_id', 'at_bats', 'runs', 'hits', 'doubles', 'triples', 
                'home_runs', 'rbi', 'walks', 'hit_by_pitch', 'strikeouts', 
                'stolen_bases', 'caught_stealing', 'avg', 'obp', 'slg', 'ops'
            )
            batter_stats_data = []
            for stat in batter_stats:
                api_game_id = stat.get("game_id")
                api_player_id = stat.get("player_id")
                local_game_id = game_mapping.get(api_game_id)
                local_player_id = player_mapping.get(api_player_id)
                if local_game_id and local_player_id:
                    at_bats = stat.get("at_bats", 0)
                    hits = stat.get("hits", 0)
                    walks = stat.get("walks", 0)
                    hit_by_pitch = stat.get("hit_by_pitch", 0)
                    sac_flies = stat.get("sac_flies", 0)
                    total_bases = stat.get("total_bases", 0)
                    avg = round(hits / at_bats, 3) if at_bats > 0 else 0.0
                    obp_denominator = (at_bats + walks + hit_by_pitch + sac_flies)
                    obp = round((hits + walks + hit_by_pitch) / obp_denominator, 3) if obp_denominator > 0 else 0.0
                    slg = round(total_bases / at_bats, 3) if at_bats > 0 else 0.0
                    ops = round(obp + slg, 3)
                    row = (
                        local_game_id,
                        local_player_id,
                        at_bats,
                        stat.get("runs", 0),
                        hits,
                        stat.get("doubles", 0),
                        stat.get("triples", 0),
                        stat.get("home_runs", 0),
                        stat.get("rbi", 0),
                        walks,
                        hit_by_pitch,
                        stat.get("strikeouts", 0),
                        stat.get("stolen_bases", 0),
                        stat.get("caught_stealing", 0),
                        avg,
                        obp,
                        slg,
                        ops
                    )
                    batter_stats_data.append(row)
            bulk_insert_stats(conn, "batter_stats", batter_columns, batter_stats_data)
            
        # Process pitcher stats
        pitcher_stats = data.get("pitcher_stats", [])
        if pitcher_stats:
            pitcher_columns = (
                'game_id', 'player_id', 'innings_pitched', 'hits_allowed', 'runs_allowed', 
                'earned_runs', 'home_runs_allowed', 'walks_allowed', 'strikeouts'
            )
            pitcher_stats_data = []
            for stat in pitcher_stats:
                api_game_id = stat.get("game_id")
                api_player_id = stat.get("player_id")
                local_game_id = game_mapping.get(api_game_id)
                local_player_id = player_mapping.get(api_player_id)
                if local_game_id and local_player_id:
                    row = (
                        local_game_id,
                        local_player_id,
                        stat.get("innings_pitched", 0.0),
                        stat.get("hits_allowed", 0),
                        stat.get("runs_allowed", 0),
                        stat.get("earned_runs", 0),
                        stat.get("home_runs_allowed", 0),
                        stat.get("walks_allowed", 0),
                        stat.get("strikeouts", 0)
                    )
                    pitcher_stats_data.append(row)
            bulk_insert_stats(conn, "pitcher_stats", pitcher_columns, pitcher_stats_data)
            
        logging.info("Data transformed and loaded successfully.")
    except Exception as e:
        logging.error(f"Error during transform and load: {e}")
        raise
    finally:
        conn.close()

# Duplicate checking based on overlapping date ranges

def extract_date_range(filename):
    """
    Extracts start_date and end_date from a filename expected to be in the format:
    mlb_raw_<start_date>_<end_date>_<timestamp>.json
    Accepts dates with hyphens (e.g. 2024-05-01).
    """
    match = re.match(r"mlb_raw_([\d-]+)_([\d-]+)_.*\.json", filename)
    if match:
        return match.group(1), match.group(2)
    else:
        return None, None

def file_overlaps_processed(conn, new_start, new_end):
    """
    Checks if the new date range [new_start, new_end] overlaps with any already processed date ranges.
    Two ranges overlap if:
      new_start <= existing_end AND new_end >= existing_start
    """
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT 1 FROM processed_files WHERE (? <= end_date) AND (? >= start_date)",
            (new_start, new_end)
        )
        return cursor.fetchone() is not None
    except sqlite3.Error as e:
        logging.error(f"Error checking processed_files for overlapping date ranges: {e}")
        return False

def record_file_processed(conn, filename, start_date, end_date):
    try:
        cursor = conn.cursor()
        processed_at = datetime.now().isoformat()
        cursor.execute(
            "INSERT INTO processed_files (file_name, start_date, end_date, processed_at) VALUES (?, ?, ?, ?)",
            (filename, start_date, end_date, processed_at)
        )
        conn.commit()
        logging.info(f"Recorded file {filename} as processed.")
    except sqlite3.Error as e:
        logging.error(f"Error recording processed file {filename}: {e}")
        raise

# Orchestrate file processing

def main():
    conn = create_connection(DATABASE_FILE)
    create_tables(conn)
    
    for filename in os.listdir(RAW_DIR):
        if filename.endswith(".json"):
            start_date, end_date = extract_date_range(filename)
            if start_date and end_date:
                if file_overlaps_processed(conn, start_date, end_date):
                    logging.info(f"Data for period {start_date} to {end_date} overlaps with already processed data. Skipping file {filename}.")
                    continue
            else:
                logging.warning(f"Filename {filename} does not match expected pattern for date extraction.")
            
            file_path = os.path.join(RAW_DIR, filename)
            logging.info(f"Processing file {file_path}")
            try:
                transform_and_load(file_path)
                if start_date and end_date:
                    record_file_processed(conn, filename, start_date, end_date)
                new_path = os.path.join(HISTORICAL_DIR, filename)
                shutil.move(file_path, new_path)
                logging.info(f"Moved processed file to {new_path}")
            except Exception as e:
                logging.error(f"Failed to process file {file_path}: {e}")
    conn.close()

if __name__ == "__main__":
    main()
