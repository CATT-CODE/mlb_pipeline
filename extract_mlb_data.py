import argparse
import requests
import json
import logging
import os
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

RAW_DIR = "raw"
os.makedirs(RAW_DIR, exist_ok=True)

BASE_URL = "https://statsapi.mlb.com/api/v1"

def get_mlb_teams(season="2024"):
    url = f"{BASE_URL}/teams"
    params = {"activeStatus": "Y", "sportId": "1", "season": season}
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    teams = data.get("teams", [])
    logging.info(f"Retrieved {len(teams)} teams from API.")
    return teams

def get_team_roster(team_id, season="2024", roster_type="active"):
    url = f"{BASE_URL}/teams/{team_id}/roster"
    params = {"season": season, "rosterType": roster_type}
    response = requests.get(url, params=params)
    if response.status_code != 200:
        logging.warning(f"Failed to fetch roster for team {team_id}: {response.status_code}")
        return []
    data = response.json()
    roster = data.get("roster", [])
    return roster

def get_schedule(season="2024", start_date="2024-04-01", end_date="2024-04-07"):
    url = f"{BASE_URL}/schedule"
    params = {"season": season, "startDate": start_date, "endDate": end_date, "sportId": "1"}
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    logging.info("Schedule data retrieved from API.")
    return data

def get_game_boxscore(gamePk):
    url = f"{BASE_URL}/game/{gamePk}/boxscore"
    response = requests.get(url)
    if response.status_code != 200:
        logging.warning(f"Failed to fetch boxscore for game {gamePk}: {response.status_code}")
        return None
    return response.json()

def parse_game_from_schedule(game):
    game_id = game.get("gamePk")
    game_date = game.get("gameDate")
    location = game.get("venue", {}).get("name", "Unknown")
    
    teams = game.get("teams", {})
    home_team = teams.get("home", {}).get("team", {})
    away_team = teams.get("away", {}).get("team", {})
    home_team_id = home_team.get("id")
    away_team_id = away_team.get("id")
    home_team_score = teams.get("home", {}).get("score")
    away_team_score = teams.get("away", {}).get("score")

    return {
        "game_id": game_id,
        "game_date": game_date,
        "location": location,
        "home_team_id": home_team_id,
        "away_team_id": away_team_id,
        "home_team_score": home_team_score,
        "away_team_score": away_team_score
    }

def parse_boxscore_stats(boxscore, game_id):
    batter_stats_list = []
    pitcher_stats_list = []
    
    teams = boxscore.get("teams", {})
    for side in ["home", "away"]:
        team = teams.get(side, {})
        players = team.get("players", {})
        for player_key, player_info in players.items():
            person = player_info.get("person", {})
            player_id = person.get("id")
            stats = player_info.get("stats", {})
            
            # Extract batting stats
            batting = stats.get("batting")
            if batting:
                batter_stats = {
                    "game_id": game_id,
                    "player_id": player_id,
                    "at_bats": int(batting.get("atBats", 0)),
                    "runs": int(batting.get("runs", 0)),
                    "hits": int(batting.get("hits", 0)),
                    "doubles": int(batting.get("doubles", 0)),
                    "triples": int(batting.get("triples", 0)),
                    "home_runs": int(batting.get("homeRuns", 0)),
                    "rbi": int(batting.get("rbi", 0)),
                    "walks": int(batting.get("baseOnBalls", 0)),
                    "hit_by_pitch": int(batting.get("hitByPitch", 0)),
                    "strikeouts": int(batting.get("strikeOuts", 0)),
                    "stolen_bases": int(batting.get("stolenBases", 0)),
                    "caught_stealing": int(batting.get("caughtStealing", 0)),
                    "total_bases": int(batting.get("totalBases", 0)),
                    "sac_flies": int(batting.get("sacFlies", 0)), 
                }
                batter_stats_list.append(batter_stats)

            
            # Extract pitching stats
            pitching = stats.get("pitching")
            if pitching:
                innings_str = pitching.get("inningsPitched", "0")
                try:
                    innings_pitched = float(innings_str)
                except Exception:
                    innings_pitched = 0.0
                pitcher_stats = {
                    "game_id": game_id,
                    "player_id": player_id,
                    "innings_pitched": innings_pitched,
                    "hits_allowed": int(pitching.get("hits", 0)),
                    "runs_allowed": int(pitching.get("runs", 0)),
                    "earned_runs": int(pitching.get("earnedRuns", 0)),
                    "home_runs_allowed": int(pitching.get("homeRuns", 0)),
                    "walks_allowed": int(pitching.get("baseOnBalls", 0)),
                    "strikeouts": int(pitching.get("strikeOuts", 0))
                }
                pitcher_stats_list.append(pitcher_stats)
    
    return batter_stats_list, pitcher_stats_list

def main():
    parser = argparse.ArgumentParser(description="Fetch and store MLB game data for a given date range.")
    parser.add_argument("start_date", help="Start date in YYYY-MM-DD format")
    parser.add_argument("end_date", help="End date in YYYY-MM-DD format")

    args = parser.parse_args()

    try:
        datetime.strptime(args.start_date, "%Y-%m-%d")
        datetime.strptime(args.end_date, "%Y-%m-%d")
    except ValueError:
        logging.error("Invalid date format. Please use YYYY-MM-DD.")
        return

    season = "2024"
    start_date = args.start_date
    end_date = args.end_date
    
    raw_data = {}
    raw_data["teams"] = get_mlb_teams(season)
    
    # Extract team rosters
    rosters = {}
    for team in raw_data["teams"]:
        team_id = team.get("id")
        team_name = team.get("name")
        roster = get_team_roster(team_id, season)
        rosters[team_name] = roster
    raw_data["rosters"] = rosters
    logging.info(f"{len(rosters)} team rosters fetched from API.")
    
    # Extract schedule
    schedule_data = get_schedule(season, start_date, end_date)
    raw_data["schedule"] = schedule_data
    
    # From schedule, extract games and boxscore stats
    games_list = []
    overall_batter_stats = []
    overall_pitcher_stats = []
    
    logging.info(f"Extracting pitcher and batter stats data from API.")
    for date_entry in schedule_data.get("dates", []):
        for game in date_entry.get("games", []):
            game_info = parse_game_from_schedule(game)
            games_list.append(game_info)
            
            gamePk = game.get("gamePk")
            boxscore = get_game_boxscore(gamePk)
            if not boxscore:
                continue
            b_stats, p_stats = parse_boxscore_stats(boxscore, game_info["game_id"])
            overall_batter_stats.extend(b_stats)
            overall_pitcher_stats.extend(p_stats)
    
    raw_data["games"] = games_list
    raw_data["batter_stats"] = overall_batter_stats
    raw_data["pitcher_stats"] = overall_pitcher_stats

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"mlb_raw_{start_date}_{end_date}_{timestamp}.json"
    filepath = os.path.join(RAW_DIR, filename)
    
    # Write the raw data to JSON file
    with open(filepath, "w") as f:
        json.dump(raw_data, f, indent=4)
    logging.info(f"Raw data written to {filepath}")

if __name__ == "__main__":
    main()
