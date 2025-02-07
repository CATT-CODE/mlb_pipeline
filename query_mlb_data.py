import sqlite3
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

DATABASE_FILE = "mlb.db"

def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        logging.info("Database connection established.")
        return conn
    except sqlite3.Error as e:
        logging.error(f"Error connecting to database: {e}")
        raise

def query_player_combinations(conn, exclude_judge=False):
    """
    Identifies the top 10 combinations of players who hit home runs on the same day.
    """
    try:
        cursor = conn.cursor()
        if not exclude_judge:
            query = '''
                WITH hr_days AS (
                    SELECT bs.player_id, date(g.game_date) as game_day
                    FROM batter_stats bs
                    JOIN games g ON bs.game_id = g.game_id
                    WHERE bs.home_runs > 0
                    GROUP BY bs.player_id, game_day
                )
                SELECT p1.name AS player1, p2.name AS player2, COUNT(*) AS frequency
                FROM hr_days h1
                JOIN hr_days h2 ON h1.game_day = h2.game_day AND h1.player_id < h2.player_id
                JOIN players p1 ON h1.player_id = p1.player_id
                JOIN players p2 ON h2.player_id = p2.player_id
                GROUP BY p1.name, p2.name
                ORDER BY frequency DESC
                LIMIT 10;
            '''
        else:
            query = '''
                WITH hr_days AS (
                    SELECT bs.player_id, date(g.game_date) as game_day
                    FROM batter_stats bs
                    JOIN games g ON bs.game_id = g.game_id
                    WHERE bs.home_runs > 0
                    GROUP BY bs.player_id, game_day
                )
                SELECT p1.name AS player1, p2.name AS player2, COUNT(*) AS frequency
                FROM hr_days h1
                JOIN hr_days h2 ON h1.game_day = h2.game_day AND h1.player_id < h2.player_id
                JOIN players p1 ON h1.player_id = p1.player_id
                JOIN players p2 ON h2.player_id = p2.player_id
                WHERE p1.name <> 'Aaron Judge' AND p2.name <> 'Aaron Judge'
                GROUP BY p1.name, p2.name
                ORDER BY frequency DESC
                LIMIT 10;
            '''
        cursor.execute(query)
        results = cursor.fetchall()
        return results
    except sqlite3.Error as e:
        logging.error(f"Error executing query: {e}")
        raise

def main():
    try:
        conn = create_connection(DATABASE_FILE)
        
        print("\nTop 10 Player Home Run Combinations (including all players):")
        combos = query_player_combinations(conn, exclude_judge=False)
        for row in combos:
            print(f"{row[0]} / {row[1]} - {row[2]} times")
        
        print("\nTop 10 Player Home Run Combinations (excluding Aaron Judge):")
        combos_excluding = query_player_combinations(conn, exclude_judge=True)
        for row in combos_excluding:
            print(f"{row[0]} / {row[1]} - {row[2]} times")
    except Exception as e:
        logging.error(f"Failed to query data: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

if __name__ == '__main__':
    main()