import psycopg2 as db
import requests
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import pandas as pd


con = db.connect(host="", port=, database="", user="", password="")

cursor = con.cursor()

url = 'https://fantasy.premierleague.com/api/bootstrap-static/'

fixtures_url = 'https://fantasy.premierleague.com/api/fixtures/'
req = requests.get(url)
fixtures_req = requests.get(fixtures_url)


def insert_players():
    data = req.json()
    print(type(data))
    elements_table = data['elements']
    for element in elements_table:
        player_id = element['id']
        yellow_cards = element['yellow_cards']
        team = element['team']
        second_name = element['second_name']
        points_per_game = element['points_per_game']
        now_cost = element['now_cost']
        goals_scored = element['goals_scored']
        form = element['form']
        first_name = element['first_name']
        assists = element['assists']
        red_cards = element['red_cards']

        cursor.execute(
            "INSERT INTO kadry.players (player_id, yellow_cards, team, second_name, points_per_game, now_cost, goals_scored, form, first_name, assists, red_cards) VALUES (%s, %s, %s, ARRAY[%s], %s, %s, %s, %s, ARRAY[%s], %s, %s)",
            (player_id, yellow_cards, team, second_name, points_per_game, now_cost, goals_scored, form, first_name,
             assists,
             red_cards)
        )
    con.commit()


def create_table_players_stats_history():
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS kadry.players_stats_history (
            change_id SERIAL PRIMARY KEY,
            player_id BIGINT,
            team TEXT,
            second_name TEXT,
            points_per_game DOUBLE PRECISION,
            now_cost INTEGER,
            goals_scored INTEGER,
            form DOUBLE PRECISION,
            first_name TEXT,
            assists INTEGER,
            red_cards INTEGER,
            stat_date TIMESTAMP WITHOUT TIME ZONE
        );
    """)
    con.commit()


def create_table_fixtures():
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS kadry.fixtures (
            fixture_id SERIAL PRIMARY KEY,
            kickoff_time TIMESTAMP,
            started BOOLEAN,
            team_h VARCHAR,
            team_a VARCHAR,
            team_h_score INTEGER,
            team_a_score INTEGER,
            finished BOOLEAN,
            minutes INTEGER,
            provisional_start_time BOOLEAN,
            finished_provisional BOOLEAN,
            event INTEGER,
            difficulty INTEGER
        )
        """)
    con.commit()


def create_table_players_stats():
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS kadry.players_stats_history (
            change_id SERIAL PRIMARY KEY,
            player_id BIGINT,
            team TEXT,
            second_name TEXT,
            points_per_game DOUBLE PRECISION,
            now_cost INTEGER,
            goals_scored INTEGER,
            form DOUBLE PRECISION,
            first_name TEXT,
            assists INTEGER,
            red_cards INTEGER
            );
        """)
    con.commit()


def create_table_teams():
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS kadry.players_stats_history (
            team_id SERIAL PRIMARY KEY,
            code BIGINT,
            name TEXT,
            strength TEXT,
            win INTEGER,
            points INTEGER,
            loss INTEGER
            );
        """)
    con.commit()


def create_trigger_for_players_stats_history():
    cursor.execute("""
    CREATE OR REPLACE FUNCTION kadry.log_player_stats_changes_trigger()
    RETURNS TRIGGER AS $$
    BEGIN
        INSERT INTO kadry.players_stats_history (
            player_id, team, second_name, points_per_game, now_cost,
            goals_scored, form, first_name, assists, red_cards, stat_date
        )
        VALUES (
            NEW.player_id, NEW.team, NEW.second_name, NEW.points_per_game, NEW.now_cost,
            NEW.goals_scored, NEW.form, NEW.first_name, NEW.assists, NEW.red_cards, NOW()
        );
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    
    CREATE TRIGGER log_player_stats_changes_trigger
    AFTER UPDATE ON kadry.players_stats
    FOR EACH ROW
    EXECUTE FUNCTION kadry.log_player_stats_changes_trigger();
    """)
    con.commit()


def insert_players_stats():
    data = req.json()
    print(type(data))
    elements_table = data['elements']
    for element in elements_table:
        player_id = element['id']
        yellow_cards = element['yellow_cards']
        team = element['team']
        second_name = element['second_name']
        points_per_game = element['points_per_game']
        now_cost = element['now_cost']
        goals_scored = element['goals_scored']
        form = element['form']
        first_name = element['first_name']
        assists = element['assists']
        red_cards = element['red_cards']
        stat_date = datetime.now()

        cursor.execute(
            "INSERT INTO kadry.players_stats (player_id, yellow_cards, team, second_name, points_per_game, now_cost, goals_scored, form, first_name, assists, red_cards, stat_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (player_id, yellow_cards, team, second_name, points_per_game, now_cost, goals_scored, form, first_name,
             assists,
             red_cards,
             stat_date
             )
        )
    con.commit()


def update_players_stats():
    data = req.json()
    elements_table = data['elements']
    for element in elements_table:
        player_id = element['id']
        stat_date = datetime.now()
        yellow_cards = element['yellow_cards']
        team = element['team']
        second_name = element['second_name']
        points_per_game = element['points_per_game']
        now_cost = element['now_cost']
        goals_scored = element['goals_scored']
        form = element['form']
        first_name = element['first_name']
        assists = element['assists']
        red_cards = element['red_cards']

        cursor.execute(
            "UPDATE kadry.players_stats SET yellow_cards = %s, team = %s, second_name = %s, points_per_game = %s, now_cost = %s, goals_scored = %s, form = %s, first_name = %s, assists = %s, red_cards = %s, stat_date = %s WHERE player_id = %s",
            (yellow_cards, team, second_name, points_per_game, now_cost, goals_scored, form, first_name, assists,
             red_cards, stat_date, player_id)
        )
    con.commit()


def update_teams_stats():
    cursor.execute("""
        UPDATE kadry.teams
        SET win = (
            SELECT COUNT(*) FROM kadry.fixtures
            WHERE finished = TRUE AND CAST(team_h AS VARCHAR) = CAST(teams.team_id AS VARCHAR) AND team_h_score > team_a_score
                 OR finished = TRUE AND CAST(team_a AS VARCHAR) = CAST(teams.team_id AS VARCHAR) AND team_a_score > team_h_score
        ),
        points = (
            SELECT SUM(CASE
                WHEN CAST(team_h AS VARCHAR) = CAST(teams.team_id AS VARCHAR) THEN
                    CASE
                        WHEN team_h_score > team_a_score THEN 3
                        WHEN team_h_score = team_a_score THEN 1
                        ELSE 0
                    END
                ELSE
                    CASE
                        WHEN team_a_score > team_h_score THEN 3
                        WHEN team_a_score = team_h_score THEN 1
                        ELSE 0
                    END
                END
            ) FROM kadry.fixtures
            WHERE finished = TRUE AND (CAST(team_h AS VARCHAR) = CAST(teams.team_id AS VARCHAR) OR CAST(team_a AS VARCHAR) = CAST(teams.team_id AS VARCHAR))
        ),
        loss = (
            SELECT COUNT(*) FROM kadry.fixtures
            WHERE finished = TRUE AND CAST(team_h AS VARCHAR) = CAST(teams.team_id AS VARCHAR) AND team_h_score < team_a_score
                 OR finished = TRUE AND CAST(team_a AS VARCHAR) = CAST(teams.team_id AS VARCHAR) AND team_a_score < team_h_score
        )
    """)
    con.commit()


def insert_teams():
    data = req.json()
    elements_table = data['teams']
    for team in elements_table:
        team_id = team['id']
        code = team['code']
        name = team['name']
        strength = team['strength']
        win = team['win']
        points = team['points']
        loss = team['loss']

        cursor.execute(
            "INSERT INTO kadry.teams (team_id, code, name, strength, win, points, loss) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (team_id, code, name, strength, win, points, loss)
        )
    con.commit()


def insert_fixtures():
    data = fixtures_req.json()
    print(type(data))
    for fixture in data:
        fixture_id = fixture['id']
        kickoff_time = fixture['kickoff_time']
        started = fixture['started']
        team_a = fixture['team_a']
        team_h = fixture['team_h']
        team_h_score = fixture['team_h_score']
        team_a_score = fixture['team_a_score']
        finished = fixture['finished']
        minutes = fixture['minutes']
        provisional_start_time = fixture['provisional_start_time']
        finished_provisional = fixture['finished_provisional']
        event = fixture['event']
        try:
            difficulty = fixture['difficulty']
        except KeyError:
            difficulty = None
        cursor.execute(
            "INSERT INTO kadry.fixtures (fixture_id, kickoff_time, started, team_a, team_h, team_h_score, team_a_score, finished, minutes, provisional_start_time, finished_provisional, event, difficulty) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (fixture_id, kickoff_time, started, team_a, team_h, team_h_score, team_a_score, finished, minutes,
             provisional_start_time, finished_provisional, event, difficulty)
        )
    con.commit()


def update_teams_data():
    data = req.json()
    print(type(data))
    elements_table = data['teams']
    for team in elements_table:
        team_id = team['id']
        strength = team['strength']
        win = team['win']
        points = team['points']
        loss = team['loss']

        cursor.execute(
            "UPDATE kadry.teams SET strength = %s, win = %s, points = %s, loss = %s WHERE team_id = %s",
            (strength, win, points, loss, team_id)
        )
    con.commit()


def create_points_pie_chart():
    cursor.execute("SELECT name, points FROM kadry.teams")
    data = cursor.fetchall()
    teams = [row[0] for row in data]
    points = [row[1] for row in data]

    plt.pie(points, labels=teams, autopct='%1.1f%%')
    plt.title('Udział punktów dla drużyn')
    plt.axis('equal')
    plt.show()


def create_stats_for_team():
    cursor.execute("SELECT * FROM kadry.fixtures")
    data = cursor.fetchall()
    team_away_fixtures = [row[4] for row in data]
    goals_away = [row[5] for row in data]
    goals_conceded_away = [row[6] for row in data]
    df = pd.DataFrame(data)

    plt.scatter(goals_away, goals_conceded_away, color='blue')
    plt.xlabel('Gole strzelone jako gość')
    plt.ylabel('Gole stracone jako gość')
    plt.title('Liczba goli strzelonych i straconych jako gość')
    plt.show()


def create_top_scorers_chart():
    cursor.execute(
        "SELECT second_name, goals_scored, stat_date FROM kadry.players_stats ORDER BY goals_scored DESC LIMIT 5")
    data = cursor.fetchall()
    players = [row[0] for row in data]
    goals_scored = [row[1] for row in data]
    latest_date = max([row[2] for row in data])

    players = [str(player) for player in players]

    plt.bar(players, goals_scored)
    plt.xlabel('Zawodnicy')
    plt.ylabel('Liczba goli')
    plt.title(f'Najlepsi strzelcy na dzień {latest_date}')
    plt.xticks(rotation=45)
    plt.show()


def create_top_assist_chart():
    cursor.execute("SELECT second_name, assists FROM kadry.players_stats ORDER BY assists DESC LIMIT 5")
    data = cursor.fetchall()
    players = [row[0] for row in data]
    assists = [row[1] for row in data]

    players = [str(player) for player in players]

    plt.bar(players, assists)
    plt.xlabel('Zawodnicy')
    plt.ylabel('Liczba asyst')
    plt.title('Najlepsi asystenci')
    plt.xticks(rotation=45)
    plt.show()


def create_points_chart():
    cursor.execute("SELECT event, team_h, team_a, team_h_score, team_a_score, kickoff_time FROM kadry.fixtures")
    data = cursor.fetchall()

    points_team_13 = []
    points_team_17 = []
    cumulative_points_team_13 = 0
    cumulative_points_team_17 = 0
    current_week_start = None
    previous_points_team_13 = None
    previous_points_team_17 = None

    for row in data:
        event = int(row[0])
        team_h = int(row[1])
        team_a = int(row[2])
        team_h_score = row[3]
        team_a_score = row[4]
        kickoff_time = row[5]

        if team_h_score is None:
            team_h_score = 0
        if team_a_score is None:
            team_a_score = 0

        if team_h == 13:
            if team_h_score > team_a_score:
                cumulative_points_team_13 += 3
            elif team_h_score == team_a_score:
                cumulative_points_team_13 += 1
        elif team_a == 13:
            if team_a_score > team_h_score:
                cumulative_points_team_13 += 3
            elif team_a_score == team_h_score:
                cumulative_points_team_13 += 1
        elif team_h == 17:
            if team_h_score > team_a_score:
                cumulative_points_team_17 += 3
            elif team_h_score == team_a_score:
                cumulative_points_team_17 += 1
        elif team_a == 17:
            if team_a_score > team_h_score:
                cumulative_points_team_17 += 3
            elif team_a_score == team_h_score:
                cumulative_points_team_17 += 1

        if current_week_start is None:
            current_week_start = kickoff_time
        elif kickoff_time - current_week_start >= timedelta(days=7):
            if cumulative_points_team_13 != previous_points_team_13:
                points_team_13.append(cumulative_points_team_13)
                points_team_17.append(cumulative_points_team_17)
                current_week_start = kickoff_time
                previous_points_team_13 = cumulative_points_team_13

    weeks = [f'Tydz. {i+1}' for i in range(len(points_team_13))]

    plt.scatter(weeks, points_team_13, label='Drużyna 13')
    plt.scatter(weeks, points_team_17, label='Drużyna 17')
    plt.xlabel('Tygodnie')
    plt.ylabel('Punkty')
    plt.title('Zdobywanie punktów przez drużynę o ID 13 i 17')
    plt.xticks(rotation=45)
    plt.legend()
    plt.show()


def percentage_stats_goals_chart():
    cursor.execute("""
        SELECT second_name, goals_scored
        FROM kadry.players_stats
        ORDER BY goals_scored DESC
    """)
    data = cursor.fetchall()

    players = [row[0] for row in data]
    goals = [row[1] for row in data]
    total_goals = sum(goals)
    top_players = players[:5]
    top_goals = goals[:5]
    top_percentages = [(goal / total_goals) * 100 for goal in top_goals]

    other_players = players[5:]
    other_goals = goals[5:]

    total_other_goals = sum(other_goals)

    other_percentage = (total_other_goals / total_goals) * 100

    labels = top_players + ['Reszta zawodników']
    percentages = top_percentages + [other_percentage]

    plt.figure(figsize=(8, 6))
    plt.pie(percentages, labels=labels, autopct='%1.1f%%')
    plt.title('Procentowy udział w liczbie strzelonych bramek')
    plt.axis('equal')
    plt.show()


def create_red_cards_chart():
    cursor.execute("""
            SELECT t.name, SUM(ps.red_cards) AS red_cards_count
            FROM kadry.players_stats AS ps
            JOIN kadry.teams AS t ON ps.team = t.team_id
            WHERE ps.red_cards > 0
            GROUP BY t.name
            ORDER BY red_cards_count DESC
        """)
    data = cursor.fetchall()

    teams = [row[0] for row in data]
    red_cards_count = [row[1] for row in data]

    plt.bar(teams, red_cards_count)
    plt.xlabel('Drużyna')
    plt.ylabel('Liczba czerwonych kartek')
    plt.title('Liczba czerwonych kartek w poszczególnych drużynach')
    plt.xticks(rotation=45)

    plt.show()


def average_goals_per_game_chart():
    cursor.execute("""
            SELECT t.name, AVG(ps.goals_scored) AS avg_goals
            FROM kadry.players_stats AS ps
            JOIN kadry.teams AS t ON ps.team = t.team_id
            GROUP BY t.name
            ORDER BY avg_goals DESC
        """)
    data = cursor.fetchall()

    teams = [row[0] for row in data]
    avg_goals = [row[1] for row in data]

    plt.bar(teams, avg_goals)
    plt.xlabel('Drużyna')
    plt.ylabel('Średnia liczba goli na mecz')
    plt.title('Średnia liczba strzelonych goli na mecz w poszczególnych drużynach')

    plt.xticks(rotation=45)
    plt.grid(True)

    plt.show()


def close_connection():
    cursor.close()
    con.close()


def show_charts():
    create_top_scorers_chart()
    create_points_chart()
    percentage_stats_goals_chart()
    create_red_cards_chart()
    average_goals_per_game_chart()
    close_connection()


def update_for_scheduler():
    update_players_stats()
    update_teams_data()
    update_teams_stats()
    close_connection()
