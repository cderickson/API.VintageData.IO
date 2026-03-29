from flask import Flask, jsonify, redirect, request, g
from flask_stats import Stats
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import psycopg2
import os
import json
from datetime import datetime, timedelta

required_db_env_vars = ("DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME")
missing_db_env_vars = [name for name in required_db_env_vars if not os.getenv(name)]
if missing_db_env_vars:
    raise RuntimeError(f"Missing required environment variables: {', '.join(missing_db_env_vars)}")

credentials = [os.getenv("DB_HOST"), os.getenv("DB_PORT"), os.getenv("DB_USER"), os.getenv("DB_PASSWORD"), os.getenv("DB_NAME")]

app = Flask(__name__)
stats = Stats(app)
limiter = Limiter(key_func=get_remote_address, default_limits=['100 per minute'])
limiter.init_app(app)

app.json.sort_keys = False
page_size = 1000

def get_db_connection():
    conn = psycopg2.connect(
        host=credentials[0],
        port=credentials[1],
        user=credentials[2],
        password=credentials[3],
        database=credentials[4]
    )
    return conn

def run_select_query(query, params=None):
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        if params is None:
            cursor.execute(query)
        else:
            cursor.execute(query, params)

        column_names = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        return [dict(zip(column_names, row)) for row in data]
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

# @app.before_request
# def log_request():
#     if request.endpoint:
#         start_time = datetime.utcnow()

#         conn = get_db_connection()
#         cursor = conn.cursor()
        
#         insert_api_logging_query = '''
#         INSERT INTO "API_LOGGING_STATS" ("ENDPOINT", "METHOD", "STATUS_CODE", "CLIENT_IP", "USER_AGENT", "REQUEST_START")
#         VALUES (%s, %s, %s, %s, %s, %s) 
#         RETURNING "ID";
#         '''

#         cursor.execute(insert_api_logging_query, (request.path, request.method, 200, request.remote_addr, request.headers.get('User-Agent'), start_time))
        
#         log_id = cursor.fetchone()[0]
#         conn.commit()
        
#         cursor.close()
#         conn.close()
        
#         g.log_id = log_id
#         g.start_time = start_time

# @app.after_request
# def update_status_code(response):
#     if hasattr(g, 'log_id'):
#         end_time = datetime.utcnow()
#         duration = (end_time - g.start_time).total_seconds() * 1000

#         conn = get_db_connection()
#         cursor = conn.cursor()

#         query_params = json.dumps(request.args.to_dict())

#         update_api_logging_query = '''
#         UPDATE "API_LOGGING_STATS"
#         SET "QUERY_PARAMS" = %s, "STATUS_CODE" = %s, "REQUEST_END" = %s, "RESPONSE_TIME_MS" = %s
#         WHERE "ID" = %s;
#         '''

#         cursor.execute(update_api_logging_query, (query_params, response.status_code, end_time, duration, g.log_id))
        
#         conn.commit()
#         cursor.close()
#         conn.close()
#     return response

@app.route('/')
def home():   
    return redirect('https://cderickson.io/vintage-data/', code=301)

@app.route('/matches/', methods=['GET'], strict_slashes=False)
def get_matches():
    start = request.args.get('start', '2024-08-25')
    end = request.args.get('end', (datetime.today() + timedelta(days=1)).strftime('%Y-%m-%d'))
    page = request.args.get('page', 1)

    try:
        start = datetime.strptime(start, '%Y-%m-%d')
        end = datetime.strptime(end, '%Y-%m-%d')
        page = int(page)
    except ValueError:
        return jsonify({"error": "Invalid URL Parameter Format."}), 400
    
    if page < 1:
        return jsonify({"error": "Invalid URL Parameter Format."}), 400
    
    offset = (page - 1) * page_size

    query = '''
    SELECT a."MATCH_ID", a."P1", b."ARCHETYPE" AS "P1_ARCH", b."SUBARCHETYPE" AS "P1_SUBARCH", a."P1_WINS", 
    a."P2", c."ARCHETYPE" AS "P2_ARCH", c."SUBARCHETYPE" AS "P2_SUBARCH", a."P2_WINS", a."MATCH_WINNER", d."EVENT_DATE"
    FROM "MATCHES" a 
    JOIN "VALID_DECKS" b 
    ON a."P1_DECK_ID" = b."DECK_ID"
    JOIN "VALID_DECKS" c
    ON a."P2_DECK_ID" = c."DECK_ID"
    JOIN "EVENTS" d  
    ON a."EVENT_ID" = d."EVENT_ID"
    WHERE d."EVENT_DATE" >= %s AND d."EVENT_DATE" <= %s
    ORDER BY "MATCH_ID" DESC
    LIMIT %s OFFSET %s
    '''

    results = run_select_query(query, (start, end, page_size, offset))
    return jsonify(results)

@app.route('/matches/<int:match_id>/', methods=['GET'])
def get_match_id(match_id):
    query = '''
    SELECT a."MATCH_ID", a."P1", b."ARCHETYPE" AS "P1_ARCH", b."SUBARCHETYPE" AS "P1_SUBARCH", a."P1_WINS", 
    a."P2", c."ARCHETYPE" AS "P2_ARCH", c."SUBARCHETYPE" AS "P2_SUBARCH", a."P2_WINS", a."MATCH_WINNER", d."EVENT_DATE"
    FROM "MATCHES" a 
    JOIN "VALID_DECKS" b 
    ON a."P1_DECK_ID" = b."DECK_ID"
    JOIN "VALID_DECKS" c
    ON a."P2_DECK_ID" = c."DECK_ID"
    JOIN "EVENTS" d  
    ON a."EVENT_ID" = d."EVENT_ID"
    WHERE a."MATCH_ID" = %s
    ORDER BY "MATCH_ID" DESC
    '''

    results = run_select_query(query, (match_id,))
    return jsonify(results)

@app.route('/matches/player/<string:P1>/', methods=['GET'], strict_slashes=False)
def get_matches_by_pid(P1):
    start = request.args.get('start', '2024-08-25')
    end = request.args.get('end', (datetime.today() + timedelta(days=1)).strftime('%Y-%m-%d'))
    page = request.args.get('page', 1)

    try:
        start = datetime.strptime(start, '%Y-%m-%d')
        end = datetime.strptime(end, '%Y-%m-%d')
        page = int(page)
    except ValueError:
        return jsonify({"error": "Invalid URL Parameter Format."}), 400
    
    if page < 1:
        return jsonify({"error": "Invalid URL Parameter Format."}), 400
    
    offset = (page - 1) * page_size

    query = '''
    SELECT a."MATCH_ID", a."P1", b."ARCHETYPE" AS "P1_ARCH", b."SUBARCHETYPE" AS "P1_SUBARCH", a."P1_WINS", 
    a."P2", c."ARCHETYPE" AS "P2_ARCH", c."SUBARCHETYPE" AS "P2_SUBARCH", a."P2_WINS", a."MATCH_WINNER", d."EVENT_DATE"
    FROM "MATCHES" a 
    JOIN "VALID_DECKS" b 
    ON a."P1_DECK_ID" = b."DECK_ID"
    JOIN "VALID_DECKS" c
    ON a."P2_DECK_ID" = c."DECK_ID"
    JOIN "EVENTS" d  
    ON a."EVENT_ID" = d."EVENT_ID"
    WHERE a."P1" = %s AND d."EVENT_DATE" >= %s AND d."EVENT_DATE" <= %s
    ORDER BY "MATCH_ID" DESC
    LIMIT %s OFFSET %s
    '''

    results = run_select_query(query, (P1, start, end, page_size, offset))
    return jsonify(results)

@app.route('/matches/event/<int:event_id>/', methods=['GET'])
def get_matches_by_eid(event_id):
    query = '''
    SELECT a."MATCH_ID", a."P1", b."ARCHETYPE" AS "P1_ARCH", b."SUBARCHETYPE" AS "P1_SUBARCH", a."P1_WINS", 
    a."P2", c."ARCHETYPE" AS "P2_ARCH", c."SUBARCHETYPE" AS "P2_SUBARCH", a."P2_WINS", a."MATCH_WINNER", d."EVENT_DATE"
    FROM "MATCHES" a 
    JOIN "VALID_DECKS" b 
    ON a."P1_DECK_ID" = b."DECK_ID"
    JOIN "VALID_DECKS" c
    ON a."P2_DECK_ID" = c."DECK_ID"
    JOIN "EVENTS" d  
    ON a."EVENT_ID" = d."EVENT_ID"
    WHERE a."EVENT_ID" = %s
    '''

    results = run_select_query(query, (event_id,))
    return jsonify(results)

@app.route('/events/', methods=['GET'], strict_slashes=False)
def get_events():
    start = request.args.get('start', '2024-08-25')
    end = request.args.get('end', (datetime.today() + timedelta(days=1)).strftime('%Y-%m-%d'))
    page = request.args.get('page', 1)

    try:
        start = datetime.strptime(start, '%Y-%m-%d')
        end = datetime.strptime(end, '%Y-%m-%d')
        page = int(page)
    except ValueError:
        return jsonify({"error": "Invalid URL Parameter Format."}), 400
    
    if page < 1:
        return jsonify({"error": "Invalid URL Parameter Format."}), 400
    
    offset = (page - 1) * page_size

    query = '''
    SELECT x."EVENT_ID", x."EVENT_DATE", x."FORMAT", x."EVENT_TYPE", count(distinct("P1")) AS "TOTAL_PLAYERS"
    FROM (
        SELECT a."P1", b."EVENT_ID", b."EVENT_DATE", c."FORMAT", c."EVENT_TYPE"
            FROM "MATCHES" a 
            JOIN "EVENTS" b 
            ON a."EVENT_ID" = b."EVENT_ID"
            JOIN "VALID_EVENT_TYPES" c 
            ON b."EVENT_TYPE_ID" = c."EVENT_TYPE_ID"
    ) x 
    WHERE x."EVENT_DATE" >= %s AND x."EVENT_DATE" <= %s
    GROUP BY x."EVENT_ID", x."EVENT_DATE", x."FORMAT", x."EVENT_TYPE"
    ORDER BY x."EVENT_ID" DESC
    LIMIT %s OFFSET %s
    '''

    results = run_select_query(query, (start, end, page_size, offset))
    return jsonify(results)

@app.route('/events/<int:event_id>/', methods=['GET'])
def get_event_id(event_id):
    query = '''
    SELECT x."EVENT_ID", x."EVENT_DATE", x."FORMAT", x."EVENT_TYPE", count(distinct("P1")) AS "TOTAL_PLAYERS"
    FROM (
        SELECT a."P1", b."EVENT_ID", b."EVENT_DATE", c."FORMAT", c."EVENT_TYPE"
            FROM "MATCHES" a 
            JOIN "EVENTS" b 
            ON a."EVENT_ID" = b."EVENT_ID"
            JOIN "VALID_EVENT_TYPES" c 
            ON b."EVENT_TYPE_ID" = c."EVENT_TYPE_ID"
            WHERE b."EVENT_ID" = %s
    ) x 
    GROUP BY x."EVENT_ID", x."EVENT_DATE", x."FORMAT", x."EVENT_TYPE"
    '''

    results = run_select_query(query, (event_id,))
    return jsonify(results)

@app.route('/events/<int:event_id>/standings/', methods=['GET'], strict_slashes=False)
def get_event_ranks(event_id):
    rank = request.args.get('rank', 0)

    try:
        rank = int(rank)
    except ValueError:
        return jsonify({"error": "Invalid URL Parameter Format."}), 400
    
    if rank < 0:
        return jsonify({"error": "Invalid URL Parameter Format."}), 400
    
    query = '''
    SELECT e."EVENT_DATE", ves."EVENT_TYPE", es."EVENT_RANK", es."P1", a."WINS", a."LOSSES", es."BYES"
    FROM "EVENTS" e 
    JOIN "EVENT_STANDINGS" es 
    ON es."EVENT_ID" = e."EVENT_ID"
    JOIN "VALID_EVENT_TYPES" ves 
    ON ves."EVENT_TYPE_ID" = e."EVENT_TYPE_ID"
    JOIN 
    (
        SELECT "EVENT_ID", "P1", 
            SUM(CASE WHEN "MATCH_WINNER" = 'P1' THEN 1 ELSE 0 END) AS "WINS", 
            SUM(CASE WHEN "MATCH_WINNER" = 'P2' THEN 1 ELSE 0 END) AS "LOSSES"
        FROM "MATCHES"
        GROUP BY "EVENT_ID", "P1"
    ) a
    ON es."EVENT_ID" = a."EVENT_ID" AND es."P1" = a."P1"
    WHERE es."EVENT_ID" = %s
    '''

    if rank > 0:
        query += ' AND es."EVENT_RANK" = %s'
    query += ' ORDER BY es."EVENT_RANK" ASC'

    if rank > 0:
        results = run_select_query(query, (event_id, rank))
    else:
        results = run_select_query(query, (event_id,))
    return jsonify(results)

@app.route('/events/<int:event_id>/player/<string:P1>/', methods=['GET'], strict_slashes=False)
def get_event_ranks_pid(event_id, P1):
    query = '''
    SELECT e."EVENT_DATE", ves."EVENT_TYPE", es."EVENT_RANK", es."P1", a."WINS", a."LOSSES", es."BYES"
    FROM "EVENTS" e 
    JOIN "EVENT_STANDINGS" es 
    ON es."EVENT_ID" = e."EVENT_ID"
    JOIN "VALID_EVENT_TYPES" ves 
    ON ves."EVENT_TYPE_ID" = e."EVENT_TYPE_ID"
    JOIN 
    (
        SELECT "EVENT_ID", "P1", 
            SUM(CASE WHEN "MATCH_WINNER" = 'P1' THEN 1 ELSE 0 END) AS "WINS", 
            SUM(CASE WHEN "MATCH_WINNER" = 'P2' THEN 1 ELSE 0 END) AS "LOSSES"
        FROM "MATCHES"
        GROUP BY "EVENT_ID", "P1"
    ) a
    ON es."EVENT_ID" = a."EVENT_ID" AND es."P1" = a."P1"
    WHERE es."EVENT_ID" = %s AND es."P1" = %s
    ORDER BY es."EVENT_RANK" ASC
    '''

    results = run_select_query(query, (event_id, P1))
    return jsonify(results)

@app.route('/events/player/<string:P1>/', methods=['GET'], strict_slashes=False)
def get_events_by_pid(P1):
    start = request.args.get('start', '2024-08-25')
    end = request.args.get('end', (datetime.today() + timedelta(days=1)).strftime('%Y-%m-%d'))
    page = request.args.get('page', 1)

    try:
        start = datetime.strptime(start, '%Y-%m-%d')
        end = datetime.strptime(end, '%Y-%m-%d')
        page = int(page)
    except ValueError:
        return jsonify({"error": "Invalid URL Parameter Format."}), 400
    
    if page < 1:
        return jsonify({"error": "Invalid URL Parameter Format."}), 400
    
    offset = (page - 1) * page_size

    query = '''
    SELECT x."EVENT_ID", x."EVENT_DATE", x."FORMAT", x."EVENT_TYPE", MAX(x."ARCHETYPE") AS "ARCHETYPE", MAX(x."SUBARCHETYPE") AS "SUBARCHETYPE",
        COUNT(CASE WHEN x."MATCH_WINNER" = 'P1' THEN 1 END) AS "WINS",
        COUNT(CASE WHEN x."MATCH_WINNER" = 'P2' THEN 1 END) AS "LOSSES"
    FROM (
        SELECT a."P1", b."EVENT_ID", b."EVENT_DATE", c."FORMAT", c."EVENT_TYPE", a."MATCH_WINNER", d."ARCHETYPE", d."SUBARCHETYPE"
            FROM "MATCHES" a 
            JOIN "EVENTS" b 
            ON a."EVENT_ID" = b."EVENT_ID"
            JOIN "VALID_EVENT_TYPES" c 
            ON b."EVENT_TYPE_ID" = c."EVENT_TYPE_ID"
            JOIN "VALID_DECKS" d
            ON a."P1_DECK_ID" = d."DECK_ID"
            WHERE a."P1" = %s
    ) x 
    WHERE x."EVENT_DATE" >= %s AND x."EVENT_DATE" <= %s
    GROUP BY x."EVENT_ID", x."EVENT_DATE", x."FORMAT", x."EVENT_TYPE"
    ORDER BY x."EVENT_ID" DESC
    LIMIT %s OFFSET %s
    '''

    results = run_select_query(query, (P1, start, end, page_size, offset))
    return jsonify(results)

@app.route('/decks/', methods=['GET'])
def get_valid_decks():
    query = '''
    SELECT "FORMAT", "ARCHETYPE", "SUBARCHETYPE", "DECK_ID"
        FROM "VALID_DECKS"
    '''

    results = run_select_query(query)
    return jsonify(results)

@app.route('/decks/<int:deck_id>/', methods=['GET'])
def get_deck_id(deck_id):
    query = '''
    SELECT "FORMAT", "ARCHETYPE", "SUBARCHETYPE", "DECK_ID"
        FROM "VALID_DECKS"
        WHERE "DECK_ID" = %s
    '''

    results = run_select_query(query, (deck_id,))
    return jsonify(results)

@app.route('/event-types/', methods=['GET'])
def get_valid_event_types():
    query = '''
    SELECT "FORMAT", "EVENT_TYPE", "EVENT_TYPE_ID"
        FROM "VALID_EVENT_TYPES"
    '''

    results = run_select_query(query)
    return jsonify(results)

@app.route('/event-types/<int:event_type_id>/', methods=['GET'])
def get_event_type_id(event_type_id):
    query = '''
    SELECT "FORMAT", "EVENT_TYPE", "EVENT_TYPE_ID"
        FROM "VALID_EVENT_TYPES"
        WHERE "EVENT_TYPE_ID" = %s
    '''

    results = run_select_query(query, (event_type_id,))
    return jsonify(results)

@app.route('/load-reports/', methods=['GET'])
def get_load_reports():
    query = '''
    SELECT *
        FROM "LOAD_REPORTS"
        ORDER BY "LOAD_RPT_ID" DESC
    '''

    results = run_select_query(query)
    return jsonify(results)

@app.route('/load-reports/<int:load_rpt_id>/', methods=['GET'])
def get_load_reports_by_load_rpt_id(load_rpt_id):
    query = '''
    SELECT *
        FROM "LOAD_REPORTS"
        WHERE "LOAD_RPT_ID" = %s
        ORDER BY "LOAD_RPT_ID" DESC
    '''

    results = run_select_query(query, (load_rpt_id,))
    return jsonify(results)

@app.route('/event-rejections/', methods=['GET'])
def get_event_rejections():
    query = '''
    SELECT *
        FROM "EVENT_REJECTIONS"
        ORDER BY "LOAD_RPT_ID" DESC
    '''

    results = run_select_query(query)
    return jsonify(results)

@app.route('/event-rejections/<int:load_rpt_id>/', methods=['GET'])
def get_event_rejections_by_load_rpt_id(load_rpt_id):
    query = '''
    SELECT *
        FROM "EVENT_REJECTIONS"
        WHERE "LOAD_RPT_ID" = %s
        ORDER BY "EVENT_REJ_ID" ASC
    '''

    results = run_select_query(query, (load_rpt_id,))
    return jsonify(results)

@app.route('/match-rejections/', methods=['GET'])
def get_match_rejections():
    query = '''
    SELECT *
        FROM "MATCH_REJECTIONS"
        ORDER BY "LOAD_RPT_ID" DESC
    '''

    results = run_select_query(query)
    return jsonify(results)

@app.route('/match-rejections/<int:load_rpt_id>/', methods=['GET'])
def get_match_rejections_by_load_rpt_id(load_rpt_id):
    query = '''
    SELECT *
        FROM "MATCH_REJECTIONS"
        WHERE "LOAD_RPT_ID" = %s
        ORDER BY "MATCH_REJ_ID" ASC
    '''

    results = run_select_query(query, (load_rpt_id,))
    return jsonify(results)

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=80)