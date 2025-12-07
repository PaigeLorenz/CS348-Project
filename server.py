import sqlite3
import threading
from flask import Flask, jsonify, request
from datetime import datetime

DB_FILE = 'records.db'

SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS Artists (
        artist_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        country TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_artists_name ON Artists(name);
    """,
    """
    CREATE TABLE IF NOT EXISTS Genres (
        genre_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE
    );

    CREATE TABLE IF NOT EXISTS Records (
        record_id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        artist_id INTEGER,
        genre_id INTEGER,
        year INTEGER,
        condition TEXT,
        price REAL,
        purchase_date TEXT,
        FOREIGN KEY (artist_id) REFERENCES Artists(artist_id),
        FOREIGN KEY (genre_id) REFERENCES Genres(genre_id)
    );
    CREATE INDEX IF NOT EXISTS idx_records_artist ON Records(artist_id);
    CREATE INDEX IF NOT EXISTS idx_records_genre ON Records(genre_id);
    CREATE INDEX IF NOT EXISTS idx_records_purchase_date ON Records(purchase_date);
    CREATE INDEX IF NOT EXISTS idx_records_year ON Records(year);
    
    CREATE TABLE IF NOT EXISTS Stores (
        store_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        state TEXT,
        address TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_stores_name ON Stores(name);
    """
]


def get_conn():
    return sqlite3.connect(DB_FILE)


def init_db():
    conn = get_conn()
    cur = conn.cursor()
    for s in SCHEMA:
        cur.executescript(s)
    conn.commit()
    conn.close()

'''POPULATE ARTIST DROPDOWN'''
def find_or_create_artist(name: str, country: str = None) -> int:
    name = (name or '').strip()
    if not name:
        return None
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('SELECT artist_id FROM Artists WHERE name = ?;', (name,))
    r = cur.fetchone()
    if r:
        artist_id = r[0]
    else:
        cur.execute('INSERT INTO Artists (name, country) VALUES (?, ?);', (name, country))
        artist_id = cur.lastrowid
        conn.commit()
    conn.close()
    return artist_id


def get_artists_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('SELECT artist_id, name, country FROM Artists ORDER BY name;')
    rows = cur.fetchall()
    conn.close()
    return rows

'''POPULATE GENRE DROPDOWN'''
def find_or_create_genre(name: str) -> int:
    name = name.strip()
    if not name:
        return None
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('SELECT genre_id FROM Genres WHERE name = ?;', (name,))
    r = cur.fetchone()
    if r:
        genre_id = r[0]
    else:
        cur.execute('INSERT INTO Genres (name) VALUES (?);', (name,))
        genre_id = cur.lastrowid
        conn.commit()
    conn.close()
    return genre_id


def get_genres_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('SELECT genre_id, name FROM Genres ORDER BY name;')
    rows = cur.fetchall()
    conn.close()
    return rows


def get_stores_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('SELECT store_id, name, state, address FROM Stores ORDER BY name;')
    rows = cur.fetchall()
    conn.close()
    return rows

'''POPULATE STORE DROPDOWN'''
def find_or_create_store(name: str, state: str = None, address: str = None) -> int:
    name = (name or '').strip()
    if not name:
        return None
    conn = get_conn()
    cur = conn.cursor()
    # try to match by name and address if provided, otherwise match by name
    if address:
        cur.execute('SELECT store_id FROM Stores WHERE name = ? AND address = ?;', (name, address))
    else:
        cur.execute('SELECT store_id FROM Stores WHERE name = ?;', (name,))
    r = cur.fetchone()
    if r:
        store_id = r[0]
    else:
        cur.execute('INSERT INTO Stores (name, state, address) VALUES (?, ?, ?);', (name, state, address))
        store_id = cur.lastrowid
        conn.commit()
    conn.close()
    return store_id

'''ADD RECORD TO DB'''
def add_record_db(data: dict) -> int:
    # defensive validation at DB layer
    if data.get('title') is None:
        raise ValueError('title required')
    y = data.get('year')
    p = data.get('price')
    if y is None or not isinstance(y, int) or y < 1800 or y > 2100:
        raise ValueError('year must be a valid number between 1800 and 2100')
    if p is None or not isinstance(p, (int, float)) or float(p) < 0:
        raise ValueError('price must be a valid non-negative number')
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        'INSERT INTO Records (title, artist_id, genre_id, year, condition, price, purchase_date) VALUES (?, ?, ?, ?, ?, ?, ?);',
        (data['title'], data.get('artist_id'), data.get('genre_id'), data.get('year'), data.get('condition'), data.get('price'), data.get('purchase_date'))
    )
    record_id = cur.lastrowid
    # link to store if provided
    store_id = data.get('store_id')
    if store_id:
        cur.execute('INSERT OR IGNORE INTO recordStores (record_id, store_id) VALUES (?, ?);', (record_id, store_id))
    
    conn.commit()
    conn.close()
    return record_id


def update_record_db(record_id: int, data: dict) -> None:
    # defensive validation at DB layer
    if data.get('title') is None:
        raise ValueError('title required')
    y = data.get('year')
    p = data.get('price')
    if y is None or not isinstance(y, int) or y < 1800 or y > 2100:
        raise ValueError('year must be a valid number between 1800 and 2100')
    if p is None or not isinstance(p, (int, float)) or float(p) < 0:
        raise ValueError('price must be a valid non-negative number')
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        'UPDATE Records SET title = ?, artist_id = ?, genre_id = ?, year = ?, condition = ?, price = ?, purchase_date = ? WHERE record_id = ?;', 
        (data['title'], data.get('artist_id'), data.get('genre_id'), data.get('year'), data.get('condition'), data.get('price'), data.get('purchase_date'), record_id)
    )
    # update store link: remove existing and add provided
    if 'store_id' in data:
        cur.execute('DELETE FROM recordStores WHERE record_id = ?;', (record_id,))
        store_id = data.get('store_id')
        if store_id:
            cur.execute('INSERT OR IGNORE INTO recordStores (record_id, store_id) VALUES (?, ?);', (record_id, store_id))
    conn.commit()
    conn.close()


def delete_record_db(record_id: int) -> None:
    conn = get_conn()
    cur = conn.cursor()
    # fetch artist id before deletion
    cur.execute('SELECT artist_id FROM Records WHERE record_id = ?;', (record_id,))
    row = cur.fetchone()
    artist_id = row[0] if row else None
    # remove recordStores links
    cur.execute('DELETE FROM recordStores WHERE record_id = ?;', (record_id,))
    cur.execute('DELETE FROM Records WHERE record_id = ?;', (record_id,))
    conn.commit()
    # optional: remove artist if no more records
    if artist_id:
        cur.execute('SELECT COUNT(*) FROM Records WHERE artist_id = ?;', (artist_id,))
        cnt = cur.fetchone()[0]
        if cnt == 0:
            cur.execute('DELETE FROM Artists WHERE artist_id = ?;', (artist_id,))
            conn.commit()
    conn.close()


def fetch_all_records_db():
    conn = get_conn()
    cur = conn.cursor()
    # Detect schema variations (old vs new column names) and build SQL accordingly
    cur.execute("PRAGMA table_info('Records');")
    rec_cols = [r[1] for r in cur.fetchall()]

    # pick primary id column
    if 'record_id' in rec_cols:
        pk = 'record_id'
    elif 'id' in rec_cols:
        pk = 'id'
    else:
        # fallback to first column
        pk = rec_cols[0] if rec_cols else 'rowid'

    # check if artist_id column exists on Records and Artists PK name
    has_artist_id = 'artist_id' in rec_cols
    cur.execute("PRAGMA table_info('Artists');")
    artist_cols = [r[1] for r in cur.fetchall()]
    artist_pk = 'artist_id' if 'artist_id' in artist_cols else ('id' if 'id' in artist_cols else None)

    # check genre
    has_genre_id = 'genre_id' in rec_cols
    cur.execute("PRAGMA table_info('Genres');")
    genre_cols = [r[1] for r in cur.fetchall()]
    genre_pk = 'genre_id' if 'genre_id' in genre_cols else ('id' if 'id' in genre_cols else None)

    # check stores & recordStores
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='recordStores';")
    has_recordstores = cur.fetchone() is not None
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Stores';")
    has_stores = cur.fetchone() is not None

    select_parts = [f"R.{pk} as record_id", "R.title"]
    # artist: either join Artists or show text column
    if has_artist_id and artist_pk:
        select_parts.append("A.name as artist")
    else:
        # maybe there's an artist text column on Records
        if 'artist' in rec_cols:
            select_parts.append("R.artist as artist")
        else:
            select_parts.append("NULL as artist")

    # genre: either join Genres or show text column
    if has_genre_id and genre_pk:
        select_parts.append("G.name as genre")
    else:
        if 'genre' in rec_cols:
            select_parts.append("R.genre as genre")
        else:
            select_parts.append("NULL as genre")

    # store: try join via recordStores -> Stores and aggregate names if multiple
    if has_recordstores and has_stores:
        select_parts.append("GROUP_CONCAT(S.name, ', ') as store")
    else:
        if 'store' in rec_cols:
            select_parts.append("R.store as store")
        else:
            select_parts.append("NULL as store")

    select_parts.extend(["R.year", "R.condition", "R.price", "R.purchase_date"])

    sql = f"SELECT {', '.join(select_parts)} FROM Records R"
    if has_artist_id and artist_pk:
        sql += f" LEFT JOIN Artists A ON R.artist_id = A.{artist_pk}"
    if has_genre_id and genre_pk:
        sql += f" LEFT JOIN Genres G ON R.genre_id = G.{genre_pk}"
    if has_recordstores and has_stores:
        sql += " LEFT JOIN recordStores RS ON R." + pk + " = RS.record_id LEFT JOIN Stores S ON RS.store_id = S.store_id"
        # when aggregating store names, group by record pk
        sql += f" GROUP BY R.{pk}"
    sql += f" ORDER BY R.{pk} DESC;"

    cur.execute(sql)
    rows = cur.fetchall()
    conn.close()
    return rows


# --- Flask API ---
app = Flask(__name__)


def parse_int(val, min_val=None, max_val=None):
    if val is None:
        return None
    try:
        i = int(val)
        if min_val is not None and i < min_val:
            return None
        if max_val is not None and i > max_val:
            return None
        return i
    except (TypeError, ValueError):
        return None


def parse_float(val, min_val=None, max_val=None):
    if val is None:
        return None
    try:
        f = float(val)
        if min_val is not None and f < min_val:
            return None
        if max_val is not None and f > max_val:
            return None
        return f
    except (TypeError, ValueError):
        return None


def parse_date_yyyy_mm_dd(val):
    if not val:
        return None
    try:
        datetime.strptime(val, "%Y-%m-%d")
        return val
    except ValueError:
        return None


def sanitize_text(val, max_len=255):
    s = (val or "").strip()
    if not s:
        return None
    return s[:max_len]


@app.route('/api/records', methods=['GET'])
def api_get_records():
    rows = fetch_all_records_db()
    cols = ['record_id', 'title', 'artist', 'genre', 'store', 'year', 'condition', 'price', 'purchase_date']
    data = [dict(zip(cols, row)) for row in rows]
    return jsonify(data)


@app.route('/api/records', methods=['POST'])
def api_add_record():
    payload = request.get_json(silent=True) or {}
    title = sanitize_text(payload.get('title'))
    if not title:
        return jsonify({'error': 'title required'}), 400
    artist_name = sanitize_text(payload.get('artist_name'))
    genre_name = sanitize_text(payload.get('genre'))
    artist_id = find_or_create_artist(artist_name) if artist_name else None
    genre_id = find_or_create_genre(genre_name) if genre_name else None
    year = parse_int(payload.get('year'), min_val=1800, max_val=2100)
    if year is None:
        return jsonify({'error': 'year must be a valid number between 1800 and 2100'}), 400
    condition = sanitize_text(payload.get('condition'))
    price = parse_float(payload.get('price'), min_val=0)
    if price is None:
        return jsonify({'error': 'price must be a valid non-negative number'}), 400
    purchase_date = parse_date_yyyy_mm_dd(payload.get('purchase_date'))
    store_id = parse_int(payload.get('store_id'), min_val=1)
    data = {
        'title': title,
        'artist_id': artist_id,
        'genre_id': genre_id,
        'year': year,
        'condition': condition,
        'price': price,
        'purchase_date': purchase_date,
        'store_id': store_id
    }
    rid = add_record_db(data)
    return jsonify({'record_id': rid}), 201


@app.route('/api/records/<int:rid>', methods=['PUT'])
def api_update_record(rid):
    payload = request.get_json(silent=True) or {}
    title = sanitize_text(payload.get('title'))
    if not title:
        return jsonify({'error': 'title required'}), 400
    artist_name = sanitize_text(payload.get('artist_name'))
    genre_name = sanitize_text(payload.get('genre'))
    artist_id = find_or_create_artist(artist_name) if artist_name else None
    genre_id = find_or_create_genre(genre_name) if genre_name else None
    year = parse_int(payload.get('year'), min_val=1800, max_val=2100)
    if year is None:
        return jsonify({'error': 'year must be a valid number between 1800 and 2100'}), 400
    condition = sanitize_text(payload.get('condition'))
    price = parse_float(payload.get('price'), min_val=0)
    if price is None:
        return jsonify({'error': 'price must be a valid non-negative number'}), 400
    purchase_date = parse_date_yyyy_mm_dd(payload.get('purchase_date'))
    store_id = parse_int(payload.get('store_id'), min_val=1)
    data = {
        'title': title,
        'artist_id': artist_id,
        'genre_id': genre_id,
        'year': year,
        'condition': condition,
        'price': price,
        'purchase_date': purchase_date,
        'store_id': store_id
    }
    update_record_db(rid, data)
    return ('', 204)


@app.route('/api/records/<int:rid>', methods=['DELETE'])
def api_delete_record(rid):
    delete_record_db(rid)
    return ('', 204)


@app.route('/api/artists', methods=['GET'])
def api_get_artists():
    rows = get_artists_db()
    # rows now include (artist_id, name, country)
    data = [{'artist_id': r[0], 'name': r[1], 'country': r[2]} for r in rows]
    return jsonify(data)


@app.route('/api/artists', methods=['POST'])
def api_create_artist():
    payload = request.get_json(silent=True) or {}
    name = sanitize_text(payload.get('name'))
    country = sanitize_text(payload.get('country'))
    if not name:
        return jsonify({'error': 'name required'}), 400
    aid = find_or_create_artist(name, country=country)
    return jsonify({'artist_id': aid}), 201


@app.route('/api/artists/<int:aid>', methods=['PUT'])
def api_update_artist(aid):
    payload = request.get_json(silent=True) or {}
    name = sanitize_text(payload.get('name'))
    country = sanitize_text(payload.get('country'))
    if not name:
        return jsonify({'error': 'name required'}), 400
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('UPDATE Artists SET name = ?, country = ? WHERE artist_id = ?;', (name, country, aid))
    conn.commit()
    conn.close()
    return ('', 204)


@app.route('/api/artists/<int:aid>', methods=['DELETE'])
def api_delete_artist(aid):
    conn = get_conn()
    cur = conn.cursor()
    # refuse deletion if artist still has records
    cur.execute('SELECT COUNT(*) FROM Records WHERE artist_id = ?;', (aid,))
    cnt = cur.fetchone()[0]
    if cnt > 0:
        conn.close()
        return jsonify({'error': 'artist has records, cannot delete'}), 400
    cur.execute('DELETE FROM Artists WHERE artist_id = ?;', (aid,))
    conn.commit()
    conn.close()
    return ('', 204)


@app.route('/api/genres', methods=['GET'])
def api_get_genres():
    rows = get_genres_db()
    data = [{'genre_id': r[0], 'name': r[1]} for r in rows]
    return jsonify(data)


@app.route('/api/genres', methods=['POST'])
def api_create_genre():
    payload = request.get_json(silent=True) or {}
    name = sanitize_text(payload.get('name'))
    if not name:
        return jsonify({'error': 'name required'}), 400
    gid = find_or_create_genre(name)
    return jsonify({'genre_id': gid}), 201


@app.route('/api/genres/<int:gid>', methods=['PUT'])
def api_update_genre(gid):
    payload = request.get_json(silent=True) or {}
    name = sanitize_text(payload.get('name'))
    if not name:
        return jsonify({'error': 'name required'}), 400
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('UPDATE Genres SET name = ? WHERE genre_id = ?;', (name, gid))
    conn.commit()
    conn.close()
    return ('', 204)


@app.route('/api/genres/<int:gid>', methods=['DELETE'])
def api_delete_genre(gid):
    conn = get_conn()
    cur = conn.cursor()
    # refuse deletion if genre is used by records
    cur.execute('SELECT COUNT(*) FROM Records WHERE genre_id = ?;', (gid,))
    cnt = cur.fetchone()[0]
    if cnt > 0:
        conn.close()
        return jsonify({'error': 'genre in use by records, cannot delete'}), 400
    cur.execute('DELETE FROM Genres WHERE genre_id = ?;', (gid,))
    conn.commit()
    conn.close()
    return ('', 204)


@app.route('/api/stores', methods=['GET'])
def api_get_stores():
    rows = get_stores_db()
    data = [{'store_id': r[0], 'name': r[1], 'state': r[2], 'address': r[3]} for r in rows]
    return jsonify(data)


@app.route('/api/stores', methods=['POST'])
def api_create_store():
    payload = request.get_json(silent=True) or {}
    name = sanitize_text(payload.get('name'))
    if not name:
        return jsonify({'error': 'name required'}), 400
    state = sanitize_text(payload.get('state'))
    address = sanitize_text(payload.get('address'))
    sid = find_or_create_store(name, state=state, address=address)
    return jsonify({'store_id': sid}), 201


@app.route('/api/stores/<int:sid>', methods=['PUT'])
def api_update_store(sid):
    payload = request.get_json(silent=True) or {}
    name = sanitize_text(payload.get('name'))
    state = sanitize_text(payload.get('state'))
    address = sanitize_text(payload.get('address'))
    if not name:
        return jsonify({'error': 'name required'}), 400
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('UPDATE Stores SET name = ?, state = ?, address = ? WHERE store_id = ?;', (name, state, address, sid))
    conn.commit()
    conn.close()
    return ('', 204)


@app.route('/api/stores/<int:sid>', methods=['DELETE'])
def api_delete_store(sid):
    conn = get_conn()
    cur = conn.cursor()
    # remove any links in recordStores, then delete store
    cur.execute('DELETE FROM recordStores WHERE store_id = ?;', (sid,))
    cur.execute('DELETE FROM Stores WHERE store_id = ?;', (sid,))
    conn.commit()
    conn.close()
    return ('', 204)


@app.route('/api/reports/records', methods=['POST'])
def api_report_records():
    """Return matching records and simple statistics given filters.
    JSON body may contain: start_date, end_date (YYYY-MM-DD), artist_id, store_id, genre_id
    """
    payload = request.get_json(silent=True) or {}
    start_date = parse_date_yyyy_mm_dd(payload.get('start_date'))
    end_date = parse_date_yyyy_mm_dd(payload.get('end_date'))
    artist_id = parse_int(payload.get('artist_id'), min_val=1)
    store_id = parse_int(payload.get('store_id'), min_val=1)
    genre_id = parse_int(payload.get('genre_id'), min_val=1)
    # Build WHERE clauses (only date, artist, genre supported)
    where = []
    params = []
    if start_date:
        where.append('R.purchase_date >= ?')
        params.append(start_date)
    if end_date:
        where.append('R.purchase_date <= ?')
        params.append(end_date)
    if artist_id:
        where.append('R.artist_id = ?')
        params.append(artist_id)
    if genre_id:
        where.append('R.genre_id = ?')
        params.append(genre_id)

    join_store = False
    if store_id:
        join_store = True
        where.append('RS.store_id = ?')
        params.append(store_id)

    where_sql = ('WHERE ' + ' AND '.join(where)) if where else ''
    # detect whether recordStores/Stores tables exist (for store info)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='recordStores';")
    has_recordstores = cur.fetchone() is not None
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Stores';")
    has_stores = cur.fetchone() is not None

    if has_recordstores and has_stores:
        sql = (
            "SELECT R.record_id, R.title, A.name as artist, G.name as genre, S.name as store, "
            "R.year, R.condition, R.price, R.purchase_date "
            "FROM Records R LEFT JOIN Artists A ON R.artist_id = A.artist_id "
            "LEFT JOIN Genres G ON R.genre_id = G.genre_id "
            "LEFT JOIN recordStores RS ON R.record_id = RS.record_id "
            "LEFT JOIN Stores S ON RS.store_id = S.store_id"
        )
        if where_sql:
            sql += ' ' + where_sql
        sql += ' ORDER BY R.record_id DESC;'
    else:
        sql = (
            "SELECT R.record_id, R.title, A.name as artist, G.name as genre, NULL as store, "
            "R.year, R.condition, R.price, R.purchase_date "
            "FROM Records R LEFT JOIN Artists A ON R.artist_id = A.artist_id "
            "LEFT JOIN Genres G ON R.genre_id = G.genre_id"
        )
        if where_sql:
            sql += ' ' + where_sql
        sql += ' ORDER BY R.record_id DESC;'

    cur.execute(sql, tuple(params))
    rows = cur.fetchall()

    # aggregate stats
    stats_sql = f"SELECT COUNT(*), AVG(R.price), MIN(R.price), MAX(R.price), AVG(R.year) FROM Records R"
    if join_store:
        stats_sql += ' LEFT JOIN recordStores RS ON R.record_id = RS.record_id'
    if where_sql:
        stats_sql += ' ' + where_sql
    cur.execute(stats_sql, tuple(params))
    stats_row = cur.fetchone()
    stats = {
        'count': stats_row[0] or 0,
        'avg_price': float(stats_row[1]) if stats_row[1] is not None else None,
        'min_price': float(stats_row[2]) if stats_row[2] is not None else None,
        'max_price': float(stats_row[3]) if stats_row[3] is not None else None,
        'avg_year': float(stats_row[4]) if stats_row[4] is not None else None,
    }

    # breakdown by artist
    by_artist = []
    if stats['count'] > 0:
        group_sql = (
            "SELECT A.artist_id, A.name, COUNT(*), AVG(R.price) "
            "FROM Records R LEFT JOIN Artists A ON R.artist_id = A.artist_id"
        )
        if join_store:
            group_sql += ' LEFT JOIN recordStores RS ON R.record_id = RS.record_id'
        if where_sql:
            group_sql += ' ' + where_sql
        group_sql += ' GROUP BY A.artist_id, A.name ORDER BY COUNT(*) DESC;'
        cur.execute(group_sql, tuple(params))
        for r in cur.fetchall():
            by_artist.append({'artist_id': r[0], 'name': r[1], 'count': r[2], 'avg_price': float(r[3]) if r[3] is not None else None})

    conn.close()

    data = [dict(zip(['record_id', 'title', 'artist', 'genre', 'store', 'year', 'condition', 'price', 'purchase_date'], r)) for r in rows]
    return jsonify({'rows': data, 'stats': stats, 'by_artist': by_artist})


def start_api_in_thread(host: str = '127.0.0.1', port: int = 5000):
    t = threading.Thread(target=lambda: app.run(host=host, port=port, threaded=True, use_reloader=False), daemon=True)
    t.start()


if __name__ == '__main__':
    # allow running server standalone for development
    init_db()
    app.run(host='127.0.0.1', port=5000, threaded=True)
