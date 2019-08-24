from flask import Flask, g, request, jsonify, abort, render_template, redirect, session, url_for
import MySQLdb.cursors
import hashlib
import html
import json
import math
import os
import operator as op
import pathlib
import random
import string
import urllib
import redis
import replacer

app = Flask(__name__, static_folder = None)

app.secret_key = 'tonymoris'

var = {}

_config = {
    'db_host':       os.environ.get('ISUDA_DB_HOST', 'localhost'),
    'db_port':       int(os.environ.get('ISUDA_DB_PORT', '3306')),
    'db_user':       os.environ.get('ISUDA_DB_USER', 'root'),
    'db_password':   os.environ.get('ISUDA_DB_PASSWORD', ''),
    'isupam_origin': os.environ.get('ISUPAM_ORIGIN', 'http://localhost:5050'),
    'redis_host':    os.environ.get('REDIS_HOST', 'localhost'),
    'redis_port':    int(os.environ.get('REDIS_PORT', '6379')),
}

def config(key):
    if key in _config:
        return _config[key]
    else:
        raise "config value of %s undefined" % key

def dbh():
    if not hasattr(g, 'db'):
        g.db = MySQLdb.connect(**{
            'host': config('db_host'),
            'port': config('db_port'),
            'user': config('db_user'),
            'passwd': config('db_password'),
            'db': 'isuda',
            'charset': 'utf8mb4',
            'cursorclass': MySQLdb.cursors.DictCursor,
            'autocommit': True,
            'sql_mode': 'TRADITIONAL,NO_AUTO_VALUE_ON_ZERO,ONLY_FULL_GROUP_BY',
        })
    return g.db

def rh():
    if not hasattr(g, 'redis'):
        g.redis = redis.StrictRedis(
            host = config('redis_host'),
            port = config('redis_port'),
            decode_responses = True,
        )
    return g.redis

@app.teardown_request
def close_db(exception=None):
    if hasattr(g, 'db'):
        g.db.close()
    if hasattr(g, 'redis'):
        g.redis.close()

@app.template_filter()
def ucfirst(str):
    return str[0].upper() + str[-len(str) + 1:]

def set_name(func):
    import functools
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if "user" in session:
            user = session['user']
            request.user_id = user['id']
            request.user_name = user['name']
        return func(*args, **kwargs)
    return wrapper

def authenticate(func):
    import functools
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if not hasattr(request, 'user_id'):
            abort(403)
        return func(*args, **kwargs)

    return wrapper

@app.route('/initialize')
def get_initialize():
    #return jsonify(result = 'ok')
    cur = dbh().cursor()
    cur.execute('DELETE FROM entry WHERE id > 7101')
    cur.execute('TRUNCATE star')
    # redis
    cur.execute('SELECT keyword FROM entry')
    r = rh()
    r.flushdb()
    r.incr('keywords_update')
    #r.hmset('rendered_keywords', {})
    r.hmset('keywords', {
        escaped_keyword: '<a href="%s">%s</a>' % (
            url_for('get_keyword', keyword = keyword),
            escaped_keyword,
        )
        for keyword, escaped_keyword in (
            (keyword, html.escape(keyword))
            for keyword in map(op.itemgetter('keyword'), cur.fetchall())
        )
    })
    return jsonify(result = 'ok')

@app.route('/')
@set_name
def get_index():
    PER_PAGE = 10
    page = int(request.args.get('page', '1'))

    con = dbh()
    con.autocommit = False
    cur = con.cursor()
    r = rh()
    try:
        cur.execute('SELECT keyword, description FROM entry ORDER BY updated_at DESC LIMIT %s OFFSET %s LOCK IN SHARE MODE',
                    (PER_PAGE, PER_PAGE * (page - 1),))
        entries = cur.fetchall()
        for entry in entries:
            keyword = entry['keyword']
            if r.hexists('rendered_keywords', keyword):
                entry['html'] = r.hget('rendered_keywords', keyword)
            else:
                entry['html'] = htmlify(entry['description'])
                r.hset('rendered_keywords', keyword, entry['html'])
            entry['stars'] = load_stars(entry['keyword'])

        cur.execute('SELECT CEIL(COUNT(*) / %s) AS last_page FROM entry', (PER_PAGE, ))
        row = cur.fetchone()
        last_page = row['last_page']
        con.commit()
    except:
        con.rollback()
        raise

    pages = range(max(1, page - 5), min(last_page, page+5) + 1)

    return render_template('index.html', entries = entries, page = page, last_page = last_page, pages = pages)

@app.route('/robots.txt')
def get_robot_txt():
    abort(404)

@app.route('/keyword', methods=['POST'])
@set_name
@authenticate
def create_keyword():
    keyword = request.form['keyword']
    if keyword == None or len(keyword) == 0:
        abort(400)

    user_id = request.user_id
    description = request.form['description']

    if is_spam_contents(description) or is_spam_contents(keyword):
        abort(400)

    con = dbh()
    con.autocommit = False
    cur = con.cursor()
    r = rh()
    try:
        escaped_keyword = html.escape(keyword)
        cur.execute('SELECT id FROM entry WHERE keyword = %s FOR UPDATE', (keyword, ))
        entry = cur.fetchone()
        if entry is None:
            # create
            sql = 'INSERT INTO entry (author_id, keyword, description, updated_at) VALUES (%s, %s, %s, NOW())'
            cur.execute(sql, (user_id, keyword, description))
            url = url_for('get_keyword', keyword = keyword)
            r.incr('keywords_update')
            r.hset('keywords', escaped_keyword, '<a href="%s">%s</a>' % (url, escaped_keyword))
            r.delete('rendered_keywords')
        else:
            # upadte
            sql = 'UPDATE entry SET author_id = %s, description = %s, updated_at = NOW() WHERE id = %s'
            cur.execute(sql, (user_id, description, entry['id']))
            r.hdel('rendered_keywords', keyword)
        con.commit()
    except:
        con.rollback()
        raise

    return redirect('/')

@app.route('/register')
@set_name
def get_register():
    return render_template('authenticate.html', action = 'register')

@app.route('/register', methods=['POST'])
def post_register():
    name = request.form['name']
    pw   = request.form['password']
    if name == None or name == '' or pw == None or pw == '':
        abort(400)

    user = register(dbh().cursor(), name, pw)
    session['user'] = user
    return redirect('/')

def register(cur, user, password):
    salt = random_string(20)
    cur.execute("INSERT INTO user (name, salt, password) VALUES (%s, %s, %s)",
                (user, salt, hashlib.sha1((salt + password).encode('utf-8')).hexdigest(),))
    user_id = cur.lastrowid
    return {'id': user_id, 'name': user}

def random_string(n):
    return ''.join([random.choice(string.ascii_letters + string.digits) for i in range(n)])

@app.route('/login')
@set_name
def get_login():
    return render_template('authenticate.html', action = 'login')

@app.route('/login', methods=['POST'])
def post_login():
    name = request.form['name']
    cur = dbh().cursor()
    cur.execute("SELECT id, name, salt, password FROM user WHERE name = %s", (name, ))
    row = cur.fetchone()
    if row == None or row['password'] != hashlib.sha1((row['salt'] + request.form['password']).encode('utf-8')).hexdigest():
        abort(403)

    session['user'] = {'id': row['id'], 'name': row['name']}
    return redirect('/')

@app.route('/logout')
def get_logout():
    session.pop('user', None)
    return redirect('/')

@app.route('/keyword/<keyword>')
@set_name
def get_keyword(keyword):
    if keyword == '':
        abort(400)

    con = dbh()
    con.autocommit = False
    cur = con.cursor()
    r = rh()
    try:
        cur.execute('SELECT keyword, description FROM entry WHERE keyword = %s LOCK IN SHARE MODE', (keyword,))
        entry = cur.fetchone()
        if entry == None:
            abort(404)

        if r.hexists('rendered_keywords', keyword):
            entry['html'] = r.hget('rendered_keywords', keyword)
        else:
            entry['html'] = htmlify(entry['description'])
            r.hset('rendered_keywords', keyword, entry['html'])
        con.commit()
    except:
        con.rollback()
        raise

    entry['stars'] = load_stars(entry['keyword'])
    return render_template('keyword.html', entry = entry)

@app.route('/keyword/<keyword>', methods=['POST'])
@set_name
@authenticate
def delete_keyword(keyword):
    if keyword == '':
        abort(400)

    con = dbh()
    con.autocommit = False
    cur = con.cursor()
    try:
        cur.execute('SELECT * FROM entry WHERE keyword = %s FOR UPDATE', (keyword, ))
        row = cur.fetchone()
        if row == None:
            abort(404)

        cur.execute('DELETE FROM entry WHERE keyword = %s', (keyword,))
        escaped_keyword = html.escape(keyword)
        r = rh()
        r.incr('keywords_update')
        r.hdel('keywords', escaped_keyword)
        r.delete('rendered_keywords')
        con.commit()
    except:
        con.rollback()
        raise

    return redirect('/')

@app.route("/stars")
def get_stars():
    cur = dbh().cursor()
    cur.execute('SELECT * FROM star WHERE keyword = %s', (request.args['keyword'], ))
    return jsonify(stars = cur.fetchall())

@app.route("/stars", methods=['POST'])
def post_stars():
    keyword = request.args.get('keyword', "")
    if keyword == None or keyword == "":
        keyword = request.form['keyword']

    cur = dbh().cursor()
    cur.execute('SELECT id FROM entry WHERE keyword = %s', (keyword,))
    entry = cur.fetchone()

    if entry is None:
        abort(404)

    cur = dbh().cursor()
    user = request.args.get('user', "")
    if user == None or user == "":
        user = request.form['user']

    cur.execute("""
        INSERT INTO star (entry_id, user_id)
        SELECT entry.id, user.id
        FROM entry JOIN user
        WHERE entry.keyword = %s
          AND user.name = %s
"""
    , (keyword, user))

    return jsonify(result = 'ok')

_keywords_update = 0
_keyword_replacer = None
def get_keyword_replacer():
    global _keywords_update, _keyword_replacer
    # check onece on every request
    if not hasattr(g, 'keywords_update'):
        r = rh()
        g.keywords_update = int(r.get('keywords_update'))
        if _keywords_update < g.keywords_update:
            # update cache
            _keywords_update = g.keywords_update
            _keyword_replacer = replacer.Replacer(r.hgetall('keywords'))
    return _keyword_replacer

def htmlify(content):
    if content is None or content == '':
        return ''

    result = html.escape(content)

    keyword_replacer = get_keyword_replacer()
    result = keyword_replacer.replace(result)

    return result.replace("\n", '<br />')

def load_stars(keyword):
    cur = dbh().cursor()
    # Used user_name only
    cur.execute("""
        SELECT user.name as user_name
        FROM star
          JOIN entry
            ON star.entry_id = entry.id
          JOIN user
            ON star.user_id = user.id
        WHERE keyword = %s
"""
    , (keyword, ))
    stars = cur.fetchall()
    return stars

def is_spam_contents(content):
    with urllib.request.urlopen(config('isupam_origin'), urllib.parse.urlencode({ "content": content }).encode('utf-8')) as res:
        data = json.loads(res.read().decode('utf-8'))
        return not data['valid']

    return False

if __name__ == "__main__":
    from wsgi_lineprof.middleware import LineProfilerMiddleware
    from wsgi_lineprof.filters import FilenameFilter, TotalTimeSorter
    filters = [
        FilenameFilter(__file__),
        TotalTimeSorter(),
    ]
    with open('lineprof.log', 'w') as f:
        app.wsgi_app = LineProfilerMiddleware(app.wsgi_app, async_stream=True, stream=f, filters=filters)
        app.run()
