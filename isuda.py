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
import regex as re
import string
import urllib
import urllib.parse
import redis
import pickle
import replacer
import datetime

app = Flask(__name__, static_folder = None)

app.secret_key = 'tonymoris'

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

def _dbh():
    return MySQLdb.connect(**{
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

def _rh():
    return  redis.StrictRedis(
        host = config('redis_host'),
        port = config('redis_port'),
        decode_responses = True,
    )

def dbh():
    if not hasattr(g, 'db'):
        g.db = _dbh()
    return g.db

def rh():
    if not hasattr(g, 'redis'):
        g.redis = _rh()
    return g.redis

@app.teardown_request
def close_db(exception=None):
    if hasattr(g, 'db'):
        g.db.close()
    if hasattr(g, 'redis'):
        g.redis.close()

@app.template_filter()
def ucfirst(str):
    return str[0].upper() + str[1:]

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
    cur = dbh().cursor()
    cur.execute('DELETE FROM entry WHERE id > 7101')
    cur.execute('TRUNCATE star')
    # redis
    r = rh()
    initialize_redis(cur, r)
    return jsonify(result = 'ok')

@app.route('/')
@set_name
def get_index():
    PER_PAGE = 10
    page = int(request.args.get('page', '1'))

    r = rh()
    count = r.zcard('z:keywords')
    # Get keywords in page
    keywords = r.zrevrange('z:keywords', PER_PAGE * (page - 1), PER_PAGE * page)
    # Get entries by keywords
    with r.pipeline() as p:
        for keyword in keywords:
            p.hgetall('hm:keywords:%s' % keyword)
        entries = p.execute()
    # Render html
    for entry in entries:
        entry['html'] = r.hget('hm:html', entry['keyword'])
        if entry['html'] is None:
            entry['html'] = htmlify(entry['description'])
            r.hset('hm:html', entry['keyword'], entry['html'])
        stars = r.lrange('list:stars:%s' % entry['keyword'], 0, -1)
        entry['stars'] = [{'user_name': star} for star in stars]

    last_page = math.ceil(count / PER_PAGE)
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

    r = rh()
    if r.zscore('z:keywords', keyword) is None:
        cur = dbh().cursor()
        cur.execute('INSERT INTO entry (author_id, keyword, description, updated_at) VALUES (%s, %s, %s, NOW())', (user_id, keyword, description))
        cur.execute('SELECT UNIX_TIMESTAMP(updated_at) as updated_at FROM entry WHERE keyword = %s', (keyword,))
        entry = cur.fetchone()
        # Create new keyword
        with r.pipeline() as p:
            p.zadd('z:keywords', {keyword: entry['updated_at']}, nx = True)
            p.hmset('hm:keywords:%s' % keyword, {
                'keyword': keyword,
                'description': description,
            })
            escaped_keyword = html.escape(keyword)
            url = url_for('get_keyword', keyword = keyword)
            link = '<a href="%s">%s</a>' % (url, escaped_keyword)
            p.delete('hm:html')
            p.hset('hm:replacements', escaped_keyword, link)
            p.incr('keyword_modified')
            p.execute()
    else:
        cur = dbh().cursor()
        cur.execute('UPDATE entry SET author_id = %s, description = %s, updated_at = NOW() WHERE keyword = %s', (user_id, description, keyword))
        cur.execute('SELECT UNIX_TIMESTAMP(updated_at) as updated_at FROM entry WHERE keyword = %s', (keyword,))
        entry = cur.fetchone()
        # Update already existing keyword
        with r.pipeline() as p:
            p.zadd('z:keywords', {keyword: entry['updated_at']}, xx = True)
            p.hmset('hm:keywords:%s' % keyword, {
                'description': description,
            })
            p.hdel('hm:html', keyword)
            p.execute()

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

    r = rh()
    # Get entry by keyword
    entry = r.hgetall('hm:keywords:%s' % keyword)
    # Render html
    entry['html'] = r.hget('hm:html', entry['keyword'])
    if entry['html'] is None:
        entry['html'] = htmlify(entry['description'])
        r.hset('hm:html', entry['keyword'], entry['html'])
    stars = r.lrange('list:stars:%s' % entry['keyword'], 0, -1)
    entry['stars'] = [{'user_name': star} for star in stars]

    return render_template('keyword.html', entry = entry)

@app.route('/keyword/<keyword>', methods=['POST'])
@set_name
@authenticate
def delete_keyword(keyword):
    if keyword == '':
        abort(400)

    r = rh()
    if r.zscore('z:keywords', keyword) is None:
        abort(404)

    cur = dbh().cursor()
    cur.execute('DELETE FROM entry WHERE keyword = %s', (keyword,))
    with r.pipeline() as p:
        p.zrem('z:keywords', keyword)
        p.delete('hm:keywords:%s' % keyword)
        escaped_keyword = html.escape(keyword)
        p.delete('hm:html')
        p.hdel('hm:replacements', escaped_keyword)
        p.incr('keyword_modified')
        p.execute()

    return redirect('/')

@app.route("/stars")
def get_stars():
    request.args['keyword']
    cur = dbh().cursor()
    cur.execute('SELECT id, keyword, user_name, created_at FROM star WHERE keyword = %s', (keyword, ))
    return jsonify(stars = cur.fetchall())

@app.route("/stars", methods=['POST'])
def post_stars():
    keyword = request.args.get('keyword', "")
    if keyword == None or keyword == "":
        keyword = request.form['keyword']

    r = rh()
    if r.zscore('z:keywords', keyword) is None:
        abort(404)

    user_name = request.args.get('user', "")
    if user_name == None or user_name == "":
        user_name = request.form['user']

    cur = dbh().cursor()
    cur.execute('INSERT INTO star (keyword, user_name, created_at) VALUES (%s, %s, NOW())', (keyword, user_name))
    r.rpush('list:stars:%s' % keyword, user_name)
    return jsonify(result = 'ok')

_keyword_modified = 0
_keyword_replacer = None
def get_keyword_replacer():
    # check onece on every request
    if not hasattr(g, 'keyword_replacer'):
        global _keyword_modified, _keyword_replacer
        r = rh()
        keyword_modified = r.get('keyword_modified')
        # update cache
        if _keyword_modified != keyword_modified:
            replacements = r.hgetall('hm:replacements')
            _keyword_replacer = replacer.Replacer(replacements)
        _keyword_modified = keyword_modified
        g.keyword_replacer = _keyword_replacer
    return g.keyword_replacer

def htmlify(content):
    if content is None or content == '':
        return ''

    keyword_replacer = get_keyword_replacer()
    result = html.escape(content)
    result = keyword_replacer.replace(result)

    return result.replace("\n", '<br />')

def is_spam_contents(content):
    with urllib.request.urlopen(config('isupam_origin'), urllib.parse.urlencode({ "content": content }).encode('utf-8')) as res:
        data = json.loads(res.read().decode('utf-8'))
        return not data['valid']

    return False

def initialize_redis(cur, r):
    with r.pipeline() as p:
        p.flushdb()
        p.set('initialized', 'OK')
        # Get entry
        cur.execute("""
            SELECT keyword, description, UNIX_TIMESTAMP(updated_at) AS updated_at
            FROM entry
        """)
        entries = cur.fetchall()
        p.zadd('z:keywords', {
            entry['keyword']: entry['updated_at']
            for entry in entries
        })
        for entry in entries:
            p.hmset('hm:keywords:%s' % entry['keyword'], {
                'keyword': entry['keyword'],
                'description': entry['description'],
            })
        p.hmset('hm:replacements', {
            escaped_keyword: '<a href="/keyword/%s">%s</a>' % (
                urllib.parse.quote(keyword, safe=''),
                escaped_keyword,
            )
            for keyword, escaped_keyword in (
                (entry['keyword'], html.escape(entry['keyword']))
                for entry in entries
            )
        })
        p.incr('keyword_modified')
        # Get star
        cur.execute("""
            SELECT keyword, user_name
            FROM star
        """)
        stars = cur.fetchall()
        for star in stars:
            p.rpush('list:stars:%s' % star['keyword'], star['user_name'])
        # Execute
        p.execute()
    return

# Initialize
with _dbh() as cur:
    r = _rh()
    if r.exists('initialized') == 0:
        initialize_redis(cur, r)
    r.close()

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
