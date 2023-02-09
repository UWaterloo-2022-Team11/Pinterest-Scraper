from flask import Flask, request
from flask import current_app as app
from flask import render_template
import os
import psutil
import signal
import subprocess
import sys

scrapers = {}
log = ''
log_tuples = []
app = Flask(__name__)


class Scraper():
    def __init__(self, pid):
        self.pid = pid
        self.active = 1
        self.scraped = []
        self.failed = []

    def stop(self):
        self.active = 0


# inject scrapers into template
@app.context_processor
def inject_scrapers():
    return dict(scrapers=scrapers)

# injects log into template
@app.context_processor
def inject_log_tuples():
    return dict(log_tuples=log_tuples)


@app.route('/start_scraper/')
def start_scraper():
    pid = subprocess.Popen([sys.executable, "pinterest_scraper.py"], shell = True)
    print(pid)
    return ''

@app.route('/stop/<int:n1>')
def stop(n1):
    # soft stop, use to tell scraper to stop after current user
    global scrapers
    scrapers[n1].stop()
    return ''

@app.route('/kill/<int:n1>')
def kill(n1):
    # soft stop, use to tell scraper to stop after current user
    global scrapers
    # os.kill(n1, signal.CTRL_BREAK_EVENT)
    subprocess.call(['taskkill', '/F', '/T', '/PID',  str(n1)])
    scrapers[n1].stop()
    return ''

@app.route('/active/<int:n1>')
def active(n1):
    # soft stop, use to tell scraper to stop after current user
    global scrapers
    return {'active': scrapers[n1].active}

@app.route('/scraped_user/<int:n1>/<id>')
def scraped_user(n1, id):
    # soft stop, use to tell scraper to stop after current user
    global scrapers
    scrapers[n1].scraped.append(id)
    log_tuples.append((n1, id, 'scraped'))
    return ''

@app.route('/failed_user/<int:n1>/<id>')
def failed_user(n1, id):
    # soft stop, use to tell scraper to stop after current user
    global scrapers
    scrapers[n1].failed.append(id)
    log_tuples.append((n1, id, 'failed'))
    return ''

@app.route('/finished/<int:n1>')
def finished(n1):
    # scrapers should request this when finished scraping, should make this POST
    global scrapers
    del scrapers[n1]
    return ''


@app.route('/register/<int:n1>')
def register(n1):
    global scraper
    scraper = Scraper(n1)
    scrapers[n1] = scraper
    return ''

@app.route('/refresh/')
def refresh():
    keys_to_remove = []
    for scraper_id in scrapers:
        if not psutil.pid_exists(scraper_id):
            keys_to_remove.append(scraper_id)
    for scraper_id in keys_to_remove:
        del scrapers[scraper_id]
    return ''

@app.route('/')
def home():
    """Landing page."""
    return render_template(
        'index.html'
    )

if __name__== '__main__':
    app.run()
    # app.run(debug=True)
