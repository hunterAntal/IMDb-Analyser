from markupsafe import escape
from flask import Flask

app = Flask(__name__)

@app.route("/<name>")
def hello(name):
    return f"Hello, {escape(name)}!"
@app.route("/")
def index():
    return 'Nothing to see Here'

@app.route('/hello')
def hello():
    return 'Hello, World'

