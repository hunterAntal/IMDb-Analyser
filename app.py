from flask import Flask, render_template
from pymongo import MongoClient

# ...existing code if any...

app = Flask(__name__)

# MongoDB connection (modify the URI/DB name as needed)
client = MongoClient("mongodb://localhost:27017")
db = client['3675Project']

@app.route('/')
def index():
    # Query an example collection (modify collection name/query as needed)
    collection = db['trimmed_title_basics']
    sample_docs = list(collection.find().limit(5))
    return render_template("index.html", sample_docs=sample_docs)

if __name__ == '__main__':
    app.run(debug=True)
