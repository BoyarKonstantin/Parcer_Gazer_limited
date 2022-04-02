from asyncio.windows_events import NULL
from flask import Flask, render_template

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/main')
def main():
    return 'філаджфілваджєфіл'

if __name__ == '__main__':
    app.run(debug = True)