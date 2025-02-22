from flask import Flask, jsonify, render_template
import psycopg2
import os
import markdown
from dotenv import load_dotenv

load_dotenv()
credentials = [os.getenv("DB_HOST"), os.getenv("DB_PORT"), os.getenv("DB_USER"), os.getenv("DB_PASSWORD"), os.getenv("DB_NAME")]

app = Flask(__name__)

def get_db_connection():
    conn = psycopg2.connect(
        host=credentials[0],
        port=credentials[1],
        user=credentials[2],
        password=credentials[3],
        database=credentials[4]
    )
    return conn

@app.route('/')
def home():   
    # Read the Markdown file
    markdown_path = os.path.join('/home/ec2-user/mtgo-vintage-metagame-data', 'README.md')
    with open(markdown_path, 'r') as file:
        markdown_content = file.read()

    # Convert Markdown to HTML
    html_content = markdown.markdown(markdown_content)

    # Pass the HTML content to the template
    return render_template('markdown_page.html', content=html_content)

@app.route('/matches', methods=['GET'])
def get_data():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM "MATCHES"')

    column_names = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    results = [dict(zip(column_names, row)) for row in data]

    conn.close()
    return jsonify(results)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=80)