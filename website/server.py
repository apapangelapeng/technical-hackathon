from flask import Flask, request, jsonify, render_template
import psycopg2
import openai

app = Flask(__name__)

# Set up database connection
password = open("data_processing/secrets/password.pem", "r").read().strip()
connection_params = {
    "sslmode": "verify-ca",
    "sslrootcert": "data_processing/secrets/server-ca.pem",
    "sslcert": "data_processing/secrets/client-cert.pem",
    "sslkey": "data_processing/secrets/client-key.pem",
    "hostaddr": "34.30.107.254",
    "port": "5432",
    "user": "postgres",
    "dbname": "postgres",
    "user": "postgres",
    "password" : password
}

def get_db_connection():
    conn = psycopg2.connect(**connection_params)
    return conn

@app.route('/')
def index():
    return render_template('index.html')
@app.route('/task_1')
def taskOnePage():
    return render_template('task1.html')
@app.route('/task_2')
def taskTwoPage():
    return render_template('task2.html')
@app.route('/task_3')
def taskThreePage():
    return render_template('task3.html')

@app.route('/taskOneSearch', methods=['POST'])
def taskOneSearch():
    data = request.json
    query = data['query']
    print(query)
    conn = get_db_connection()
    cursor = conn.cursor()
    search_query = """
        SELECT id, name, industry, description
        FROM company_profiles
        WHERE to_tsvector('english', name || ' ' || industry || ' ' || description) @@ plainto_tsquery(%s);
    """
    cursor.execute(search_query, (query,))
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    profiles = [{'id': r[0], 'name': r[1], 'industry': r[2], 'description': r[3]} for r in results]
    print(f"found {len(profiles)} results") # debuggin code incase there is 0
    return jsonify(results=profiles)


if __name__ == '__main__':
    app.run(debug=True)
