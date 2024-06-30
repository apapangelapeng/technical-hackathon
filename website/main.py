from flask import Flask, request, jsonify, render_template
import psycopg2
from openai import OpenAI
import json
import os
import firebase_admin
from firebase_admin import credentials, storage
import atexit
import shutil
import stat


# Initialize Firebase Admin SDK
cred = credentials.Certificate("technical-hackathon-firebase-adminsdk-tzghq-e6b98ccad8.json")
firebase_admin.initialize_app(cred, {
    'storageBucket': 'technical-hackathon.appspot.com'
})

bucket = storage.bucket()

# Temporary directory for storing downloaded files
temp_dir = "/tmp/firebase_downloads"
os.makedirs(temp_dir, exist_ok=True)

def get_file_from_firebase(filename):
    blob = bucket.blob(f"secrets/{filename}")
    local_path = os.path.join(temp_dir, filename)  # Use temp_dir for temporary storage
    blob.download_to_filename(local_path)
    print(f"Downloaded {filename} from Firebase Storage to {local_path}")
    # update permission
    os.chmod(local_path, stat.S_IRUSR | stat.S_IWUSR)
    return local_path

# Correctly assign the OpenAI API key

client = OpenAI(api_key=open(get_file_from_firebase("open-ai-key.pem")).read().strip())
password = open(get_file_from_firebase("password.pem"),"r").read().strip()
app = Flask(__name__)

# Set up database connection
sslrootcert_path = get_file_from_firebase("server-ca.pem")
sslcert_path = get_file_from_firebase("client-cert.pem")
sslkey_path = get_file_from_firebase("client-key.pem")

connection_params = {
    "sslmode": "verify-ca",
    "sslrootcert": sslrootcert_path,
    "sslcert": sslcert_path,
    "sslkey": sslkey_path,
    "hostaddr": "34.30.107.254",
    "port": "5432",
    "user": "postgres",
    "dbname": "postgres",
    "password": password
}


# Load table schemas
table_schema_path = get_file_from_firebase("table_schema.json")
with open(table_schema_path, 'r') as file:
    table_schema_content = file.read()
    print(f"Content of table_schema.json: {table_schema_content}")  # Debug: Print file contents to ensure correctness

try:
    table_schemas = json.loads(table_schema_content)
except json.JSONDecodeError as e:
    print(f"Failed to decode JSON from table_schema.json: {e}")
    table_schemas = {}



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




@app.route('/taskTwoSearch', methods=['POST'])
def taskTwoSearch():
    data = request.json
    query = data['query']

    # Get query embedding
    #query_embedding_response = openai.embeddings.create(input=query, model="text-embedding-ada-002")
    query_embedding_response = client.embeddings.create(input=query, model="text-embedding-ada-002")
    query_embedding = query_embedding_response.data[0].embedding

    conn = get_db_connection()
    cursor = conn.cursor()

    # Fetch profiles and their embeddings
    cursor.execute("""
        SELECT id, name, industry, description, embedding
        FROM company_profiles
        WHERE embedding IS NOT NULL;
    """)
    profiles = cursor.fetchall()
    print("got all the profiles, now calculating the similarity score")
    # Calculate similarity scores
    profile_similarities = []
    for profile in profiles:
        profile_id, name, industry, description, embedding = profile
        embedding = json.loads(embedding) if isinstance(embedding, str) else embedding

        # Calculate cosine similarity
        similarity = sum(q * p for q, p in zip(query_embedding, embedding))
        profile_similarities.append((profile_id, name, industry, description, similarity))

    # Sort profiles by similarity
    profile_similarities.sort(key=lambda x: x[4], reverse=True)
    top_profiles = profile_similarities[:10]  # Get top 10 results

    results = [{'id': p[0], 'name': p[1], 'industry': p[2], 'description': p[3], 'similarity': p[4]} for p in top_profiles]

    cursor.close()
    conn.close()

    return jsonify(results=results)





### following code for task 3



## ask for the db query , given the scheme provided
def generate_db_query(natural_language_query, table_schemas):
    schema_description = "\n".join([
        f"Table {table_name}:\n{', '.join([f'{col} {dtype}' for col, dtype in schema.items()])}"
        for table_name, schema in table_schemas.items()
    ])
    prompt = f"Given the following table schemas:\n\n{schema_description}\n\nConvert the following natural language query to an SQL query where there still needs to be id, name, industry, description as part of the column :\n\n{natural_language_query}\n\nSQL Query:"

    response = client.chat.completions.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You are an expert database assistant who will only return valid sql queries"},
            {"role": "user", "content": prompt}
        ],
        max_tokens=150,
        temperature=0
    )
    
    sql_query = response.choices[0].message.content.strip()
    print(f"token_used: {response.usage.total_tokens}")
    return sql_query
    # query =  "SELECT * FROM company_profiles \nWHERE company_size_start=10;"
    
    # return query

def get_explanation(db_response,natural_language_query):
    
    prompt = f"given the question asked {natural_language_query}, and the response from the database {db_response}. Can you please explain in simple words what the answer is?"

    response = client.chat.completions.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You are an expert database assistant who is very good at explaning what the data mean"},
            {"role": "user", "content": prompt}
        ],
        max_tokens=150,
        temperature=0
    )
    
    answer = response.choices[0].message.content.strip()
    print(answer)
    return answer
    # query =  "SELECT * FROM company_profiles \nWHERE company_size_start=10;"
    
    # return query


def execute_db_query(cursor, sql_query):
    cursor.execute(sql_query)
    profiles = cursor.fetchall()
    result = [{'id': p[0], 'name': p[1], 'industry': p[2], 'description': p[3]} for p in profiles]
    return result

def process_natural_language_query( natural_language_query, table_schemas):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        sql_query = generate_db_query(natural_language_query, table_schemas)
        print(f"Generated SQL Query: {sql_query}")

        result = execute_db_query(cursor, sql_query)
        print("this is the result:", result)
        
        return result
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    finally:
        cursor.close()
        conn.close()


@app.route('/taskThreeSearch', methods=['POST'])
def task_three_search():
    data = request.json
    natural_language_query = data.get('query')
    if not natural_language_query:
        return jsonify({'error': 'No query provided'}), 400

    results = process_natural_language_query(natural_language_query, table_schemas)
    explanation = get_explanation(results,natural_language_query)
    if results is None:
        return jsonify({'error': 'An error occurred while processing the query'}), 500

    return jsonify(results=results, explanation = explanation)

# Cleanup function to remove temporary files
def cleanup():
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
        print(f"Cleaned up temporary directory {temp_dir}")

atexit.register(cleanup)

if __name__ == '__main__':
    app.run(debug=True)
    app.run(host='0.0.0.0', port=8080)
