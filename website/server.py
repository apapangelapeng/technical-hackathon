from flask import Flask, request, jsonify, render_template
import psycopg2
from openai import OpenAI
import json

client = OpenAI(api_key=open("data_processing/secrets/open-ai-key.pem", "r").read().strip())

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




@app.route('/taskTwoSearch', methods=['POST'])
def taskTwoSearch():
    data = request.json
    query = data['query']

    # Get query embedding
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

table_schemas = {}
# Table schemas
with open("data_processing/secrets/table_schema.json", "r") as file:
    table_schemas = json.load(file)




## ask for the db query , given the scheme provided
def generate_db_query(natural_language_query, table_schemas):
    schema_description = "\n".join([
        f"Table {table_name}:\n{', '.join([f'{col} {dtype}' for col, dtype in schema.items()])}"
        for table_name, schema in table_schemas.items()
    ])
    prompt = f"Given the following table schemas:\n\n{schema_description}\n\nConvert the following natural language query to an SQL query:\n\n{natural_language_query}\n\nSQL Query:"

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


def execute_db_query(cursor, sql_query):
    cursor.execute(sql_query)
    result = cursor.fetchall()
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
    if results is None:
        return jsonify({'error': 'An error occurred while processing the query'}), 500

    return jsonify({'results': results})



if __name__ == '__main__':
    app.run(debug=True)
