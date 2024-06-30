import psycopg2
from openai import OpenAI
import json
from tqdm import tqdm


def get_db_connection(connection_params):
    conn = psycopg2.connect(**connection_params)
    return conn

def fetch_profiles(cursor, table_name):
    cursor.execute(f"""
        SELECT id, public_identifier, profile_pic_url, background_cover_image_url, first_name, last_name, full_name, 
               occupation, headline, summary, country, country_full_name, city, state
        FROM {table_name} 
        WHERE embedding IS NULL;
    """)
    profiles = cursor.fetchall()
    return profiles

def update_profile_embedding(cursor, table_name, profile_id, embedding):
    cursor.execute(f"UPDATE {table_name} SET embedding = %s WHERE id = %s;", (json.dumps(embedding), profile_id))

def check_and_initialize_column(cursor, table_name):
    cursor.execute(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name='{table_name}' AND column_name='embedding';
    """)
    column_exists = cursor.fetchone()

    if column_exists is None:
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN embedding jsonb;")
        print(f"Initialized new 'embedding' column in {table_name}.")

def compute_embeddings(connection_params, table_name):
    conn = get_db_connection(connection_params)
    cursor = conn.cursor()

    try:
        check_and_initialize_column(cursor, table_name)
        profiles = fetch_profiles(cursor, table_name)
        for profile in tqdm(profiles, desc=f"Inserting profiles into {table_name}"):
            profile_id, *fields = profile
            combined_text = " ".join(str(field) for field in fields if field).strip()

            if not combined_text:
                print(f"Skipping profile {profile_id} in {table_name} due to empty combined text.")
                continue

            try:
                response = client.embeddings.create(input=combined_text, model="text-embedding-ada-002")
                embedding = response.data[0].embedding
                update_profile_embedding(cursor, table_name, profile_id, embedding)
            except Exception as e:
                error_message = str(e)
                if 'Error code: 400' in error_message and 'Please submit an `input`' in error_message:
                    print(f"Skipping profile {profile_id} in {table_name} due to empty input error.")
                else:
                    raise e

        conn.commit()  # Commit all changes at once
    except Exception as e:
        conn.rollback()  # Rollback in case of error
        print(f"An error occurred: {e}")
    finally:
        cursor.close()
        conn.close()
if __name__ == "__main__" :

    client = OpenAI(api_key=open("data_processing/secrets/open-ai-key.pem", "r").read().strip())
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
    compute_embeddings(connection_params, "user_profiles")