import json
import psycopg2
from tqdm import tqdm

# read person_profile
def read_profiles_from_file(file_path):
    profiles = []
    with open(file_path, 'r') as file:
        for line in file:
            # Remove any leading,trailing whitespace & parse file
            profile_data = json.loads(line.strip())
            profiles.append(profile_data)
    return profiles


def insert_user_profiles_to_postgres(profiles, connection_params):
    try:
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()
        
        create_user_profile_table_if_not_exists(cursor)
        
        for profile in tqdm(profiles, desc="Inserting profiles"):
            insert_query = """
            INSERT INTO user_profiles (public_identifier, profile_pic_url, background_cover_image_url, first_name, last_name, full_name, occupation, headline, summary, country, country_full_name, city, state)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
            """
            data_tuple = (
                profile.get('public_identifier', None),
                profile.get('profile_pic_url', None),
                profile.get('background_cover_image_url', None),
                profile.get('first_name', None),
                profile.get('last_name', None),
                profile.get('full_name', None),
                profile.get('occupation', None),
                profile.get('headline', None),
                profile.get('summary', None),
                profile.get('country', None),
                profile.get('country_full_name', None),
                profile.get('city', None),
                profile.get('state', None),
            )
            cursor.execute(insert_query, data_tuple)
            id = cursor.fetchone()[0]
            
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print.error("Error connecting to PostgreSQL database: %s", e)
        raise

def create_user_profile_table_if_not_exists(cursor):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS user_profiles (
        id SERIAL PRIMARY KEY,
        public_identifier VARCHAR,
        profile_pic_url VARCHAR,
        background_cover_image_url VARCHAR,
        first_name VARCHAR,
        last_name VARCHAR,
        full_name VARCHAR,
        occupation VARCHAR,
        headline VARCHAR,
        summary TEXT,
        country VARCHAR,
        country_full_name VARCHAR,
        city VARCHAR,
        state VARCHAR
    );
    """
    cursor.execute(create_table_query)





if __name__ == "__main__" :
    person_file_path = 'data/us_person_profile.txt'
    person_profiles = read_profiles_from_file(person_file_path)
    password = open("secrets/password.pem", "r").read().strip()
    connection_params = {
            "sslmode": "verify-ca",
            "sslrootcert": "secrets/server-ca.pem",
            "sslcert": "secrets/client-cert.pem",
            "sslkey": "secrets/client-key.pem",
            "hostaddr": "34.30.107.254",
            "port": "5432",
            "user": "postgres",
            "dbname": "postgres",
            "user": "postgres",
            "password" : password
        }
    insert_user_profiles_to_postgres(person_profiles, connection_params)