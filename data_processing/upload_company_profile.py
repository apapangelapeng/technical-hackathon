import json
import psycopg2
from tqdm import tqdm

def read_profiles_from_file(file_path):
    profiles = []
    with open(file_path, 'r') as file:
        for line in file:
            # Remove any leading,trailing whitespace & parse file
            profile_data = json.loads(line.strip())
            profiles.append(profile_data)
    return profiles
def drop_company_profile_if_table_exists(cursor):
    print("dropped old company_profile table")
    drop_table_query = "DROP TABLE IF EXISTS company_profiles;"
    cursor.execute(drop_table_query)
def create_company_profile_table(cursor):
    print("creating new company_profile table")
    create_table_query = """
    CREATE TABLE IF NOT EXISTS company_profiles (
        id SERIAL PRIMARY KEY,
        linkedin_internal_id VARCHAR,
        description TEXT,
        website VARCHAR,
        industry VARCHAR,
        company_size_start INT,
        company_size_end INT,
        company_size_on_linkedin INT,
        hq_country VARCHAR,
        hq_city VARCHAR,
        hq_postal_code VARCHAR,
        hq_line_1 VARCHAR,
        hq_is_hq BOOLEAN,
        hq_state VARCHAR,
        company_type VARCHAR,
        founded_year INT,
        specialities TEXT[],
        name VARCHAR,
        tagline TEXT,
        universal_name_id VARCHAR,
        profile_pic_url VARCHAR,
        background_cover_image_url VARCHAR,
        search_id VARCHAR,
        follower_count INT
    );
    """
    cursor.execute(create_table_query)

def insert_company_profiles_to_postgres(profiles, connection_params):
    try:
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()
        
        drop_company_profile_if_table_exists(cursor)
        create_company_profile_table(cursor)
        print("start to insert into company_profile table")
        for profile in tqdm(profiles, desc="Inserting profiles"):
            insert_query = """
            INSERT INTO company_profiles (linkedin_internal_id, description, website, industry, company_size_start, company_size_end, company_size_on_linkedin, hq_country, hq_city, hq_postal_code, hq_line_1, hq_is_hq, hq_state, company_type, founded_year, specialities, name, tagline, universal_name_id, profile_pic_url, background_cover_image_url, search_id, follower_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
            """
            
            linkedin_internal_id = profile.get('linkedin_internal_id')
            description = profile.get('description')
            website = profile.get('website')
            industry = profile.get('industry')
            company_size = profile.get('company_size', [None, None])
            company_size_start = company_size[0]
            company_size_end = company_size[1]
            company_size_on_linkedin = profile.get('company_size_on_linkedin')
            hq = profile.get('hq', {})
            hq_country = hq.get('country')
            hq_city = hq.get('city')
            hq_postal_code = hq.get('postal_code')
            hq_line_1 = hq.get('line_1')
            hq_is_hq = hq.get('is_hq')
            hq_state = hq.get('state')
            company_type = profile.get('company_type')
            founded_year = profile.get('founded_year')
            specialities = profile.get('specialities', [])
            name = profile.get('name')
            tagline = profile.get('tagline')
            universal_name_id = profile.get('universal_name_id')
            profile_pic_url = profile.get('profile_pic_url')
            background_cover_image_url = profile.get('background_cover_image_url')
            search_id = profile.get('search_id')
            follower_count = profile.get('follower_count')
            
            data_tuple = (
                linkedin_internal_id, description, website, industry, company_size_start, company_size_end, company_size_on_linkedin,
                hq_country, hq_city, hq_postal_code, hq_line_1, hq_is_hq, hq_state, company_type, founded_year, specialities,
                name, tagline, universal_name_id, profile_pic_url, background_cover_image_url, search_id, follower_count
            )
            
            cursor.execute(insert_query, data_tuple)
            id = cursor.fetchone()[0]
           
        
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print.error("Error connecting to PostgreSQL database: %s", e)
        raise   
if __name__ == "__main__" :
    company_file_path = 'data_processing/data/united_states_companies.txt'
    company_profiles = read_profiles_from_file(company_file_path)

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
    insert_company_profiles_to_postgres(company_profiles, connection_params)