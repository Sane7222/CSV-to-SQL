import pymysql
from prettytable import PrettyTable 
import sys
import json
import csv

def averageBudgetMovies(cur):
    sql = "USE a5"
    cur.execute(sql)

    sql = '''SELECT AVG(budget) FROM movies;''' # Get the avg budget of all movies
    cur.execute(sql)
    result = cur.fetchone()

    pt = PrettyTable(['Average Budget of All Movies']) # Pretty Table with this colomn name
    for att in pt.align:
        pt.align[att] = "l"

    pt.add_row(result)
    print(pt)

def moviesMadeUS(cur):
    sql = "USE a5"
    cur.execute(sql)

    # I am taking the smallest company name listed as the assignment does Not specifiy that all production companies must be present
    sql = '''SELECT movies.title, MIN(production_companies.company_name)
    FROM movies JOIN movie_company ON movies.movie_id = movie_company.movie_id
    JOIN production_companies ON movie_company.company_id = production_companies.company_id
    JOIN movie_country ON movies.movie_id = movie_country.movie_id
    JOIN production_countries ON movie_country.country_id = production_countries.country_id
    WHERE production_countries.country_id = 'US'
    GROUP BY movies.title
    '''
    cur.execute(sql)

    results = cur.fetchall()

    pt = PrettyTable(['Movie Title', 'Production Company']) # Pretty Table those results :)
    for att in pt.align:
        pt.align[att] = "l"

    for row in results:
        pt.add_row(row)

    print(pt)

def mostRevenue(cur):
    sql = "USE a5"
    cur.execute(sql)

    # Get the top 5 movies with the most revenue
    sql = '''SELECT title, revenue
         FROM movies
         ORDER BY revenue DESC
         LIMIT 5;
         '''
    cur.execute(sql)

    results = cur.fetchall()
    pt = PrettyTable(['Movie Title', 'Revenue']) # Pretty table those results :)
    for att in pt.align:
        pt.align[att] = "l"

    for row in results:
        pt.add_row(row)

    print(pt)

def scifiAndMystery(cur):
    sql = "USE a5"
    cur.execute(sql)

    # Group ConCat ties together all the strings in the specified colomn when group by the movie title
    sql = '''SELECT DISTINCT movies.title, GROUP_CONCAT(genres.genre_name)
    FROM movies JOIN movie_genres ON movies.movie_id = movie_genres.movie_id
    JOIN genres ON movie_genres.genre_id = genres.genre_id
    WHERE movies.title IN (
        SELECT movies.title
        FROM movies JOIN movie_genres ON movies.movie_id = movie_genres.movie_id
        JOIN genres ON movie_genres.genre_id = genres.genre_id
        WHERE genres.genre_name = 'Science Fiction'
    )
    AND movies.title IN (
        SELECT movies.title
        FROM movies
        JOIN movie_genres ON movies.movie_id = movie_genres.movie_id
        JOIN genres ON movie_genres.genre_id = genres.genre_id
        WHERE genres.genre_name = 'Mystery'
    )
    GROUP BY movies.title;
    '''
    cur.execute(sql)

    results = cur.fetchall()
    pt = PrettyTable(['Movie Title', 'Genres']) # Prettyify those results ;)
    for att in pt.align:
        pt.align[att] = "l"

    for row in results:
        pt.add_row(row)

    print(pt)

def aboveAvgPop(cur):
    sql = "USE a5"
    cur.execute(sql)

    # Nested Selection of avg popularity
    sql = '''SELECT title, popularity
    FROM movies
    WHERE popularity > (
    SELECT AVG(popularity)
    FROM movies
    );
    '''
    cur.execute(sql)

    results = cur.fetchall()
    pt = PrettyTable(['Movie Title', 'Popularity']) # Pretty Printer
    for att in pt.align:
        pt.align[att] = "l"

    for row in results:
        pt.add_row(row)

    print(pt)

def insertData(conn, cursor):
    sql = "USE a5"
    cursor.execute(sql)

    with open(sys.argv[1], "r") as f:
        reader = csv.reader(f)
        attr = next(reader) # extract the header row as a list
        data = {attr[i]: "" for i in range(len(attr))} # Populate empty dictionary with all attributes as keys
        
        for row in reader: # For each row
            for i in range(len(data)):
                data[attr[i]] = row[i] # Override previous data dictionary values with new ones for the row

            runtime = data['runtime'] # Unusual case with runtime. This is a fix for it
            if runtime == '':
                runtime = None
            else:
                runtime = float(runtime)

            # Add non repeating data to movie table
            cursor.execute(
                """
                INSERT INTO movies (
                movie_id,
                budget,
                homepage,
                original_language,
                original_title,
                overview,
                popularity,
                release_date,
                revenue,
                runtime,
                status,
                tagline,
                title,
                vote_average,
                vote_count ) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (int(data['id']), int(data['budget']), data['homepage'], data['original_language'], data['original_title'], data['overview'], float(data['popularity']), data['release_date'], 
                int(data['revenue']), runtime, data['status'], data['tagline'], data['title'], float(data['vote_average']), int(data['vote_count']))
            )


            genres = json.loads(data['genres']) # Unloads the json format of the non-atomic data into its own dictionary
            for genre in genres:
                genre_id = genre['id'] # Grab ID and Name
                genre_name = genre['name']
                
                cursor.execute("INSERT IGNORE INTO genres (genre_id, genre_name) VALUES (%s, %s)", (genre_id, genre_name)) # Insert the genre_id and genre_name into the genre table
                cursor.execute("INSERT INTO movie_genres (movie_id, genre_id) VALUES (%s, %s)", (int(data['id']), genre_id)) # Insert into the movie_genre relation to connect the tables

            keywords = json.loads(data['keywords']) # Unloads the json format of the non-atomic data into its own dictionary
            for keyword in keywords:
                keyword_id = keyword['id'] # Grab ID and Name
                keyword_name = keyword['name']

                cursor.execute("INSERT IGNORE INTO keywords (keyword_id, keyword_name) VALUES (%s, %s)", (keyword_id, keyword_name)) # Insert the keyword_id and keyword_name into the keywrod table
                cursor.execute("INSERT INTO movie_keywords (movie_id, keyword_id) VALUES (%s, %s)", (int(data['id']), keyword_id)) # Insert into the movie_keyword relation to connect the tables

            companies = json.loads(data['production_companies']) # Unloads the json format of the non-atomic data into its own dictionary
            for company in companies:
                company_id = company['id'] # Grab ID and Name
                company_name = company['name']

                cursor.execute("INSERT IGNORE INTO production_companies (company_id, company_name) VALUES (%s, %s)", (company_id, company_name)) # Insert the company_id and company_name into the prod_company table
                cursor.execute("INSERT INTO movie_company (movie_id, company_id) VALUES (%s, %s)", (int(data['id']), company_id)) # Insert into the movie_company relation to connect the tables

            countries = json.loads(data['production_countries'])
            for country in countries:
                country_id = country['iso_3166_1'] # Grab ID and Name
                country_name = country['name']

                cursor.execute("INSERT IGNORE INTO production_countries (country_id, country_name) VALUES (%s, %s)", (country_id, country_name)) # Insert the country_id and country_name into the prod_country table
                cursor.execute("INSERT INTO movie_country (movie_id, country_id) VALUES (%s, %s)", (int(data['id']), country_id)) # Insert into the movie_country relation to connect the tables

            languages = json.loads(data['spoken_languages'])
            for lang in languages:
                lang_id = lang['iso_639_1'] # Grab ID and Name
                lang_name = lang['name']

                cursor.execute("INSERT IGNORE INTO spoken_languages (language_id, language_name) VALUES (%s, %s)", (lang_id, lang_name)) # Insert the language_id and language_name into the language table
                cursor.execute("INSERT INTO movie_language (movie_id, language_id) VALUES (%s, %s)", (int(data['id']), lang_id)) # Insert into the movie_language relation to connect the tables
            
        # commit the changes to the database
        conn.commit()

def createSchema(conn, cur):

    sql = "DROP DATABASE IF EXISTS a5" # Drop, Create, and Use New Database called a5
    cur.execute(sql)
    sql = "CREATE DATABASE a5"
    cur.execute(sql)
    sql = "USE a5"
    cur.execute(sql)

    # Movies
    sql = '''CREATE TABLE movies (
    movie_id INT PRIMARY KEY,
    budget INT,
    homepage VARCHAR(255),
    original_language VARCHAR(50),
    original_title VARCHAR(255),
    overview TEXT,
    popularity FLOAT,
    release_date TEXT,
    revenue BIGINT,
    runtime INT,
    status VARCHAR(50),
    tagline TEXT,
    title VARCHAR(255),
    vote_average FLOAT,
    vote_count INT
    );'''
    cur.execute(sql)

    # Genres
    sql = '''CREATE TABLE genres (
    genre_id INT PRIMARY KEY,
    genre_name VARCHAR(50)
    );'''
    cur.execute(sql)

    # Movie Genre Relation
    sql = '''CREATE TABLE movie_genres (
    movie_id INT,
    genre_id INT,
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
    FOREIGN KEY (genre_id) REFERENCES genres(genre_id),
    PRIMARY KEY (movie_id, genre_id)
    );'''
    cur.execute(sql)

    # Keywords
    sql = '''CREATE TABLE keywords (
    keyword_id INT PRIMARY KEY,
    keyword_name VARCHAR(50)
    );'''
    cur.execute(sql)

    # Movie Keyword Relation
    sql = '''CREATE TABLE movie_keywords (
    movie_id INT,
    keyword_id INT,
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
    FOREIGN KEY (keyword_id) REFERENCES keywords(keyword_id),
    PRIMARY KEY (movie_id, keyword_id)
    );'''
    cur.execute(sql)

    # Companies
    sql = '''CREATE TABLE production_companies (
    company_id INT PRIMARY KEY,
    company_name VARCHAR(50)
    );'''
    cur.execute(sql)

    # Movie Company Relation
    sql = '''CREATE TABLE movie_company (
    movie_id INT,
    company_id INT,
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
    FOREIGN KEY (company_id) REFERENCES production_companies(company_id),
    PRIMARY KEY (movie_id, company_id)
    );'''
    cur.execute(sql)

    # Countries
    sql = '''CREATE TABLE production_countries (
    country_id VARCHAR(5) PRIMARY KEY,
    country_name VARCHAR(50)
    );'''
    cur.execute(sql)

    # Movie Country Relation
    sql = '''CREATE TABLE movie_country (
    movie_id INT,
    country_id VARCHAR(5),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
    FOREIGN KEY (country_id) REFERENCES production_countries(country_id),
    PRIMARY KEY (movie_id, country_id)
    );'''
    cur.execute(sql)

    # Languages
    sql = '''CREATE TABLE spoken_languages (
    language_id VARCHAR(5) PRIMARY KEY,
    language_name VARCHAR(50)
    );'''
    cur.execute(sql)

    # Movie Language Relation
    sql = '''CREATE TABLE movie_language (
    movie_id INT,
    language_id VARCHAR(5),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
    FOREIGN KEY (language_id) REFERENCES spoken_languages(language_id),
    PRIMARY KEY (movie_id, language_id)
    );'''
    cur.execute(sql)

    conn.commit()

def main():
    if len(sys.argv) < 2: # Check for a file
        print("Please provide the correct command line args. See README for details")
        sys.exit(1)

    conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='password')
    cur = conn.cursor()

    ### Create Schema & Insert Data ###
    createSchema(conn, cur)
    insertData(conn, cur)
    
    ### These are my query methods ###
    # averageBudgetMovies(cur)
    # moviesMadeUS(cur)
    # mostRevenue(cur)
    # scifiAndMystery(cur)
    # aboveAvgPop(cur)

    cur.close()
    conn.close()

if __name__ == '__main__':
    main()