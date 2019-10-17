# Current tables:
# googlescholararticles:
# id-integer
# name-varchar (actual name of primary author, not id)
# affiliation-varchar (School name of primary author)
# citedby-varchar (number article was cited by)
# pub_title-varchar (Article title)
# pub_year-varchar (Year published)
# citations-varchar (number of citations it has?)
# pub_author-text (List of authors seperated by " and ")
# eprint-varchar (URL to pdf)
# pub_number-varchar (not important)
# pub_publisher-varchar (Publisher)
# pub_url-varchar (URL to publication)
# journal-varchar (Journal)

# googlescholarauthors:
# id-integer
# name-varchar (name of author)
# affiliation-varchar (School name)
# citedby-varchar (Cited by number)
# attributes-text (total citations, total citations since 2014, h-index,
# h-index through 2014, i10-index, i-10 index through 2014, list of research interests,
#  citations by year in the format {YYYY1: n, YYYY2: m, …})
# page-integer (Not important)
# email-varchar (End of email, don't include)
# interests-text (List of interests seperated by ", ")
# url_picture-text (Not important)

# Target tables:
# Article (ArticleId, PrimaryAuthorId, CitedBy, Citations, Title, Year, Url, Publisher, Journal)
# Author (AuthorId, Name, Affiliation, CitedBy, Email, h-index, i10-index)
# YearlyCitations (AuthorId, Year, citations)
# Authored (AuthorId, ArticleId)
# InterestedIn (AuthorId, Interest)

import mysql.connector
import sqlite3
import time

def createArticlesTable(cursor, liteConn):
    seconds = time.time()
    cursor.execute("CREATE TABLE IF NOT EXISTS Article (ArticleId INTEGER PRIMARY KEY, PrimaryAuthorId INTEGER, CitedBy INTEGER, Citations INTEGER, Title TEXT NOT NULL, Year INTEGER, Url TEXT, Publisher TEXT, Journal TEXT)")
    liteCur = liteConn.cursor()
    liteCur.execute('SELECT * FROM googlescholararticles')
    i = 0
    for row in liteCur:
        if i % 100 == 0:
            print(i/113298675*100)
        i += 1
        innerCur = liteConn.cursor()
        innerCur.execute("SELECT id FROM googlescholarauthors WHERE name='" + row["name"].replace("'", "''") + "'")
        PrimaryAuthorId = innerCur.fetchone()
        citedBy = None
        citations = None
        year = None
        if (not row["citedby"] == ''):
            citedBy = row["citedby"]
        if (not row["citations"] == ''):
            citations = row["citations"]
        if (not row["pub_year"] == ''):
            year = row["pub_year"]
        values = (row["id"], PrimaryAuthorId["id"], citedBy, citations, row["pub_title"], year, row["pub_url"], row["pub_publisher"], row["journal"])
        cursor.execute("INSERT INTO Article VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", values)
    print("FINISHED!")
    finalSeconds = time.time()
    print("Seconds to run: " + str(finalSeconds - seconds))

def displayTableData(cursor, table):
    cursor.execute("SELECT * FROM " + table)
    for row in cursor:
        print(row)

def dropTable(cursor, table):
    cursor.execute("DROP TABLE " + table)

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="password",
    auth_plugin='mysql_native_password',
    database="googleScholar"
)

mycursor = mydb.cursor()
# mycursor.execute("CREATE DATABASE IF NOT EXISTS googleScholar") # Execute only once before defining database above
liteConn = sqlite3.connect("google-scholar.db")
liteConn.row_factory = sqlite3.Row

# dropTable(mycursor, "Article")
createArticlesTable(mycursor, liteConn)
# displayTableData(mycursor, "Article")