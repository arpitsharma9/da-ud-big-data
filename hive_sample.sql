
Introduction

You are given a book-crossing database. The Data Set Comprises of 3 Tables with the following format. 

BX-USERS ("User-ID";"Location";"Age") Contains the users. Note that user IDs (User-ID) have been anonymized and map to integers. Demographic data is provided (Location, Age) if available. Otherwise, these fields contain NULL-values.

BX-Books ("ISBN";"Book-Title";"Book-Author";"Year-Of-Publication";"Publisher";"Image-URL-S";"Image-URL-M";"Image-URL-L") Books are identified by their respective ISBN. Invalid ISBNs have already been removed from the dataset. Moreover, some content-based information is given (Book-Title, Book-Author, Year-Of-Publication, Publisher), obtained from Amazon Web Services. Note that in the case of several authors, only the first is provided. URLs linking to cover images are also given, appearing in three different flavours (Image-URL-S, Image-URL-M, Image-URL-L), i.e., small, medium, large. These URLs point to the Amazon web site.

BX-Book-Ratings ("User-ID";"ISBN";"Book-Rating") Contains the book rating information. Ratings (Book-Rating) are either explicit, expressed on a scale from 1-10 (higher values denoting higher appreciation), or implicit, expressed by 0.


Launch and connect to EMR cluster


Download sample files using this command: "wget https://video.udacity-data.com/topher/2020/October/5f8f4412_bx-csv-data/bx-csv-data.zip" and unzip the folder.

wget https://video.udacity-data.com/topher/2020/October/5f8f4412_bx-csv-data/bx-csv-data.zip
unzip bx-csv-data.zip

hdfs dfs -mkdir /user/bx-books/
hdfs dfs -mkdir /user/bx-users/
hdfs dfs -mkdir /user/bx-book-rating/

# list and check file size
ls -llh

# Recursive listing of directory to see files in directory
ls -R

cd BX-CSV-Data/
hdfs dfs -put BX-Books.csv /user/bx-books/
hdfs dfs -put BX-Users.csv /user/bx-users/
hdfs dfs -put BX-Book-Ratings.csv /user/bx-book-rating/

hdfs dfs -ls /user/bx-books/
hdfs dfs -ls /user/bx-users/
hdfs dfs -ls /user/bx-book-rating/


--Develop and execute Hive DDL statement to create a database called 'book_db'

CREATE DATABASE IF NOT EXISTS book_db;
set hive.cli.print.header=true;

show DATABASES;
use book_db;
show tables;


-- Develop and execute Hive DDL statement to create table "bx-users" [External Table]

CREATE EXTERNAL TABLE IF NOT EXISTS book_db.bx_users (
    userid string,
    Location_name string,
    Age string
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'
    Stored as TEXTFILE 
    LOCATION '/user/bx_users'
    TBLPROPERTIES("skip.header.line.count"="1");

    describe formatted bx_users;


-- Develop and execute Hive DDL statement to create table "bx-books" [External Table]

CREATE EXTERNAL TABLE IF NOT EXISTS book_db.bx_books(
    ISBN string,
    Book_Title string,
    Book_Author string,
    Year_Of_Publication INT,
    Publisher string,
    Image_URL_S string,
    Image_URL_M string,
    Image_URL_L string
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'
    Stored as Textfile 
    LOCATION '/user/bx_books'
    TBLPROPERTIES("skip.header.line.count"="1");

    describe formatted bx_books;

-- Develop and execute Hive DDL statement to create table "bx-book-ratings" [External Table]


CREATE EXTERNAL TABLE IF NOT EXISTS book_db.bx_book_ratings(
    userid string,
    ISBN string,
    Book_Rating int
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'
    Stored as Textfile 
    LOCATION '/user/bx-book-ratings'
    TBLPROPERTIES("skip.header.line.count"="1");

    describe formatted bx_book_ratings;


-- Load data to each of the three table

LOAD DATA LOCAL INPATH 'BX-CSV-Data/BX-Book-Ratings.csv' OVERWRITE INTO TABLE book_db.bx_book_ratings;
LOAD DATA LOCAL INPATH 'BX-CSV-Data/BX-Users.csv' OVERWRITE INTO TABLE book_db.bx_users;
LOAD DATA LOCAL INPATH 'BX-CSV-Data/BX-Books.csv' OVERWRITE INTO TABLE book_db.bx_books;

--display 10 rows from each table.

SELECT * FROM book_db.bx_users LIMIT 10;
SELECT * FROM book_db.bx_books LIMIT 10;
SELECT * FROM book_db.bx_book_ratings LIMIT 10;




--Develop query to find most active user (use who has provided the most ratings) (bx- book-ratings table)


SELECT userid, COUNT(Book_Rating) AS num_of_ratings from bx_book_ratings GROUP BY UserID ORDER BY num_of_ratings DESC limit 10;

 --Develop query to find total number of unique users from bx-users table where age is not null

 select count(distinct userid) from bx_users where Age is not null;

Develop query to find total number of users between age 21-30

 select count(userid) from bx_users where Age between 21 and 30;

Develop query to find the top 5 publishers in descending order based on the number of books they have published.

 select publisher, count(book_title) as num_of_books from  bx_books group by publisher order by num_of_books desc limit 5;

 --The name of the highest-rated book author is (or something similar)



select bxb.book_author ,avg(bbr.book_rating) as avg_rating,count(bxb.book_title) as num_books 
from book_db.bx_books as bxb
join book_db.bx_book_ratings as bbr
on bxb.isbn= bbr.isbn
group by bxb.book_author
having num_books>4
order by avg_rating DESC
limit 10;

/** new problem -- Dynamic partitioning **//

-- Step 1 - get the file and move it to the Hadoop cluster

wget https://video.udacity-data.com/topher/2020/November/5fabec84_game-reviews.tsv/game-reviews.tsv.gz

hdfs dfs -mkdir /user/game_review/

hdfs dfs -put game-reviews.tsv.gz /user/game_review/

hdfs dfs -ls /user/game_review/

gunzip game-reviews.tsv.gz

hdfs dfs -put  game-reviews.tsv /user/game_review/


--- Step 2 : Create databsee , table and describe table

CREATE DATABASE IF NOT EXISTS demo_db;
set hive.cli.print.header=true;

show DATABASES;
use demo_db;
show tables;


-- Develop and execute Hive DDL statement to create table "bx-users" [External Table]

CREATE EXTERNAL TABLE IF NOT EXISTS demo_db.game_reviews (
    marketplace string,    
customer_id string, 
review_id string,    
product_id string,    
product_parent string,    
product_title string,    
product_category string, 
star_rating float,
helpful_votes int,
total_votes int,
vine string,
verified_purchase string,
review_headline string,
review_body string,
review_date date
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    Stored as TEXTFILE 
    LOCATION '/user/game_review'
    TBLPROPERTIES("skip.header.line.count"="1");

    describe formatted demo_db.game_reviews;

-- load data into the table
LOAD DATA LOCAL INPATH 'game-reviews.tsv' OVERWRITE INTO TABLE demo_db.game_reviews;

select * from demo_db.game_reviews limit 1;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;


CREATE EXTERNAL TABLE IF NOT EXISTS demo_db.game_reviews_partitioned (
  marketplace string,    
customer_id string, 
review_id string,    
product_id string,    
product_parent string,    
product_title string,    
product_category string, 
star_rating float,
helpful_votes int,
total_votes int,
vine string,
verified_purchase string,
review_headline string,
review_body string,
review_date date
    )

PARTITIONED BY (year string)
   ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    Stored as TEXTFILE 
    LOCATION '/user/game_review'
    TBLPROPERTIES("skip.header.line.count"="1");

    describe formatted demo_db.game_reviews_partitioned;

    INSERT OVERWRITE TABLE demo_db.game_reviews_partitioned PARTITION (year) SELECT *, year(review_date) FROM demo_db.game_reviews;


--- drop the tables
drop table demo_db.game_reviews_partitioned;
drop table demo_db.game_reviews;
show tables; 

hdfs dfs -rm -r /user/game_review/
hdfs dfs -rm -r /user/


hdfs dfs -ls /user/game_review/


