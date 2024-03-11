# YOUTUBEPROJECT
YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit App

This project aims to develop a user-friendly Streamlit application that utilizes the Google API to extract information on a YouTube channel, stores it in a MongoDB database, migrates it to a SQL data warehouse, and enables users to search for channel details and join tables to view data in the Streamlit app.
There are 4 steps involved in this project:
                          1.DATA EXTRACT
                          2.MONGODB UPDATE
                          3.MONGODB TO SQL DB
                          4.STREAMLIT AND QUERY PART
                          


DATA EXTRACT:
The first step is to extract data from YouTube. This can be done using the YouTube Data API. The API provides access to a wide range of data, including channel information, video statistics, and contentdetails.we have created functions to extract channel,video,playlist and comments.we have function called Final to call the functions.we also have function called duration to convert the alphanumeric value to seconds for querying purpose.

MONGODB UPDATE:
Once  the data  extraction is done ,our next step is to store the data in a MongoDB . MongoDB is a great because it can handle unstructured and semi-structured data easily.
we acheive this by establishing the connection and creating database named youtube1 and in that database,we have created collection named youtubedata.we have created json file using json.dump and update that file in to mongodb database.

MONGODB TO SQL DB:
SQL is a relational database that is well-suited for querying and analyzing structured data. we are stablishing sql connection and created database.In that database,we have created tables for
                  1.channel
                  2.video
                  3.playlist
                  4.comment
once table creation is done,we have inserted values into the tables.

STREAMLIT AND QUERY PART:
Using Streamlit,we have created the front end to display channel details and updating mongodb and sql .You can use SQL queries to join the tables in the SQL data warehouse and retrieve data for specific channels based on user input and displaying the data in the Streamlit app.

This project has demonstrated how to harvest and warehouse YouTube data using SQL, MongoDB, and Streamlit. This approach can be used to collect, store, and analyze data.
This approach can be used to collect large amounts of data from YouTube. The data can be stored in a variety of ways, including MongoDB and SQL. The data can be analyzed using a variety of tools, including Streamlit. This approach can be used to identify trends, make predictions, and improve decision-making.






                  

