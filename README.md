Gathering and Storing YouTube Data using MongoDB, SQL, and Streamlight
Using Streamlit to build a straightforward, user-friendly interface, data was retrieved from the YouTube API, stored in a MongoDB data lake, moved to a SQL data warehouse, queried using SQL, and then shown in the Streamlit application.


Python prerequisite: Set up Python
MySQL Install the MySQL client and server on your computer.
Install MongoDB on your computer.
Setting up
Clone the repository, then establish and turn on the project's environment.
Use the command "pip install -r requirements.txt" to install all necessary packages from the requirements.txt file.
Application
Use the following command to launch the app: "streamlit run youtube_dataharvest.py"
using Channel ID to do a channel search.
Get the channel data and store it in MongoDB.
channel data is being moved from MongoDB to SQL database.
Visit the "Q & A" section to learn more about the migrated channel data in general.
Features
Configuring the Streamlit application: The Streamlit application is used to generate a basic user interface (UI) that allows users to examine channel details, enter a YouTube channel ID, save channel data to MongoDB, and choose to migrate channel data from MongoDB to a MySQL database. on order to gain further insight into the data gathered, we can also show the results of the SQL query on the Streamlit app. Additionally, we may make charts and graphs using Streamlit's data visualization tools to assist users in analyzing the data.
Establish a connection with the YouTube API to obtain information about channels, videos, and comments. To send requests to the API, we can utilize the Python Google API client library.
Store data in a MongoDB data lake: The data lake contains the data that was retrieved from the YouTube API. Because MongoDB can easily handle unstructured and semi-structured data, it's a perfect option for a data lake. Engaging with the MongoDB database via the "pymongo" Python driver.
Preprocessing Data: Before saving the data in the SQL Database, it is first preprocessed using Pandas. Prior to showing the results of the SQL database query on the streamlit user interface, Pandas was also utilized to preprocess the data.
Transfer data to a SQL data warehouse: MongoDB can be used to transfer collected data for various channels to a SQL data warehouse. This instance uses "MySQL" as the SQL database. utilizing the "pymysql" Python SQL package to communicate with the SQL database
Query the SQL data warehouse: Using SQL queries to join the tables in the SQL data warehouse and retrieve data for solving some general questions.
