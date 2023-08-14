import streamlit as st
from googleapiclient.discovery import build
import pandas as pd
import numpy as np
from io import BytesIO
import pymongo as pg
import pymysql
from sqlalchemy import create_engine
from datetime import datetime
import time
import isodate
pymysql.install_as_MySQLdb()
from streamlit_option_menu import option_menu
from PIL import Image
import requests
# SET PAGE CONFIGURATION
st.set_page_config(
    page_title= " YouTube Data Harvesting and Warehousing | By Nirosh Kumar",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={'About': """# This app is created by Nirosh Kumar!"""})

# CREATING OPTION MENU
with st.sidebar:
    selected = option_menu(None, ["Home","Extract and Analysis","View Insights"], 
                icons=["house-door-fill","tools","card-text"],
                default_index=0,
                orientation="vertical",
                styles={"nav-link": {"font-size": "30px", "text-align": "center", "margin": "0px", 
                                    "--hover-color": "#C80101"},
                        "icon": {"font-size": "30px"},
                        "container" : {"max-width": "6000px"},
                        "nav-link-selected": {"background-color": "#87CEEB"}})  # Change background color here
# BUILDING CONNECTION WITH YOUTUBE API
api_key = 'AIzaSyAd8Kc09Vnzkpdj355v_6j71OG6w_tL5aI' 
youtube = build('youtube','v3',developerKey= api_key)
# FUNCTION  TO  GET CHANNEL DETAILS
@st.cache_data
def get_channel_stats(channel_id):
    channel_table = []
    response = youtube.channels().list(part = 'snippet,contentDetails,statistics',id= channel_id).execute()
    
    for item in response['items']:
        data = dict(
                    profile_image=item['snippet']['thumbnails']['default']['url'],
                    channel_id = item['id'],
                    channel_name = item['snippet']['title'],
                    channel_subscribers= item['statistics']['subscriberCount'],
                    channel_views = item['statistics']['viewCount'],
                    total_videos = item['statistics']['videoCount'],
                    playlist_id = item['contentDetails']['relatedPlaylists']['uploads'],
                    channel_description = item['snippet']["description"])
        channel_table.append(data)
    
    return channel_table



# FUNCTION  TO  GET  PLAYLIST_ID

@st.cache_data
def get_playlist_data(df):
    playlist_ids = []
     
    for i in df["playlist_id"]:
        playlist_ids.append(i)

    return playlist_ids

# FUNCTION  TO  GET  VIDEO_IDS

@st.cache_data
def get_video_ids(playlist_id):
    video_id = []

    for i in playlist_id:
        next_page_token = None
        more_pages = True

        while more_pages:
            request = youtube.playlistItems().list(
                        part = 'contentDetails',
                        playlistId = i,
                        maxResults = 50,
                        pageToken = next_page_token)
            response = request.execute()
            
            for j in response["items"]:
                video_id.append(j["contentDetails"]["videoId"])
        
            next_page_token = response.get("nextPageToken")
            if next_page_token is None:
                more_pages = False
    return video_id

# FUNCTION  TO  GET  VIDEO DETAILS

@st.cache_data
def get_video_details(video_id):

    all_video_stats = []
   

    for i in range(0,len(video_id),50):
        
        request = youtube.videos().list(
                  part="snippet,contentDetails,statistics",
                  id = ",".join(video_id[i:i+50]))
        response = request.execute()
        
        
        for video in response["items"]:
            published_dates = video["snippet"]["publishedAt"]
            parsed_dates = datetime.strptime(published_dates,'%Y-%m-%dT%H:%M:%SZ')
            format_date = parsed_dates.strftime('%Y-%m-%d')
            duration = video["contentDetails"]["duration"]
            Duration = isodate.parse_duration(duration)
            v_duration = Duration.total_seconds()
            

            videos = dict(video_id = video["id"],
                          channel_name = video['snippet']['channelTitle'],
                          channel_id = video["snippet"]["channelId"],
                          video_name = video["snippet"]["title"],
                          video_duration =v_duration ,
                          video_published_date = format_date ,
                          video_views = video["statistics"].get("viewCount",0),
                          video_likes = video["statistics"].get("likeCount",0),
                          video_comments= video["statistics"].get("commentCount",0))
            all_video_stats.append(videos)

    return (all_video_stats)

#  FUNCTION  TO  GET COMMENT

@st.cache_data
def get_comments(video_id):
    comments_data= []
    try:
        next_page_token = None
        for i in video_id:
            while True:
                request = youtube.commentThreads().list(
                    part = "snippet,replies",
                    videoId = i,
                    textFormat="plainText",
                    maxResults = 100,
                    pageToken=next_page_token)
                response = request.execute()
                

                for item in response["items"]:
                    published_date= item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                    parsed_dates = datetime.strptime(published_date,'%Y-%m-%dT%H:%M:%SZ')
                    format_date = parsed_dates.strftime('%Y-%m-%d')
                    

                    comments = dict(comment_id = item["id"],
                                    video_id = item["snippet"]["videoId"],
                                    comment_text = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                                    comment_author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                    comment_published_date = format_date)
                    comments_data.append(comments) 
                
                next_page_token = response.get('nextPageToken')
                if next_page_token is None:
                    break       
    except Exception as e:
        print("An error occured",str(e))          
            
    return comments_data

# USER  INPUT
channel_id = []


#HOME

if selected == "Home":
# Display YouTube logo image
    youtube_logo_url = "/Users/nirosh/Downloads/youtube_logo.png"  
    col1,col2 ,col3,col4,col5,col6,col7,col8 = st.columns(8,gap= 'large')
    col1.image( youtube_logo_url, width=100,) # Adjust the width as needed
    col2.title(" :red[YouTube]")
    col3.markdown(" ")
    col4.markdown(" ")
    col5.markdown(" ")
    col6.markdown(" ")
    col7.markdown(" ")
    col8.markdown(" ")
    
    st.title(" :blue[YouTube Data Harvesting and Warehousing]")
    st.write("Welcome to the YouTube Channel Analysis dashboard!")
    st.write("Use the sidebar to navigate to different sections.")
    st.markdown("#### :green[Domain] : Social Media")
    st.markdown("#### :green[Technologies used] : Python,MongoDB, Youtube Data API, MySql, Streamlit")
    st.markdown("#### :green[Overview] : Retrieving the Youtube channels data from the Google API, storing it in a MongoDB as data lake, migrating and transforming data into a SQL database,then querying the data and displaying it in the Streamlit app.")
    

# EXTRACT AND ANALYSIS

if selected == "Extract and Analysis":
    analysis_logo_url= "/Users/nirosh/Downloads/Analysis_logo.png"
    st.image(analysis_logo_url, width=100,) 
    st.header("Channel Analysis")
    st.write("Perform various analyses on your YouTube channel here.")

    channel_id = st.text_input("Enter Channel IDs  ").split(',')    
# CONNECTION WITH MONGODB       
    mongodb_local = pg.MongoClient("mongodb://localhost:27017")
    youtube_db = mongodb_local["youtube"]    

    channel_coll = youtube_db['channel_data']
    video_coll = youtube_db['video_data']
    comment_coll = youtube_db['comments_data']

    if channel_id and st.button("Extract Data"):  
      EX_details = get_channel_stats(channel_id)
      ex_res = pd.DataFrame(EX_details)
      
      for index, row in ex_res.iterrows():
        st.image(row['profile_image'],width=100, caption=f"Profile Picture - {row['channel_name']}" )
        st.write(f"Channel Name: {row['channel_name']}")
        st.write(f"Channel Subscribers: {row['channel_subscribers']}")
        st.write(f"Channel Views: {row['channel_views']}")
        st.write(f"Total Videos: {row['total_videos']}")
        st.write(f"Channel Description: {row['channel_description']}")
        
      
    submit = st.button(" :green[Channel details Upload into MongoDB Database]") 
    if submit:
        if channel_id:
            channel_details = get_channel_stats(channel_id)
            df = pd.DataFrame(channel_details) 
            playlist_id = get_playlist_data(df)
            video_id = get_video_ids(playlist_id)
            video_details = get_video_details(video_id)
            get_comment_data = get_comments(video_id)

            with st.spinner('Please wait for it!! '):
                time.sleep(5)

                if channel_details:
                    channel_coll.insert_many(channel_details)
                if video_details:
                    video_coll.insert_many(video_details)
                if get_comment_data:
                    comment_coll.insert_many(get_comment_data)

            with st.spinner('Wait a little bit!! '):
                time.sleep(5)
                st.success('Done!, Data Successfully Uploaded')  
    def channel_names():   
        ch_name = []
        for i in youtube_db.channel_data.find():
            ch_name.append(i['channel_name'])
        return ch_name

    st.subheader(":blue[ Migrate  Data  into MySQL ]")

    user_input =st.multiselect("Select the channel to be inserted into MySQL Tables",options = channel_names())

    Transfer = st.button(" Transfer the data into MySQL")

    #Fetch the channel_details

    if Transfer:
        
        with st.spinner('Please wait a bit '):
            
            def get_channel_details(user_input):
                query = {"channel_name":{"$in":list(user_input)}}
                projection = {"_id":0,"channel_id":1,"channel_name":1,"channel_views":1,"channel_subscribers":1,"total_videos":1,"playlist_id":1}
                x = channel_coll.find(query,projection)
                channel_table = pd.DataFrame(list(x))
                return channel_table

            channel_data = get_channel_details(user_input)
            
            #Fetch the Video details:
            
            def get_video_details(channel_list):
                query = {"channel_id":{"$in":channel_list}}
                projection = {"_id":0,"video_id":1,"channel_id":1,"channel_name":1,"video_name":1,"video_published_date":1,"video_views":1,"video_likes":1,"video_comments":1,"video_duration":1}
                x = video_coll.find(query,projection)
                video_table = pd.DataFrame(list(x))
                return video_table
            video_data = get_video_details(channel_id)

            #Fetch the comment_details
            def get_comment_details(video_ids):
                query = {"video_id":{"$in":video_ids}}
                projection = {"_id":0,"comment_id":1,"video_id":1,"comment_text":1,"comment_author":1,"comment_published_date":1}
                x = comment_coll.find(query,projection)
                comment_table = pd.DataFrame(list(x))
                return comment_table
            
            #Fetch video_ids from mongoDb

            video_ids = video_coll.distinct("video_id")
            
            comment_data = get_comment_details(video_ids)
            
            mongodb_local.close()
            
            # MYSQL CONNECTION

            mydb = pymysql.connect(
                host="localhost",
                port = 3306,
                user="root",
                password="12345678",
                database="youtube_data_warehousing")

            mycursor = mydb.cursor()

            #create an SQLAlchemy engine
            
            engine = create_engine('mysql+pymysql://root:12345678@localhost/youtube_data_warehousing')

            #Inserting Channel data into the table using try and except method

            try:
                #inserting data
                channel_data.to_sql('channel_data', con=engine, if_exists='append', index=False, method='multi')
                print("Data inserted successfully")
            except Exception as e:
                if 'Duplicate entry' in str(e):
                    print("Duplicate data found. Ignoring duplicate entries.")
                else:
                    print("An error occurred:", e)

        
            #Inserting Video data into the table using try and except method

            try:
                video_data.to_sql('video_data', con=engine, if_exists='append', index=False, method='multi')
                print("Data inserted successfully")
            except Exception as e: 
                if 'Duplicate entry' in str(e):
                    print("Duplicate data found. Ignoring duplicate entries.")
                else:
                    print("An error occurred:", e)
            st.success("Data Uploaded Successfully")
            engine.dispose()
#MYSQL Database connection

mydb = pymysql.connect(
    host="localhost",
    port = 3306,
    user="root",
    password="12345678",
    database="youtube_data_warehousing")

mycursor = mydb.cursor()  
# VIEW  INSIGHTS                  
if selected == "View Insights":
    view_logo_url= "/Users/nirosh/Downloads/view _instics.jpeg"
    st.image(view_logo_url, width=300,) 
    st.header("Insights and Statistics")
    st.write("View in-depth insights and statistics about your channel.")
    questions = st.selectbox("Select any questions given below:",
    ['Click the question that you would like to query',
    '1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])
#store the queries

    if questions == '1. What are the names of all the videos and their corresponding channels?':
        query = "select distinct channel_name as Channel_name , video_name as Video_name from video_data order by cast(video_name as unsigned) asc;"
        table = pd.read_sql(query,mydb)
        table.index += 1
        st.write(table)
        st.toast('Good job',icon="😍")
    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        query = "select distinct channel_name as Channel_name,count(video_name) as Most_Number_of_Videos from video_data group by channel_name order by cast(Most_Number_of_Videos as unsigned) desc;"
        mycursor.execute(query)
        result = mycursor.fetchall()
        table = pd.DataFrame(result, columns=['Channel_name', 'Number_of_Videos']).reset_index(drop=True)
        table.index += 1
        st.dataframe(table)
    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
                    a = pd.read_sql("SELECT channel_Name FROM channel_data", mydb)
                    channels_id = []
                    for i in range(len(a)):
                        channels_id.append(a.loc[i].values[0])

                    ans3 = pd.DataFrame()
                    for each_channel in channels_id:
                        Q3 = f"SELECT * FROM video_data WHERE channel_name='{each_channel}' ORDER BY video_views DESC LIMIT 10"
                        ans3 = pd.concat([ans3, pd.read_sql(Q3, mydb)], ignore_index=False)
                        ans3.index += 1
                    st.write(ans3[['video_name', 'channel_name', 'video_views']])
    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        query = "select distinct channel_name as Channel_name , video_name as Video_name , video_comments as Comments_count from video_data order by cast(channel_name as unsigned) desc;"
        table = pd.read_sql(query,mydb)
        table.index += 1
        st.write(table)
    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        query = "select distinct channel_name as Channel_name,video_name as Video_name,video_likes as Number_of_likes from video_data order by cast(video_likes as unsigned) desc limit 10;"
        table = pd.read_sql(query,mydb)
        table.index += 1
        st.write(table)
    elif questions == '6. What is the total number of likes for each video, and what are their corresponding video names?':
        query = "select distinct video_name as Video_name,video_likes as Like_count from video_data order by cast(Like_count as unsigned) desc;"
        table = pd.read_sql(query,mydb)
        table.index += 1
        st.write(table)
    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        query = " select distinct channel_name as Channel_name , channel_views as total_number_of_views from channel_data order by cast(channel_views as unsigned) desc;"
        table = pd.read_sql(query,mydb)
        table.index += 1
        st.write(table)
    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        query = "select distinct channel_name as Channel_name , year(video_published_date) as published_year from video_data where year(video_published_date) = 2022;"
        table = pd.read_sql(query,mydb)
        table.index += 1
        st.write(table)
    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        query ="SELECT channel_name AS Channel_Name,AVG(video_duration) AS Average_duration FROM video_data GROUP BY channel_name ORDER BY AVG(video_duration) DESC;"
        table = pd.read_sql(query,mydb)
        table.index += 1
        st.write(table)
    elif questions =='10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        query = "select distinct channel_name as Channel_name , video_name as Video_name, video_comments as No_of_comments from video_data order by cast(video_comments as unsigned) desc limit 10;"
        table = pd.read_sql(query,mydb)
        table.index += 1
        st.write(table) 
        st.write('Thank you 😍 Good to see you Again') 

    mycursor.close()
    mydb.close()
