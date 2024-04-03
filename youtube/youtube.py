from googleapiclient.discovery import build
import pymongo
import psycopg2
import certifi
import pandas as pd
import streamlit as st 

ca = certifi.where()

client=pymongo.MongoClient("mongodb+srv://saran75400:saran@cluster0.dbr4b2s.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",tlsCAFile=ca)
db=client["youtube_data"]


def Api_connect():
    Api_id = "AIzaSyAsZ0mb6ovbfv_OaEKd6xLqnTOxbQK2sFw"

    Api_service_name = "youtube" 
    Api_version = "v3"

    youtube = build(Api_service_name,Api_version,developerKey=Api_id)

    return youtube

youtube = Api_connect()


#channel data extraction
def get_channel_info(channel_id):
    request = youtube.channels().list(
                  part = "snippet,ContentDetails,statistics",
                  id = channel_id
    )
    response = request.execute()

    for i in response['items']:
       data = dict(Channel_Name =i['snippet']['title'],
                   Channel_id =i['id'],
                   Subscribers =i['statistics']['subscriberCount'],
                   Views =i['statistics']['viewCount'],
                   Total_Videos =i['statistics']['videoCount'],
                   Channel_Descripition =i['snippet']['description'],
                   Playlist_id =i['contentDetails']['relatedPlaylists']['uploads'])
       
       return data
    


# video Extraction
def get_videos_ids(channel_id):
    video_ids=[]
    response = youtube.channels().list(id =channel_id,
                                  part = 'contentDetails').execute()
    Playlist_Id =response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:     
       response1=youtube.playlistItems().list(
                                  part ='snippet',
                                  playlistId =Playlist_Id,
                                  maxResults=50,
                                  pageToken=next_page_token).execute()
       for i in range(len(response1['items'])):
          video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
       next_page_token=response1.get('nextPageToken')

       if next_page_token is None:
         break
    return video_ids

#video details 
def get_video_info(video_ids):
    video_data=[]   
    for video_id in video_ids:
        request=youtube.videos().list(
           part = 'snippet,ContentDetails,statistics',
           id=video_id
        )
        
        response=request.execute()
        for items in response ['items']:
            data =dict(Channel_Name= items ['snippet']['channelTitle'],
                   Channel_Id =items ['snippet']['channelId'],
                   Video_id = items['id'],
                   Title= items['snippet']['title'],
                   Tags= items.get('tags'),
                   Thumbnail= items['snippet']['thumbnails'],
                   Descripition=items.get('description'),
                   Published_Dates= items['snippet']['publishedAt'],
                   Duration= items['contentDetails']['duration'],
                   Views= items.get('viewCount'),
                   comments= items.get('commentCount'),
                   Favorite_Count=items['statistics']['favoriteCount'],
                   Definition= items['contentDetails']['definition'],
                   Caption_status=items['contentDetails']['caption'],
                   )
            video_data.append( data )
        return video_data
    

#get playlist details
def get_playlist_details(channel_id):
        next_page_token=None
        Data=[]
        while True:
                request = youtube.playlists().list(
                        part='snippet,contentDetails',
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=next_page_token
                )
                response=request.execute()

                for item in response['items']:
                        data=dict(Playlist_id=item['id'],
                                Title=item['snippet']['title'],
                                Channel_Id=item['snippet']['channelId'],
                                Channel_name=item['snippet']['channelTitle'],
                                PublishedAt=item['snippet']['publishedAt'],
                                Video_count=item['contentDetails']['itemCount'])
                        
                        Data.append(data)

                next_page_token=response.get('nextPageToken') 
                if next_page_token is None:
                        break 

                
        return Data



#command details
def get_comment_info(video_ids):
    Comment_data=[]

    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        video_Id =item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                Comment_data.append(data)
            
    except:
        pass
    return Comment_data
        

def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)

    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                      "video_information":vi_details,"comment_information":com_details})
    return "upload completed successfully"  


#table creation 

def channels_table():
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="saran",
                        database="youtube_data",
                        port="5432")
    cursor=mydb.cursor()


    drop_query='''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                        Channel_id varchar(80) primary key,
                                                        Subscribers bigint,
                                                        Views bigint,
                                                        Total_Videos int,
                                                        Channel_Descripition text,
                                                        Playlist_id varchar(80))'''
        
        cursor.execute(create_query)
        mydb.commit()

    except:
        print("channels table already created")


    ch_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list) 



    for index,row in df.iterrows():
        insert_query='''insert into channels(Channel_Name,     
                                            Channel_id ,
                                            Subscribers,
                                            Views,
                                            Total_Videos,
                                            Channel_Descripition,
                                            Playlist_id)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        
        values=(row['Channel_Name'],
                row['Channel_id'],
                row['Subscribers'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_Descripition'],
                row['Playlist_id'])
        
        try:
            cursor.execute(insert_query,values)
            mydb.commit()

        except:
            print("channels values are inserted")




def playlist_tables():
    mydb=psycopg2.connect(host="localhost",
                            user="postgres",
                            password="saran",
                            database="youtube_data",
                            port="5432")
    cursor=mydb.cursor()


    drop_query='''drop table if exists playlists'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query='''create table if not exists playlists(Playlist_id varchar(100),
                                                            Title varchar(100) primary key,
                                                            Channel_Id varchar(100),
                                                            Channel_name varchar(100),
                                                            PublishedAt timestamp,
                                                            Video_count int
                                                            )'''

            
    cursor.execute(create_query)
    mydb.commit()

    
def videos_tables():
    mydb=psycopg2.connect(host="localhost",
                                user="postgres",
                                password="saran",
                                database="youtube_data",
                                port="5432")
    cursor=mydb.cursor()


    drop_query='''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query = '''create table if not exists videos(Channel_name varchar(100),
                                                    Channel_Id varchar(100),
                                                    Video_Id varchar(30) primary key,
                                                    Title varchar(150),
                                                    Tags text,
                                                    Thumbnail varchar(150),
                                                    Description text,
                                                    Published_Data timestamp,
                                                    Duration interval,
                                                    Views bigint,
                                                    Likes bigint,
                                                    Comments int,
                                                    Favorite_Count int,
                                                    Definition varchar(10),
                                                    Caption_Status varchar(50) 
                                                        )'''

    cursor.execute(create_query)
    mydb.commit()


    vi_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
            for i in range(len(vi_data["video_information"])):
                vi_list.append(vi_data["video_information"][i])

    df2=pd.DataFrame(vi_list) 





    for index,row in df2.iterrows():
            insert_query='''insert into channels(Channel_name ,
                                                Channel_Id,
                                                Video_Id,
                                                Title,
                                                Tags,
                                                Thumbnail,
                                                Description,
                                                Published_Data,
                                                Duration,
                                                Views,
                                                Likes,
                                                Comments,
                                                Favorite_Count,
                                                Definition,
                                                Caption_Status 
                                                )
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

                




            values=(row['Channel_Name'],
                    row['Channel_Id'],
                    row['Video_id'],
                    row['Title'],
                    row['Tags'],
                    row['Thumbnail'],
                    row['Descripition'],
                    row['Published_Dates'],
                    row['Duration'],
                    row['Views'],
                    row['comments'],
                    row['Favorite_Count'],
                    row['Definition'],
                    row['Caption_status']
                    )
            
            
            try:
                cursor.execute(insert_query,values)
                mydb.commit()

            except:
                print("channels values are inserted")
            
            


def comments_table():
    mydb=psycopg2.connect(host="localhost",
                                user="postgres",
                                password="saran",
                                database="youtube_data",
                                port="5432")
    cursor=mydb.cursor()


    drop_query='''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query='''create table if not exists comments(Comment_Id varchar(100),
                                                        video_Id varchar(50), 
                                                        Comment_Text text,
                                                        Comment_Author varchar(150),
                                                        Comment_Published timestamp
                                                                )'''

                
    cursor.execute(create_query)
    mydb.commit()


    com_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
                for i in range(len(com_data["comment_information"])):
                    com_list.append(com_data["comment_information"][i])

    df4=pd.DataFrame(com_list) 



    for index,row in df4.iterrows():
                insert_query='''insert into comments(Comment_Id,
                                                    video_Id, 
                                                    Comment_Text,
                                                    Comment_Author,
                                                    Comment_Published 
                                            )  
                                                    
                                                    values(%s,%s,%s,%s,%s)'''

                    

        

                values=(row['Comment_Id'],
                        row['video_Id'],
                        row['Comment_Text'],
                        row['Comment_Author'],
                        row['Comment_Published']
                        
                        )
                
                
                
    cursor.execute(insert_query,values)
    mydb.commit()



def tables():
    channels_table()
    playlist_tables()
    videos_tables()
    comments_table() 

    return "Tables Created successfully"      



def show_channels_table():
    ch_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
                        for i in range(len(ch_data["channel_information"])):
                            ch_list.append(ch_data["channel_information"][i])

    df=st.dataframe(ch_list) 

    return df



def show_playlist_table():
    pl_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
                    for i in range(len(pl_data["playlist_information"])):
                        pl_list.append(pl_data["playlist_information"][i])

    df3=st.dataframe(pl_list) 

    return df3



def show_videos_table():
        vi_list=[]
        db=client["youtube_data"]
        coll1=db["channel_details"]
        for vi_data in coll1.find({},{"_id":0,"video_information":1}):
                    for i in range(len(vi_data["video_information"])):
                        vi_list.append(vi_data["video_information"][i])

        df2=st.dataframe(vi_list) 

        return df2


def show_comment_table():
    com_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
                    for i in range(len(com_data["comment_information"])):
                        com_list.append(com_data["comment_information"][i])

    df4=st.dataframe(com_list) 

    return df4



#streamlit

with st.sidebar:
    st.title(":blue[YOUTUBE DATA HARVESTING AND WARHOUSEING]")
    st.header("skill take away")
    st.caption("python scriptting")
    st.caption("data collection")
    st.caption("mongodb")
    st.caption("API integration")
    st.caption("data management using mongodb and Sql")

channel_id=st.text_input("enter the channel ID")    

if st.button("collect and store data"):
    ch_ids=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
       ch_ids.append(ch_data["channel_information"]["Channel_id"])


    if channel_id in ch_ids:
        st.success("channel details of given channel id already exists")

    else:
        insert=channel_details(channel_id)
        st.success(insert)

if st.button("migrate to Sql"):
    Table=tables()
    st.success(Table)  

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLIST","VIDEOS","COMMENTS"))  

if show_table=="CHANNELS":
    show_channels_table()

elif show_table=="PLAYLIST":
    show_playlist_table()  

elif show_table=="VIDEOS":
    show_videos_table() 

elif show_table=="COMMENTS":
    show_comment_table()    



#Sql connection
mydb=psycopg2.connect(host="localhost",
                                user="postgres",
                                password="saran",
                                database="youtube_data",
                                port="5432")
cursor=mydb.cursor()


question=st.selectbox("select your questions",("1. All the videos and the channelname",
                                               "2. channels with most number of videos",
                                               "3. 10 most viewed videos",
                                               "4. comments in each videos",
                                               "5. videos with higest likes",
                                               "6. likes and dislike of all videos",
                                               "7. views of each channel",
                                               "8. videos published in the year of 2022",
                                               "9. average duration of all videos in each channel",
                                               "10. videos with highest number of comments"))


if question=="1.All the videos and the channelname":
    query1='''select title as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)


elif question=="1.channels with most number of videos":
    query2='''select channel_name as channelname,total_videos as no_videos from channels
            order by total_videos desc'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df=pd.DataFrame(t2,columns=["channel name","no of videos"])
    st.write(df)


elif question=="3. 10 most viewed videos":
    query3='''select views as views,channel_name as channelname,title as videotitle from videos
            where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channelname","videotitle"])
    st.write(df3)


elif question=="4. comments in each videos":
    query4='''select comments as no_comments,title as videotitle from videos where comment is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["no of comments","vieotitle"])
    st.write(df4)


elif question=="5. videos with higest likes":
    query5='''select title as video title,channel_names as channelname,like as likecount
                from videos where like is not null order by likes desc'''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["video title","channel name"])
    st.write(df5)

elif question=="6. likes and dislike of all videos":
    query6='''select likes as likecount,tile as videotitle from videos'''
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likecount","videotitle"])
    st.write(df6)

elif question=="7. views of each channel":
    query7='''select channel_name as channelname,views as totalviews from channels'''
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channelname","totalviews"])
    st.write(df7)

elif question=="8. videos published in the year of 2022":
    query8='''select title as video_title,published_date as videorelease,channel_name as channelname from videos
                where extract(year from published_date)=2022'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["",""])
    st.write(df8)

elif question=="9. average duration of all videos in each channel":
    query9='''select channel_name as channelname,AVG(duration) as averagedurationfrom videos group by channel_name'''
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname","averageduration"])
    st.write(df9)

    T9=[]
    for index,row in df9.iterrows():
        channel_title=row['channelname']
        average_duration=row['averageduration']
        average_duration_str=str(average_duration)
        T9.append(dict(channeltile=channel_title,avgduration=average_duration_str))
    df1=pd.DataFrame(T9)

elif question=="10. videos with highest number of comments":
    query10='''select title as videotitle,channel_name as channelname,comments as cmments from videos where comments is
                not null order by comments desc'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["videotitle","channel name","comments"])
    st.write(df10)

      

