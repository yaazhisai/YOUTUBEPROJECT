import os
import googleapiclient.discovery
import googleapiclient.errors
from pprint import pprint
import streamlit as st
import pymongo
import json
from pymongo import MongoClient, InsertOne
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import mysql.connector
import time as t



class project: 
    # def __init__(self):
    #connection
    api_key='AIzaSyCujiyPxAVYK_ctAQOihWfNI6bNhsH6Fso'
    api_service_name = "youtube"
    api_version = "v3"
    youtube = googleapiclient.discovery.build(
            api_service_name, api_version,developerKey=api_key)
    
    def get_channel_info(self, channelid):
        data={}
        
        request = self.youtube.channels().list(
            part="snippet,contentDetails,statistics,status,topicDetails",
            id=channelid
        )
        
        response = request.execute()
        #pprint(response)

        data['Channel_Name']={"channel_name":response['items'][0]["snippet"]["title"],
            "channel_id":response['items'][0]['id'],
            "subscription_count":response['items'][0]['statistics']['subscriberCount'],
            "channel_view":response['items'][0]['statistics']['viewCount'],
            "channel_info":response['items'][0]["snippet"]['description'],
            "playlist_id":response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
            "channel_type":response['items'][0]['kind'],
            "channel_status":response['items'][0]['status']['privacyStatus'],
            "video_count":response['items'][0]['statistics']['videoCount']
        }
        return data


    def get_playlist_info(self, channelid):
        request = self.youtube.channels().list(
        part="snippet,contentDetails",
        id=channelid,
        )
        response = request.execute()
        playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

        request1 = self.youtube.playlistItems().list(
            part="snippet,contentDetails,status",
            playlistId=playlist_id,
            maxResults=50

                )
        response1=request1.execute()
        #pprint(response1)
        data={
        "playlistid":playlist_id,
        "channel_id":response['items'][0]['id'],
        "playlist_name":response1['items'][0]['snippet']['title']
        }
        return data


    def get_video_info(self, channelid):
        emp_s=""
        Video_list=[]
        videodic={}
        counter=0
        request = self.youtube.channels().list(
        part="snippet,contentDetails",
        id=channelid,
        )
        response = request.execute()
        playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        next_page_token=None

        while True:
            emp_s=""
            request1 = self.youtube.playlistItems().list(
                part="snippet",
                playlistId=playlist_id,
                maxResults=50,pageToken=next_page_token

                    )
            response1=request1.execute()
            for i in response1['items']:
                Video_Id=i['snippet']['resourceId']['videoId']
                Video_list.append(Video_Id)
                if emp_s!="":
                    emp_s=emp_s+","+Video_Id
                else:
                    emp_s=emp_s+Video_Id
                next_page_token=response1.get('nextPageToken')

                request2=self.youtube.videos().list(
                part="snippet,statistics,contentDetails",
                id=emp_s
                )
                response2=request2.execute()

            for i in response2['items']:
                videodic[f'Video_Id_{counter+1}']={
                    "Video_Id":i['id'],
                        #"playlist_id":response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
                    "Video_Name":i['snippet']['title'],
                    "Video_Description":i['snippet']['description'],  
                    "Tags":i['snippet'].get('tags'),
                    "PublishedAt":i['snippet']['publishedAt'],
                    "View_Count":i['statistics']['viewCount'],
                    "Like_Count":i['statistics'].get('likeCount'),
                    "Favorite_Count":i['statistics']['favoriteCount'],
                    "Comment_Count":i['statistics'].get('commentCount'),
                    "Duration":self.get_duration_info(i['contentDetails']['duration']),
                    "Thumbnail":i['snippet']['thumbnails']['default']['url'],
                    "Caption_Status":i['contentDetails']['caption'],
                    "Comments":self.get_comments_info(i['id'])

                }
                counter += 1

            if next_page_token is None:
                break

        return videodic
    

    def get_duration_info(self,duration):
        H=0
        M=0
        S=0
        duration=duration[2:]
        for idx,i in enumerate(duration):
            if i=='H':
                H=duration[idx-1]
            elif i=='M':
                M=duration[idx-1]
            elif i=='S':
                S=duration[idx-1]
        #print(H,M,S)
        res=int(H)*3600+int(M)*60+int(S)
        #print(res)
        return res


    def get_comments_info(self, videoid):
        comments={}

        request= self.youtube.commentThreads().list(
                part="snippet",
                videoId=videoid,
                maxResults=100,
                textFormat="plainText"
            )
        try:
            response = request.execute()
            #pprint(response)

            for idx, i in enumerate(response['items']):
                comments[f'Comment_Id_{idx+1}']={
                    "Comment_Id":i.get('id'),
                    "Comment_Text": i['snippet']['topLevelComment']['snippet']['textDisplay'],
                    "Comment_Author": i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    "Comment_PublishedAt":i['snippet']['topLevelComment']['snippet']['publishedAt']
                }
            # pprint(comments)
        except:
            pass

        return comments

def final(channel_id):
    pr = project()
    channel=pr.get_channel_info(channel_id)
    playlist_response=pr.get_playlist_info(channel_id)
    video_response=pr.get_video_info(channel_id)
    # channel.update(playlist_response)
    # channel.update(video_response)
    final_output = {'Channel': channel,
                    'Playlist': playlist_response,
                    'Videos': video_response}
    return final_output
    

def create_dump(chn_out,filename):
   with open(filename,'w') as js:
         json.dump(chn_out, js)

def update_mongo(doc_name):
    uri = "mongodb+srv://yaazhisai:yaazhguvi@cluster0.d8lqkub.mongodb.net/?retryWrites=true&w=majority"

    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))
    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")        
        db=client.youtube1
        record=db.youtubedata
        # Write the json data to MongoDB
        with open(doc_name) as f:
            add=[]
            for i in f:
                mydict=json.loads(i)
                add.append(InsertOne(mydict))
            record.bulk_write(add)
    
    except Exception as e:
        print(e)
    
    finally:
        client.close()


# SQL CONNECTION 
         
def sql_connection():
    mydb=mysql.connector.connect(
            host='localhost',
            user="root",
            password=""
    )
    mycursor=mydb.cursor(buffered=True)

    mycursor.execute("CREATE DATABASE ss3")
    mycursor.execute("USE ss3")
    mycursor.execute("CREATE TABLE Channel(channel_id VARCHAR(255) PRIMARY KEY,channel_name VARCHAR(255),channel_type VARCHAR(255),channel_views INT,channel_description TEXT,channel_status VARCHAR(255))")
    mycursor.execute("CREATE TABLE Playlist(playlist_id VARCHAR(255) PRIMARY KEY,channelid VARCHAR(255),FOREIGN KEY(channelid)REFERENCES Channel(channel_id),playlist_name VARCHAR(255))")
    mycursor.execute("CREATE TABLE Video(video_id VARCHAR(255) PRIMARY KEY,playlistid VARCHAR(255),FOREIGN KEY(playlistid)REFERENCES Playlist(playlist_id),video_name VARCHAR(255),\
                   video_description TEXT,published_date DATETIME,view_count INT,like_count INT,favourite_count INT,comment_count INT,duration INT,thumbnail VARCHAR(255),caption_status VARCHAR(255))")
    mycursor.execute("CREATE TABLE Comment (comment_id VARCHAR(255) PRIMARY KEY,videoid VARCHAR(255),FOREIGN KEY(videoid)REFERENCES Video(video_id),comment_text TEXT,comment_author VARCHAR(255),comment_published_date DATETIME)")
    
    #print("DATABASE AND TABLE CREATED SUCCESSFULLY")

def table_insert(doc_name):
    mydb=mysql.connector.connect(
        host='localhost',
        user="root",
        password=""
)
    mycursor=mydb.cursor(buffered=True)

    mycursor.execute("USE ss3")



    mydict={}
    with open(doc_name) as f:
        add=[]
        for i in f:
            mydict=json.loads(i)
        

    query1="INSERT INTO Channel (channel_id,channel_name,channel_type,channel_views,channel_description,channel_status)\
                VALUES(%s,%s,%s,%s,%s,%s)"
    mycursor.execute(query1,(mydict['Channel']["Channel_Name"]['channel_id'],\
                    mydict['Channel']["Channel_Name"]['channel_name'],\
                    mydict['Channel']["Channel_Name"]['channel_type'],\
                    mydict['Channel']["Channel_Name"]['channel_view'],\
                    mydict['Channel']["Channel_Name"]['channel_info'],\
                    mydict['Channel']["Channel_Name"]['channel_status']))
    mydb.commit()

    query2="INSERT INTO Playlist(playlist_id,channelid,playlist_name)VALUES(%s,%s,%s)"

    mycursor.execute(query2,(mydict['Playlist']["playlistid"],\
                        mydict['Channel']["Channel_Name"]["channel_id"],\
                        mydict['Playlist']["playlist_name"]))

    mydb.commit()


    query3="INSERT INTO Video(video_id,playlistid,video_name,video_description,published_date,view_count,like_count,favourite_count,comment_count,duration,thumbnail,caption_status)VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    query4="INSERT INTO Comment(comment_id,videoid,comment_text,comment_author,comment_published_date)VALUES(%s,%s,%s,%s,%s)"


    vc=int(mydict['Channel']['Channel_Name']['video_count'])

    for i in range(vc):
        mycursor.execute(query3,(mydict['Videos'][f'Video_Id_{i+1}']['Video_Id'],\
                        mydict['Playlist']['playlistid'],\
                        mydict['Videos'][f'Video_Id_{i+1}']['Video_Name'],\
                        mydict['Videos'][f'Video_Id_{i+1}']['Video_Description'],\
                        mydict['Videos'][f'Video_Id_{i+1}']['PublishedAt'],\
                        mydict['Videos'][f'Video_Id_{i+1}']['View_Count'],\
                        mydict['Videos'][f'Video_Id_{i+1}']['Like_Count'],\
                        mydict['Videos'][f'Video_Id_{i+1}']['Favorite_Count'],\
                        mydict['Videos'][f'Video_Id_{i+1}']['Comment_Count'],\
                        mydict['Videos'][f'Video_Id_{i+1}']['Duration'],\
                        mydict['Videos'][f'Video_Id_{i+1}']['Thumbnail'],\
                        mydict['Videos'][f'Video_Id_{i+1}']['Caption_Status']))
        mydb.commit()

        # Ignore errors in case there are no comments for videos
        try:
            for j in range(len(mydict['Videos'][f'Video_Id_{i+1}']['Comments'])):
                mycursor.execute(query4,(mydict['Videos'][f'Video_Id_{i+1}']['Comments'][f'Comment_Id_{j+1}']['Comment_Id'],\
                                    mydict['Videos'][f'Video_Id_{i+1}']['Video_Id'],\
                                    mydict['Videos'][f'Video_Id_{i+1}']['Comments'][f'Comment_Id_{j+1}']['Comment_Text'],\
                                    mydict['Videos'][f'Video_Id_{i+1}']['Comments'][f'Comment_Id_{j+1}']['Comment_Author'],\
                                    mydict['Videos'][f'Video_Id_{i+1}']['Comments'][f'Comment_Id_{j+1}']['Comment_PublishedAt']))

                mydb.commit()
        except:
            pass


st.title(":blue[GUVI PROJECT1:YOUTUBE DATA HARVESTING]")
st.sidebar.header(":blue[YOUTUBE CHANNEL ID]")
c=st.sidebar.text_input("ENTER THE CHANNEL ID:")
chan_b=st.sidebar.button("CLICK TO VIEW CHANNEL DETAILS")
mongo_b=st.sidebar.button("UPLOAD TO MONGO")
sql_b=st.sidebar.button("MOVE TO SQL DB:")
if c !="":
    out=final(c)
    x=out['Channel']['Channel_Name']['channel_id']
    filename=x+".json"
    print(filename)

    if chan_b:
        try:
            with st.spinner("loading"):
                t.sleep(15)
            st.json(out['Channel'])
            st.snow()
        except:
            st.error("UNABLE TO FETCH DETAILS FROM YOUTUBE")

    if mongo_b :
        create_dump(out,filename)
        try:
            update_mongo(filename)
            st.success("MONGODB UPDATED SUCCESSFULLY")
            st.snow()
        except:
            st.error("MONGODB UPLOAD FAILED")                   

                    
    if sql_b:
            table_insert(filename)
            st.success("DATA UPLOADED TO SQL SUCESSFULLY")
            #st.snow()
            #st.error("DATA UPLOAD TO DB FAILED")
            
       
query_list=["                                                          ",
'1.What are the names of all the videos and their corresponding channels?',
'2.Which channels have the most number of videos, and how many videos do they have?',
'3.What are the top 10 most viewed videos and their respective channels?',
'4.How many comments were made on each video, and what are their corresponding video names?',
'5.Which videos have the highest number of likes, and what are their corresponding channel names?',
'6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
'7.What is the total number of views for each channel, and what are their corresponding channel names?',
'8.What are the names of all the channels that have published videos in the year 2022?',
'9.What is the average duration of all videos in each channel, and what are their corresponding channel names?',
'10.Which videos have the highest number of comments, and what are their corresponding channel names?']


mydb=mysql.connector.connect(
        host='localhost',
        user="root",
        password=""
)
mycursor=mydb.cursor(buffered=True)
mycursor.execute("USE ss3")
with st.container():
    option=st.selectbox('SELECT THE QUERY',query_list)
    if option=='1.What are the names of all the videos and their corresponding channels?':
        #mycursor.execute("select Channel.channel_name,Channel.channel_id,Video.video_name from Channel JOIN Video ON Channel.channel_id=Video.channel_id")
        mycursor.execute("select Channel.channel_name,Video.video_name from Playlist JOIN Video on Playlist.playlist_id = Video.Playlistid JOIN Channel ON Channel.channel_id = Playlist.channelid") 
        out=mycursor.fetchall()
        st.table(out)
    elif option=='2.Which channels have the most number of videos, and how many videos do they have?':
        mycursor.execute("select channel_name,Channel.channel_id,Channel.video_count from Channel ORDER BY video_count DESC")
        out=mycursor.fetchall()
        st.table(out)
    elif option=='3.What are the top 10 most viewed videos and their respective channels?':
        mycursor.execute("select Channel.channel_name,Video.video_id,Video.view_count from Playlist JOIN Video ON Playlist.playlist_id=Video.playlistid JOIN Channel ON Channel.channel_id=Playlist.channelid  ORDER BY view_count DESC LIMIT 10")
        out=mycursor.fetchall()
        st.table(out)
    elif option=='4.How many comments were made on each video, and what are their corresponding video names?':
        mycursor.execute("select video_name,comment_count from Video")
        out=mycursor.fetchall()
        st.table(out)
    elif option=='5.Which videos have the highest number of likes, and what are their corresponding channel names?':
        mycursor.execute("select Channel.channel_name,Video.like_count from Channel JOIN Video ON Channel.channel_id=Video.channel_id ORDER BY Video.like_count DESC")
        out=mycursor.fetchall()
        st.table(out)
    elif option=='6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
         mycursor.execute("select video_name,like_count from Video")
         out=mycursor.fetchall()
         st.table(out)
    elif option=='7.What is the total number of views for each channel, and what are their corresponding channel names?':
        mycursor.execute("select Channel.channel_name,Channel.channel_views from Channel Group by Channel.channel_name")
        out=mycursor.fetchall()
        st.table(out)
    elif option=='8.What are the names of all the channels that have published videos in the year 2022?':
        mycursor.execute("select Channel.channel_name,Video.published_date from Playlist JOIN Video ON Playlist.playlist_id=Video.playlistid JOIN Channel ON Channel.channel_id=Playlist.channelid where substr(Video.published_date,1,4) = '2022'")
        out=mycursor.fetchall()
        st.table(out)
    elif option=='9.What is the average duration of all videos in each channel, and what are their corresponding channel names?':
         mycursor.execute("select Channel.channel_name,AVG(Video.duration) from Video join Playlist on Playlist.playlist_id = Video.Playlistid JOIN Channel ON Channel.channel_id = Playlist.channelid GROUP BY Channel.channel_name")
         out=mycursor.fetchall()
         st.table(out)

    elif option=='10.Which videos have the highest number of comments, and what are their corresponding channel names?':
        mycursor.execute("select Channel.channel_name,MAX(Video.comment_count) from Playlist JOIN Video ON Playlist.playlist_id= Video.playlistid JOIN Channel ON Channel.channel_id=Playlist.channelid GROUP BY Channel.channel_name")
        out=mycursor.fetchall()
        st.table(out)


         


    

        
