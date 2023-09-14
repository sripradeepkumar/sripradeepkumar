from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
import pymongo
import psycopg2
import re
import streamlit as st
import json

#to connect youtube Api
api_key = 'AIzaSyCzWntQ4djGU1CnKlSZC1NWdwEPDucA8HQ' 
youtube = build('youtube', 'v3', developerKey=api_key)
#to connect MongoDb
conn1=pymongo.MongoClient("mongodb+srv://pradeepsri277:pradeep277@valuecompany.kdhcue0.mongodb.net/?retryWrites=true&w=majority") #mongodb atlas->sign->database->connect->drivers ->Add your connection string into your application code copy the link and paste it here and give userid and password
db = conn1["youtube_api_demo"] #create database
coll = db["youtube_project"] # create collection

#to connect SQL
conn=psycopg2.connect(host="localhost",
                      user="postgres",
                      password="sripradeep",
                      port=5432,
                      database="guvi") #go to sql tool create database use the same name in database and password to connect sql
cursor=conn.cursor()

#this function used to get youtube data
def get_channel_stats(youtube, channel_id):
    all_data = []    
    request = youtube.channels().list(
        part='snippet,contentDetails,statistics',
        id=channel_id)
    response = request.execute()

    for i in response["items"]:
        data = {"Channel_Name": {"Channel_Name": i['snippet'].get('title', None),
                                 "Channel_id":   i.get("id", None),
                                 "Subscription_Count": i['statistics'].get('subscriberCount', None),
                                 "Channel_Views": i['statistics'].get('viewCount', None),
                                 "Channel_Description": i['snippet'].get('description', None),
                                 "playlist_id": i['contentDetails']['relatedPlaylists'].get('uploads', None)}}
        all_data.append(data)
        playlist = data["Channel_Name"].get("playlist_id")

        request = youtube.playlistItems().list(
            part='contentDetails',
            playlistId=playlist,
            maxResults=50)
        response = request.execute()

        a = 1
        for i in response['items']:
            video_ids1 = i['contentDetails']['videoId']
            # for getting video details
            request = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=video_ids1)
            response1 = request.execute()
            try:

                request = youtube.commentThreads().list(
                          part="snippet,replies",
                          videoId=video_ids1
                          )
                response2 = request.execute()
            except HttpError:
                continue
            comments = []

            for comment in range(len(response2["items"])):
                try:
                    Comment_id = response2["items"][comment]["snippet"]["topLevelComment"]["id"],
                    comment_text = response2["items"][comment]["snippet"]["topLevelComment"]["snippet"]["textOriginal"]
                    comment_Author = response2["items"][comment]["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                    Comment_PublishedAt = response2["items"][comment]["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                    
                    data = {f"comments_id_{comment+1}": {"Comment_id": Comment_id[0],
                                                         "comment_text": comment_text, "comment_Author": comment_Author[0],
                                                         "Comment_PublishedAt": Comment_PublishedAt}}
                    comments.append(data)
                except KeyError:
                    continue

            if "items" in response1:
                video_stats = {f"video_id_{a}": dict(Video_Id=response1['items'][0].get("id", None),
                               Video_Name=response1['items'][0]['snippet'].get('title', None),
                               PublishedAt=response1['items'][0]['snippet'].get('publishedAt', None),
                               Video_Description=response1['items'][0]['snippet'].get('description', None),
                               Tags=response1['items'][0]['snippet'].get('tags', None),
                               View_Count=response1['items'][0]['statistics'].get('viewCount', 0),
                               Like_Count=response1['items'][0]['statistics'].get('likeCount', 0),
                               Comment_Count=response1['items'][0]['statistics'].get('commentCount', 0),
                               Favorite_Count=response1['items'][0]['statistics'].get('favoriteCount', 0),
                               Duration=response1['items'][0]['contentDetails'].get('duration', None),
                               Thumbnail=response1['items'][0]['snippet']['thumbnails']['default'].get('url', None),
                               Caption_Status=response1['items'][0]['contentDetails'].get('caption', None),
                               Comments=comments)}
                all_data.append(video_stats)
                next_page_token = response.get("nextPageToken")
                a = a+1
            else:
                continue

            while next_page_token is not None:
                request = youtube.playlistItems().list(
                    part="contentDetails",
                    maxResults=50,
                    playlistId=playlist,
                    pageToken=next_page_token
                )
                response = request.execute()

                # for i in range(len(response['items'])):
                for i in response['items']:
                    video_ids1 = i['contentDetails']['videoId']
                    request = youtube.videos().list(
                        part="snippet,contentDetails,statistics",
                        id=video_ids1)
                    response4 = request.execute()
                    
                    try:

                        request = youtube.commentThreads().list(
                        part="snippet,replies",
                        videoId=video_ids1
                            )
                        response5 = request.execute()
                    except HttpError:
                        continue
                    comments = []

                    for comment in range(len(response5["items"])):
                        try:
                            Comment_id = response5["items"][comment]["snippet"]["topLevelComment"]["id"],
                            comment_text = response5["items"][comment]["snippet"]["topLevelComment"]["snippet"]["textOriginal"]
                            comment_Author = response5["items"][comment]["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                            Comment_PublishedAt = response5["items"][comment]["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                            
                            data = {f"comments_id_{comment+1}": {"Comment_id": Comment_id[0],
                                                                 "comment_text": comment_text, "comment_Author": comment_Author[0],
                                                                 "Comment_PublishedAt": Comment_PublishedAt}}
                            comments.append(data)
                        except KeyError:
                            continue

                    if "items" in response4:
                        video_stats = {f"video_id_{a}": dict(Video_Id=response4['items'][0].get("id", None),
                                                             Video_Name=response4['items'][0]['snippet'].get(
                                                                 'title', None),
                                                             PublishedAt=response4['items'][0]['snippet'].get(
                                                                 'publishedAt', None),
                                                             Video_Description=response4['items'][0]['snippet'].get(
                                                                 'description', None),
                                                             Tags=response4['items'][0]['snippet'].get(
                                                                 'tags', None),
                                                             View_Count=response4['items'][0]['statistics'].get(
                                                                 'viewCount', 0),
                                                             Like_Count=response4['items'][0]['statistics'].get(
                                                                 'likeCount', 0),
                                                             Comment_Count=response4['items'][0]['statistics'].get(
                                                                 'commentCount', 0),
                                                             Favorite_Count=response4['items'][0]['statistics'].get(
                                                                 'favoriteCount', 0),
                                                             Duration=response4['items'][0]['contentDetails'].get(
                                                                 'duration', None),
                                                             Thumbnail=response4['items'][0]['snippet']['thumbnails']['default'].get(
                                                                 'url', None),
                                                             Caption_Status=response4['items'][0]['contentDetails'].get(
                                                                 'caption', None),
                                                             Comments=comments)}
                        all_data.append(video_stats)
                        next_page_token = response.get("nextPageToken")
                        a = a+1
                    else:
                        continue

    list_of_dicts = all_data
    result_dict = {
        key: value for d in list_of_dicts for key, value in d.items()}

    return result_dict

#this function will get data from mongodb then it convert in dataframe then it will move data into sql
def migrate_to_sql(mongodb_imported_data):
    #for getting channel details
    df_channel_details = pd.DataFrame(columns=["channel_id", "channel_name", "subscription_count", "channel_views", "channel_description"])
    for i in mongodb_imported_data:    
        df_channel_details.loc[0] = [i["Channel_Name"]['Channel_id'],
                                     i["Channel_Name"]['Channel_Name'],
                                     i["Channel_Name"]['Subscription_Count'],
                                     i["Channel_Name"]['Channel_Views'],
                                     i["Channel_Name"]['Channel_Description']
                                     ]
    df_channel_table = pd.DataFrame(df_channel_details)
    df_channel_table['subscription_count'] = pd.to_numeric(df_channel_table['subscription_count'])
    df_channel_table['channel_views'] = pd.to_numeric(df_channel_table['channel_views'])
    

    channel_values = df_channel_table.values.tolist()
    insert_into_channel_table = ("insert into channel values (%s, %s, %s, %s, %s)")
    cursor.executemany(insert_into_channel_table, channel_values)
    conn.commit()
    
    #for getting playlist_id
    df_playlist_details = pd.DataFrame(columns = ["Channel_id", " Playlist_id"])

    for i in mongodb_imported_data:    
        df_playlist_details.loc[0] = [i["Channel_Name"]['Channel_id'],
                                      i["Channel_Name"]['playlist_id']]
   
    df_playlist_table = pd.DataFrame(df_playlist_details)

    
    playlist_values = df_playlist_table.values.tolist()
    insert_into_playlist_table = ("insert into playlist values (%s, %s)")
    cursor.executemany(insert_into_playlist_table, playlist_values)
    conn.commit()
    
    #for getting video details 
    df_video_details = pd.DataFrame(columns=["Video_Id", "playlist_id", "Video_Name", "Video_Description",
                                             "Published_date", "View_Count", "Like_Count",
                                             "Favorite_Count", "Comment_Count", "Duration",
                                             "Thumbnail", "Caption_Status"])
    for i in range(len(mongodb_imported_data[0])-1):
    
        df_video_details.loc[i] = [mongodb_imported_data[0][f"video_id_{i+1}"]["Video_Id"],
                                   mongodb_imported_data[0]["Channel_Name"]['playlist_id'],
                                   mongodb_imported_data[0][f"video_id_{i+1}"]['Video_Name'],
                                   mongodb_imported_data[0][f"video_id_{i+1}"]['Video_Description'],
                                   mongodb_imported_data[0][f"video_id_{i+1}"]['PublishedAt'],
                                   mongodb_imported_data[0][f"video_id_{i+1}"]['View_Count'],
                                   mongodb_imported_data[0][f"video_id_{i+1}"]['Like_Count'],
                                   mongodb_imported_data[0][f"video_id_{i+1}"]['Favorite_Count'],
                                   mongodb_imported_data[0][f"video_id_{i+1}"]['Comment_Count'],
                                   mongodb_imported_data[0][f"video_id_{i+1}"]['Duration'],
                                   mongodb_imported_data[0][f"video_id_{i+1}"]['Thumbnail'],
                                   mongodb_imported_data[0][f"video_id_{i+1}"]['Caption_Status']]
 

     
    df_video_table = pd.DataFrame(df_video_details)
    df_video_table['View_Count'] = pd.to_numeric(df_video_table['View_Count'])
    df_video_table['Like_Count'] = pd.to_numeric(df_video_table['Like_Count'])
    df_video_table['Favorite_Count'] = pd.to_numeric(df_video_table['Favorite_Count'])
    df_video_table['Comment_Count'] = pd.to_numeric(df_video_table['Comment_Count'])
    df_video_table['Published_date'] = pd.to_datetime(df_video_table['Published_date'])
    def convert_duration(duration): #this function is used to convert duration format
        time_pattern = re.compile(r'PT(\d+H)?(\d+M)?(\d+S)?')
        match = time_pattern.match(duration)
        if match:
            hours = int(match.group(1)[0:-1]) if match.group(1) else 0
            minutes = int(match.group(2)[0:-1]) if match.group(2) else 0
            seconds = int(match.group(3)[0:-1]) if match.group(3) else 0
            total_seconds = hours * 3600 + minutes * 60 + seconds  # Calculate total duration in seconds
            return total_seconds            
        else:
            return None


    df_video_table['Duration'] = df_video_table['Duration'].astype(str)  # Convert to string format
    df_video_table['Duration'] = pd.to_numeric(df_video_table['Duration'].apply(convert_duration))
   
    video_values = df_video_table.values.tolist()
    insert_into_video_table = ("insert into video values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    cursor.executemany(insert_into_video_table, video_values)
    conn.commit()
    
    #for getting comments
    all_comments =[]
    for i in range(len(mongodb_imported_data[0])-1):
    
        video_id = mongodb_imported_data[0][f"video_id_{i+1}"]["Video_Id"]
        comment_details = mongodb_imported_data[0][f"video_id_{i+1}"]['Comments']
        for j in range(len(comment_details)):
            comments = [comment_details[j][f"comments_id_{j+1}"]['Comment_id'],
                    video_id,
                    comment_details[j][f"comments_id_{j+1}"]['comment_text'],
                    comment_details[j][f"comments_id_{j+1}"]['comment_Author'],
                    comment_details[j][f"comments_id_{j+1}"]['Comment_PublishedAt']]
            all_comments.append(comments)           
            
            
        df_comment_table = pd.DataFrame(all_comments, columns=["Comment_Id", "Video_Id", "Comment_Text",
                                                     "Comment_Author", "comment_published_date"])

        channel_values = df_comment_table.values.tolist()
        insert_into_comment_table = ("insert into comments values (%s, %s, %s, %s, %s)")
        cursor.executemany(insert_into_comment_table, channel_values)
        conn.commit()

    return

#this function used to create table in sql
def create_table():

    channel_table = ("create table channel (channel_id varchar(255),channel_name varchar(255),subscription_count int, channel_views int,channel_description text);")
    cursor.execute(channel_table)
    conn.commit()

    playlist_table = ("create table playlist (channel_id varchar(255) ,playlist_id varchar(255));")
    cursor.execute(playlist_table)
    conn.commit()                                                       

    video_table = ("create table video (video_Id varchar(255), playlist_id varchar(255), video_Name varchar(255), video_description text, published_date date, view_count int, like_count int, favorite_count int, comment_count int, duration int, thumbnail varchar(255), caption_status varchar(255));")
    cursor.execute(video_table)
    conn.commit()

    comment_table = ("create table comments (comment_Id varchar(255), video_Id varchar(255), comment_text text, comment_author varchar(255), comment_published_date date);")
    cursor.execute(comment_table)
    conn.commit() 

    return

#this fuction will display the json file in column 3
def display_json(json_file):
    with col3:    
        with st.container():        
            #with st.expander("Channel Details", expanded=True):           
                json_output = json.dumps(json_file, indent=4)
                st.text_area("Json Display:", value=json_output, height=200)
            
#this code will check whether the mentioned table is there in database if not it will call the function and create table else it will pass
query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name in ('channel', 'playlist', 'video', 'comments'));"
cursor.execute(query)
conn.commit()
check_table = cursor.fetchall()

for i in check_table:    
    
    if i[0] is False:
        create_table()        
    else:
        pass
    
#this code will display heading center
text = "YouTube Data Harvesting"   
st.markdown(f"<h2 style='color: red; text-align: center;'>{text} &#127909;</h2>", unsafe_allow_html=True)

    

# st.columns will make screen as three columns and it will gives the width of each columns
col1, col2, col3 = st.columns([2,1,3])

channel_id= col1.text_input("Enter Channel ID:") # channel_id get the channel_id as input
col2.text("")
col2.text("")
search_button = col2.button(":red[GET]:mag_right:") #this button created for passing channel_id as input 

#if 'get' button is pressed search_button will become true then if condition will run it run get_channel_stats to get the channel details and it will display the output by calling display_json
if search_button:
    try:
        channel_details = get_channel_stats(youtube, channel_id)            
        display_json(channel_details)
    except KeyError:
        col1.warning("Enter the valid channel id")       

# st.columns will make screen as three columns and it will gives the width of each columns
col4, col5, col6 = st.columns([2,3.3,0.9])

col5.text("")
col5.text("")
Migrate_to_Sql =col5.button(":red[Migrate to Sql]")
mongodb = col6.button(":red[Export to MongoDB]")


#if import to mongodb when become True this if condition will run it get the channel details and it will insert the data into mongodb
if mongodb:
    try:
        try:
            get_id = {"Channel_Name.Channel_id": channel_id}
            mongodb_id_list= coll.find(get_id,{"_id":0,"Channel_Name.Channel_id":1})
            x=[]
            for _id in mongodb_id_list:
                x.append(_id)
            mongodb_id = x[0]["Channel_Name"]["Channel_id"]
            if channel_id == mongodb_id:
                col5.warning("Data already imported to Mongodb")
            else:
                pass
        except IndexError:
            youtube_data = get_channel_stats(youtube, channel_id)
            mongodb_imported_data = coll.insert_many([youtube_data])
            col5.warning("Data successfully uploaded to Mongodb")
    except KeyError:
        col5.warning("Please search the channel id before exporting into mongodb")
        
       
with col4:
    try:
        option = [""] #storing channel name        
        mongodb_list= coll.find({},{"_id":0,"Channel_Name.Channel_Name":1})        
        for i in mongodb_list:
            mon = i["Channel_Name"]["Channel_Name"]
            option.append(mon)
    except KeyError:
        pass
    select_option = col4.selectbox("Select channel name:",option) # stored channel name is passed to select_option and if we select the channel name in selectbox it will be stored in select_option
    json_file = [] #it will store the channel details
    #if selection_option become True it will display the channel details in Json display box for selected channel from dropdown
    if select_option:        
        channel_name = {"Channel_Name.Channel_Name": select_option}
        import_from_mongodb = coll.find(channel_name,{"_id":0})
        
        for i in import_from_mongodb:
            json_file.append(i)
        display_json(json_file)
    # if migrate_to_sql become True it will call the migrate_to_sql function and insert the values into sql table
    if Migrate_to_Sql:        
        try:
            try:
                query = f"SELECT channel_name FROM channel WHERE channel_name = '{select_option}';"
                cursor.execute(query)
                conn.commit()
                results = cursor.fetchall()
                df = pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])
                x=df.values.tolist()
                k=x[0][0]
                if select_option ==k:                    
                    col5.warning("Duplicate channel_id, Data already exists")
                else:
                    pass
            except IndexError:
                migrate_to_sql= migrate_to_sql(json_file)
                col5.warning("Data successfully migrated to SQL")
        except IndexError:
            col5.warning("Please select the channel name")

       
        
        
        

questions = [
            "",
            "1.What are the names of all the videos and their corresponding channels?",
            "2.Which channels have the most number of videos, and how many videos do they have?",
            "3.What are the top 10 most viewed videos and their respective channels?",
            "4.How many comments were made on each video, and what are their corresponding video names?",
            "5.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
            "6.What is the total number of views for each channel, and what are their corresponding channel names?",
            "7.Which videos have the highest number of comments, and what are their corresponding channel names?",
            "8.Which videos have the highest number of likes, and what are their corresponding channel names?",
            "9.What are the names of all the channels that have published videos in the year 2022?",
            "10.What is the average duration of all videos in each channel, and what are their corresponding channel names?"
            ]




queries = [
    "",
    "SELECT c.video_name, A.channel_name FROM public.channel A INNER JOIN public.playlist B ON A.channel_id = B.channel_id JOIN public.video C ON B.playlist_id = C.playlist_id",
    "SELECT a.channel_name, COUNT(*) AS video_count FROM public.channel A  INNER JOIN public.playlist B ON A.channel_id = B.channel_id JOIN public.video C ON B.playlist_id = C.playlist_id GROUP BY a.channel_name ORDER BY video_count DESC LIMIT 1;",
    "SELECT A.channel_name,c.video_name, C.view_count FROM public.channel A INNER JOIN public.playlist B ON A.channel_id = B.channel_id JOIN public.video C ON B.playlist_id = C.playlist_id ORDER BY C.view_count DESC LIMIT 10",
    "SELECT c.video_name, c.comment_count FROM public.channel A INNER JOIN public.playlist B ON A.channel_id = B.channel_id JOIN public.video C ON B.playlist_id = C.playlist_id",
    "SELECT c.video_name, C.like_count FROM public.channel A INNER JOIN public.playlist B ON A.channel_id = B.channel_id JOIN public.video C ON B.playlist_id = C.playlist_id",
    "SELECT A.channel_name, SUM(C.view_count) AS total_views FROM public.channel A INNER JOIN public.playlist B ON A.channel_id = B.channel_id JOIN public.video C ON B.playlist_id = C.playlist_id GROUP BY A.channel_name",
    "SELECT  A.channel_name,c.video_name, c.comment_count FROM public.channel A INNER JOIN public.playlist B ON A.channel_id = B.channel_id JOIN public.video C ON B.playlist_id = C.playlist_id ORDER BY c.comment_count DESC LIMIT 1;",
    "SELECT A.channel_name,c.video_name, C.like_count FROM public.channel A INNER JOIN public.playlist B ON A.channel_id = B.channel_id JOIN public.video C ON B.playlist_id = C.playlist_id ORDER BY C.like_count DESC LIMIT 1;",
    "SELECT DISTINCT A.channel_name,EXTRACT(YEAR FROM DATE '2022-01-01') as year FROM public.channel A INNER JOIN public.playlist B ON A.channel_id = B.channel_id JOIN public.video C ON B.playlist_id = C.playlist_id WHERE EXTRACT(YEAR FROM C.published_date) = 2022;",
    "SELECT A.channel_name, ROUND(AVG(C.duration), 2) AS average_duration FROM public.channel A INNER JOIN public.playlist B ON A.channel_id = B.channel_id JOIN public.video C ON B.playlist_id = C.playlist_id GROUP BY A.channel_name;"
]

# Display the dropdown to select the question
selected_question = st.selectbox("Select Query:", questions)

# Execute the corresponding query based on the selected question
query_index = questions.index(selected_question)
query = queries[query_index]
if query:
    conn.commit()
    cursor.execute(query)
    results = cursor.fetchall()
    
    # Convert the results to a DataFrame
    df = pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])

    # Display the table in Streamlit
    st.subheader(selected_question)
    st.dataframe(df)
    conn.commit()
    
    
    
        



