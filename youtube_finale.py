import cgi
import gdata.youtube
import gdata.youtube.service
import urllib2
import urllib
import re
import sys
import psycopg2
import exceptions
from bs4 import BeautifulSoup

def yt_channel_search(channel_name,numb,srch_query,yt_link,titles,flag=True):
    yt_service = gdata.youtube.service.YouTubeService()
    query = gdata.youtube.service.YouTubeVideoQuery()
    query.vq = srch_query
    query.racy = 'include'
    query.max_results=numb
    query.region='IN'
    query.orderby = 'relevance'
    query.start_index=1
    if flag:
        query.author=channel_name
    feed = yt_service.YouTubeQuery(query)

    for entry in feed.entry:
        print entry.id.text
        response = urllib2.urlopen(entry.id.text)
        page_source = response.read()
        page_source = page_source.decode('utf-8')
        print '\nLINK :'
        patFinderLink=re.compile('http:\/\/www.youtube\.com\/watch\?v=([a-zA-z0-9-]*)\&amp;')
        findPatLink = re.findall(patFinderLink,page_source)
        print findPatLink[1]
        yt_link.append(findPatLink[1])
       
        soup = BeautifulSoup(page_source)
        for node in soup.findAll('title', type="text"):
            title_name = ''.join(node.findAll(text=True))
            print title_name

        title_name = title_name.replace("'",' ')
        titles.append(title_name)
        
    return yt_link,titles;

def yt_main_search(search_query,yt_link,titles):
    yt_link,titles=yt_channel_search('crashcourse',1,search_query,yt_link,titles)
    yt_link,titles=yt_channel_search('historychannel',1,search_query,yt_link,titles)
    yt_link,titles=yt_channel_search('dizzo95',1,search_query,yt_link,titles)
    yt_link,titles=yt_channel_search('HistoryIndiaTV',1,search_query,yt_link,titles)
    yt_link,titles=yt_channel_search('DiscoveryChannelInd',1,search_query,yt_link,titles)
    yt_link,titles=yt_channel_search('natgeoindia',1,search_query,yt_link,titles)
    yt_link,titles=yt_channel_search('INBMINISTRY',1,search_query,yt_link,titles)
    yt_link,titles=yt_channel_search('reflect7',1,search_query,yt_link,titles)
    yt_link,titles=yt_channel_search('',5,search_query,yt_link,titles,flag=False)
    return yt_link,titles;

if __name__ == '__main__':
    try:
        conn = psycopg2.connect("dbname=PUSTACK user=postgres host=localhost password=tiger port=5433");
        print "Connection successful"
    except:
        print "I am unable to connect to the database"

    cur = conn.cursor()
    table='keyword_identification_extraction'

    statement= "SELECT keyword,page_no FROM " + table + " where keyword_type in ('#1','#5')"
    cur.execute(statement)
    rows = cur.fetchall()
    cnt=1
    for row in rows:
        links=[]
        tit=[]
        links,tit=yt_main_search(row[0],links,tit)
        i=0
        for l in links:
            try:
                t=tit[i]
                statement= "INSERT INTO youtube_data VALUES("+str(cnt)+","+str(row[1])+",\'"+row[0]+"\',\'"+l+"\',\'"+t+"\')"
                print statement
                print
                cur.execute(statement)
                cnt=cnt+1
            except psycopg2.IntegrityError:
                conn.rollback()
                print "*******************************************************"
                print "\tDuplicate Entry : Skipping"
                print "*******************************************************"
            else:
                conn.commit()
            i=i+1
    
    cur.close()
    conn.close()
