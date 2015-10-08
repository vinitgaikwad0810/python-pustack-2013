import os
import sys
import time
import urllib2
import simplejson
import psycopg2
import google_urlshortener

def google_img_search(searchTerm,links,titles):
    # Replace spaces ' ' in search term for '%20' in order to comply with request
    searchTerm = searchTerm.replace(' ','%20')
    for i in range(0,2):
        url = ('https://ajax.googleapis.com/ajax/services/search/images?' + 'v=1.0&q='+searchTerm+'&start='+str(4*i)+'&userip=MyIP')
        print url
        print 
        request = urllib2.Request(url, None, {'Referer': 'testing'})
        response = urllib2.urlopen(request)

        # Get results using JSON
        results = simplejson.load(response)
        data = results['responseData']
        dataInfo = data['results']

        # Iterate for each result and get 'unescapedUrl'
        for myUrl in dataInfo:
            print myUrl['unescapedUrl']
            links.append(myUrl['unescapedUrl'])
            t=myUrl['titleNoFormatting']
            t = t.replace("'",' ')
            titles.append(t)


        # Sleep for one second to prevent IP blocking from Google
        time.sleep(1)
    return links;


if __name__ == '__main__':
    try:
        conn = psycopg2.connect("dbname=PUSTACK user=postgres host=localhost password=tiger port=5433");
        print "Connection successful"
    except:
        print "I am unable to connect to the database"

    cur = conn.cursor()
    table='keyword_identification_extraction'

    statement= "SELECT keyword,page_no FROM " + table + " where keyword_type in ('#1','#2','#5')"
    cur.execute(statement)
    rows = cur.fetchall()
    leg_cnt=1
    for row in rows:
        
        links=[]
        Title=[]
        i=0
        links=google_img_search(row[0],links,Title)
        for l in links:
            try:
                t=Title[i]
                print t
                statement= "INSERT INTO images_data VALUES("+str(leg_cnt)+","+str(row[1])+",\'"+row[0]+"\',\'"+l+"\',\'"+t+"\')"
                print statement
                print
                cur.execute(statement)
                leg_cnt=leg_cnt+1
            except psycopg2.ProgrammingError:
                conn.rollback()
                l=google_urlshortener.main(l)
                statement= "INSERT INTO images_data VALUES("+str(leg_cnt)+","+str(row[1])+",\'"+row[0]+"\',\'"+l+"\',\'"+t+"\')"
                print statement
                print
                cur.execute(statement)
                leg_cnt=leg_cnt+1
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
