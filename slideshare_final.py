import urllib2
import urllib
import time
import sys
import sha
import re
import psycopg2

api_key = '4dTWSjB8'
api_secret = '4aRxc3Om'

class SlideShareApi:
    def __init__(self,params_as_dict,proxy=None):
        if ('api_key' not in params_as_dict) or ('api_secret' not in params_as_dict):
            print >> sys.stderr, "API Key and Secret Missing"
            return 0
        if proxy:
            self.use_proxy = True
            if not isinstance(proxy,dict):
                print >> sys.stderr," Proxy Config should be a dictionary"
                return 0
            self.proxy = proxy
        else:
            self.use_proxy = False
 
        self.params = params_as_dict
        if self.use_proxy:
            self.setup_proxy()
 
    def search_slideshow(self,q,itemsperpage,sort):
        timestamp = int(time.time())
        parameters = {'api_key' : self.params['api_key'],'ts' :timestamp,'hash' : sha.new(self.params['api_secret'] + str(timestamp)).hexdigest()}
        apicall_link="https://www.slideshare.net/api/2/search_slideshows?q="+q+"&page=1&items_per_page="+str(itemsperpage)+"&lang=en&sort="+sort+"&upload_date=any&fileformat=all&file_type=all&api_key="+str(parameters['api_key'])+"&hash="+str(parameters['hash'])+"&ts="+str(parameters['ts'])

        print apicall_link
        
        response = urllib2.urlopen(apicall_link)
        page_source = response.read()
        page_source = page_source.decode('utf-8')

        patFinderLink=re.compile('<URL>(.*)</URL>')
        Links = re.findall(patFinderLink,page_source)

        patFinderTitle=re.compile("<Title>(.*)</Title>")
        Title_name = re.findall(patFinderTitle,page_source)

        for t in Title_name:
            t = t.replace("'",' ')

        return Links,Title_name;
            
if __name__ == '__main__':
    
    obj=SlideShareApi(locals())
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
        Title=[]
        searchTerm=row[0]
        searchTerm = searchTerm.replace(' ','%20')
        print row[0]
        links,Title=obj.search_slideshow(searchTerm,10,"relevance")
        i=0
        for l in links:
            try:
                t=Title[i]
                print t
                statement= "INSERT INTO slideshare_data VALUES("+str(cnt)+","+str(row[1])+",\'"+row[0]+"\',\'"+l+"\',\'"+t+"\')"
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

