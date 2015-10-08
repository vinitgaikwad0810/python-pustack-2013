import pprint
import sys
from apiclient.discovery import build
import psycopg2

def get_book_details(search_query):
  book_id = []
  book_title=[]
  thumbnail_link=[]
  api_key ='AIzaSyAfeD4ORUK_63ldCoIP13zQ7VW6zYXPdcM'
  service = build('books', 'v1', developerKey='AIzaSyAfeD4ORUK_63ldCoIP13zQ7VW6zYXPdcM')

  request = service.volumes().list(source='public', q=search_query)

  response = request.execute()
  #print 'Found %d books:' % len(response['items'])
  book_cnt=0
  for book in response.get('items', []):
      book_id.append(book['id'])
      book_title.append(book['volumeInfo']['title'])
      thumbnail_link.append(book['volumeInfo']['imageLinks']['thumbnail'])
      book_cnt=book_cnt+1
      if(book_cnt==5):
        break;

  return (book_id,book_title,thumbnail_link);


#################       IMP         ########################
# http://books.google.co.in/books?id=


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
        bid = []
        btit=[]
        tlink=[]
        (bid,btit,tlink)=get_book_details(row[0])
        for i in range(0,5):
          try:
            statement= "INSERT INTO gbooks_data VALUES("+str(cnt)+","+str(row[1])+",\'"+row[0]+"\',\'"+bid[i]+"\',\'"+tlink[i]+"\',\'"+btit[i]+"\')"
            print statement
            cur.execute(statement)
            cnt=cnt+1
          except:
            print "Duplicate Entry : Skipping"
          i=i+1
       
    conn.commit()
    cur.close()
    conn.close()
