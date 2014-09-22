
import json
import unicodedata


from tweepy import Stream
from tweepy import OAuthHandler, API
from tweepy.streaming import StreamListener

import sqlite3
from optparse import OptionParser

import time
import calendar
import re




def run(options):
    locs = {'london':[-0.417686,51.324605,0.157722,51.66063],
            'manhattan':[-74.019356,40.701695,-73.906746,40.825205]
            }
    loc = locs[options.location]
    
    ckey = options.api_key 
    csecret = options.api_secret   #api secret
    atoken =  options.access_token  #access token
    asecret = options.access_token_secret #access token secret
    
    class listener(StreamListener):
        
        def __init__(self,db_path,verbose,*args,**kwargs):
            self._verbose = verbose
            #setup database:
            self._conn = sqlite3.connect(options.db_path)
            c = self._conn.cursor()
            # Create table
#             c.execute('''CREATE TABLE IF NOT EXISTS GeoTweet
#                          (text text, lat real, lng real, time text, rt text, rt_count text, tw_id text)''')
            cmd = """
                CREATE TABLE IF NOT EXISTS "capture_geotweet" (
                    "id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, -- table ID
                    "time" varchar(100) NOT NULL, -- time in string, e.g., "Sun Jun 22 14:24:56 +0000 2014"
                    "text" varchar(200) NOT NULL, -- content of tweet
                    "lat" decimal NOT NULL, -- latitude of tweet (all tweets in the database are geotagged)
                    "lng" decimal NOT NULL, -- longitude of tweet
                    "rt" bool NOT NULL,  -- whether the tweet was retweeted
                    "rt_count" integer NOT NULL, -- the number of times the tweet has been retweeted
                    "tw_id" varchar(50) NOT NULL -- the Twitter assigned ID
                , "epoch_time" integer);  -- time of tweet in seconds since Thursday Jan 1st, 1970 00:00:00 GMT
            """
            c.execute(cmd)
            self._conn.commit()
            self._time_format = '%a %b %d %H:%M:%S +0000 %Y'
            self._pattern = re.compile("[\W,.:/\']+")
            StreamListener.__init__(self,*args,**kwargs)
    
        def __del__(self):
            self._conn.close() #close database connection
            
        def on_data(self, line):
            if line:
                val = json.loads(line)
                #retrieve text and coordinates
                text = val['text']
                #convert content to strip it of exotic characters
                content = unicodedata.normalize('NFKD', text).encode('ascii','ignore')
                coords = None
                if 'coordinates' in val:
                    coords_full = val['coordinates']
                    if coords_full is not None:
                        if coords_full['type']=='Point':
                            coords = coords_full['coordinates']
                if coords is not None:
                    #store data
                    c = self._conn.cursor()
                    g = {}                    
                    g['time'] = str(val['created_at'])
                    #g['text'] = self._pattern.sub(' ', content)
                    g['text'] = str(content.decode('string_escape').replace("\'",""))
                    g['lat'] = float(coords[1])
                    g['lng'] = float(coords[0])
                    g['rt'] = str(val['retweeted'])
                    g['rt_count'] = int(val['retweet_count'])
                    g['tw_id'] = str(val['id_str'])
                    g['epoch_time'] = int(calendar.timegm(time.strptime(g['time'], self._time_format))) #time (JUST IGNORE TIMEZONE FOR NOW...)
                    keys = ['text','lat','lng','time','rt','rt_count','tw_id','epoch_time']
                    g_tuple = tuple([g[k] for k in keys])
                    if self._verbose: print 'saving',g_tuple
                    # Insert a row of data
                    c.execute("INSERT INTO capture_geotweet %s VALUES %s" % (str(tuple(keys)), str(g_tuple))) #('2006-01-05','BUY','RHAT',100,35.14)
                
                    # Save (commit) the changes
                    self._conn.commit()
                
            return True
    
        def on_error(self, status):
            print status

    
    auth = OAuthHandler(ckey, csecret)
    auth.set_access_token(atoken, asecret)
    
    api = API(auth_handler=auth) 
#     print api.get_status(480579067691806720)
#     sys.exit(0)
#

    for attempts in range(100):
        print '\n\nconnecting...\n'
        twitterStream = Stream(auth, listener(options.db_path,options.verbose))
        #twitterStream.filter(track=["car"])
        twitterStream.filter(locations=loc)

        #try to reconnect:
        wait_time = min(1.5**attempts, 300)
        time.sleep(wait_time)

    
if __name__ == '__main__':
    parser = OptionParser()
    #parser.set_defaults()
    
    parser.add_option("--db_path", type="string", dest="db_path",
                      help="Full path to database filename")
    parser.add_option("--location", type="string", dest="location",
                      help="Which location to select tweets from")
    parser.add_option("--verbose", action="store_true", dest="verbose",
                      help="Verbose mode")
    parser.add_option("--access_token", type="string", dest="access_token",
                      help="")
    parser.add_option("--access_token_secret", type="string", dest="access_token_secret",
                      help="")
    parser.add_option("--api_key", type="string", dest="api_key",
                      help="")
    parser.add_option("--api_secret", type="string", dest="api_secret",
                      help="")
    (options, args) = parser.parse_args()
    if options.verbose: print 'verbose mode on'
    run(options)