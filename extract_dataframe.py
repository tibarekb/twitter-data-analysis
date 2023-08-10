import json
from re import sub
from numpy import source
import pandas as pd
from textblob import TextBlob


def read_json(json_file: str)->list:
    """
    json file reader to open and read json files into a list
    Args:
    -----
    json_file: str - path of a json file
    
    Returns
    -------
    length of the json file and a list of json
    """
    
    tweets_data = []
    for tweets in open(json_file,'r'):
        tweets_data.append(json.loads(tweets))
    
    
    return len(tweets_data), tweets_data

class TweetDfExtractor:
    """
    this function will parse tweets json into a pandas dataframe
    
    Return
    ------
    dataframe
    """
    def __init__(self, tweets_list):
        
        self.tweets_list = tweets_list

    # an example function

    def find_statuses_count(self)->list:
        statuses_count = [data['user']['statuses_count']
            if 'user' in data else '' for data in self.tweets_list]
        return statuses_count
        
    def find_full_text(self)->list:
        text = []
        for row in self.tweets_list:
            text.append(row['full_text'])
        return text
       
    
    def find_sentiments(self, text)->list:
        
         polarity, subjectivity = [], []
         for tweet in text:
            blob = TextBlob(tweet)
            sentiment = blob.sentiment
            polarity.append(sentiment.polarity)
            subjectivity.append(sentiment.subjectivity)

         return polarity, subjectivity

    def find_created_time(self)->list:
        created_at = []
        for row in self.tweets_list:
            created_at.append(row['created_at'])
        return created_at

    def find_source(self)->list:
        source = []
        for row in self.tweets_list:
            source.append(row['source'])
        return source

    def find_screen_name(self)->list:
        screen_name = [data['user']['screen_name']
                if 'user' in data else '' for data in self.tweets_list]
        return screen_name


    def find_followers_count(self)->list:
        followers_count = [data['user']['followers_count']
                           if 'user' in data else 0 for data in self.tweets_list]
        return followers_count


    def find_friends_count(self)->list:
        friends_count = [data['user']['friends_count']
                if 'user' in data else 0 for data in self.tweets_list]
        return friends_count

    def is_sensitive(self) -> list:

        is_sensitive = [tweet['possibly_sensitive'] if 'possibly_sensitive' in tweet.keys()
                        else None for tweet in self.tweets_list]

        return is_sensitive


    def find_favourite_count(self)->list:
        favorite_count = [data['retweeted_status']['favorite_count']
                if 'retweeted_status' in data.keys() else 0 for data in self.tweets_list]
        return favorite_count
    
    def find_retweet_count(self)->list:
        retweet_count = [data['retweeted_status']['retweet_count']
                         if 'retweeted_status' in data.keys() else 0 for data in self.tweets_list]
        return retweet_count

    def find_hashtags(self)->list:
        hashtags = [data['entities']['hashtags'] if 'entities' in data.keys(
        ) else '' for data in self.tweets_list]
        return hashtags

    def find_mentions(self)->list:
        mentions = [data['entities']['user_mentions']
                    if 'entities' in data.keys(
        ) else '' for data in self.tweets_list]
        return mentions

    def find_lang(self) -> list:
       try:
            lang = [x['lang'] for x in self.tweets_list]
       except TypeError:
            lang = ''
       return lang

    def find_location(self) -> list:
        location = [tweet['user']['location'] for tweet in self.tweets_list]

        return location
    
        
        
    def get_tweet_df(self, save = True)->pd.DataFrame:
        """required column to be generated you should be creative and add more features"""
        
        # columns = [' 'lang',  'original_author',   'user_mentions', 'place']
        
        columns = ['created at','source','original_text','followers_count','friends_count','retweet_count',
                   'favorite_count','original_author','statuses_count','hashtags','user_mentions','polarity','subjectivity'
                   'possibly_sensitive','lang','location']

        statuses_count = self.find_statuses_count()
        created_at = self.find_created_time()
        source = self.find_source()
        text = self.find_full_text()
        polarity, subjectivity = self.find_sentiments(text)
        lang = self.find_lang()
        fav_count = self.find_favourite_count()
        retweet_count = self.find_retweet_count()
        screen_name = self.find_screen_name()
        follower_count = self.find_followers_count()
        friends_count = self.find_friends_count()
        sensitivity = self.is_sensitive()
        hashtags = self.find_hashtags()
        mentions = self.find_mentions()
        location = self.find_location()
        #data = zip(  lang, screen_name,   mentions, location)
        data = zip(created_at, text, source, follower_count,friends_count,retweet_count,
                fav_count,screen_name,statuses_count,hashtags,mentions,polarity,
                subjectivity,sensitivity,lang)
        df = pd.DataFrame(data=data, columns=columns)

        if save:
            df.to_csv('processed_tweet_data.csv', index=True)
            print('File Successfully Saved.!!!')
        
        return df

                
if __name__ == "__main__":
    # required column to be generated you should be creative and add more features
    # columns = ['clean_text', 'sentiment','polarity','subjectivity', 'lang',  
    #'screen_count', friends_count','possibly_sensitive','place', 'place_coord_boundaries']
    columns = ['created_at','source','original_text','follower_count','friends_count',
               'retweet_count','faviourite_count','original_author','statuses_count','hashtags','user_mentions'
               'polarity', 'subjectivity','possibly_sensitive','lang']
    _, tweet_list = read_json("data/africa_twitter_data.json")
    tweet = TweetDfExtractor(tweet_list)
    tweet_df = tweet.get_tweet_df()
    tweet_df.head(10)


    # use all defined functions to generate a dataframe with the specified columns above