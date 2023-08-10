import pandas as pd
import textblob

class Clean_Tweets:
    """
    The PEP8 Standard AMAZING!!!
    """
    def __init__(self, df:pd.DataFrame):
        self.df = df
        print('Automation in Action...!!!')
        
    def drop_unwanted_column(self, df:pd.DataFrame)->pd.DataFrame:
        """
        remove rows that has column names. This error originated from
        the data collection stage.  
        """
        unwanted_rows = df[df['retweet_count'] == 'retweet_count' ].index
        df.drop(unwanted_rows , inplace=True)
        df = df[df['polarity'] != 'polarity']
        
        return df

    def drop_duplicate(self, df:pd.DataFrame)->pd.DataFrame:
        """
        drop duplicate rows
        """
        self.df = df.drop_duplicates(subset='original_text')

        return df

    # def convert_to_datetime(self, df:pd.DataFrame)->pd.DataFrame:
    #     """
    #     convert column to datetime
    #     """
    #     self.df['created_at'] = pd.to_datetime(
    #         df['created_at'], errors='coerce')

    #     self.df= df[df['created_at'] >= '2020-12-31']

    #     self.convert_to_numbers(self.df)
    #     return self.df
    
    def convert_to_numbers(self, df:pd.DataFrame)->pd.DataFrame:
        """
        convert columns like polarity, subjectivity, retweet_count
        favorite_count etc to numbers
        """
        self.df['polarity'] = pd.to_numeric(
            df['polarity'], errors='coerce')
        self.df['retweet_count'] = pd.to_numeric(
            df['retweet_count'], errors='coerce')
        self.df['favourite_count'] = pd.to_numeric(
            df['favourite_count'], errors='coerce')

        return self.df
    
    def remove_non_english_tweets(self, df:pd.DataFrame)->pd.DataFrame:
        """
        remove non english tweets from lang
        """
        
        self.df = df.query("lang == 'en' ")
        
        return df

if __name__ == "__main__":
    tweet_df = pd.read_csv("processed_tweet_data.csv")
    cleaned = Clean_Tweets(tweet_df)
   
    df = cleaned.drop_duplicate(cleaned.df)
    # df = cleaned.convert_to_datetime(df)
    df = cleaned.remove_non_english_tweets(df)

    df.to_csv('newclean_tweet_data.csv', index=True)
    df.to_json('data/newclean_tweet_data.json')