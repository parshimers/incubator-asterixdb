import math,sys
import pickle;
import sklearn;

class TweetSent(object):

    def __init__(self):
        f = open('sentiment_pipeline3','rb')
        self.pipeline = pickle.load(f)
        f.close()

    def sentiment(self, *args):
        return self.pipeline.predict(args)[0].item()
