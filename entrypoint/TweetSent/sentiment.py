import math,sys
import pickle;
import sklearn;
import os;
class TweetSent(object):

    def __init__(self):

        pickle_path = os.path.join(os.path.dirname(__file__), 'sentiment_pipeline3')
        f = open(pickle_path,'rb')
        self.pipeline = pickle.load(f)
        f.close()

    def sentiment(self, *args):
        return self.pipeline.predict(args[0])[0].item()
