# AsterixDB User Defined Functions for Sentiment Analysis of tweets by CPU- and GPU-implementations of Naive Bayes. 

UDF's for AsterixDB created during my pre-thesis-project (prosjektoppgave) at the Norwegian University of Science and Technology. Will link to paper here once it is uploaded.

## Installation

To install the UDF, first compile it by running

```bash
mvn package
```
and then deploy it to your AsterixDB-cluster by sending the created zip-file to 

`{CC-adress}/admin/udf/{DataVerseName}/{LibraryName}`

using HTTP POST.

The UDF's are built to run on an open datatype `TweetType` with the mandatory field of `id: int64`, so if you have no such datatype defined in your dataverse, consider creating it by I.E. running the SQL++ command
```SQL
CREATE type TweetType IF NOT EXISTS AS OPEN {
        id: int64
};
```
or changing the UDF's `argument_type` and `return_type` inside of library_descriptor.xml to fit your own datatypes.

## Training data for the Naive Bayes model
The training data I used to train the model can be downloaded from the Stanford Sentiment140 project [here](http://cs.stanford.edu/people/alecmgo/trainingandtestdata.zip). I would have included it in the repo, but the file is too big. I personally shuffled the data and renamed it to `training.shuffle.csv`, then put it inside of the classifier-folder.

## Usage
To run the UDF on a dataset `Tweets` of existing tweets of `TweetType`, using the library name specified when you deployed the UDF to the cluster, you can do something like

```SQL
SELECT {LibraryName}#CPUClassify(t) FROM Tweets t;
```

To run the UDF on a stream of tweets to process them while they're being ingested into AsterixDB, you can do something like

```SQL
CREATE DATASET StreamedTweetsDataset(TweetType) primary key id;

CREATE FEED SocketTweetFeed WITH {
      "adapter-name": "socket_adapter",
      "sockets": "127.0.0.1:4040”,
      "address-type": "IP",
      "type-name": "TweetType",
      "format": "adm"
};

CONNECT FEED SocketTweetFeed TO DATASET StreamedTweetsDataset APPLY FUNCTION {LibraryName}#CPUClassify;

START FEED SocketTweetFeed;
```
then stream tweets into the socket by using `tweet-streamer.py`.

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.


## License
[MIT](https://choosealicense.com/licenses/mit/)