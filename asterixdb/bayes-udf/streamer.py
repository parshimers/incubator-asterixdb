from socket import socket
import csv, json, time

ip = '127.0.0.1'
port = 4040

sock = socket()
sock.connect((ip, port))

# Replace with own csv file
tweet_csv_file = open("testing.csv")

# Full throttle
with tweet_csv_file as input_data:
    rdr= csv.reader( input_data )
    for i, line in enumerate(rdr):
        if i > 1499999:
            break
        sock.sendall(json.dumps({'id': i, 'text': line[1]}).encode('utf-8'))
    sock.close()


# 10'000 tweets per second
# with tweet_csv_file as input_data:
#     rdr= csv.reader( input_data )
#     tweets = []
#     for i, line in enumerate(rdr):
#         if i > 14999999:
#             break
#         if len(tweets) < 1000:
#             tweets.append({'id': i, 'text': line[1]})
#         else:
#             sock1.sendall(json.dumps(tweets).encode('utf-8'))
#             time.sleep(1)
#             tweets = []
#     sock1.close()

