from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import csv

analyzer = SentimentIntensityAnalyzer()

with open('sample.txt') as csvfile:
    reader = csv.reader(csvfile,delimiter=',')
    for row in reader:
        vs = analyzer.polarity_scores(row[0])
        print("{:-<65} {}".format(row[0], str(vs)))
        
