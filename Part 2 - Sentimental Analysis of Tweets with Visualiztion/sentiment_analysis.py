import sys, csv, re
import operator
import nltk
from nltk.corpus import stopwordds
from nltk.classify import SklearnClassifier
from string import punctuation

def clean_input(input):
    # this function will clean the input by removing unnecessary punctuation

    # input must be of type string
    if type(input) != str:
        raise TypeError("input not a string type")

    # first, lets remove any words that are encapsulated by brackets
    # since we're looking at song lyrics, this will most likely be words like
    # (chorus) or (repeat). Since these aren't actual words in the song we can get rid of them
    temp = re.sub(r'(\(\w*\s*\))', '', input)

    # Remove the word chorus since its not an actual lyric
    temp = temp.replace("chorus", "")

    # next, the input must be stripped of punctuation
    # this is done by replacing them with whitespace
    # apostrophes are left in for now
    temp = re.sub(r'[^\w\s\']', ' ', temp)

    # now, any contractions are collapsed by removing the apostrophes
    temp = re.sub(r'[\']', '', temp)
    # any numbers will also be removed since they hold very little meaning later on
    temp = re.sub(r'[\d]', '', temp)

    # replace all whitespace with the space character, this joins all the text into one scenetence
    temp = re.sub(r'[\s]', ' ', temp)

    # Eliminates all stopwords and punctuation
    # All stopwords are in english lib
    stop_words = set(stopwords.words('english'))

    # Replace apostrophes with "" so important words are not separated and contraction words are taken as separate words
    tokened_text = temp.split();

    stopword_text = ""
    for t in tokened_text:
        if len(t) > 1 and t not in stop_words:
            if t not in punctuation:
                stopword_text += " " + t

    return stopword_text


def sentiment_analysis(text, positive_words, negative_words):
    p_score = 0
    n_score = 0
    cleaned_text = clean_input(text)
    for word in cleaned_text.split(" "):
        for wordp in positive_words:
            wordp = wordp.replace("\n","")
            if word == wordp:
                p_score = p_score + 1
        for wordn in negative_words:
            wordn = wordn.replace("\n", "")
            if word == wordn:
                n_score = n_score + 1
    print(p_score)
    print(n_score)
    if p_score > 1.25 * n_score:
        return 1
    elif n_score > 1.25 * p_score:
        return -1
    else:
        return 0

def main():
    test_text = "I am having a good day"
    positive_words = open("./positive.txt").readlines()
    negative_words = open("./negative.txt").readlines()
    print("Result =", sentiment_analysis(test_text, positive_words, negative_words))


if __name__ == "__main__": main()
