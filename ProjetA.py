from bs4             import BeautifulSoup
from pymongo         import MongoClient
import nltk
from selenium import webdriver
from nltk.corpus import stopwords
nltk.download('stopwords')
nltk.download('punkt')
nltk.download('wordnet')
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
from collections import Counter
import pandas as pd
import re

#--------authors-----------
    #Elisa Etienne
    #Hugo Poncelet
    #Valentin Hamers

#----------------------------------------- 
#Utils functions
#----------------------------------------- 

def saveToMongo(collectionName, data):
    client = MongoClient('localhost', 27017)
    db = client['ProjetA']
    collection = db[collectionName]
    collection.insert_one(data)
    
#----------------------------------------- 
#Crawler functions   
#-----------------------------------------     
        
def getPage(URL):
    print("Getting the full page...")
    driver = webdriver.Chrome()
    driver.get(URL)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    driver.close()
    return soup
    
def getArticlesLinks(soup):
    links = []
    ul = soup.findAll("table", {"class": "table"}) 
    for a in ul[0].findAll("a"):
        links.append(str(a["href"]))
    return links
    
def getDataFromPage(page):
    print("getting information")
    myDict = {}

    #---------title-----------
    title = page.title.string
    myDict["title"] = title
    print(title)

    #---------dates-----------
    ul = page.findAll("div",{"class":"panel-body context"})

    #published
    for published in ul[0].findAll("abbr"):
        publishedDate = str(published["title"])[:10]
        myDict["published"] = publishedDate

    #submitted and period ending
    for dates in ul[0]:
        
        if (dates.find("Submitted") ==0):
            submitted = dates[11:]
            myDict["submitted"] = submitted
            
        if(dates.find("Period Ending")==0):
            periodEnding = dates[18:]
            myDict["periodEnding"] = periodEnding
    
    #--------- text -----------
    panelBodyList = page.findAll("div",{"class":"html-body"})
    text = ""
    for div in panelBodyList:
        try:
            tag=div.find("ix:header")
            tag.replaceWith('')
        except:
            None
            
        try:
            tables = div.findAll("table")
            for table in tables:
                table.replaceWith('')
        except:
            None
        
        try:
            text += div.get_text()
        except:
            None
        
    myDict["text"] = text
    
    #--------- return dict -----------
    return myDict


def retrieveDataAndStoreInMongoDB():
    companies = ["UBER"]
    for company in companies:
        URL = f'https://sec.report/Document/Search/?formType=10-K&queryCo={company}#results'
        page     = getPage(URL)
        links    = getArticlesLinks(page) 
        if(len(links) == 0):
            print("Nothing here")
        
        for link in links:
            page = getPage(link)
            dataFromPage = getDataFromPage(page)
            saveToMongo("ProjectACollection", dataFromPage)
            
            
#----------------------------------------- 
#Query
#-----------------------------------------         
def count_occurrences(text, word):
    return text.lower().count(word)


#----------------------------------------- 
#Bigrams detection
#-----------------------------------------      
def clean_text(text):
  text = text.lower()
  
  text = re.sub("\n"," ",text)
  text = re.sub(r"\S*https?:\S*", "", text) #delete the url
  text = re.sub("&amp;","&",text) #transform &amp; by &
  text = re.sub('[^a-zA-Z0-9 \n\.]', ' ', text) #remove all special charachters (everything that is not a letter or a number)
  text = remove_stopwordsAndTokenize(text) #remove all stopwords and tokenize
  return text

def remove_stopwordsAndTokenize(text): 
  wordnet_lemmatizer = WordNetLemmatizer() #unite all similar words: ex: playing-plays-played =>play
  all_stopwords = stopwords.words('english') #stopwords are all words we don't
  all_stopwords.append(".") #add the regular dot to stopwords
  text_tokens = word_tokenize(text) #tokenize all words
  
  #lemmatize
  tokens_without_sw = [wordnet_lemmatizer.lemmatize(word, pos="v") for word in text_tokens if not word in all_stopwords]
  
  return tokens_without_sw
#----------------------------------------- 
#Main
#----------------------------------------- 

if __name__ == '__main__':
    
    #----------- step 1 and 2-------------
    retrieveDataAndStoreInMongoDB()
    
    #----------- step 3 -------------
    print("\n\n\n")
    print("-------------------------")
    print("STEP 3: Database Querying")
    print("-------------------------")
    
    #connection to the database
    client = MongoClient("mongodb://localhost:27017/")
    db = client["ProjetA"]
    collection = db["ProjectACollection"]
    
    #count total number of documents
    total_nb_of_documents = collection.count_documents({})
    print("total number of documents: " + str(total_nb_of_documents))
    
    #count the number of occurences of the word "competition" for each document
    query = { "text":{"$regex":"competition"}}
    count = collection.count_documents(query)
    print("total number of documents containing the word competition: " + str(count))
    
    #recover all data
    print("\n\n")
    all_docs = collection.find()
    docs = collection.find(query)
    
    title = []
    competition_occurrence = []
    for doc in all_docs:
        title.append(doc['title'])
        competition_occurrence.append(count_occurrences(doc["text"], "competition"))
        
    data = {
      "title": title,
      "competition": competition_occurrence
    }
    
    #load data into a DataFrame object:
    df = pd.DataFrame(data)
    print(df) 
    
    
    #----------- step 4 -------------
    print("\n\n\n")
    print("-------------------------")
    print("STEP 4: Bigrams")
    print("-------------------------")
    
    #recover all texts in a single String
    totalText =""
    for doc in collection.find():
        totalText += doc['text']
    
    #clean, tokenize and get bigrams
    tokens = (clean_text(totalText))
    bi_grams = list(nltk.bigrams(tokens))
    counter = Counter(bi_grams)
    most_common_bigrams = counter.most_common(20)
    bigrams = []
    occurrences = []
    for bigram in most_common_bigrams:
        bigrams.append(bigram[0])
        occurrences.append(bigram[1])
        
    data = {
      "bigram": bigrams,
      "occurrence": occurrences
    }
    
    #load data into a DataFrame object:
    df = pd.DataFrame(data)
    print(df) 
    
    
    


    
    
    

    
        
        
        