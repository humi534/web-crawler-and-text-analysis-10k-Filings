

from bs4             import BeautifulSoup
from pymongo         import MongoClient
from multiprocessing import Pool
import progressbar
import datetime
import requests
import time
import sys

#----------------------------------------- 
#Utils functions
#----------------------------------------- 
def saveToMongo(collName, data):  
    clt  = MongoClient('localhost', 27017)
    db   = clt['PhDData']
    coll = db[collName]
    coll.insert_many(data)
    
def getBs4ElementOrEmptyString(soup, tag, values):
    try:
        return soup.findAll(tag, values)[0].get_text()
    except IndexError:
        return ""

def requestLinkWithRetry(link):
    res = None
    tries = 0
    while(tries < 3):
        try:
            res = requests.get(link)
            return res
        except TimeoutError: 
            tries += 1
    return None
    
#----------------------------------------- 
#Crawler functions   
#-----------------------------------------     
        
def getPage(y, m):
    print("Getting the full page...")
    URL = f'https://www.digitaltrends.com/news/archive/{y}/{m}/'
    page = requestLinkWithRetry(URL)    
    soup = BeautifulSoup(page.content, 'html.parser')
    return soup
    
def getArticlesLinks(soup):
    links = []
    ul = soup.findAll("ul", {"class": "b-catalog__list"})
    for a in ul[0].findAll("a"):
        links.append(str(a["href"]))
    return links
    
def getArticlesFromLinks(links):
    print("Extracting articles from links...")
    
    #Request all articles in parralel
    p = Pool(10)
    results = p.map(requestLinkWithRetry, links)
    p.close()
    
    #make soup from articles
    soups = []
    for result in progressbar.progressbar(results):
        if(not result is None):
            soups.append(BeautifulSoup(result.content, 'html.parser'))
    return soups
    
def getFormattedArticles(articles):
    print("Formatting articles...")
    formArt = []
    for a in progressbar.progressbar(articles):
    
        #check for empty article
        content     = a.findAll("article")
        if(not content):
            continue
           
        #extract content and clean
        content      = getBs4ElementOrEmptyString(a,"article", {})
        code         = content.rfind("});")
        if(code > 0):            
            content = content[code:]
        editorec = content.rfind("Editors' Recommendations")
        if(editorec > 0):            
            content = content[0:editorec]
            
        #extract various meta data
        author      = getBs4ElementOrEmptyString(a,"a", {"class": "author"})
        title       = getBs4ElementOrEmptyString(a,"h1", {"class": "b-headline__title"})
        type        = getBs4ElementOrEmptyString(a,"div", {"class": "b-headline__crumbs"})
                
        #extract and format time data
        time        = a.findAll("time", {"class": "b-byline__time"})
        if(len(time)>0):
            time = time[0]
        else:
            continue
        time        = datetime.datetime.strptime(time["datetime"][0:-6], '%Y-%m-%dT%H:%M:%S')
        date        = f'{time.year}-{time.month}-{time.day}'
        
        #format data for storage
        metaData    = {"author":author.replace("\n",""),
                        "title":title.replace("\n",""),
                        "type":type.replace("\n","")}
        formArt.append({"date":date,"metaData":metaData,"txt":content})
    return formArt

#----------------------------------------- 
#Main
#----------------------------------------- 
if __name__ == '__main__':        

    #set crawl time period
    years   = [str(y) for y in range(2010,2022)]
    months  = [str(m) for m in range(1,13)]
    
    #Crawl main loop
    links   = []
    for y in years:
        for m in months:
            print('------------------------')
            print(f'CURRENT PAGE : {y}/{m}')
            print('------------------------')

            page     = getPage(y,m)
            links    = getArticlesLinks(page) 
            if(len(links) == 0):
                print("Nothing here")
                continue
            articles = getArticlesFromLinks(links)
            formArt  = getFormattedArticles(articles)
            if(len(formArt) > 0):
                saveToMongo("DigitalTrend",formArt)     
                
                