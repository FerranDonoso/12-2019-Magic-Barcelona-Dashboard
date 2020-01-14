#load libraries
import pandas as pd
from math import ceil
import datetime as dt

#load data frames
OrderData = pd.read_csv(r"C:\...\OrderData.csv")
ArticleData = pd.read_csv(r"C:\...\ArticleData.csv")

#order data
#round up second decimal comission
OrderData["Comission"] = OrderData["Comission"].apply(lambda x: ceil(x*100)/100)

#reestructure datetime, date and time
OrderData.dtypes
#utc = True rests an hour, its not needed since its already in utc
OrderData["Date"] = pd.to_datetime(OrderData["Date"], utc = True)

#create date and time
OrderData.rename(columns = {"Date": "DateTime"}, inplace = True)
OrderData["Date"] = OrderData["DateTime"].dt.date
OrderData["Time"] = OrderData["DateTime"].dt.time

#undo the utc = True by adding an hour
OrderData["Time"] = OrderData["Time"].apply(lambda x: (dt.datetime.combine(dt.datetime(1,1,1), x,) + dt.timedelta(hours = 1)).time())

#create datetime
OrderData["DateTime"] = OrderData["Date"].map(str) + " " + OrderData["Time"].map(str)

#remove duplicates
OrderData = OrderData.drop_duplicates(subset = None, keep = "first", inplace = False)

#save as csv
OrderData.to_csv (r"C:\...\FOrderData.csv", index = None, header = True)

#article data
#create datetime, date and time
ArticleData["Date"] = pd.to_datetime(ArticleData["Date"], utc = True)
ArticleData.rename(columns={"Date": "DateTime"}, inplace = True)
ArticleData["Date"] = ArticleData["DateTime"].dt.date
ArticleData["Time"] = ArticleData["DateTime"].dt.time
ArticleData["Time"] = ArticleData["Time"].apply(lambda x: (dt.datetime.combine(dt.datetime(1,1,1), x,) + dt.timedelta(hours = 1)).time())
ArticleData["DateTime"] = ArticleData["Date"].map(str) + " " + ArticleData["Time"].map(str)

#remove duplicates
OrderData = OrderData.drop_duplicates(subset = None, keep = "first", inplace = False)

#save as csv
ArticleData.to_csv (r"C:\...\FArticleData.csv", index = None, header = True)






































