#load libraries
import pandas as pd
import math
import os

#set access keys
os.environ['MKM_APP_TOKEN'] = ""
os.environ['MKM_APP_SECRET'] = ""
os.environ['MKM_ACCESS_TOKEN'] = ""
os.environ['MKM_ACCESS_TOKEN_SECRET'] = ""

#connect with the API
from mkmsdk.mkm import Mkm
from mkmsdk.api_map import _API_MAP

mkm = Mkm(_API_MAP["2.0"]["api"], _API_MAP["2.0"]["api_root"])

#order dataframe
#order data set - cancelled

#check last total
CTotals = pd.read_csv(r"C:\...\CTotals.csv")
last_cancelled_total = CTotals["Last_Cancelled_Total"][0]

#check new cancelled total
new_cancelled_total = 36

#get start points
fs = (int(math.floor(last_cancelled_total / 100.0)) * 100) + 1
ls = (int(math.floor(new_cancelled_total / 100.0)) * 100) + 1

cancelled_start_points = list(range(fs, ls + 1, 100))

del fs, ls

#request for each start count
Canceled = []
for i in cancelled_start_points:
    response = mkm.order_management.filter_order_paginated(actor = 1, state = 128, start = i, maxResponses = 100)
    canceled = response.json()
    Canceled = Canceled + canceled["order"]

del i, canceled

#save new cancelled total
CTotals = pd.DataFrame(columns = ["Last_Cancelled_Total"])
CTotals["Last_Cancelled_Total"] = pd.Series(new_cancelled_total)
CTotals.to_csv (r"C:\...\CTotals.csv", index = None, header = True)

del last_cancelled_total, new_cancelled_total

#transform into dataframe
OrderDataC = pd.DataFrame(columns = ["OrderID"])
OrderID = []
Username = []
Name = []
Street = []
Zip = []
City = []
Country = []
Date = []
Article = []
Value = []
Shipping = []
Total = []
State = []

for i in range(len(Canceled)):
    OrderID.append(Canceled[i]["idOrder"])
    Username.append(Canceled[i]["buyer"]["username"])
    Name.append(Canceled[i]["buyer"]["address"]["name"])
    Street.append(Canceled[i]["buyer"]["address"]["street"])
    Zip.append(Canceled[i]["buyer"]["address"]["zip"])
    City.append(Canceled[i]["buyer"]["address"]["city"])
    Country.append(Canceled[i]["buyer"]["address"]["country"])
    Date.append(Canceled[i]["state"]["dateBought"])
    Article.append(Canceled[i]["articleCount"])
    Value.append(Canceled[i]["articleValue"])
    Shipping.append(Canceled[i]["shippingMethod"]["price"])
    Total.append(Canceled[i]["totalValue"])
    if Canceled[i]["state"]["wasMergedInto"] == 0:
        State.append(Canceled[i]["state"]["state"])
    else:
        State.append(Canceled[i]["state"]["wasMergedInto"])

OrderDataC["OrderID"] = pd.Series(OrderID)
OrderDataC["Username"] = pd.Series(Username)
OrderDataC["Name"] = pd.Series(Name)
OrderDataC["Street"] = pd.Series(Street)
OrderDataC["Zip"] = pd.Series(Zip)
OrderDataC["City"] = pd.Series(City)
OrderDataC["Country"] = pd.Series(Country)
OrderDataC["Date"] = pd.Series(Date)
OrderDataC["Article"] = pd.Series(Article)
OrderDataC["Value"] = pd.Series(Value)
OrderDataC["Shipping"] = pd.Series(Shipping)
OrderDataC["Total"] = pd.Series(Total)
OrderDataC["Comission"] = OrderDataC["Value"] * 0.05
OrderDataC["State"] = pd.Series(State)

del i, OrderID, Username, Name, Street, Zip, City, Country, Date, Article, Value, Shipping, Total, State

#order data set - received
#check last total
RTotals = pd.read_csv(r"C:\...\RTotals.csv")
last_received_total = RTotals["Last_Received_Total"][0]

#check new received total
response = mkm.account_management.account()
account = response.json()
new_received_total = account["account"]["sellCount"]

del response

#get start points
fs = (int(math.floor(last_received_total / 100.0)) * 100) + 1
ls = (int(math.floor(new_received_total / 100.0)) * 100) + 1

received_start_points = list(range(fs, ls + 1, 100))

del fs, ls

#request for each start count
Received = []
for i in received_start_points:
    response = mkm.order_management.filter_order_paginated(actor = 1, state = 8, start = i, maxResponses = 100)
    received = response.json()
    Received = Received + received["order"]

del i, received

#save new received total
RTotals = pd.DataFrame(columns = ["Last_Received_Total"])
RTotals["Last_Received_Total"] = pd.Series(new_received_total)
RTotals.to_csv (r"C:\...\RTotals.csv", index = None, header = True)


#transform into dataframe
OrderDataR = pd.DataFrame(columns = ["OrderID"])
OrderID = []
Username = []
Name = []
Street = []
Zip = []
City = []
Country = []
Date = []
Article = []
Value = []
Shipping = []
Total = []
State = []

for i in range(len(Received)):
    OrderID.append(Received[i]["idOrder"])
    Username.append(Received[i]["buyer"]["username"])
    Name.append(Received[i]["buyer"]["address"]["name"])
    Street.append(Received[i]["buyer"]["address"]["street"])
    Zip.append(Received[i]["buyer"]["address"]["zip"])
    City.append(Received[i]["buyer"]["address"]["city"])
    Country.append(Received[i]["buyer"]["address"]["country"])
    Date.append(Received[i]["state"]["dateBought"])
    Article.append(Received[i]["articleCount"])
    Value.append(Received[i]["articleValue"])
    Shipping.append(Received[i]["shippingMethod"]["price"])
    Total.append(Received[i]["totalValue"])
    State.append(Received[i]["state"]["state"])

OrderDataR["OrderID"] = pd.Series(OrderID)
OrderDataR["Username"] = pd.Series(Username)
OrderDataR["Name"] = pd.Series(Name)
OrderDataR["Street"] = pd.Series(Street)
OrderDataR["Zip"] = pd.Series(Zip)
OrderDataR["City"] = pd.Series(City)
OrderDataR["Country"] = pd.Series(Country)
OrderDataR["Date"] = pd.Series(Date)
OrderDataR["Article"] = pd.Series(Article)
OrderDataR["Value"] = pd.Series(Value)
OrderDataR["Shipping"] = pd.Series(Shipping)
OrderDataR["Total"] = pd.Series(Total)
OrderDataR["Comission"] = OrderDataR["Value"] * 0.05
OrderDataR["State"] = pd.Series(State)

del i, OrderID, Username, Name, Street, Zip, City, Country, Date, Article, Value, Shipping, Total, State

#combine both dataframes
OrderData = pd.concat([OrderDataR, OrderDataC])

del OrderDataR, OrderDataC

OrderData.to_csv (r"C:\...\OrderData.csv", index = None, header = True)

#article dataframe
#article data set - canceled
#transform into dataframe
ArticleDataC = pd.DataFrame(columns = ["OrderID"])
Expansion = []
OrderID = []
Date = []
Article = []
Category = []
Amount = []
Price = []
Comments = []
for i in range(len(Canceled)):
    x = len(Canceled[i]["article"])
    for j in range(x):
        OrderID.append(Canceled[i]["idOrder"])
        Date.append(Canceled[i]["state"]["dateBought"])
        Article.append(Canceled[i]["article"][j]["product"]["enName"])
        Amount.append(Canceled[i]["article"][j]["count"])
        Price.append(Canceled[i]["article"][j]["price"])
        Comments.append(Canceled[i]["article"][j]["comments"])
        if "expansion" in Canceled[i]["article"][j]["product"]:
            Expansion.append(Canceled[i]["article"][j]["product"]["expansion"])
        else:
            y = Canceled[i]["article"][j]["idProduct"]
            response = mkm.market_place.product(product = y)
            prod = response.json()
            Expansion.append(prod["product"]["expansion"]["enName"])
        z = Canceled[i]["article"][j]["idProduct"]
        response = mkm.market_place.product(product = z)
        cate = response.json()
        Category.append(cate["product"]["categoryName"])

ArticleDataC["OrderID"] = pd.Series(OrderID)
ArticleDataC["Date"] = pd.Series(Date)
ArticleDataC["Article"] = pd.Series(Article)
ArticleDataC["Expansion"] = pd.Series(Expansion)
ArticleDataC["Category"] = pd.Series(Category)
ArticleDataC["Amount"] = pd.Series(Amount)
ArticleDataC["Price"] = pd.Series(Price)
ArticleDataC["Total"] = ArticleDataC["Amount"] * ArticleDataC["Price"]
ArticleDataC["Comments"] = pd.Series(Comments)

del i, j, x, y, z, prod, cate, Canceled, Expansion, OrderID, Date, Article, Category, Amount, Price, Comments

#article data set - received
#transform into dataframe
ArticleDataR = pd.DataFrame(columns = ["OrderID"])
Expansion = []
OrderID = []
Date = []
Article = []
Category = []
Amount = []
Price = []
Comments = []
for i in range(len(Received)):
    x = len(Received[i]["article"])
    for j in range(x):
        OrderID.append(Received[i]["idOrder"])
        Date.append(Received[i]["state"]["dateBought"])
        Article.append(Received[i]["article"][j]["product"]["enName"])
        Amount.append(Received[i]["article"][j]["count"])
        Price.append(Received[i]["article"][j]["price"])
        Comments.append(Received[i]["article"][j]["comments"])
        if "expansion" in Received[i]["article"][j]["product"]:
            Expansion.append(Received[i]["article"][j]["product"]["expansion"])
        else:
            y = Received[i]["article"][j]["idProduct"]
            response = mkm.market_place.product(product = y)
            prod = response.json()
            Expansion.append(prod["product"]["expansion"]["enName"])
        z = Received[i]["article"][j]["idProduct"]
        response = mkm.market_place.product(product = z)
        cate = response.json()
        Category.append(cate["product"]["categoryName"])

ArticleDataR["OrderID"] = pd.Series(OrderID)
ArticleDataR["Date"] = pd.Series(Date)
ArticleDataR["Article"] = pd.Series(Article)
ArticleDataR["Expansion"] = pd.Series(Expansion)
ArticleDataR["Category"] = pd.Series(Category)
ArticleDataR["Amount"] = pd.Series(Amount)
ArticleDataR["Price"] = pd.Series(Price)
ArticleDataR["Total"] = ArticleDataR["Amount"] * ArticleDataR["Price"]
ArticleDataR["Comments"] = pd.Series(Comments)

del i, j, x, y, z, prod, cate, Received, Expansion, OrderID, Date, Article, Category, Amount, Price, Comments

#combine both dataframes
ArticleData = pd.concat([ArticleDataR, ArticleDataC])

del ArticleDataR, ArticleDataC

ArticleData.to_csv (r"C:\...\ArticleData.csv", index = None, header = True)

