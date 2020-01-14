#load libraries
import pandas as pd
import os
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import statistics as st

#set access keys
os.environ['MKM_APP_TOKEN'] = ""
os.environ['MKM_APP_SECRET'] = ""
os.environ['MKM_ACCESS_TOKEN'] = ""
os.environ['MKM_ACCESS_TOKEN_SECRET'] = ""

#connect with the API
from mkmsdk.mkm import Mkm
from mkmsdk.api_map import _API_MAP

mkm = Mkm(_API_MAP["2.0"]["api"], _API_MAP["2.0"]["api_root"])

#price graph
#select card
ESearch = "Throne of Eldraine"
CSearch = "Oko, Thief of Crowns"

response = mkm.market_place.expansions(game = 1)
Card12 = response.json()

for i in range(len(Card12["expansion"])):
    if Card12["expansion"][i]["enName"] == ESearch:
        idexp = Card12["expansion"][i]["idExpansion"]
        response = mkm.market_place.expansion_singles(expansion = idexp)
        ed = response.json()
        for i in range(len(ed["single"])):
            if ed["single"][i]["enName"] == CSearch:
                idcar = ed["single"][i]["idProduct"]
                response = mkm.market_place.articles(product = idcar)
                Card1 = response.json()

# transform into dataframe
PriceCard1 = pd.DataFrame(columns = ["Price"])
Quantity = []
Seller = []
Price = []

for i in range(len(Card1["article"])):
    if Card1["article"][i]["isFoil"] == False:
        if Card1["article"][i]["isPlayset"] == False:
            Quantity.append(Card1["article"][i]["count"])
        if Card1["article"][i]["isPlayset"] == True:
            Quantity.append(Card1["article"][i]["count"] * 4)
    if Card1["article"][i]["isFoil"] == False:
        if Card1["article"][i]["isPlayset"] == False:
            Price.append(Card1["article"][i]["price"])
        if Card1["article"][i]["isPlayset"] == True:
            Price.append(Card1["article"][i]["price"] / 4)
    if Card1["article"][i]["seller"]["isCommercial"] == 0:
        Seller.append("Private")
    if Card1["article"][i]["seller"]["isCommercial"] == 1:
        Seller.append("Professional")
    if Card1["article"][i]["seller"]["isCommercial"] == 2:
        Seller.append("Powerseller")

PriceCard1["Quantity"] = pd.Series(Quantity)
PriceCard1["Price"] = pd.Series(Price)
PriceCard1["Seller"] = pd.Series(Seller)


#multiply rows by card quantity
PriceCard1 = PriceCard1.loc[np.repeat(PriceCard1.index.values, PriceCard1.Quantity)]

#add index column for the dashboard
PriceCard1 = PriceCard1.reset_index(drop = True)
PriceCard1["Index"] = pd.Series(range(len(PriceCard1)))

#detect outliers
sns.boxplot(x = PriceCard1["Price"])

#remove extreme outliers
Q1 = PriceCard1.quantile(0.05)
Q3 = PriceCard1.quantile(0.95)
IQR = Q3 - Q1

PriceCard1 = PriceCard1[~((PriceCard1 < (Q1 - 1.5 * IQR)) |(PriceCard1 > (Q3 + 1.5 * IQR))).any(axis = 1)]

#save as csv
PriceCard1.to_csv (r"C:\...\PriceCard1.csv", index = None, header = True)

#check stock by edition
#check stock total
response = mkm.stock_management.get_stock()
stock = response.json()

Stock = pd.DataFrame(columns = ["Total"])
Expansion = []
Total = []
Price = []
Quantity = []
PriceCat = []

for i in range(len(stock["article"])):
    if "expansion" in stock["article"][i]["product"]:
        if stock["article"][i]["isPlayset"] == False:
            Quantity.append(stock["article"][i]["count"])
        if stock["article"][i]["isPlayset"] == True:
            Quantity.append(stock["article"][i]["count"] * 4)
    if "expansion" in stock["article"][i]["product"]:
        if stock["article"][i]["isPlayset"] == False:
            Price.append(stock["article"][i]["price"])
        if stock["article"][i]["isPlayset"] == True:
            Price.append(stock["article"][i]["price"] / 4)
    if "expansion" in stock["article"][i]["product"]:
        Expansion.append(stock["article"][i]["product"]["expansion"])

Stock["Price"] = pd.Series(Price)
Stock["Quantity"] = pd.Series(Quantity)
Stock["Total"] = Stock["Price"] * Stock["Quantity"]
Stock["Expansion"] = pd.Series(Expansion)

#add feature stock "liquidity"
def f(row):
    if row["Price"] < 100:
        val = "Very High"
        if row["Price"] < 50:
            val = "High"
            if row["Price"] < 10:
                val = "Low"
                if row["Price"] < 1:
                    val = "Very Low"        
    else:
        val = "AA"
    return val

Stock["PriceCat"] = Stock.apply(f, axis = 1)

#save as csv
Stock.to_csv (r"C:\...\Stock.csv", index = None, header = True)

#group stock by expansion and save as csv
StockEx = Stock.groupby("Expansion", as_index=False)["Total"].sum()
StockEx.to_csv (r"C:\...\StockEx.csv", index = None, header = True)

#card sheet info
#market price
plt.plot(PriceCard1["Price"])
st.mode(PriceCard1["Price"])

h = []
for i in range(len(PriceCard1["Price"])):
    h.append(int(PriceCard1["Price"][i] + 0.5))

plt.plot(h)
st.mode(h)

#card info from my stock and from the market place
response = mkm.market_place.product(product = idcar)
product = response.json()

MyPrice = []
MyQuantity = []

for i in range(len(stock["article"])):
    if stock["article"][i]["idProduct"] == idcar:
        MyPrice.append(stock["article"][i]["price"])
        MyQuantity.append(stock["article"][i]["count"])

CardSheet = pd.DataFrame(columns = ["MarketPrice"])
CardSheet["MarketPrice"] = pd.Series(format(st.mode(h), ".2f"))
CardSheet["CardName"] = pd.Series(product["product"]["enName"])
CardSheet["Expansion"] = pd.Series(product["product"]["expansion"]["enName"])
CardSheet["MyPrice"] = pd.Series(MyPrice)
CardSheet["MyQuantity"] = pd.Series(MyQuantity)
CardSheet["Trend"] = pd.Series(product["product"]["priceGuide"]["TREND"])
CardSheet["MKMRecommendation"] = pd.Series(product["product"]["priceGuide"]["SELL"])

#save as csv
CardSheet.to_csv (r"C:\...\CardSheet.csv", index = None, header = True)






