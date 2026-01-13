import requests
import json
# from datetime import datetime
#for economical indicators

def TREASURY_YIELD(interval):
    url = f'https://www.alphavantage.co/query?function=TREASURY_YIELD&interval={interval}&maturity=10year&apikey=E8VCMZJEKYQS5Q7W'
    r = requests.get(url)
    data = r.json()

    # print(data)
    return data
def FEDERAL_FUNDS_RATE(interval):
 
    # replace the "E8VCMZJEKYQS5Q7W" apikey below with your own key from https://www.alphavantage.co/support/#api-key
    url = f'https://www.alphavantage.co/query?function=FEDERAL_FUNDS_RATE&interval={interval}&apikey=E8VCMZJEKYQS5Q7W'
    r = requests.get(url)
    data = r.json()

    # print(data)
    return data

def CPI(interval):
 
    # replace the "E8VCMZJEKYQS5Q7W" apikey below with your own key from https://www.alphavantage.co/support/#api-key
    url = f'https://www.alphavantage.co/query?function=CPI&interval={interval}&apikey=E8VCMZJEKYQS5Q7W'
    r = requests.get(url)
    data = r.json()

    # print(data)
    return data

def RETAIL_SALES():
 
    # replace the "E8VCMZJEKYQS5Q7W" apikey below with your own key from https://www.alphavantage.co/support/#api-key
    url = 'https://www.alphavantage.co/query?function=RETAIL_SALES&apikey=E8VCMZJEKYQS5Q7W'
    r = requests.get(url)
    data = r.json()

    # print(data)
    return data

def INFLATION():
 
    # replace the "E8VCMZJEKYQS5Q7W" apikey below with your own key from https://www.alphavantage.co/support/#api-key
    url = 'https://www.alphavantage.co/query?function=INFLATION&apikey=E8VCMZJEKYQS5Q7W'
    r = requests.get(url)
    data = r.json()

    # print(data)
    return data

def DURABLES():
 
    # replace the "E8VCMZJEKYQS5Q7W" apikey below with your own key from https://www.alphavantage.co/support/#api-key
    url = 'https://www.alphavantage.co/query?function=DURABLES&apikey=E8VCMZJEKYQS5Q7W'
    r = requests.get(url)
    data = r.json()

    # print(data)
    return data

def UNEMPLOYMENT():
 
    # replace the "E8VCMZJEKYQS5Q7W" apikey below with your own key from https://www.alphavantage.co/support/#api-key
    url = 'https://www.alphavantage.co/query?function=UNEMPLOYMENT&apikey=E8VCMZJEKYQS5Q7W'
    r = requests.get(url)
    data = r.json()

    # print(data)
    return data

def NONFARM_PAYROLL():
 
    # replace the "E8VCMZJEKYQS5Q7W" apikey below with your own key from https://www.alphavantage.co/support/#api-key
    url = 'https://www.alphavantage.co/query?function=NONFARM_PAYROLL&apikey=E8VCMZJEKYQS5Q7W'
    r = requests.get(url)
    data = r.json()

    # print(data)
    return data

def REAL_GDP(interval):
    url = f'https://www.alphavantage.co/query?function=REAL_GDP&interval={interval}&apikey=E8VCMZJEKYQS5Q7W'
    r = requests.get(url)
    data = r.json()

    # print(data)
    return data

def REAL_GDP_PC():
    url = 'https://www.alphavantage.co/query?function=REAL_GDP_PER_CAPITA&apikey=E8VCMZJEKYQS5Q7W'
    r = requests.get(url)
    data = r.json()

    # print(data)
    return data

# Alpha Vantage functions for financial events
def EARNINGS(symbol):
    url = f'https://www.alphavantage.co/query?function=EARNINGS&symbol={symbol}&apikey=E8VCMZJEKYQS5Q7W'
    r = requests.get(url)
    try:
        data = r.json()
    except:
        data = {"error": "Failed to parse JSON response"}
    return {"status": r.status_code, "data": data}

def DIVIDENDS(symbol):
    url = f'https://www.alphavantage.co/query?function=DIVIDENDS&symbol={symbol}&apikey=E8VCMZJEKYQS5Q7W'
    r = requests.get(url)
    try:
        data = r.json()
    except:
        data = {"error": "Failed to parse JSON response"}
    return {"status": r.status_code, "data": data}

def STOCK_SPLITS(symbol):
    url = f'https://www.alphavantage.co/query?function=SPLITS&symbol={symbol}&apikey=E8VCMZJEKYQS5Q7W'
    r = requests.get(url)
    try:
        data = r.json()
    except:
        data = {"error": "Failed to parse JSON response"}
    return {"status": r.status_code, "data": data}

def EARNINGS_CALENDAR(symbol):
    url = f'https://www.alphavantage.co/query?function=EARNINGS_CALENDAR&symbol={symbol}&horizon=3month&apikey=E8VCMZJEKYQS5Q7W'
    r = requests.get(url)
    try:
        data = r.json()
    except:
        data = r.text
    return {"status": r.status_code, "data": data}