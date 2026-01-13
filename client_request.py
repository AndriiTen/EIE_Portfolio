import requests
import json
import pandas as pd 
import os


pd.options.display.max_columns = None

EIE_config = {
    'tickers_list': [],
}

# GraphQL query using variables; schema supports only tickers_list
query = '''
query($tickers: [String!]) {
  EIE_Calculator(tickers_list: $tickers) {
    success
    error
    message
    indicators_inserted
    events_inserted
  }
}
'''

variables = {"tickers": EIE_config["tickers_list"]}

headers = {
        'Accept-Encoding': 'gzip, deflate, br',
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Connection': 'keep-alive',
        'DNT': '1'
    }

resp = requests.post(
    'SERVER_URL/graphql',  # Replace SERVER_URL with actual server URL
    json={"query": query, "variables": variables},
    headers=headers
)

# Guard against non-JSON or server errors
try:
    response = resp.json()
except Exception:
    print('HTTP error:', resp.status_code, resp.text)
    raise

# If GraphQL returned errors, print them and exit gracefully
if 'errors' in response and response.get('errors'):
    print('GraphQL errors:', json.dumps(response['errors'], ensure_ascii=False, indent=2))
    # Optionally stop here
    raise SystemExit(1)

# Safe access to data
data = response.get('data', {})
result = data.get('EIE_Calculator', {})

success = result.get('success')
error = result.get('error')
message = result.get('message')
ind = result.get('indicators_inserted')
ev = result.get('events_inserted')

print('success:', success)
print('error:', error)
print('message:', message)
print('indicators_inserted:', ind)
print('events_inserted:', ev)