# import libraries
import requests
from bs4 import BeautifulSoup
import json

def lambda_handler(event, context):
    # set url
    stk = event.get("payload")
    url = "https://finance.yahoo.com/quote/" + stk

    # get the url page results
    response = requests.get(url)

    # try to parse Beautiful Soup
    try:
        soup = BeautifulSoup(response.text, "html.parser")
    except Exception as e: # handle error gracefully
        return {
            'statusCode': 200,
            'body': json.dumps(f'Here is the error message: {e}'),
            } # send the error message back to the user

    # find the price
    try:
        price = soup.find("fin-streamer", {'data-test':"qsp-price"}).text
        return {
        'statusCode': 201,
        'price': json.dumps(price)
        }
    except Exception as e:
        return {
            'statusCode': 202,
            'body': json.dumps(f'Cannot find stock {stk}'),
            }