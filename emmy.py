import os
import asyncio
import logging
from logging.handlers import RotatingFileHandler
from metaapi_cloud_sdk import MetaApi
from tenacity import retry, wait_fixed, stop_after_attempt
import json
from datetime import datetime
from flask import Flask, request, jsonify

# Enhanced logging setup
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log_file = 'emmy.log'
file_handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=2)
file_handler.setFormatter(log_formatter)
file_handler.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
console_handler.setLevel(logging.INFO)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Retrieve token and account ID from environment variables or use placeholders
TOKEN = os.getenv('TOKEN') or 'eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiI2MzE2MDM1Mzg0M2JkZjA2MGU0MjY2OTE3ZmYxYTViZSIsInBlcm1pc3Npb25zIjpbXSwiYWNjZXNzUnVsZXMiOlt7ImlkIjoidHJhZGluZy1hY2NvdW50LW1hbmFnZW1lbnQtYXBpIiwibWV0aG9kcyI6WyJ0cmFkaW5nLWFjY291bnQtbWFuYWdlbWVudC1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciIsIndyaXRlciJdLCJyZXNvdXJjZXMiOlsiYWNjb3VudDokVVNFUl9JRCQ6ZmNiMDVlY2UtYTFmNS00MDY3LWI0ZGMtM2IzNGE3Y2RhMzA1Il19LHsiaWQiOiJtZXRhYXBpLXJlc3QtYXBpIiwibWV0aG9kcyI6WyJtZXRhYXBpLWFwaTpyZXN0OnB1YmxpYzoqOioiXSwicm9sZXMiOlsicmVhZGVyIiwid3JpdGVyIl0sInJlc291cmNlcyI6WyJhY2NvdW50OiRVU0VSX0lEJDpmY2IwNWVjZS1hMWY1LTQwNjctYjRkYy0zYjM0YTdjZGEzMDUiXX0seyJpZCI6Im1ldGFhcGktcnBjLWFwaSIsIm1ldGhvZHMiOlsibWV0YWFwaS1hcGk6d3M6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiLCJ3cml0ZXIiXSwicmVzb3VyY2VzIjpbImFjY291bnQ6JFVTRVJfSUQkOmZjYjA1ZWNlLWExZjUtNDA2Ny1iNGRjLTNiMzRhN2NkYTMwNSJdfSx7ImlkIjoibWV0YWFwaS1yZWFsLXRpbWUtc3RyZWFtaW5nLWFwaSIsIm1ldGhvZHMiOlsibWV0YWFwaS1hcGk6d3M6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiLCJ3cml0ZXIiXSwicmVzb3VyY2VzIjpbImFjY291bnQ6JFVTRVJfSUQkOmZjYjA1ZWNlLWExZjUtNDA2Ny1iNGRjLTNiMzRhN2NkYTMwNSJdfSx7ImlkIjoibWV0YXN0YXRzLWFwaSIsIm1ldGhvZHMiOlsibWV0YXN0YXRzLWFwaTpyZXN0OnB1YmxpYzoqOioiXSwicm9sZXMiOlsicmVhZGVyIl0sInJlc291cmNlcyI6WyJhY2NvdW50OiRVU0VSX0lEJDpmY2IwNWVjZS1hMWY1LTQwNjctYjRkYy0zYjM0YTdjZGEzMDUiXX0seyJpZCI6InJpc2stbWFuYWdlbWVudC1hcGkiLCJtZXRob2RzIjpbInJpc2stbWFuYWdlbWVudC1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciIsIndyaXRlciJdLCJyZXNvdXJjZXMiOlsiYWNjb3VudDokVVNFUl9JRCQ6ZmNiMDVlY2UtYTFmNS00MDY3LWI0ZGMtM2IzNGE3Y2RhMzA1Il19LHsiaWQiOiJtZXRhYXBpLXJlYWwtdGltZS1zdHJlYW1pbmctYXBpIiwibWV0aG9kcyI6WyJtZXRhYXBpLWFwaTp3czpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciIsIndyaXRlciJdLCJyZXNvdXJjZXMiOlsiYWNjb3VudDokVVNFUl9JRCQ6ZmNiMDVlY2UtYTFmNS00MDY3LWI0ZGMtM2IzNGE3Y2RhMzA1Il19LHsiaWQiOiJjb3B5ZmFjdG9yeS1hcGkiLCJtZXRob2RzIjpbImNvcHlmYWN0b3J5LWFwaTpyZXN0OnB1YmxpYzoqOioiXSwicm9sZXMiOlsicmVhZGVyIiwid3JpdGVyIl0sInJlc291cmNlcyI6WyJhY2NvdW50OiRVU0VSX0lEJDpmY2IwNWVjZS1hMWY1LTQwNjctYjRkYy0zYjM0YTdjZGEzMDUiXX1dLCJ0b2tlbklkIjoiMjAyMTAyMTMiLCJpbXBlcnNvbmF0ZWQiOmZhbHNlLCJyZWFsVXNlcklkIjoiNjMxNjAzNTM4NDNiZGYwNjBlNDI2NjkxN2ZmMWE1YmUiLCJpYXQiOjE3MjY1NDg5NjksImV4cCI6MTcyOTE0MDk2OX0.M6IDnFN_MFEbvPZ7tH1QK8qq5j1GmFww5HDUdJ5RDaF6e29EgBCh0xtRHdaZcb48x2s8_65RQ4a9DN93zzKFy2U1fe-cvGL66tukgpW5ldedMwqxqR6QtV3yfhu34GGP2lqvDN1yLVzbWu8bmT5pNKJUsi58YLFOYBYiScHZyi654nR7H3b6fozKABsIlZlJ9WC8Jr3quLljWVstCWA-88hwSYHre4LxL7UPWhjBOZTuTrpLgpV8QI6BC1BucYTq4R7Tz8j3Gv2-KAXcf1MKqSMWLAR4e7QHZyWEmJoaoauHwFSmDpABlkGzEC-2fBHXvYhpLStE7ck2cbmIfMCn02Eap0Fm3okGqa5H_gamc8SmBGrf-4Gb1-QFHkR76igTjld7Asm-QYZtiXXaVcsnEcgykcJOoLsTyQBBujTfvF1aBS_uZrFf10iQ1Rkf7MIX-gZU9B1uGRfAwcwQtUDQEbQ8hs_S8dqH9NJ5lC87aIH83QEvjfl940mM_SbfIyn1FK7xXSNxl1qqB_0qm5w_sANTfisq2beEm-JqMsx11e9zo_pC51UFFOg4nhFAijIee9SYfEYeFB0Rfh6tZ02uOE66AY4oxHm6dwZSH_h9Ya5BQfvm9cwFmnjoPDFOIGIL5GurE3L3e6HqW5Gr_7a7oW5CVdpLTN-cxGFkHWMmJxE'
ACCOUNT_ID = os.getenv('ACCOUNT_ID') or 'fcb05ece-a1f5-4067-b4dc-3b34a7cda305'

# Retry logic in case of failures, with reduced retries for a constrained environment
@retry(wait=wait_fixed(3), stop=stop_after_attempt(2), reraise=True)
async def get_account(api, account_id):
    try:
        account = await api.metatrader_account_api.get_account(account_id)
        logger.info(f"Successfully fetched account details for {account_id}")
        return account
    except Exception as e:
        logger.error(f"Error fetching account details: {e}", exc_info=True)
        raise

def serialize_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")

async def fetch_market_data(ticker):
    logger.info(f"Fetching market data for {ticker}")
    try:
        api = MetaApi(TOKEN)
        account = await get_account(api, ACCOUNT_ID)
        await account.wait_connected()
        connection = account.get_streaming_connection()
        await connection.connect()
        await connection.wait_synchronized()
        await connection.subscribe_to_market_data(ticker)

        terminal_state = connection.terminal_state
        price = terminal_state.price(ticker)
        ticker_specification = terminal_state.specification(ticker)

        market_data = {
            "ticker_info": {
                "symbol": ticker,
                "bid": price.get('bid'),
                "ask": price.get('ask'),
                "timestamp": datetime.now().isoformat()
            },
            "specifications": ticker_specification,
            "account_info": terminal_state.account_information,
            "positions": terminal_state.positions,
            "orders": terminal_state.orders,
            "history_orders": connection.history_storage.history_orders[-5:]
        }

        logger.info(f"Successfully fetched market data for {ticker}")
        return json.dumps(market_data, default=serialize_datetime, indent=2)

    except Exception as err:
        logger.error(f"Error fetching market data for {ticker}: {err}", exc_info=True)
        return json.dumps({"error": str(err)}, indent=2)

# Flask application
app = Flask(__name__)

@app.route('/fetch_market_data', methods=['GET'])
async def fetch_market_data_route():
    ticker = request.args.get('ticker')
    if not ticker:
        return jsonify({"error": "Ticker parameter is required"}), 400

    result = await fetch_market_data(ticker)
    return jsonify(json.loads(result))

if __name__ == "__main__":
    app.run(debug=True)