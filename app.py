from flask import Flask, request, jsonify, Response
import emmy
import asyncio
import io
import sys
import traceback
import json  # Ensure this import is present

app = Flask(__name__)

@app.route('/')
def index():
    # Redirect stdout to a StringIO object
    old_stdout = sys.stdout
    sys.stdout = mystdout = io.StringIO()

    # Redirect stderr to a StringIO object for error capturing
    old_stderr = sys.stderr
    sys.stderr = mystderr = io.StringIO()

    try:
        # Run the async function
        asyncio.run(emmy.test_meta_api_synchronization())
    except Exception as e:
        # Capture the traceback
        traceback_str = traceback.format_exc()
        print(f"An error occurred: {e}\n{traceback_str}", file=sys.stderr)
    finally:
        # Restore stdout and stderr
        sys.stdout = old_stdout
        sys.stderr = old_stderr

    # Get the output from the StringIO objects
    output = mystdout.getvalue()
    errors = mystderr.getvalue()

    # Combine output and errors
    combined_output = f"Output:\n{output}\n\nErrors:\n{errors}"

    # Return the combined output as a response
    return Response(combined_output, mimetype='text/plain')

@app.route('/fetch_market_data', methods=['GET'])
async def fetch_market_data_route():
    ticker = request.args.get('ticker')
    if not ticker:
        return jsonify({"error": "Ticker parameter is required"}), 400

    try:
        result = await emmy.fetch_market_data(ticker)
        return jsonify(json.loads(result))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)