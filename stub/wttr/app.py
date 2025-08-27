#!/usr/bin/env python3
"""WTTR.IN stub service for testing pipelines.

This service responds to GET /<city>?format=j1 with a static sample JSON.
"""

import argparse
import json
from flask import Flask, request, jsonify, abort

app = Flask(__name__)


def load_data(data_file: str):
    with open(data_file, 'r') as f:
        return json.load(f)


@app.route('/<path:city>')
def wttr(city):
    fmt = request.args.get('format', '')
    if fmt != 'j1':
        abort(404)
    return jsonify(DATA)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='WTTR.IN stub service')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--port', type=int, default=5000, help='Port to listen on')
    parser.add_argument(
        '--data-file',
        default='data/j1_basic.json',
        help='Path to JSON file with sample WTTR.IN j1 response',
    )
    args = parser.parse_args()
    DATA = load_data(args.data_file)
    app.run(host=args.host, port=args.port)