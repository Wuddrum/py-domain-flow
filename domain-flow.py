#!/usr/bin/env python

import sys
import json
import gzip
import getopt
import urllib.request

from itertools import groupby

separator = ' > '
input_ = None
output = None

def main():
    global input_, output

    read_args()

    data = fetch_data(input_)
    data_json = json.loads(data)

    paths = get_paths(data_json)
    generate_output(output, paths)

def fetch_data(url):
    response = urllib.request.urlopen(url)
    data = response.read()
    if response.info().get('Content-Encoding') == 'gzip':
        data = gzip.decompress(data)

    return data

def generate_output(output, paths):
    json_obj = { 'paths': [] }

    for _, path in paths.items():
        json_obj['paths'].append(path)

    with open(output, 'w') as output_file:
        json.dump(json_obj, output_file)

    return

def get_empty_path_object():
    return {
        'name': '',
        'uniqueHits': 0,
        'totalHits': 0,
        'domains': [],
        'users': []
    }

def get_path_name(path_domains):
    global separator
    return separator.join(path_domains)

def extract_user_paths(chronological_domains):
    user_paths = []
    current_path_domains = []

    for domain, _ in groupby(chronological_domains, key=lambda x:x[0]):
        if domain in current_path_domains:
            path_name = get_path_name(current_path_domains)
            user_paths.append([path_name, current_path_domains])
            current_path_domains = []
        current_path_domains.append(domain)

    if (len(current_path_domains) > 1):
        path_name = get_path_name(current_path_domains)
        user_paths.append([path_name, current_path_domains])

    return user_paths

def get_paths(data_json):
    paths = {}
    user_buckets = data_json['aggregations']['UserID']['buckets']
    for user_bucket in user_buckets:
        user_domains = []

        for origin_bucket in user_bucket['Origin']['buckets']:
            for date_bucket in origin_bucket['Date']['buckets']:
                user_domains.append([origin_bucket['key'], date_bucket['key']])

        chronological_domains = sorted(user_domains, key=lambda x:x[1])
        user_paths = extract_user_paths(chronological_domains)

        unique_paths = []
        for path_name, path_domains in user_paths:
            if path_name not in paths:
                paths[path_name] = get_empty_path_object()

            path = paths[path_name]
            path['users'].append(user_bucket['key'])
            path['name'] = path_name   
            path['domains'] = path_domains
            path['totalHits'] += 1

            if path_name not in unique_paths:
                path['uniqueHits'] += 1
                unique_paths.append(path_name)

            paths[path_name] = path

    return paths

def read_args():
    global input_, output, separator

    try:
        opts, _ = getopt.getopt(sys.argv[1:], 'i:o:s:h', ['input=', 'output=', 'separator=', 'help'])
    except getopt.GetoptError as error:
        print(error)
        print_usage()
        sys.exit(1)

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print_usage()
            sys.exit()
        elif opt in ('-i', '--input'):
            input_ = arg
        elif opt in ('-o', '--output'):
            output = arg
        elif opt in ('-s', '--separator'):
            separator = arg

    if not input_ or not output:
        print_usage()
        sys.exit(1)

def print_usage():
    print('Usage: domain-flow.py [-i] [-o]')
    print('Required arguments:')
    print('-i, --input      URL of the input JSON file')
    print('-o, --output     Absolute or relative path/name for the output JSON file')
    print()
    print('Optional arguments:')
    print('-s, --separator  Specifies the separator used for path names (default: " > ")')
    print('-h, --help       Show this help message and exit')

if __name__ == '__main__':
    main()
