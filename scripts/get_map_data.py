#!/usr/bin/env python
"""
Scraper that gets the tree data from mapa.um.warszawa.pl for given boundaries.
Note that:
- EPSG:2178 projection is used (http://spatialreference.org/ref/epsg/2178/)
- Coordinates range for Warsaw are simplified to fit in a box
Bxxxxxxxxx
xxxxxxxxxx
xxxxxxxxxx
xxxxxxxxxx
xxxxxxxxxB
"""


import csv
from datetime import datetime
import json
import requests
from typing import List

import pyproj
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


TILE_SIDE = 200

# Roughly Warsaw box coordinates
BOUND_UP = (7489046.2903, 5801540.5856)
BOUND_DOWN = (7518990.0477, 5774703.9076)

proj = pyproj.Proj('+proj=tmerc +lat_0=0 +lon_0=21 +k=0.999923 +x_0=7500000 +y_0=0 +ellps=GRS80 +units=m +no_defs')


def fix_json(reader) -> str:
    print('Fixing json')
    data = reader.replace('id:', '"id": ')
    data = data.replace(',sc"id":', ',"scid":')
    data = data.replace('name:', '"name": ')
    data = data.replace('gtype:', '"gtype": ')
    data = data.replace('imgurl:', '"imgurl": ')
    data = data.replace('x:', '"x": ')
    data = data.replace(',y:', ',"y": ')
    data = data.replace('width:', '"width": ')
    data = data.replace('height:', '"height": ')
    data = data.replace('attrnames:', '"attrnames": ')
    data = data.replace('themeMBR:', '"themeMBR": ')
    data = data.replace('isWholeImg:', '"isWholeImg": ')
    data = data.replace(',rw:', ',"rw":')
    data = data.replace(',rh:', ',"rh":')
    return data


def extract_tree_attributes(name_value: str) -> dict:
    attrs = {}
    field_values = name_value.split('\n')
    for record in field_values:
        record = record.split(': ')
        attrs[record[0]] = record[1]

    return attrs


def parse_tree_data(data: str) -> dict:
    print('Parsing json')
    parsed_data = json.loads(data)
    for tree in parsed_data['foiarray']:
        attrs = extract_tree_attributes(tree['name'])
        tree.update(attrs)
        tree.pop('name')

    return parsed_data


def get_data_chunk(bbox: List) -> str:
    url = 'http://mapa.um.warszawa.pl/mapviewer/foi'
    payload = {
        'request': 'getfoi',
        'version': '1.0',
        'bbox': ':'.join(str('{0:.4f}'.format(c)) for c in bbox),
        'width': 10,
        'height': 10,
        'theme': 'dane_wawa.GESTOSC_ZALUDNIENIA_2018',
        'clickable': 'yes',
        'area': 'yes',
        'dstsrid': '2178',
        'cachefoi': 'yes',
        'aw': 'no',
        'tid': '649_58860',
    }
    response = requests.get(url, params=payload)
    return response.text


def project(proj, x, y):
    if np.isnan(x) or np.isnan(y):
        return np.nan
    lon, lat = proj(x, y, inverse=True)
    return lon, lat


def save_to_csv(tree_data: dict, bbox: List) -> None:
    print('Saving csv')
    fieldnames = [
        'Aktualność danych na dzień',
        'Jednostka zarządzająca',
        'Nazwa polska',
        'Nazwa łacińska',
        'Numer inwentaryzacyjny',
        'Obwód pnia w cm',
        'Wysokość w m',
        'gtype',
        'height',
        'id',
        'imgurl',
        'width',
        'x',
        'y',
    ]

    today = datetime.now().strftime('%Y-%m-%d')
    with open(f'data/trees_{today}.csv', 'w') as desc:
        writer = csv.DictWriter(desc, fieldnames=fieldnames)
        writer.writeheader()
        for row in tree_data:
            writer.writerow(row)


def main() -> None:
    data = []
    counter = 0
    len1 = len(list(range(int(BOUND_UP[0]), int(BOUND_DOWN[0]), TILE_SIDE)))
    len2 = len(list(range(int(BOUND_DOWN[1]), int(BOUND_UP[1]), TILE_SIDE)))
    for long in range(int(BOUND_UP[0]), int(BOUND_DOWN[0]), TILE_SIDE):
        for lat in reversed(range(int(BOUND_DOWN[1]), int(BOUND_UP[1]), TILE_SIDE)):
            bbox = [
                long, lat - TILE_SIDE,
                long + TILE_SIDE, lat
            ]
            bbox = list(map(float, bbox))
            req_data = get_data_chunk(bbox)
            data_str = fix_json(req_data)
            list_of_data = json.loads(data_str)['foiarray']
            new_row = {}
            new_row['y'] = long + TILE_SIDE / 2
            new_row['x'] = lat - TILE_SIDE / 2
            for one_dict in list_of_data:
                if 'imgurl' not in one_dict:
                    continue

                img = plt.imread(one_dict['imgurl'])
                r, g, b = img[:, :, 1:].mean(axis=0).mean(axis=0)
                new_row['r'] = new_row.get('r', 0) + r
                new_row['g'] = new_row.get('g', 0) + g
                new_row['b'] = new_row.get('b', 0) + b

            if list_of_data:
                for key in ['r', 'g', 'b']:
                    new_row[key] /= len(list_of_data)

            data.append(new_row)
            print(f'{counter} out of {len1*len2}')
            counter += 1

    frame = pd.DataFrame(data)

    frame[['lon', 'lat']] = frame.apply(lambda row: project(row['y'], row['x']), axis=1).apply(pd.Series)

    frame.to_csv('frame.csv', index=False)


if __name__ == '__main__':
    main()


