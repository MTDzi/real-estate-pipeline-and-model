import click
import json
import requests
from typing import List, Dict, Any, Tuple

# import pyproj
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


TILE_SIDE = 200
MAP_URL = 'http://mapa.um.warszawa.pl/mapviewer/foi'

# Roughly Warsaw box coordinates
BOUND_UP = (7489046, 5801540)
BOUND_DOWN = (7518990, 5774703)


def fix_json(request_data: str) -> str:
    data = request_data.replace('id:', '"id": ')
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


def get_payload(bbox: List) -> Dict[str, Any]:
    return {
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


def get_data_chunk(bbox: List, map_url: str) -> str:
    payload = get_payload(bbox)
    response = requests.get(map_url, params=payload)
    return response.text


def project(proj: pyproj.Proj, x: float, y: float) -> Tuple[float, float]:
    if np.isnan(x) or np.isnan(y):
        return np.nan, np.nan
    lon, lat = proj(x, y, inverse=True)
    return lon, lat


@click.command()
@click.option('--map_url', '-m', type=str, required=True)
@click.option('--projection_recipe', '-p', type=str, required=True)
@click.option('--tile_side', '-t', type=str, required=True)
@click.option('--bound_up', '-u', type=str, requests=True)
@click.option('--bound_down', '-d', type=str, requests=True)
@click.option('--csv_dump_filename', '-o', type=str, requests=True)
def scrape_population_density(
        map_url: str,
        projection_recipe: str,
        tile_side: int,
        bound_up: List[int],
        bound_down: List[int],
        csv_dump_filename: str,
) -> None:
    proj = pyproj.Proj(projection_recipe)
    data = []
    for long in range(bound_up[0], bound_down[0], tile_side):
        for lat in reversed(range(bound_down[1], bound_up[1], tile_side)):
            bbox = [
                long,             lat - tile_side,
                long + tile_side, lat
            ]
            bbox = list(map(float, bbox))
            req_data = get_data_chunk(bbox, map_url)
            data_str = fix_json(req_data)
            list_of_data = json.loads(data_str)['foiarray']

            new_row = {}
            new_row['y'] = long + tile_side / 2
            new_row['x'] = lat - tile_side / 2
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

    frame = pd.DataFrame(data)

    frame[['lon', 'lat']] = (
        frame
        .apply(lambda row: project(proj, row['y'], row['x']), axis=1)
        .apply(pd.Series)
    )

    frame.to_csv(csv_dump_filename, index=False)


if __name__ == '__main__':
    pass