import json
import simplejson
import logging

import pandas as pd
import geopandas as gpd
import requests
from rasterstats import zonal_stats

import util


def calc_api(local_geojson, valid_adm2_tuples):

    resp_list = []

    with open(local_geojson) as thefile:
        data = json.load(thefile)

    url = 'https://0yvx7602sb.execute-api.us-east-1.amazonaws.com/dev/umd-loss-gain'

    for feat in data['features']:
        props = feat['properties']

        if (props['iso'], int(props['id_1']), int(props['id_2'])) in valid_adm2_tuples:
            print feat['properties']

            payload = {'geojson': {'features': [feat]}}
            params = {'aggregate_values': False}

            r = requests.post(url, json=payload, params=params)

            valid_zstats = False

            try:
                resp = r.json()
                data = resp['data']['attributes']
                valid_zstats = True

            # catch JSON server error response, also non-JSON response
            except (simplejson.JSONDecodeError, KeyError):
                logging.error('invalid JSON response from Lambda API:')
                logging.error(resp)
                logging.error(feat['properties'])

            if valid_zstats:
                print data

                for loss_year, loss_val in data['loss'].iteritems():
                    resp_dict = {'year': loss_year, 'area_loss': loss_val, 'area_gain': data['gain'],
                                 'area_extent_2000': data['treeExtent'], 'area_poly_aoi': data['areaHa']}

                    row = util.merge_two_dicts(feat['properties'], resp_dict)
                    resp_list.append(row)

    return pd.DataFrame(resp_list)


def calc_rasterio(local_geojson):

    # run the rasterstats process, returing the geojson object with stats
    loss_vrt = r's3://gfw2-data/forest_change/hansen_2017_masked_30tcd/data.vrt'
    kwargs = {'categorical': True, 'geojson_out': True, 'prefix': '_'}
    geojson_with_stats = zonal_stats(local_geojson, loss_vrt, **kwargs)

    # convert to GDF
    df = gpd.GeoDataFrame.from_features(geojson_with_stats)

    # calculate approximate area of pixel for each poly, based on latitude
    # of the polygon's centroid
    df['pixel_area'] = df.apply(lambda row: util.get_pixel_area(row.geometry.centroid.y), axis=1)

    # normalize the df and create a years field
    # make a unique column list, filtering out all the _ prefixed columns
    unique_cols = list([x for x in df.columns if x[0] != '_' and x != 'geometry'])

    df = pd.wide_to_long(df, '_', unique_cols, 'year').reset_index()

    df.year = df.year.astype(int) + 2000
    df = df.rename(columns={'_': 'pixel_count', 'field_7': 'iso', 'field_8': 'id_1', 'field_9': 'id_2'})

    df.id_1 = df.id_1.astype(int)
    df.id_2 = df.id_2.astype(int)

    df['loss_ha'] = df.pixel_area * df.pixel_count

    return df
