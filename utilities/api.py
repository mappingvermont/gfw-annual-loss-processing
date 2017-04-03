from requests import Request, Session

import util


def create(s3_url, environment, tags, dataset_name):

    api_url, headers = get_url_and_headers(environment)

    s3_url_split = s3_url.split(r'/')
    bucket_name = s3_url_split[2]

    dataset_path = r'/'.join(s3_url_split[3:])
    http_url = r'http://{0}.s3.amazonaws.com/{1}'.format(bucket_name, dataset_path)

    if environment == 'staging':
        datasets_url = r'{0}/v1/dataset'.format(api_url)
        status_code_result = 200

        payload = {
                       "name": dataset_name,
                       "application": ["gfw"],
                       "connectorType": "document",
                       "provider": "json",
                       "connectorUrl": http_url,
                       "dataPath": "data",
                       "tags": tags
                    }
    else:
        datasets_url = r'{0}/dataset'.format(api_url)
        status_code_result = 201

        payload = {
            "dataset":
                {
                    "connectorType": "json",
                    "provider": "rwjson",
                    "dataPath": "data",
                    "application": ["gfw"],
                    "name": dataset_name,
                    "tags": tags,
                    "connectorUrl": http_url
                },
        }

    dataset_id = make_request(headers, datasets_url, 'POST', payload, status_code_result, ['data', 'id'])

    print 'Created new dataset:\n{0}'.format(dataset_id)

    return dataset_id


def overwrite(s3_url, environment, dataset_id):

    api_url, headers = get_url_and_headers(environment)

    print 'Overwriting dataset: {0}'.format(dataset_id)
    dataset_url = r'{0}/dataset/{1}'.format(api_url, dataset_id)

    modify_attributes_payload = {"dataset": {"overwrite": True}}
    make_request(headers, dataset_url, 'PATCH', modify_attributes_payload, 200)

    data_overwrite_url = r'{0}/data-overwrite'.format(dataset_url)
    overwrite_payload = {"url": s3_url, "dataPath": "data", "provider": "json"}

    make_request(headers, data_overwrite_url, 'POST', overwrite_payload, 200)


def make_request(headers, api_endpoint, request_type, payload, status_code_required, json_map_list=None):

    s = Session()
    req = Request(request_type, api_endpoint, json=payload, headers=headers)

    prepped = s.prepare_request(req)
    r = s.send(prepped)

    if r.status_code == status_code_required:

        if json_map_list:
            # http://stackoverflow.com/questions/14692690
            return_val = reduce(lambda d, k: d[k], json_map_list, r.json())

        else:
            return_val = r.json()

        return return_val

    else:
        print r.text
        print r.status_code
        raise ValueError("Request failed")


def get_url_and_headers(environment):

    if environment == 'prod':
        api_url = r'http://production-api.globalforestwatch.org'
        token = util.load_json_from_token('dataset_api_creds.json')['token']

    else:
        api_url = r'http://staging-api.globalforestwatch.org'
        token = util.load_json_from_token('dataset_api_creds_staging.json')['token']

    headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer {0}'.format(token)}

    return api_url, headers
