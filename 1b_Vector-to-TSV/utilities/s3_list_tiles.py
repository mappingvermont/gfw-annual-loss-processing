from boto.s3.connection import S3Connection
from urlparse import urlparse


conn = S3Connection(host="s3.amazonaws.com")


def find_local_overlap(layer_a, layer_b):

    layer_a_ids = [x.tile_id for x in layer_a.tile_list]
    layer_b_ids = [x.tile_id for x in layer_b.tile_list]

    overlap = set(layer_a_ids).intersection(layer_b_ids)
    
    for l in layer_a, layer_b:
        for i, t in enumerate(l.tile_list):

            if t.tile_id not in overlap:
                del l.tile_list[i]
            

def find_tile_overlap(wildcard_a, wildcard_b, s3_dir, is_test):

    parsed = urlparse(s3_dir)

    # connect to the s3 bucket
    bucket = conn.get_bucket(parsed.netloc)

    # remove leading slash, for some reason
    prefix = parsed.path[1:]

    # loop through file names in the bucket
    full_path_list = [key.name for key in bucket.list(prefix=prefix)]

    # unpack the filename from the list of files
    filename_only_list = [x.split('/')[-1] for x in full_path_list]

    # make dictionary of {'boundary name': [tile ids]}
    boundary_dict = {}

    for boundary in [wildcard_a, wildcard_b]:
        boundary_tiles = []

        for name in filename_only_list:

            # ignore files with multiple __, they're already 
            # combination TSVs (like wdpa__primary__10N_030W.tsv)
	    if len(name.split("__")) > 2:
                pass
            elif name.split("__")[0] == boundary:
                tile_id = name.split("__")[-1:][0].strip(".tsv")
                boundary_tiles.append(tile_id)

        boundary_dict[boundary] = boundary_tiles

    # find tiles that are the same in both lists from the dictionary
    match_list = list(set(boundary_dict[wildcard_a]) & set(boundary_dict[wildcard_b]))

    print boundary_dict[wildcard_a]
    print boundary_dict[wildcard_b]
    if is_test:
        match_list = match_list[0:1]

    return match_list

