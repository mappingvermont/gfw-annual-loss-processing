from boto.s3.connection import S3Connection
# connect to the s3 bucket
conn = S3Connection(host="s3.amazonaws.com")
bucket = conn.get_bucket('gfw2-data')
    
    
def check_output_exists(processed_dir, out_csv):

    # loop through file names in the bucket
    full_path_list = [key.name for key in bucket.list(prefix=processed_dir)]

    # unpack the filename from the list of returned processed files
    filename_only_list = [x.split('/')[-1] for x in full_path_list]
    
    return out_csv in filename_only_list
    
    
def extra_boundary():
    return ['forest_model_sust_cons_int_diss_gadm28_large_processed', 'tiger_cons_landscapes_reg_and_tx2_int_diss_gadm28', 
    'bra_biomes_int_gadm28', 'wdpa_final_int_diss_wdpaid_gadm28_large', 'fao_ecozones_bor_tem_tro_sub_int_diss_gadm28_large']
 
 
def check_processed(p):
    final_output = None
    for line in iter(p.stdout.readline, b''):

        if 'Cumsummed CSV is saved here' in line:
            final_output = line.strip('Cumsummed CSV is saved here:').strip("\r\n")

    return final_output

def set_source_extent():
    s3_source = []
        
    for x in unprocessed_data:
        x.split("/")[:-1]
        folder_name = x.split("/")[:-1]
        if "/".join(folder_name) not in s3_source:
            s3_source.append("/".join(folder_name))
    return s3_source
        
def set_source(analysis_type):

    # since there is a folder in output2016/analysistype/ called "processed" with all the processed files, need to ignore these
    unprocessed_data = [key.name for key in bucket.list(prefix='alerts-tsv/output2016/{}/'.format(analysis_type)) if 'processed' not in key.name]
    
    # get the list of folders for extent
    if analysis_type == 'extent':
        s3_source = set_source_extent()
        
    else:
        s3_source = unprocessed_data
                
    return s3_source

    
def set_out_csv(analysis_type, in_csv):
    if analysis_type == 'extent':
        out_csv = in_csv + "_processed.csv"
    else:
        out_csv = in_csv.replace(".csv", '_processed.csv')
        
    return out_csv
    