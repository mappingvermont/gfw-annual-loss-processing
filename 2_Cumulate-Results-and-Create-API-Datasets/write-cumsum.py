from boto.s3.connection import S3Connection
import shutil
import os
import subprocess
from subprocess import Popen, PIPE

conn = S3Connection(host="s3.amazonaws.com")
bucket = conn.get_bucket('gfw2-data')

'''
run this to make the cumsum files from the original hadoop output. this will loop over all possible files, check for their
processed file, if it isn't there, run the tabulate-and-push.py script. if the input csv has a special extra column, this is
hardcoded, add -b to command.
'''
def check_output_exists(processed_dir, out_csv):

    # loop through file names in the bucket avoiding files within the subdir "processed", where all files end with _processed
    full_path_list = [key.name for key in bucket.list(prefix=processed_dir)]
    
    # unpack the filename from the list of returned processed files
    filename_only_list = [x.split('/')[-1] for x in full_path_list]
    
    return out_csv in filename_only_list

def extra_boundary():
    return ['forest_model_sust_cons_int_diss_gadm28_large_processed.csv', 
        'bra_biomes_int_gadm28.csv', 'wdpa_final_int_diss_wdpaid_gadm28_large.csv', 'fao_ecozones_bor_tem_tro_sub_int_diss_gadm28_large.csv']
        
def check_procssed(p):
    final_output = None
    for line in iter(p.stdout.readline, b''):

        if 'Cumsummed CSV is saved here' in line:
            final_output = line.strip('Cumsummed CSV is saved here:').strip("\r\n")

    return final_output
            
analysis_type = 'loss'
unprocessed_data = [key.name for key in bucket.list(prefix='alerts-tsv/output2016/{}/'.format(analysis_type)) if 'processed' not in key.name]
unprocessed_file = 'alerts-tsv/output2016/{0}/{1}'
processed_dir = 'alerts-tsv/output2016/{}/processed/'.format(analysis_type)
failed_outputs = []


# list contents of the unprocssed csv outputs
for x in unprocessed_data:

    in_csv = x.split("/")[-1]
    print "\n{}".format(in_csv)
    # get name of processed csv to check if it exists
    out_csv = in_csv.replace(".csv", '_processed.csv')
    
    if not check_output_exists(processed_dir, out_csv):
        
        s3_in_csv = unprocessed_file.format(analysis_type, in_csv)
        s3_in_csv_path = 's3://gfw2-data/{}'.format(unprocessed_file.format(analysis_type, in_csv))
        cmd = ['python', 'tabulate-and-push.py', '-i', s3_in_csv_path, '--local']
        
        if in_csv in extra_boundary():
            cmd += ['-b', 'bound', 'ISO', 'adm1', 'adm2']
        print cmd
        print "...processing"
        #subprocess.check_call(cmd)
        p = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
      
        # check if final output worked, if so, copy output to s3
        final_output = check_procssed(p)
        
        if final_output:
            
            # copy to s3
            s3_processed_dir = 's3://gfw2-data/{}'.format(processed_dir)
            cmd = ['aws', 's3', 'cp', final_output, s3_processed_dir]
            print "...uploading"
            subprocess.check_call(cmd)
            
        else:
            print "cum sum didn't work"
            failed_outputs.append(in_csv)
               
    else:
        print "...already exists.".format(out_csv)
        
with open("failed_outputs.txt", 'w') as foutput:
    for i in failed_outputs:
        foutput.write(i+'\n')
        
        
    #foutput.write([failed_ouptuts for i in failed_ouptuts])
    
    

