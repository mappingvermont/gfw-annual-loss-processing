import argparse
import os
import subprocess
from subprocess import Popen, PIPE

from utilities import batch_util


def main():
    '''
    run this to make the cumsum files from the original hadoop output. loops over all possible files, check for their
    processed file, if it isn't there, run the tabulate-and-push.py script. csv's with extra boundary are hard coded and
    referenced to add -b to command.
    '''
       
    parser = argparse.ArgumentParser(description='write the cum sum csv for all hadoop outputs on s3')
    parser.add_argument('--analysis', '-a', required=True, help='loss or extent')
    
    args = parser.parse_args()
    
    analysis_type = args.analysis

    # determine if we are processing a single csv or a folder
    unprocessed_data = batch_util.set_source(analysis_type)

    unprocessed_file = 'alerts-tsv/output2016/{0}/{1}'
    processed_dir = 'alerts-tsv/output2016/{}/processed'.format(analysis_type)

    # list contents of the unprocssed csv outputs
    for x in unprocessed_data:

        in_csv = x.split("/")[-1]
        print "\n{}".format(in_csv)
        
        # get name of processed csv to check if it exists
        out_csv = batch_util.set_out_csv(analysis_type, in_csv)

        if not batch_util.check_output_exists(processed_dir, out_csv):
            
            s3_in_csv_path = 's3://gfw2-data/{}'.format(unprocessed_file.format(analysis_type, in_csv))
            cmd = ['python', 'tabulate-and-push.py', '-i', s3_in_csv_path, '--local']
            
            # check if this file has that extra boundary column
            if in_csv.strip(".csv") in batch_util.extra_boundary():
                cmd += ['-b', 'bound', 'ISO', 'adm1', 'adm2']
                
            if analysis_type == 'extent':
                cmd += ['--no-emissions', '--no-years']

            print "...processing"
            p = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
          
            # check if final output worked, if so, copy output to s3
            final_output = batch_util.check_processed(p)
            
            if final_output:
                
                # copy to s3
                s3_processed_dir = 's3://gfw2-data/{}'.format(processed_dir)
                
                # the tabulate and push output doens't give extent outputs a name, so making that here
                if analysis_type == 'extent':
                    s3_processed_dir = 's3://gfw2-data/{0}/{1}'.format(processed_dir, out_csv)
                    
                cmd = ['aws', 's3', 'cp', final_output, s3_processed_dir]
                print "...uploading"
                subprocess.check_call(cmd)
                
            else:
                print "cum sum didn't work"
                   
        else:
            print "...already exists.".format(out_csv)

if __name__ == "__main__":
    main()            