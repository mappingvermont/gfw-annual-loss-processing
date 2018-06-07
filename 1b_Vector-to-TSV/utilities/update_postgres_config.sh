#!/bin/bash

# find what directory this script is in
# from: https://www.cyberciti.biz/faq/linux-unix-shell-script-find-out-in-which-directory-script-file-resides/
script="$0"
basename="$(dirname $script)"
 
psql -f $basename/tune_postgres.sql
sudo service postgresql restart
