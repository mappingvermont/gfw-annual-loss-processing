# gfw-create-umd-summary-excel

This repo builds a user-friendly representation of the loss data. The currrent spreadsheet can be downloaded [here](http://www.globalforestwatch.org/countries/overview).

Input data for this process need be in a folder named `source` and in the following format:

extent2000.json: 
```
{"data": [{"thresh": 75, "iso": "ZWE", "adm1": 10, "adm2": 60, "area": 14.449537965083993}, {"thresh": 75, "iso": "THA", "adm1": 42, "adm2": 541, "area": 24.96481275555211}, . . . ]}
```

loss.json:
```
{"data": [{"area": 0.0, "thresh": 75, "year": 2015, "iso": "ZWE", "adm1": 10, "adm2": 60}, {"area": 253.3711837744357, "thresh": 75, "year": 2007, "iso": "GTM", "adm1": 8, "adm2": 120}, . . .]}
```

gain.csv
```
JPN,12,501,3050427.177629845
NLD,7,177,43956.34025198901
```

Loss and extent2000 data need to be cumsummed by threshold for this process to work. The repo [here](https://github.com/wri/gfw-tabulate-loss-data) can take care of that if the data is not yet processeed.

Data in these files will be joined by ISO, ADM1 and ADM2 code to the admin names in `adm_lkp.csv`, then written out in various tables to `tree_cover_stats_2015.xlsx`.

The `load_data.py` script must be executed first-- this brings all the source data into a SQLite3 database for easy querying. Given the memory required by this process, it should be run in 64 bit python on a decent machine.

The additional scripts in this directory (`extent_2000_by_iso.py`, `gain.py`), etc, will add worksheets to the output file. Run them all to have a complete output, then follow the steps in the QC README to be sure your output is correct.

For an example of this output, see `example_output.xlsx`.