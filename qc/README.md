# Standard QC of Summary Data Output

In addition to comparison with previous data, run `qc.py /path/to/my/spreadsheet.xlsx` to check the following:

- Area values decline as the extent threshold increases for each country or country_admin row
    - loss_iso
    - loss_subnat
    - extent_iso
    - extent_subnat

- Sum of loss from 2001 - 2014 is always less than forest extent area for country or country_admin
    - loss_iso vs extent_iso
    - loss_subnat vs extent_subnat

- Subnat values should add up to nat totals
    - gain_subnat vs gain_iso
    - loss_subnat vs loss_iso
    - extent_subnat vs extent_iso


##### Spreadsheet Prep

Unfortunately some manual preprocessing is required to run this QC check. In addition to removing the second header from all sheets, the loss_iso and loss_subnat data is in terrible format for automated analysis. 

Loss_iso and loss_subnat data must be in the following format for this to work:

| Country | Thresh | 2001 | 2002 | 2003 | 2004 | etc |
|---------|--------|------|------|------|------|-----|
| Afghanistan	| 10	| 92 |	190	| 253 |	207 | 246 |
|Akrotiri and Dhekelia	|10|	2	|1|	0 |2 |0|
|Albania	|10|	3816|	909|	636|	3328|719|
|Algeria	|10	|3696	|3144|	3951	|4558|4667|

This is a total pain, but saves a lot of the headache of doing this transformation/parsing this ridiculous format in pandas.

For a full example of a working spreadsheet, see qc_example.xlsx.
