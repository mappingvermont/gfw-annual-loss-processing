# Standard QC of Summary Data Output

In addition to comparison with previous data, run `qc.py /path/to/my/spreadsheet.xlsx` to check the following:

- Area values decline as the extent threshold increases for each country or country_admin row
    - loss_iso
    - loss_subnat
    - extent_iso
    - extent_subnat

- Sum of loss from 2001 - {{max year}} is always less than forest extent area for country or country_admin
    - loss_iso vs extent_iso
    - loss_subnat vs extent_subnat

- Subnat values should add up to nat totals
    - gain_subnat vs gain_iso
    - loss_subnat vs loss_iso
    - extent_subnat vs extent_iso
