import os
import shutil
from openpyxl.styles import Border, Side
from openpyxl import load_workbook


def format_excel(excel_path):

    wb = load_workbook(filename=excel_path)
    sheet_list = wb.get_sheet_names()
    
    # todo
    # add new front sheet (copy from somewhere?)
    # fix float format (and pct format?)
    # highlight top left cell
    
    thin = Side(border_style="thin", color="000000")
    border = Border(top=thin, left=thin, right=thin, bottom=thin)
    
    for sheet in sheet_list:
        ws = wb.get_sheet_by_name(sheet)
        
        # bold top row
        # for cell in ws[1]:
        #    cell.font = cell.font.copy(bold=True)
        
        # bold and do border for left-most row
        # for cell in ws['A']:
        #    cell.font = cell.font.copy(bold=True)
            
        #    cell.border = border
                
    
    wb.save(excel_path)
    


if __name__ == '__main__':
    root_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    output_dir = os.path.join(root_dir, 'output')

    src_file = os.path.join(output_dir, 'tree_cover_stats_2015.xlsx')
    path_to_excel = os.path.join(output_dir, 'dummy.xlsx')

    if os.path.exists(path_to_excel):
        os.remove(path_to_excel)

    shutil.copy2(src_file, path_to_excel)

    format_excel(path_to_excel)