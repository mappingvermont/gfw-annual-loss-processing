import os
import shutil
from openpyxl.styles import Border, Side
from openpyxl import load_workbook


def format_excel(excel_path):

    wb = load_workbook(filename=excel_path)
    sheet_list = wb.get_sheet_names()
    
    print 'sheet list hardcoded--remove'
    sheet_list = ['Loss (2001-2015) by Subnat1']
    
    # todo
    # add new front sheet (copy from somewhere?)
    # highlight top left cell
    # adjust column spacing -- need to ignore merged cells
    
    for sheet in sheet_list:
        ws = wb.get_sheet_by_name(sheet)
        
        # bold top row
        # for cell in ws[1]:
        #    cell.font = cell.font.copy(bold=True)
        
        # unbold left-most column after first two rows
        for cell in ws['A'][2:]:
            cell.font = cell.font.copy(bold=False)
            
        for row in ws.iter_rows(row_offset=2):
            for cell in row:
                cell.number_format = '#,##0'
        
        for column_cells in ws.columns:
            length = max(len(as_text(cell.value).strip()) for cell in column_cells)
            print column_cells[0].column, length
            ws.column_dimensions[column_cells[0].column].width = length
            
            if column_cells[0].column == 'B':
                for cell in column_cells:
                    print as_text(cell.value), len(as_text(cell.value))
    
    wb.save(excel_path)
    
# http://stackoverflow.com/a/40935194/4355916
def as_text(value): 
    return str(value) if value is not None else ""

if __name__ == '__main__':
    root_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    output_dir = os.path.join(root_dir, 'output')

    #src_file = os.path.join(output_dir, 'tree_cover_stats_2015.xlsx')
    src_file = os.path.join(output_dir, 'tree_cover_stats_2015_IRL.xlsx')
    path_to_excel = os.path.join(output_dir, 'dummy.xlsx')

    if os.path.exists(path_to_excel):
        os.remove(path_to_excel)

    shutil.copy2(src_file, path_to_excel)

    format_excel(path_to_excel)