import os
import shutil
import openpyxl


def format_excel(excel_path):

    wb = openpyxl.load_workbook(filename=excel_path)
    print wb.get_sheet_names()


if __name__ == '__main__':
    root_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    output_dir = os.path.join(root_dir, 'output')

    src_file = os.path.join(output_dir, 'tree_cover_stats_2015.xlsx')
    path_to_excel = os.path.join(output_dir, 'dummy.xlsx')

    if os.path.exists(path_to_excel):
        os.remove(path_to_excel)

    shutil.copy2(src_file, path_to_excel)

    format_excel(path_to_excel)