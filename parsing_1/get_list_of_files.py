import os
import py7zr


path = r'example\path'

def check_file(path):
    list_of_files = []
    def recur(path):
        nonlocal list_of_files
        file_name, file_extension = os.path.splitext(path)
        if file_extension in ('.xls', '.xlsx'):
            if file_name.lower().split('\\')[-1].startswith('7б'):
                list_of_files.append(path)
        elif os.path.isdir(path):
            current_files = os.listdir(path)
            for directory in [os.path.join(path, el) for el in current_files]:
                recur(directory)
        elif os.path.splitext(path)[1] == '.7z':
            try:
                with py7zr.SevenZipFile(path, mode='r') as archieve:
                    archieve.extractall('to_parse')
            except:
                pass
    def recur_2(path):
        nonlocal list_of_files
        file_name, file_extension = os.path.splitext(path)
        if file_extension in ('.xls', '.xlsx'):
            if file_name.lower().split('\\')[-1].startswith('7б'):
                list_of_files.append(path)
        elif os.path.isdir(path):
            current_files = os.listdir(path)
            for directory in [os.path.join(path, el) for el in current_files]:
                recur_2(directory)
    recur(path)
    recur_2('to_parse')
    return list_of_files


with open('objects.txt', 'w', encoding='utf-8') as f:
    for row in check_file(path):
        f.write(row + '\n\n')
