import pandas as pd
import numpy as np


def parse_file_1(file_path):
    with pd.ExcelFile(file_path, engine='openpyxl') as file:
        if 'exact_sheet' in file.sheet_names:
            df = pd.read_excel(file, 'exact_sheet', usecols="C,F", header=1)
            df.columns = ['record', 'value']
            df = df.loc[(df.record.str.strip() == 'certain row') & (df.value.map(lambda el: type(el) != str))]
            return df.value.notna().any()

def parse_file_2(file_path):
    with pd.ExcelFile(file_path, engine='openpyxl') as file:
        for sheet in filter(lambda x: 'smth' in x.lower(), [el for el in file.sheet_names]):
            df = pd.read_excel(file, sheet, usecols='A,F:H').to_numpy()
            for counter, row in enumerate(df):
                if row[0] == 'certain row'.strip():
                    counter += 1
                    while counter < df.shape[0]:
                        if df[counter, 0] is not np.nan:
                            if any(el > 0 for el in df[counter, 1:]):
                                return True
                            else: counter += 1
                        else:
                            break
    return False

def get_DataFrame(list_of_filepaths):
    ar = []
    n = len(list_of_filepaths)
    for i, file_path in enumerate(list_of_filepaths, 1):
        ar.append([file_path.split('\\')[-1], parse_file_1(file_path), parse_file_2(file_path)])
        print(f"Progress: {round(i / n * 100, 2)}%")
    ar.append(['Итого', sum([el[1] for el in ar]), sum([el[2] for el in ar])])
    return pd.DataFrame(ar, columns=['Filename', 'col_1', 'col_2'])
def main():
    with open('objects.txt', 'r', encoding='utf-8') as file:
        list_of_files = [el.strip() for el in file.read().split('\n\n') if el != '']
    df = get_DataFrame(list_of_files)
    df.to_excel('res.xlsx')
    print('Конец')

main()