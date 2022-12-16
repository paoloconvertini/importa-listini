import os
import pandas as pd
import yaml
import shutil
import logging


def get_list():
    lista = []
    for key in mappa.keys():
        if "aumento_perc" != key:
            lista.append(key)
    return lista


def process_sheet(df, new_dict, fornitore, key):
    for k, v in new_dict.items():
        if "|" in v:
            splitted = v.split("|")
            df[k] = df[splitted[0]] + " " + df[splitted[1]].astype(str)
            df.drop(columns=[splitted[0], splitted[1]], inplace=True)
        elif "unitamisura" == k or "unitamisura2" == k or "unitamisuraSec" == k:
            list_to_replace = ['M²', 'MIX 2', 'MIX 3', 'MIX 4', 'MIX 5', 'MIX 6', 'PZ PC']
            new_list = ['MQ', 'PZ', 'PZ', 'PZ', 'PZ', 'PZ', 'PZ']
            df[k] = df[v].replace(list_to_replace, new_list)
            df.drop(columns=[v], inplace=True)
        elif "aumento_perc" == k:
            df["Prezzo"] = df.apply(lambda row: row.v + (row.v * v), axis=1)
            df.drop(columns=[v], inplace=True)
        else:
            df.rename(columns={v: k}, inplace=True)
    if "aumento_perc" in mappa.keys():
        df["Prezzo"] = df["Prezzo"] + (df["Prezzo"] * mappa["aumento_perc"] / 100)
    new_header = []
    for h in header:
        if h not in new_dict:
            new_header.append(h)
    for he in new_header:
        df[he] = ""
    df["Costo"] = df["Prezzo"]
    df["quantitauser01"] = df["Prezzo"]
    df["quantitauser02"] = df["Prezzo"]
    df = df[header]
    df.to_csv(output_dir + "/" + fornitore + "/" + fornitore + "_" + key + ".csv", index=False, sep="	")


def read_excel_file(file, fornitore):
    new_dict = {}
    for k, v in mappa.items():
        if v in new_dict:
            new_dict[v] += "|" + k
        else:
            new_dict[v] = k
    data = pd.read_excel(file, sheet_name=None, nrows=30)
    for key in data:
        data_sheet = data[key]
        header_loc = data_sheet[data_sheet == list(mappa.keys())[0]].dropna(axis=1, how='all').dropna(how='all')
        if not header_loc.empty:
            row = header_loc.index.item()
            new_data_sheet = pd.read_excel(file, sheet_name=0, skiprows=row+1, usecols=get_list())
        else:
            new_data_sheet = pd.read_excel(file, sheet_name=0, usecols=get_list())
        process_sheet(new_data_sheet, new_dict, fornitore, key)


def read_fornitore_folder(fornitore):
    dir_fornitore = input_dir + '/' + fornitore
    for file in os.listdir(dir_fornitore):
        file_sr_path = dir_fornitore + "/" + file
        file_dt_path = imported_dir + '/' + file
        try:
            if file.endswith(".xlsx") or file.endswith(".xlx"):
                try:
                    read_excel_file(file_sr_path, fornitore)
                    shutil.move(file_sr_path, file_dt_path)
                except Exception as ex:
                    logger.error("Error reading file!", ex)
        except Exception as e:
            logger.error("No files found here!", e)


def execute():
    global logger
    logger = logging.getLogger('Importa_Log')
    logger.setLevel(logging.ERROR)
    fh = logging.FileHandler('importaListini.log')
    fh.setLevel(logging.ERROR)
    logger.addHandler(fh)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    with open('application.yml') as f:
        logger.debug('Inizio importazione')
        data = yaml.load(f, Loader=yaml.FullLoader)
        global input_dir
        global output_dir
        global imported_dir
        global fornitori_mapper
        global header
        global mappa
        input_dir = data['properties']['input_dir']
        output_dir = data['properties']['output_dir']
        imported_dir = data['properties']['imported_dir']
        fornitori_mapper = data['properties']['fornitorimapper']
        header = data['properties']['header']
    for fornitore in fornitori_mapper:
        mappa = fornitori_mapper[fornitore]
        read_fornitore_folder(fornitore)


if __name__ == '__main__':
    execute()
