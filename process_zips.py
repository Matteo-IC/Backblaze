import polars as pl
import os
import zipfile
import polars.exceptions


def select_zip():
    path = r'D:\Coding\PC Projects\backblaze\data'
    for year in range(2013, 2024):
        if year <= 2015:
            file_path = os.path.join(path, f'data_{year}.zip')
            unique_df = process_csv(file_path)
            main_df.vstack(unique_df, in_place=True)
        else:
            for quarter in range(1, 5):
                file_path = os.path.join(path, f'data_Q{quarter}_{year}.zip')
                unique_df = process_csv(file_path)
                main_df.vstack(unique_df, in_place=True)


def process_csv(file_path):
    with zipfile.ZipFile(file_path) as zip_file:
        csv_files = [filename for filename in zip_file.namelist() if
                     'DS_Store' not in filename and
                     '__MACOSX' not in filename and
                     'csv' in filename]

        this_zip_df = pl.DataFrame({'date': '2024-12-15',
                                    'serial_number': 'some_serial',
                                    'model': 'model_name',
                                    'failure': 0,
                                    'capacity_bytes': 3000592982016,
                                    'smart_9_raw': 9000})
        for csv in csv_files:
            with zip_file.open(csv) as csv_file:
                print(csv)
                df = pl.read_csv(csv_file).select(pl.col('date', 'serial_number', 'model', 'failure', 'capacity_bytes', 'smart_9_raw'))
                try:
                    this_zip_df.vstack(df, in_place=True)
                except polars.exceptions.SchemaError:
                    print('FILE CAUSED ISSUE')
                    continue

        unique_df_this_zip = this_zip_df.unique(subset=['serial_number', 'failure'], keep='first')
        return unique_df_this_zip


if __name__ == "__main__":
    main_df = pl.DataFrame({'date': '2024-11-15',
                            'serial_number': 'some_serial',
                            'model': 'model_name',
                            'failure': 0,
                            'capacity_bytes': 3000592982016,
                            'smart_9_raw': 9000})
    select_zip()
    newer = main_df.unique(subset=['serial_number', 'failure'], keep='first')
    newer.write_csv('fih.csv')
