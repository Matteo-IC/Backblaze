import requests
import os

path = r'D:\Coding\PC Projects\backblaze\data'


def download_zips():
    for year in range(2020, 2024):
        for quarter in range(1, 5):
            url = rf'https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_Q{quarter}_{year}.zip'
            response = requests.get(url, stream=True)

            if response.status_code == 200:
                file_path = os.path.join(path, f'data_Q{quarter}_{year}.zip')
                with open(file_path, 'wb') as file:
                    file.write(response.content)

                print('downloaded')


download_zips()
