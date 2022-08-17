# Created by vidit.singh at 28-06-2022
import os


def base_dir():
    return 'D:\\data\\'


def get_json_path(dir_name):
    base_dirs = base_dir()
    json_file = os.listdir(f'{base_dirs}{dir_name}')[0]  # assuming there is only 1 file in directory
    return f'{base_dirs}{dir_name}\\{json_file}'


def output_dir():
    return 'D:\\data\\outputs\\'
