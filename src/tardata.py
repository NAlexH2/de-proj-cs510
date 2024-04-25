import os, sys, tarfile

script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))

if "/src" in script_dir:
    from utils import DATA_MONTH_DAY, FULL_DATA_PATH, curr_time_micro
else:
    from src.utils import DATA_MONTH_DAY, FULL_DATA_PATH, curr_time_micro


def tar_data():
    file_exists = os.path.exists(f"{FULL_DATA_PATH}.tar")
    if file_exists:
        os.remove(f"{FULL_DATA_PATH}.tar")
    tar = tarfile.open(f"{FULL_DATA_PATH}.tar", "w")
    files = os.listdir(f"{FULL_DATA_PATH}")
    files.sort()
    for file in files:
        file_path = os.path.join(FULL_DATA_PATH, file)
        tar.add(file_path, arcname=file_path)
        print(f"{curr_time_micro()} {file} added to tar.")
    tar.close()
    print(f"{curr_time_micro()} tar for {DATA_MONTH_DAY} complete.")
