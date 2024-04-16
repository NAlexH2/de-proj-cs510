import os, sys, tarfile


script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))

if "/src" in script_dir:
    from vars import DATA_MONTH_DAY, DATA_FOLDER, FULL_DATA_PATH
else:
    from src.vars import DATA_MONTH_DAY, DATA_FOLDER, FULL_DATA_PATH


def tar_data():
    print("\n")
    tar = tarfile.open(f"{FULL_DATA_PATH}.tar", "w")
    files = os.listdir(f"{FULL_DATA_PATH}")
    files.sort()
    for file in files:
        tar.add(FULL_DATA_PATH + "/" + file, arcname=DATA_MONTH_DAY + "/" + file)
    tar.close()


def data_emailer(ok_size: int, bad_size: int) -> None:
    email = "nharris@pdx.edu"
    message = """
    Subject: test!
        
    This was a test :) from yourself :)"""


if __name__ == "__main__":
    # tar_data()
    data_emailer(0, 0)
