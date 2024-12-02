import requests


def extract(
    url="https://raw.githubusercontent.com/nogibjj/ag825_sqlite_lab/refs/heads/main/Cancer_Data.csv",
    file_path="Cancer_Data.csv",
):
    with requests.get(url, timeout=10) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)
    return file_path
