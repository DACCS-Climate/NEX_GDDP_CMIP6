"""
Use this script to download the NEX_GDDP_CMIP6 dataset.

Requirements: gddp-cmip6-files.csv from the S3 server hosting the data. Before
running this script, download this csv as the script needs this file to know
about the URLs to the data files and their checksums.

Warning: This is a 30T dataset! Expect the script to run for a couple days non-stop.
"""

import multiprocessing
import requests
import hashlib
import shutil
import urllib
import os
from datetime import datetime

# TODO: amend code to remove the downlaoded files that fail checksum


def verify_checksum(fname: str, refchecksum: str) -> bool:
    checksum = hashlib.md5(open(fname, "rb").read()).hexdigest()
    return checksum == refchecksum


def download_file(url: str, local_filename: str) -> int:
    resp = requests.get(url, stream=True)

    if resp.status_code == 200:
        with open(local_filename, "wb") as f:
            shutil.copyfileobj(resp.raw, f)

    return resp.status_code


def worker(worker_data: list) -> list[str, str, str]:
    refchecksum, url, local_filename = worker_data
    p = multiprocessing.current_process()
    p = p._identity[0]
    print(f"Process {p:2d}: {local_filename}")

    if os.path.exists(local_filename):
        if verify_checksum(local_filename, refchecksum):
            print(f"Process {p:2d}: Skipping {local_filename}")
            return ["Skip", None, None]

    resp = download_file(url, local_filename)
    if resp == 200:
        return ["Pass" if verify_checksum(local_filename, refchecksum) else "Fail", "checksum", url]
    else:
        return ["Fail", "download", url]


def make_dirs(input_data: list[list[str, str]]) -> list[list[str, str]]:
    for item in input_data:
        url = item[1]
        local_filename = os.path.join("/data/Datasets", urllib.parse.urlparse(url).path.strip("/"))
        local_path = os.path.dirname(local_filename)
        os.makedirs(local_path, exist_ok=True)
        item.append(local_filename)

    return input_data


def read_csv(filename: str) -> list[list[str, str]]:
    with open(S3_csv_filename, "r") as f:
        # Read header
        _ = f.readline()

        input_data = []
        for line in f:
            line = line.strip().split(",")
            item = [line[0].strip(), line[1].strip()]
            input_data.append(item)

    return input_data


def print_diagnostics(diagnostic_data: list) -> None:
    total = 0
    success = 0
    skip = 0
    download_error = 0
    checksum_error = 0
    download_error_files = []
    checksum_error_files = []
    for item in diagnostic_data:
        if item[0] == "Pass":
            success += 1
        elif item[0] == "Fail":
            if item[1] == "download":
                download_error += 1
                download_error_files.append(item[2])
            elif item[1] == "checksum":
                checksum_error += 1
                checksum_error_files.append(item[2])
        elif item[0] == "Skip":
            skip += 0

        total += 1

    print("\n\n\nSUMMARY")
    print(f"Total files attempted: {total}")
    print(f"Files successfully downloaded: {success}")
    print(f"Files that failed download   : {download_error}")
    print(f"Files that failed checksum   : {checksum_error}")
    print(f"Files that were skipped      : {skip}")

    if download_error > 0:
        print("\n\n\nFiles that failed downloading")
        with open(f"download_error_files_{datetime.strftime(datetime.now(), '%Y%m%d')}.txt", "w") as f:
            for item in download_error_files:
                print(item)
                f.write(item + "\n")

    if checksum_error > 0:
        print("\n\n\nFiles that failed checksum")
        with open(f"checksum_error_files_{datetime.strftime(datetime.now(), '%Y%m%d')}.txt", "w") as f:
            for item in checksum_error_files:
                print(item)
                f.write(item + "\n")


def main(filename: str, nprocesses: int) -> None:
    input_data = make_dirs(read_csv(filename))

    pool = multiprocessing.Pool(processes=nprocesses)
    returneddata = pool.map(worker, input_data[1000:])
    pool.close()

    print_diagnostics(returneddata)


if __name__ == "__main__":
    S3_csv_filename = "gddp-cmip6-files.csv"
    # 10 workers seems to be the optimal number of parallel downloads that do not trip S3's rate limiting
    main(S3_csv_filename, 10)
