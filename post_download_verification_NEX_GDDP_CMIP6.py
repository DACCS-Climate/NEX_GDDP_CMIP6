"""
Run this script after the download process to check if all data files in the dataset have been
correctly downloaded.
"""

import os
import hashlib
from datetime import datetime
import multiprocessing


def verify_checksum(fname: str, refchecksum: str) -> bool:
    checksum = hashlib.md5(open(fname, "rb").read()).hexdigest()
    return checksum == refchecksum


def worker(input_data: list[str, str]) -> list[str, str]:
    checksum, filename = input_data

    if not os.path.exists(filename):
        return ["DNE", filename]
    else:
        vrfy = verify_checksum(filename, checksum)
        if not vrfy:
            return ["WCS", filename]

    return ["PASS", filename]


def read_csv() -> list[list[str, str]]:
    with open(S3_csv_filename, "r") as f:
        # Read header
        _ = f.readline()

        input_data = []
        for line in f:
            line = line.strip().split(",")
            item = [
                line[0].strip(),
                line[1].strip().replace("https://nex-gddp-cmip6.s3.us-west-2.amazonaws.com", data_dir),
            ]
            input_data.append(item)

    return input_data


def main():
    input_data = read_csv()

    pool = multiprocessing.Pool(processes=20)
    returneddata = pool.map(worker, input_data)
    pool.close()

    does_not_exist = []
    wrong_checksum = []

    num_passed = 0

    for code, filename in returneddata:
        if code == "PASS":
            num_passed += 1
        elif code == "DNE":
            does_not_exist.append(filename)
        elif code == "WCS":
            wrong_checksum.append(filename)

    num_not_present = len(does_not_exist)
    num_checksum_error = len(wrong_checksum)

    print("\n\n\nSUMMARY")
    print(f"Total files attempted          : {len(input_data)}")
    print(f"Files that passed verification : {num_passed}")
    print(f"Files that are not present     : {num_not_present}")
    print(f"Files that failed checksum     : {num_checksum_error}")

    if num_not_present > 0:
        print("\n\n\nFiles that do not exist")
        with open(f"download_error_files_{datetime.strftime(datetime.now(), '%Y%m%d')}.txt", "w") as f:
            for item in does_not_exist:
                print(item)
                f.write(item + "\n")

    if num_checksum_error > 0:
        print("\n\n\nFiles that failed checksum")
        with open(f"checksum_error_files_{datetime.strftime(datetime.now(), '%Y%m%d')}.txt", "w") as f:
            for item in wrong_checksum:
                print(item)
                f.write(item + "\n")


if __name__ == "__main__":
    S3_csv_filename = "gddp-cmip6-files.csv"
    data_dir = "/data/Datasets"
    main()
