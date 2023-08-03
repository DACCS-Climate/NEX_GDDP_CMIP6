# NEX_GDDP_CMIP6
Scripts to download the [NEX_GDDP_CMIP6 dataset](https://www.nccs.nasa.gov/services/data-collections/land-based-products/nex-gddp-cmip6).

Two scripts are provided. The first script `download_NEX_GDDP_CMIP6.py` downloads the data while `post_download_verification_NEX_GDDP_CMIP6.py` is used to do post-download verification.

Note that the entire dataset is 30 T in size so running both scripts takes a very long time even though they are parallelized. With 10 processes, which is the optimal number of processes I've found that will not trip Amazon S3 to limit data transfer rates, it still take more than 2 days to download the entire dataset.

Before running the scripts you need to download [this CSV file](https://nex-gddp-cmip6.s3-us-west-2.amazonaws.com/gddp-cmip6-files.csv) containing the links to the files and their checksums. Ideally, you should download a fresh copy of the file, but a backup copy is also included in this repository.

