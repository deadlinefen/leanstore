# LeanStore
[LeanStore](https://db.in.tum.de/~leis/papers/leanstore.pdf) is a high-performance OLTP storage engine optimized for many-core CPUs and NVMe SSDs. Our goal is to achieve performance comparable to in-memory systems when the data set fits into RAM, while being able to fully exploit the bandwidth of fast NVMe SSDs for large data sets. While LeanStore is currently a research prototype, we hope to make it usable in production in the future.

## Compiling
Install dependencies:

`sudo apt-get install cmake libaio-dev libtbb-dev`

`mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && make -j`

## TPC-C Example
`build/frontend/tpcc --ssd_path=./ssd_block_device_or_file --worker_threads=120 --pp_threads=4 --dram_gib=240 --tpcc_warehouse_count=100 --notpcc_warehouse_affinity --csv_path=./log --cool_pct=40 --free_pct=1 --contention_split --xmerge --print_tx_console --run_for_seconds=60 --isolation_level=si`

check `build/frontend/tpcc --help` for other options

## Cite
The code we used for our VLDB 2023 that covers alternative SI commit protocols is in a different [branch](https://github.com/leanstore/leanstore/tree/mvcc).
```
@inproceedings{alhomssi23,
    author    = {Adnan Alhomssi and Viktor Leis},
    title     = {Scalable and Robust Snapshot Isolation for High-Performance Storage Engines},
    booktitle = {VLDB},
    year      = {2023}
}
```

The code we used for our BTW 2023 paper that covers alternative dependency tracking and is in a different [branch](https://github.com/leanstore/leanstore/tree/btw).
```
@inproceedings{leanstore23,
    author    = {Adnan Alhomssi, Michael Haubenschild and Viktor Leis},
    title     = {The Evolution of LeanStore},
    booktitle = {BTW},
    year      = {2023}
}
```

The code we used for our CIDR 2021 paper is in a different (and outdated) [branch](https://github.com/leanstore/leanstore/tree/cidr).
```
@inproceedings{alhomssi21,
    author    = {Adnan Alhomssi and Viktor Leis},
    title     = {Contention and Space Management in B-Trees},
    booktitle = {CIDR},
    year      = {2021}
}
```
