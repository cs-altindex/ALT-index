# ALT-index (Updating)

ALT-index: A concurrent hybrid learned index for high-performance storage

This project contains the code of ALT-index. Our evaluation is done through [GRE](https://github.com/gre4index/GRE).

## Compile and Run our project

```bash
mkdir build
cd build
cmake ..
make -j8
```

- Run multithread:

```bash
./build/multithread
```

- Run with your own dataset with statistics:

1. change the dataset path in ./examples/base.cpp:

```c++
string data_path = ${your own dataset path}
```
2. set the statistic flag in ./include/alt_index:

```c++
#define USE_STATISTIC true
```
3. run 
```bash
./build/ALT_index
```

- Configurations for benchmark:
```c++
#define PREALLOC_NODE_NUMS 1000000
#define VECTOR_RESERVE_NUMS 10000000
#define ARR_GAPS 2

#define USE_FAST_POINTER true
#define USE_DYNAMIC_RETRAIN true

#define USE_STATISTIC false   

//on line 1031: the error bound of GPL model is set to bulkload number / 1000
segmentPartition(keys + used_index, remain_nums, segment, num_keys / 1000);
```

## Coming Soon

We evaluate our ALT-index with GRE benchmarking tool.

## Acknowledgement

- Our optimized ART is based on [ARTSynchronized](https://github.com/flode/ARTSynchronized) 
