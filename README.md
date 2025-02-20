# ALT-index

ALT-index: A concurrent hybrid learned index for high-performance storage

This project contains the code of ALT-index. Our evaluation is based on [GRE](https://github.com/gre4index/GRE).

## Compile and Run our project

```bash
#install requirements
sudo apt update && sudo apt install libjemalloc-dev libtbb-dev libopenmpi-dev

#build
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
2. run 
```bash
./build/ALT_index
```

- Configurations for benchmark:
```c++
#define ARR_GAPS ${your gaps} 

#define USE_FAST_POINTER true
#define USE_DYNAMIC_RETRAIN true

#define USE_STATISTIC false     //warning: this will damage the performance

//on line 1069: the error bound of GPL model is set to bulkload number / 1000
segmentPartition(keys + used_index, remain_nums, segment, num_keys / 1000);
```

## Overall Benchmark

We evaluate our ALT-index with GRE benchmarking tool, please refer to our repo [GRE_alt](https://github.com/cs-altindex/GRE_alt) for more details.

## Acknowledgement

- Our optimized ART is based on [ARTSynchronized](https://github.com/flode/ARTSynchronized)
- Our benchmark GRE_alt is based on [GRE](https://github.com/gre4index/GRE)
