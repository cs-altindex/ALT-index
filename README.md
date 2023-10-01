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

## Coming Soon

We evaluate our ALT-index with GRE benchmarking tool. We will update that code soon.

## Acknowledgement

- Our optimized ART is based on [ARTSynchronized](https://github.com/flode/ARTSynchronized) 
