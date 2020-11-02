# MapReduceTextProcessing
# Simple MapReduce Word Counter

Simple multi-threaded MapReduce text processing tool.


## Installation and Usage

This program requires [Python](https://python.org) 3+  to run.

There is no external dependencies so you can start the program below commands:

```sh
$ cd ProjectMR
$ python ./MapReduce/Runner.py [-h] [-v] [-mc MAPPER_COUNT] [-rc REDUCER_COUNT]
                 (-if FILE [FILE ...] | -id FOLDER) [-od FOLDER]
optional arguments:
  -h, --help            show this help message and exit
  -v, --verbose         Show debug information
  -mc MAPPER_COUNT, --mapper-count MAPPER_COUNT
                        Number of mapper thread
  -rc REDUCER_COUNT, --reducer-count REDUCER_COUNT
                        Number of reducer thread
  -if FILE [FILE ...], --input-files FILE [FILE ...]
                        Text files
  -id FOLDER, --input-dir FOLDER
                        The folder that contains text files
  -od FOLDER, --output-dir FOLDER
                        Output folder
```
#### Sample Runs
- Sample run 1:
```sh
cd ProjectMR
python ./MapReduce/Runner.py -mc 5 -rc 2 -id data/ -od out/
```
- Sample run 2:
```sh
cd ProjectMR
python ./MapReduce/Runner.py -mc 2 -rc 2 -if data/pg-grimm.txt data/pg-frankenstein.txt data/pg-being_ernest.txt -od out/
```

#### Testing
- Run all the unit tests
```sh
cd ProjectMR
python -m unittest discover
```

### Todos
- Replace thread with process.
- Profile the program for cpu and memory usage and try to optimize.
- There are I/O bound operations, e.g. opening/closing files for every operations in Mapper class. Try to optimize them using caching, etc.
- Share partial results from one worker that is needed by another.