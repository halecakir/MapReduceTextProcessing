import os
import re
from argparse import ArgumentParser
from collections import defaultdict

from LogUtils import LOGGER
from MapReduce import Mapper, Reducer


# Helper methods
# ----------------------------------------------
class MapperCountError(Exception):
    pass

def _init(args):
    """This function sets logger level and creates input and output directories.

    Args:
        args (ArgumentParser): Command line options
    """
    if args.verbose:
        LOGGER.setLevel('DEBUG')
    else:
        LOGGER.setLevel('WARNING')
    LOGGER.debug("args: %s", args)

    if not os.path.exists(args.output_dir):
        os.mkdir(args.output_dir)
        LOGGER.debug("{} is created".format(args.output_dir))
    if not os.path.exists(os.path.join(args.output_dir, "intermediate")):
        LOGGER.debug("{} is created".format(os.path.join(args.output_dir, "intermediate")))
        os.mkdir(os.path.join(args.output_dir, "intermediate"))
    if not os.path.exists(os.path.join(args.output_dir, "final")):
        LOGGER.debug("{} is created".format(os.path.join(args.output_dir, "final")))
        os.mkdir(os.path.join(args.output_dir, "final"))

def _split_input_files(input_files, mapper_count):
    """Manages splitting input files.

    Args:
        input_files (list): List of file paths.
        mapper_count (int): Mapper count.

    Raises:
        Exception: Raises Exception when the mapper count is bigger than length of input files.
    Returns:
        list: Returns list of list. Each of nested list corresponds to a mapper input.
    """
    splitted_input_files = []
    begin = 0 
    batch = len(input_files) // mapper_count 
    extra = len(input_files) % mapper_count 
    for _ in range(mapper_count): 
        end = begin + batch + (1 if extra > 0 else 0) 
        splitted_input_files.append(input_files[begin:end])
        extra -= 1 
        begin = end
    
    if mapper_count > len(input_files):
        LOGGER.error("Mapper count should be less than the number of files")
        raise MapperCountError("Mapper count should be less than or equal to the number of files")

    return splitted_input_files

def _split_intermediate_files(intermediate_files):
    """Groups intermediate files, which are mapper outputs, according to their reducer id.

    Args:
        intermediate_files (list): List of file paths. Each of them has "/path/to/your/file/intermediate/mr-{mapper-id}-{reducer-id}.txt" format.

    Returns:
        dict: Returns reducer id to reducer input list map, i.e. {0:[path1, path2, ...]}. 
    """
    idx2file = defaultdict(list)
    regex = r"mr-(?P<map_id>\d+)-(?P<reducer_id>\d+).txt"
    for f in intermediate_files:
        matches = re.search(regex, f)
        if matches:
            idx2file[int(matches.group("reducer_id"))].append(f)
    return idx2file

# Command line arguments parser
# ----------------------------------------------

def parse_arguments():
    parser = ArgumentParser(description="Simple multi-threaded MapReduce text processing tool.")
    parser.add_argument(
        "-v",
        "--verbose",
        help="Show debug information",
        action="store_true"
    )
    parser.add_argument(
        "-mc",
        "--mapper-count",
        dest="mapper_count",
        help="Number of mapper thread",
        type=int,
        default=2
    )
    parser.add_argument(
        "-rc",
        "--reducer-count",
        dest="reducer_count",
        help="Number of reducer thread",
        type=int,
        default=2
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-if",
        "--input-files",
        dest="input_files",
        help="Text files",
        metavar="FILE",
        nargs="+",
        type=str
    )
    group.add_argument(
        "-id",
        "--input-dir",
        dest="input_dir",
        help="The folder that contains text files",
        metavar="FOLDER",
        type=str
    )
    parser.add_argument(
        "-od",
        "--output-dir",
        dest="output_dir",
        help="Output folder",
        metavar="FOLDER",
        type=str,
        default="outputs"
    )

    args = parser.parse_args()
    return args

 
if __name__ == "__main__":
    args = parse_arguments()
    _init(args)

    input_files = args.input_files if args.input_files else [os.path.join(args.input_dir, f) for f in os.listdir(args.input_dir)]
    input_files = [f for f in input_files if f.endswith(".txt")]

    lm = []
    for i, files in enumerate(_split_input_files(input_files, args.mapper_count)): 
        m = Mapper(i, files, args.reducer_count, args.output_dir)
        lm.append(m)
        m.start()

    for m in lm:
        m.join()

    intermediate_files_dir = os.path.join(args.output_dir, "intermediate")
    intermediate_files = [os.path.join(intermediate_files_dir, f) for f in os.listdir(intermediate_files_dir)]
    idx2file = _split_intermediate_files(intermediate_files)

    rl = []
    for i in range(args.reducer_count):
        r = Reducer(i, idx2file[i], args.output_dir)
        rl.append(r)
        r.start()
    
    for r in rl:
        r.join()

