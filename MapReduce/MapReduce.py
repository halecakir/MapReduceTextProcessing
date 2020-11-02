import os
import threading
from collections import defaultdict

from MapReduce.NLPUtils import NLPUtils
from MapReduce.LogUtils import LOGGER

# Mapper class
# ----------------------------------------------
class Mapper(threading.Thread):
    """Mapper class
    """
    def __init__(self, thread_id, files, reducer_count, output_dir):
        """Init function for Mapper class.

        Args:
            thread_id (int): Mapper ID.
            files (list): Mapper input paths.
            reducer_count (int): Reducer count.
            output_dir (str): Output directory for intermediate files.
        """
        threading.Thread.__init__(self)
        self.id = thread_id
        self.files = files
        self.M = reducer_count
        self.output_dir = output_dir

    def run(self):
        """Produces the intermediate files.
        """
        LOGGER.debug("Mapper {} is started".format(self.id))
        for inp_path in self.files:
            with open(inp_path) as inp_handler:
                for line in inp_handler:
                    sentence = NLPUtils.preprocess(line)
                    for word in NLPUtils.word_tokenization(sentence):
                        word_idx = ord(word[0])%self.M
                        output_path = os.path.join(self.output_dir, "intermediate", "mr-{}-{}.txt".format(self.id, word_idx))
                        with open(output_path, "a") as out_handler:
                            out_handler.write(word+"\n")
        LOGGER.debug("Mapper {} is finished".format(self.id))

# Reducer class
# ----------------------------------------------
class Reducer(threading.Thread):
    """Reducer class
    """
    def __init__(self, thread_id, files, output_dir):
        """Init function for Reducer class.

        Args:
            thread_id (int): Reducer ID.
            files (list): Reducer input paths.
            output_dir (str): Directory for reducer output.
        """
        threading.Thread.__init__(self)
        self.files = files
        self.id = thread_id
        self.output_dir = output_dir
       
    def run(self):
        """Produces the reducer (final) outputs.
        """
        LOGGER.debug("Reducer {} is started".format(self.id))
        word2count = defaultdict(int)
        for inp_path in self.files:
            with open(inp_path) as inp_handler:
                for word in inp_handler:
                    word2count[word.strip()] += 1

        output_path = os.path.join(self.output_dir, "final", "out-{}.txt".format(self.id))
        with open(output_path, "w") as out_handler:
            for w in word2count:
                out_handler.write("{}: {}\n".format(w, word2count[w]))
        LOGGER.debug("Reducer {} is finished".format(self.id))
