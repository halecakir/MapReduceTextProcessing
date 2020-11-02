import filecmp
import os
import shutil
import unittest

from MapReduce.LogUtils import LOGGER
from MapReduce.MapReduce import Mapper, Reducer
from MapReduce.NLPUtils import NLPUtils

THIS_DIR = os.path.dirname(os.path.abspath(__file__))

def map_single_thread(input_files, output_dir, reducer_count):
    for map_id, map_inputs in enumerate(input_files):
        for f in map_inputs:
            inp_handler = open(f)
            for line in inp_handler:
                for word in NLPUtils.word_tokenization(NLPUtils.preprocess(line)):
                    with open(os.path.join(output_dir, "intermediate", "mr-{}-{}.txt".format(map_id, ord(word[0])%reducer_count)), "a") as target:
                        target.write(word+"\n")
            inp_handler.close()

def reduce_single_thread(idx2file, output_dir):
    for reducer_id in idx2file:
        word2count = dict()
        for f in idx2file[reducer_id]:
            with open(f) as inp_handler:
                for word in inp_handler:
                    word = word.strip()
                    word2count[word] = word2count.get(word, 0) + 1
        output_path = os.path.join(output_dir, "final", "out-{}.txt".format(reducer_id))
        with open(output_path, "w") as out_handler:
            for w in word2count:
                out_handler.write("{}: {}\n".format(w, word2count[w]))


class TestMapReduce(unittest.TestCase):
    def setUp(self):
        LOGGER.setLevel('DEBUG')
        LOGGER.debug("In TestMapReduce setUp()")

    def tearDown(self):
        LOGGER.debug("In TestMapReduce tearDown()")

    def test_mapper_intermediate_files(self):
        LOGGER.debug("In test_mapper_intermediate_files()")

        file_names = ["data1.txt", "data2.txt", "data3.txt"]
        test_input_files= [os.path.join(THIS_DIR, "test_data", f) for f in file_names]
        test_splitted_input_files = [[test_input_files[0], test_input_files[1]],
                                          [test_input_files[2]]]

        test_output_dir_expected = os.path.join(THIS_DIR, "test_data", "expected")
        test_output_dir_actual = os.path.join(THIS_DIR, "test_data", "actual")

        #Remove old output directories
        if os.path.exists(test_output_dir_expected):
            shutil.rmtree(test_output_dir_expected)
        if os.path.exists(test_output_dir_actual):
            shutil.rmtree(test_output_dir_actual)

        #Create new output directories
        os.mkdir(test_output_dir_expected)
        os.mkdir(os.path.join(test_output_dir_expected, "intermediate"))   

        os.mkdir(test_output_dir_actual)
        os.mkdir(os.path.join(test_output_dir_actual, "intermediate"))   

        lm = []
        reducer_count = 2
        for i, files in enumerate(test_splitted_input_files): 
            m = Mapper(i, files, reducer_count, test_output_dir_actual)
            lm.append(m)
            m.start()

        for m in lm:
            m.join()

        #Run the single threaded map for expected intermediate output
        map_single_thread(test_splitted_input_files, test_output_dir_expected, reducer_count)
        
        #Compare actual and expected files.
        actual_intermediate_dir = os.path.join(test_output_dir_actual, "intermediate")
        expected_intermediate_dir = os.path.join(test_output_dir_expected, "intermediate")
        actual_files = [f for f in os.listdir(actual_intermediate_dir)]
        expected_files = [f for f in os.listdir(expected_intermediate_dir)]

        self.assertEqual(len(actual_files), len(expected_files), "Number of files have to be equal!")
        for f in actual_files:
            self.assertTrue(filecmp.cmp(os.path.join(actual_intermediate_dir, f),
                                    os.path.join(expected_intermediate_dir, f)), "Expected and Actual output for file {} is not equal!".format(f))

    def test_reducer_output_files(self):
        LOGGER.debug("In test_reducer_output_files()")

        file_names = ["data1.txt", "data2.txt", "data3.txt"]
        test_input_files= [os.path.join(THIS_DIR, "test_data", f) for f in file_names]
        test_splitted_input_files = [[test_input_files[0], test_input_files[1]],
                                          [test_input_files[2]]]

        test_output_dir_expected = os.path.join(THIS_DIR, "test_data", "expected")
        test_output_dir_actual = os.path.join(THIS_DIR, "test_data", "actual")

        #Remove old output directories
        if os.path.exists(test_output_dir_expected):
            shutil.rmtree(test_output_dir_expected)
        if os.path.exists(test_output_dir_actual):
            shutil.rmtree(test_output_dir_actual)

        #Create new output directories
        os.mkdir(test_output_dir_expected)
        os.mkdir(test_output_dir_actual)
        os.mkdir(os.path.join(test_output_dir_expected, "intermediate"))
        os.mkdir(os.path.join(test_output_dir_actual, "intermediate"))
        os.mkdir(os.path.join(test_output_dir_expected, "final"))
        os.mkdir(os.path.join(test_output_dir_actual, "final"))

        #Run the single threaded map for gold intermediate output
        reducer_count = 2
        map_single_thread(test_splitted_input_files, test_output_dir_expected, reducer_count)


        idx2file = {0:[os.path.join(test_output_dir_expected, "intermediate", "mr-0-0.txt"),os.path.join(test_output_dir_expected, "intermediate", "mr-1-0.txt")],
                    1:[os.path.join(test_output_dir_expected, "intermediate", "mr-0-1.txt"),os.path.join(test_output_dir_expected, "intermediate", "mr-1-1.txt")]}

        rl = []
        for i in range(reducer_count):
            r = Reducer(i, idx2file[i], test_output_dir_actual)
            rl.append(r)
            r.start()
        
        for r in rl:
            r.join()

        #Run the single threaded reduce for expected final output
        reducer_count = 2
        reduce_single_thread(idx2file, test_output_dir_expected)

        #Compare actual and expected files.
        actual_final_dir = os.path.join(test_output_dir_actual, "final")
        expected_final_dir = os.path.join(test_output_dir_expected, "final")
        actual_files = [f for f in os.listdir(actual_final_dir)]
        expected_files = [f for f in os.listdir(expected_final_dir)]

        self.assertEqual(len(actual_files), len(expected_files), "Number of files have to be equal!")
        for f in actual_files:
            self.assertTrue(filecmp.cmp(os.path.join(actual_final_dir, f),
                                    os.path.join(expected_final_dir, f)), "Expected and Actual output for file {} is not equal!".format(f))
if __name__ == '__main__':
    unittest.main()
