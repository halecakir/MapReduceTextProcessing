import unittest

from MapReduce.LogUtils import LOGGER
from MapReduce.Runner import _split_input_files, _split_intermediate_files , MapperCountError

class TestRunner(unittest.TestCase):
    def setUp(self):
        LOGGER.setLevel('DEBUG')
        LOGGER.debug("In TestRunner setUp()")
        self.input_files = [ "pg-dorian_gray.txt",
                        "pg-grimm.txt",
                        "pg-metamorphosis.txt",
                        "pg-tom_sawyer.txt",
                        "pg-being_ernest.txt",
                        "pg-frankenstein.txt",
                        "pg-huckleberry_finn.txt",
                        "pg-sherlock_holmes.txt"]

    def tearDown(self):
        LOGGER.debug("In TestRunner tearDown()")
        del self.input_files

    def test_split_input_files_valid(self):
        LOGGER.debug("In test_split_input_files_valid()")
        expected = [["pg-dorian_gray.txt",
                        "pg-grimm.txt",
                        "pg-metamorphosis.txt"],
                    ["pg-tom_sawyer.txt",
                        "pg-being_ernest.txt",
                        "pg-frankenstein.txt"],
                    ["pg-huckleberry_finn.txt",
                        "pg-sherlock_holmes.txt"]]
        self.assertEqual(_split_input_files(self.input_files, 3), expected, "Should be {}".format(expected))
    
    def test_split_input_files_throws_exception(self):
        LOGGER.debug("In test_split_input_files_throws_exception()")
        self.assertRaises(MapperCountError, _split_input_files, self.input_files, 9)
    
    def test_split_intermediate_files(self):
        LOGGER.debug("In test_split_intermediate_files()")
        intermediate_files = [ "out/intermediate/mr-0-0.txt",
                                "out/intermediate/mr-0-1.txt",
                                "out/intermediate/mr-0-2.txt",
                                "out/intermediate/mr-0-3.txt",
                                "out/intermediate/mr-1-0.txt",
                                "out/intermediate/mr-1-1.txt",
                                "out/intermediate/mr-1-2.txt"]
        expected = {0: ["out/intermediate/mr-0-0.txt", "out/intermediate/mr-1-0.txt"],
                    1: ["out/intermediate/mr-0-1.txt", "out/intermediate/mr-1-1.txt"],
                    2: ["out/intermediate/mr-0-2.txt", "out/intermediate/mr-1-2.txt"],
                    3: ["out/intermediate/mr-0-3.txt"]}
        self.assertEqual(_split_intermediate_files(intermediate_files), expected, "Should be {}".format(expected))
        
if __name__ == '__main__':
    unittest.main()