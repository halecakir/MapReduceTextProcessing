import unittest

from MapReduce.LogUtils import LOGGER
from MapReduce.NLPUtils import NLPUtils

class TestNLPUtils(unittest.TestCase):
    def setUp(self):
        LOGGER.setLevel('DEBUG')
        LOGGER.debug("In TestNLPUtils setUp()")

    def tearDown(self):
        LOGGER.debug("In TestNLPUtils tearDown()")

    def test_punctuation_removal(self):
        LOGGER.debug("In test_punctuation_removal()")
        sentence = "This is, an absurd? test senten:ce!"
        expected = "This is  an absurd  test senten ce "
        self.assertEqual(NLPUtils.punctuation_removal(sentence), expected , "Should be {}".format(expected))

    def test_word_tokenization(self):
        LOGGER.debug("In test_word_tokenization()")
        sentence = "This is  an absurd  test senten ce "
        expected = ["This", "is", "an", "absurd", "test", "senten", "ce"]
        self.assertEqual(NLPUtils.word_tokenization(sentence), expected , "Should be {}".format(expected))

    def test_preprocess(self):
        LOGGER.debug("In test_preprocess()")
        sentence = "This is, an absurd? test senten:ce!\n"
        expected = "This is  an absurd  test senten ce "
        self.assertEqual(NLPUtils.preprocess(sentence), expected , "Should be {}".format(expected))

if __name__ == '__main__':
    unittest.main()