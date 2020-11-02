import string

class NLPUtils:
    """Utility fuctions for text processing
    """
    @staticmethod
    def preprocess(sentence):
        """Preprocesses the sentence.

        Args:
            sentence (str): Sequence of words.

        Returns:
            list: Returns preprocessed sentence.
        """
        sentence = sentence.strip()
        sentence = NLPUtils.punctuation_removal(sentence)
        return sentence

    @staticmethod
    def word_tokenization(sentence):
        """Tokenizes the input string.

        Args:
            sentence (str): Sequence of words.

        Returns:
            [list]: Returns list of splitted words.
        """
        return [token for token in sentence.split(" ") if token]
    
    @staticmethod
    def punctuation_removal(sentence):
        """Removes the punctuations.

        Args:
            sentence (str): Sequence of words.

        Returns:
            str: Returns sentence without punctuations.
        """
        d = {w: " " for w in string.punctuation}
        translator = str.maketrans(d)
        return sentence.translate(translator)