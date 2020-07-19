""" Functions to preprocess in pararell text (lowecasing, normalize, lematize)

Attributes:
    LEMMATIZER (TYPE): nltk word lemattizer
    NLP (TYPE): spaCy model
"""

import re
import spacy
from nltk import WordNetLemmatizer


NLP = spacy.load('en')
LEMMATIZER = WordNetLemmatizer()


def mp_lower(input_t):
    """Lower case the chunker input

    Args:
        input_t (line_chunker output): list[int, int, line]

    Returns:
        str: Lowered line
    """
    if input_t[2]:
        return(input_t[2]).lower().strip()


def mp_normalize(input_t):
    """Lowering and clean no alphanumeric chars

    Args:
        input_t (line_chunker output): list[int, int, line]

    Returns:
        str: Lowered and cleaned line
    """
    if input_t[2]:
        input_t[2] = re.sub(
            r'\w+:\/{2}[\d\w-]+(\.[\d\w-]+)*(?:(?:\/[^\s/]*))*',
            '',
            input_t[2])
        input_t[2] = re.sub(r'\W+',
                            ' ',
                            input_t[2]).strip().lower()
        return input_t[2]


def mp_spacy_lemmatize(input_t):
    """Lowering, clean no alphanumeric chars, and lemmatize whit spaCy

    Args:
        input_t (line_chunker output): list[int, int, line]

    Returns:
        str: Lowered, cleaned and lemmatized with spaCy line.
    """
    if input_t[2]:

        doc = NLP(input_t[2])
        input_t[2] = " ".join([token.lemma_ for token in doc])
        input_t[2] = re.sub(
            r'\w+:\/{2}[\d\w-]+(\.[\d\w-]+)*(?:(?:\/[^\s/]*))*',
            '',
            input_t[2])
        input_t[2] = re.sub(r'\W+', ' ', input_t[2]).strip().lower()
        return input_t[2]


def mp_nltk_lematize(input_t):
    """Lowering, clean no alphanumeric chars, and lemmatize whit NLTK
    WordnetLemmatizer  

    Args:
        input_t (line_chunker output): list[int, int, line]

    Returns:
        str: Lowered, cleaned and lemmatized with NLTK line.
    """

    if input_t[2]:
        tokens = []
        for t in input_t[2].split():
            if len(t) < 4:
                tokens.append(t)
            else:
                tokens.append(lemmatizer.lemmatize(t))
        input_t[2] = " ".join(tokens)
        input_t[2] = re.sub(
            r'\w+:\/{2}[\d\w-]+(\.[\d\w-]+)*(?:(?:\/[^\s/]*))*',
            '',
            input_t[2])
        input_t[2] = re.sub(r'\W+', ' ', input_t[2]).strip().lower()
        return input_t[2]