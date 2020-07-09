""" Functions to work with text file manipulation in parallel
"""
import os
import multiprocessing as mp
from txt_normalizations_mp import mp_lower, mp_normalize, mp_spacy_lemmatize, mp_nltk_lematize
from line_chunker import line_counter, chunker


def mprocess(input_file, output_file, funct, turns, workers=False):
    """ Parallel text preprocess for big text files.

    Args:
        input_file (TYPE): The full path of the text file to process.
        output_file (TYPE): The full path of the text file to process.
        funct (TYPE): Function to apply over each line of the file
        turns (TYPE): Number of cycles (More for less ram usage, more io operations)
        workers (bool, optional): Number of CPUs to use.
    """

    lines_in_file = line_counter(input_file)

    if 'sched_getaffinity' in dir(os):
        MAX_WORKERS = len(os.sched_getaffinity(0))
    else:
        MAX_WORKERS = 1

    if not workers:
        if 'sched_getaffinity' in dir(os):
            workers = len(os.sched_getaffinity(0))
        else:
            workers = 1

    if workers > MAX_WORKERS:
        workers = MAX_WORKERS

    if lines_in_file <= workers:
        workers = lines_in_file
        turns = 1

    if lines_in_file <= workers * turns:
        turns = int(lines_in_file / workers)

    lines_per_turn = int(round(lines_in_file / turns))

    lines_per_workerturn = int(round(lines_per_turn / workers))

    with mp.Pool(workers) as p:
        for enum, chunk in enumerate(chunker(input_file, lines_per_turn)):
            percentage = enum / turns * 100
            print(f"Ciclo:{enum} de {turns}, {percentage}%",
                  end="\r", flush=True)
            if chunk:
                save_in_file("\n".join(p.map(funct, chunk)), output_file)


def save_in_file(input_text, output_file):
    """ Function to save output file

    Args:
        input_text (str): Output of each pool
        output_file (str): Full path to the output file.
    """
    try:
        with open(output_file, "a+") as f:
            f.write(input_text)
    except:
        with open(output_file, "w") as f:
            f.write(input_text)
