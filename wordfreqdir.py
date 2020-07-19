import os
import multiprocessing as mp
import collections, functools, operator
from itertools import repeat
from line_chunker import line_counter, chunker
import pickle


def wordfreqdict(input_t):
    wf_dict = {}
    if input_t[2]:
        for t in input_t[2].split():
            try:
                wf_dict[t] += 1
            except:
                wf_dict[t] = 1
        return wf_dict
    
    
def mprocess_wordfrecdict(input_file, output_file, turns, workers=False):
    """ Parallel text preprocess for big text files.

    Args:
        input_file (TYPE): The full path of the text file to process.
        output_file (TYPE): The full path of the text file to process.
        funct (TYPE): Function to apply over each line of the file
        turns (TYPE): Number of cycles (More for less ram usage, more io operations)
        workers (bool, optional): Number of CPUs to use.
    """
    print(f"Empecé con {input_file}")
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

    results = []

    with mp.Manager() as manager:
        wf_dict = manager.dict()
        with mp.Pool(workers) as p:
            for enum, chunk in enumerate(chunker(input_file, lines_per_turn)):
                percentage = enum / turns * 100
                print(f"Ciclo:{enum} de {turns}, {percentage}%, Procesando {input_file}",
                      end="\r", flush=True)
                if chunk:
                    print(f"Ahi voy en paralelo, Procesando {input_file}")
                    partial_results = p.map(wordfreqdict, chunk)
                    print(f"Ya terminé el paralelo, Procesando {input_file}")
                    results += [dict(functools.reduce(operator.add, 
                             map(collections.Counter, partial_results)))]
                    print(f"Junté diccionarios, Procesando {input_file}")
            results = [dict(functools.reduce(operator.add, 
                       map(collections.Counter, results)))]
            print(f"Termine este ciclo, voy pal otro, Procesando {input_file}")
    results = dict(functools.reduce(operator.add, map(collections.Counter, results)))
    save_in_file(results, output_file)
    print(f"Final final no va más :), Procesando {input_file}")


def save_in_file(input_dict, output_file):
    """ Function to save output file

    Args:
        input_text (str): Output of each pool
        output_file (str): Full path to the output file.
    """
    with open(output_file, "wb") as f:
         pickle.dump(input_dict,f)