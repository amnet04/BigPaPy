import os
import multiprocessing as mp
from itertools import repeat
from .chunkers import by_position_chunker
from nltk.probability import FreqDist
from nltk import word_tokenize
import collections, functools, operator
import pickle
import time

def wordfrecdict(sentence_list):
    fdict =  FreqDist()
    for sentence in sentence_list[1]:
        for word in word_tokenize(sentence):
            fdict[word] += 1
    return fdict
            
            

def mprocess_wordfrecdict(input_file, output_file, turns, workers=False, min_chunk_size_bytes=8388608):
    """ Parallel text preprocess for big text files.

    Args:
        input_file (TYPE): The full path of the text file to process.
        output_file (TYPE): The full path of the text file to process.
        funct (TYPE): Function to apply over each line of the file
        turns (TYPE): Number of cycles (More for less ram usage, more io operations)
        workers (bool, optional): Number of CPUs to use.
    """
    #print(f"Empecé con {input_file}")
    #lines_in_file = line_counter(input_file)
    
    file_size = os.path.getsize(input_file)

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
        
    if file_size > min_chunk_size_bytes*workers*turns: # El tamaño del archivo es mas grande que el minimo
        chunk_size = int(round(file_size/(workers*turns)))
        
    elif file_size > min_chunk_size_bytes*workers: # Ajustar turnos
        turns = int(round(file_size/(min_chunk_size_bytes*workers)))
        if turns == 0:
            turns = 1
        chunk_size = int(round(file_size/(workers*turns)))
    
    else: # Ajustar workers
        turns = 1
        workers = int(round(file_size)/min_chunk_size_bytes)
        if workers == 0:
            workers = 1
        chunk_size = int(round(file_size/(workers)))

    freq_dist = FreqDist()
    for enum, chunk in enumerate(by_position_chunker(input_file, workers, turns)):
        percentage = enum / turns * 100
        print(f"Ciclo:{enum+1} de {turns}, {round(percentage)}%, Procesando {input_file}", end="\r", flush=True)
        with mp.Pool(workers) as p:
            partial_freq_dist = FreqDist(functools.reduce(operator.add, p.map(wordfrecdict, chunk)))
            freq_dist += partial_freq_dist
    return freq_dist    
        
        

    """with mp.Manager() as manager:
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
    print(f"Final final no va más :), Procesando {input_file}")"""


def save_in_file(input_dict, output_file):
    """ Function to save output file

    Args:
        input_text (str): Output of each pool
        output_file (str): Full path to the output file.
    """
    with open(output_file, "wb") as f:
         pickle.dump(input_dict,f)