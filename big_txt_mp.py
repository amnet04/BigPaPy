""" Functions to work with text file manipulation in parallel
"""
import os
import time
import multiprocessing as mp
from chunkers import by_line_chunker, by_position_chunker
from debug_utilities import secure_limits, duration_log, mem_log, gen_log
from txt_normalizations_mp import *


def mprocess_to_file(input_file,
                     output_file,
                     funct,
                     turns,
                     workers=False,
                     by_lines=False):
    """ Parallel text preprocess for big text files.

    Args:
        input_file (TYPE): The full path of the text file to process.
        output_file (TYPE): The full path of the text file to process.
        funct (TYPE): Function to apply over each line of the file
        turns (TYPE): Number of cycles (More for less ram usage,
                      more io operations)
        workers (bool, optional): Number of CPUs to use.
    """

    start = time.time()

    not_overwrite_msg = "Proceso detenido, intente con otro nombre de archivo"

    if os.path.isfile(output_file):

        if __name__ == "__main__":
            overwrite = ""
            while overwrite not in ["y", "n"]:
                overwrite = input("{} ya existe, lo sobrescribo (y/n):".format(
                    output_file
                ))
            if overwrite == "y":
                with open(output_file, "w") as f:
                    f.write("")
            else:
                return(print(not_overwrite_msg))
        else:
            return(print(not_overwrite_msg))

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

    if by_lines:
        chunker = by_line_chunker(input_file,
                                  file_size,
                                  workers,
                                  turns)

    else:
        available, secure = secure_limits()
        if turns < (file_size / secure):
            print(f"{file_size}/{turns} usa mucha memoria, ajustando turnos")
            turns = int(file_size / secure)
            print(f"turnos seguros: {turns}")
        chunker = by_position_chunker(input_file,
                                      file_size,
                                      workers,
                                      turns)

    end = time.time()
    duration_log(start, end, notes="Configuring parameters")
    gen_log(f"Workers : {workers}")
    gen_log(f"Turns   : {turns}")
    gen_log(f"ExMaxMem: {int(file_size / turns / 1048576)} Mb")
    gen_log(f"MaxMemPp: {int(file_size / (turns * workers) / 1048576)} Mb")
    gen_log(f"Chunker : {chunker}")
    gen_log(f"Function: {funct}")
    mem_log("Antes del pool")

    start_loop = time.time()
    with mp.Pool(workers) as p:
        for enum, chunk in enumerate(chunker):
            start_turn = time.time()
            percentage = (enum / turns) * 100
            mem_log(f"En loop {enum} de {turns}, {percentage}%")
            if chunk:
                mem_log("Justo antes de procesar en paralelo")
                save_in_file("\n".join(p.map(funct, chunk)),
                             output_file, enum)
                mem_log("Justo luego de procesar en paralelo")

            end_turn = time.time()

            duration_log(start_turn, end_turn, f"Turn {enum}")
    end_loop = time.time()
    duration_log(start_loop, end_loop, "Total loop")
    mem_log("Fuera del pool")


def save_in_file(input_text, output_file, enum):
    """ Function to save output file

    Args:
        input_text (str): Output of each pool
        output_file (str): Full path to the output file.
    """

    try:
        with open(output_file, "a+") as f:
            f.write(input_text)

    except FileNotFoundError:
        with open(output_file, "w") as f:
            f.write(input_text)


def main():
    help = "Help: ./big_txt_mp.py input_file output_file, func*, turns,"
    help += "[workers], [by_lines]\n"

    funct_dict = {"lower": mp_lower,
                  "normalize": mp_normalize,
                  "nltk_lema": mp_nltk_lematize,
                  "spacy_lema": mp_spacy_lemmatize
                  }

    if 4 < len(argv) < 8:
        input_file = argv[1]
        output_file = argv[2]
        funct = funct_dict[argv[3]]
        turns = int(argv[4])
        if len(argv) > 5:
            cpus = int(argv[5])
        else:
            cpus = False
        if len(argv) > 6:
            by_l = bool(argv[6])
        else:
            by_l = False
        mprocess_to_file(input_file,
                         output_file,
                         funct,
                         turns,
                         workers=cpus,
                         by_lines=by_l)
    else:
        print(help)


if __name__ == "__main__":
    from sys import argv
    main()
