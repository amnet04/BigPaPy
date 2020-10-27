""" Module to chunk a file per number of lines
"""
from .debug_utilities import duration_log, mem_log, gen_log
import sys
import os

def line_counter(input_file):
    """Count the number of lines in a file.

    Args:
        input_file (TYPE): The full path of the text file to process.

    Returns:
        int: The number of lines in the file
    """
    line_count = 0
    with open(input_file, "r") as f:
        for line in f:
            line_count += 1
    return line_count

def list_split_n(lst, parts):
  
    splitted_list = []
  
    list_size = len(lst)
    res = list_size % parts
    part_size = int(list_size / parts)  
  
    # Auxiliar counters 
    ax = 0
    ay = 1
  
    for enum, x in enumerate(range(0,list_size,part_size),1):

        if res == 0 or enum-1 < parts-res:
            splitted_list.append(lst[x:x+part_size])
            lst[x:x+part_size] = [False for x in range(x,x+part_size)]
        elif x+part_size+ay < list_size:
            splitted_list.append(lst[x+ax:x+part_size+ay])
            lst[x+ax:x+part_size+ay] = [False for x in range(x+ax,x+part_size+ay)]
            ax += 1
            ay += 1
        elif enum==parts:
            splitted_list.append(lst[x+ay-1:])
            lst[x+ay-1:] = [False for x in range(x+ay-1,list_size-1)]

    return splitted_list


def by_line_chunker(input_file, file_size, workers, turns):
    """ Chunk generator to obtain a defined number of lines from a text file.

    Args:
        input_file (str): The full path of the text file to process.
        lines_per_chunk (int): Number of lines in each chunk.

    Yields:
        list[list[int, int,list(str)]]:
                                list[list[chuk_count, line_count, line]]
        (bool): Return false when the file ends.
    """
    fileEnd = file_size
    chunk_enum = 0
    line_enum = 0
    keep = True
    lines_in_file = line_counter(input_file)
    if lines_in_file >= (workers * turns):
        lines_per_chunk = int(lines_in_file / (workers * turns))
    elif lines_in_file > workers:
        while (lines_in_file / (workers * turns)) < 1:
            turns -= 1
        lines_per_chunk = int(lines_in_file / workers)
    else:
        turns = 1
        lines_per_chunk = lines_in_file
    with open(input_file, 'r') as f:
        chunkEnd = f.tell()
        while keep:
            chunk = []
            chunk_enum += 1
            while len(chunk) < lines_per_chunk and chunkEnd < fileEnd:

                line = f.readline()
                chunkEnd = f.tell()
                line_enum += 1
                chunk.append([chunk_enum, line_enum, line])

                if chunkEnd >= fileEnd:
                    break
            yield chunk
            if chunkEnd >= fileEnd:
                break
        yield False


def by_size_position_chunker(input_file, file_size, chunk_size):
    mem_log()
    chunk_end = 0

    while True:

        chunk_start = chunk_end
        with open(input_file, "rb") as f:
            mem_log()
            f.seek(chunk_start + chunk_size, 1)  # Offset of chunksize
            f.readline()  # Complete the slice until the next line break

            chunk_end = f.tell()  # Save the position of such line break
            mem_log()

        yield chunk_start, chunk_end - chunk_start
        mem_log("pasando por el chunker intermedio")
        if chunk_end >= file_size:
            break


def by_position_chunker(input_file, workers, turns):
    mem_log("Init")
    enum = 0
    file_size = os.path.getsize(input_file)
    chunk_size = int(file_size / turns)
    for chunk_sta, chunk_siz in by_size_position_chunker(input_file,
                                                         file_size,
                                                         chunk_size):
        mem_log("comenzando chunk")
        with open(input_file, "rb") as f:
            f.seek(chunk_sta)
            mem_log(f"------>>>> antes de cargar texto, chunk size: {int(chunk_siz / 1048576)} Mb")
            txt = f.read(chunk_siz)
            mem_log(f"------>>>> antes de decodificar txt: {int(sys.getsizeof(txt) / 1048576)}Mb")

            txt = txt.decode("utf-8")
            mem_log(f"------>>>> luego de decodificar txt: {int(sys.getsizeof(txt) / 1048576)}Mb")

            txt = txt.splitlines()
            mem_log(f"------>>>> luego de splitlines txt: {int(sys.getsizeof(txt) / 1048576)}Mb")

            txt = list_split_n(txt, workers)
            mem_log(f"------>>>> luego de dividirlo en workers txt: {int(sys.getsizeof(txt) / 1048576)}Mb")

            chunk = []
            for enum, x in enumerate(txt):
                mem_log(f"En loop {enum}, antes de cargar chunk: {int(sys.getsizeof(chunk) / 1048576)}Mb")
                chunk.append([enum, x, "\n".join(x)])
                mem_log(f"En loop {enum}, luego de cargar chunk: {int(sys.getsizeof(chunk) / 1048576)}Mb")
                txt[enum] = False 
                mem_log(f"En loop {enum}, luego de reducir txt: {int(sys.getsizeof(txt) / 1048576)}Mb")
            del(txt)
            mem_log(f"Fuera del loop con txt borrado")
            gen_log(f"#############>>>>>>>>>>>>>>>>>>> ChunkLen: {len(chunk)}")
            yield chunk
        mem_log("Luego de cerrar archivo")
    mem_log("Al finalizar el generador")
