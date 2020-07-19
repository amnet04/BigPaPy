""" Module to chunk a file per number of lines
"""
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


def chunker(input_file, lines_per_chunk):
    """ Chunk generator to obtain a defined number of lines from a text file.

    Args:
        input_file (str): The full path of the text file to process.
        lines_per_chunk (int): Number of lines in each chunk.

    Yields:
        list[list[int, int,list(str)]]: 
        			list[list[chuk_count, line_count, line]]
        (bool): Return false when the file ends. 
    """

    fileEnd = os.path.getsize(input_file)
    chunk_counter = 0
    line_counter = 0
    keep = True
    with open(input_file, 'r') as f:
        chunkEnd = f.tell()
        while keep:
            chunk = []
            chunk_counter += 1
            while len(chunk) < lines_per_chunk and chunkEnd < fileEnd:

                line = f.readline()
                chunkEnd = f.tell()
                line_counter += 1
                chunk.append([chunk_counter, line_counter, line])

                if chunkEnd >= fileEnd:
                    break
            yield chunk
            if chunkEnd >= fileEnd:
                break
        yield False