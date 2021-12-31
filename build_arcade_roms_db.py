#!/bin/python3

import json
import subprocess
import tempfile
import os
from pathlib import Path
import xml.etree.cElementTree as ET
import sys

_print = print
def print(text=""):
    _print(text, flush=True)
    sys.stdout.flush()

def main():
    print('START!')

    with open('arcade_mra_sources.json', 'r') as f:
        sources = json.load(f)

    mra_dirs = 'delme/'

    for mra_url in sources:
        with tempfile.NamedTemporaryFile() as temp:
            print('Downloading ' + mra_url)
            result = subprocess.run(['curl', '-L', '-o', temp.name, mra_url], stderr=subprocess.STDOUT)
            if result.returncode == 0:
                print('Ok')
            else:
                print("FAILED!")
                exit(-1)

            result = subprocess.run(['unzip', temp.name, sources[mra_url], '-d', mra_dirs], stderr=subprocess.STDOUT)
            if result.returncode == 0:
                print('Ok')
            else:
                print("FAILED!")
                exit(-1)
        
        print()

    for mra in find_all_mras(mra_dirs):
        mameversion, zips = read_mra_fields(mra)
        print(mameversion)
        print(zips)

    print('Done.')

def find_all_mras(directory):
    return sorted(_find_all_mras_scan(directory), key=lambda mra: mra.name.lower())

def _find_all_mras_scan(directory):
    for entry in os.scandir(directory):
        if entry.is_dir(follow_symlinks=False):
            yield from _find_all_mras_scan(entry.path)
        elif entry.name.lower().endswith(".mra"):
            yield Path(entry.path)

def read_mra_fields(mra_path):
    mameversion = None
    zips = set()

    context = ET.iterparse(str(mra_path), events=("start",))
    for _, elem in context:
        elem_tag = elem.tag.lower()
        if elem_tag == 'mameversion':
            if mameversion is not None:
                print('WARNING! Duplicated mameversion tag on file %s, first value %s, later value %s' % (str(mra_path),mameversion,elem.text))
                continue
            mameversion = elem.text.strip().lower()
        elif elem_tag == 'rom':
            attributes = {k.strip().lower(): v for k, v in elem.attrib.items()}
            if 'zip' in attributes:
                zips |= [z.strip().lower() for z in attributes['zip'].strip().lower().split('|')]

    return mameversion, list(zips)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(e)
        exit(-1)
