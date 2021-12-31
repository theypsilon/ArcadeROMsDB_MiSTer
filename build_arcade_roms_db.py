#!/bin/python3

import json
import subprocess
import tempfile
import os
from pathlib import Path
from xml.etree.ElementTree import ParseError
import xml.etree.cElementTree as ET
import sys

_print = print
def print(text=""):
    _print(text, flush=True)
    sys.stdout.flush()

def main():
    print('START!')

    with open('arcade_sources.json', 'r') as f:
        sources = json.load(f)

    mra_dirs = 'delme/'

    for mra_url in sources['mra']:
        with tempfile.NamedTemporaryFile() as temp:
            print('Downloading ' + mra_url)
            result = subprocess.run(['curl', '-L', '-o', temp.name, mra_url], stderr=subprocess.STDOUT)
            if result.returncode == 0:
                print('Ok')
            else:
                print("FAILED!")
                exit(-1)

            result = subprocess.run(['unzip', temp.name, sources['mra'][mra_url], '-d', mra_dirs], stderr=subprocess.STDOUT)
            if result.returncode == 0:
                print('Ok')
            else:
                print("FAILED!")
                exit(-1)
        
        print()

    hash_dbs_storage = {}
    files = {}

    for mra in find_all_mras(mra_dirs):
        mameversion, zips = read_mra_fields(mra)
        hash_db = load_hash_db(mameversion, hash_dbs_storage)
        if hash_db is None:
            print('WARNING! mameversion "%s" missing for mra %s, falling back to 0217.' % (str(mameversion), str(mra)))
            mameversion = '0217'
            hash_db = load_hash_db(mameversion, hash_dbs_storage)
        for z in zips:
            if z == 'jtbeta.zip':
                continue

            is_hbmame = 'hbmame/' in z
            zip_name = Path(z).name
            games_path = ('games/hbmame/%s' % zip_name) if is_hbmame else ('games/mame/%s' % zip_name)
            if games_path in files:
                print('WARNING! File %s tried to be redefined during mra %s' % (games_path, str(mra)))
                continue
            
            if zip_name not in hash_db:
                print('INFO: zip_name %s not in hash_db %s for mra %s' % (zip_name, mameversion, str(mra)))
                continue

            hash_description = hash_db[zip_name]
            files[games_path] = {
                "hash": hash_description['md5'],
                "size": hash_description['size'],
                "url": sources['ao'][mameversion] + zip_name
            }

    with open('result.json', 'w') as f:
        json.dump(files, f, indent=4, sort_keys=True)

    print('Done.')

def load_hash_db(mameversion, hash_dbs_storage):
    if mameversion not in hash_dbs_storage:
        hash_dbs_storage[mameversion] = hash_db_from_mameversion(mameversion)

    return hash_dbs_storage[mameversion]

def hash_db_from_mameversion(mameversion):
    if mameversion is None:
        return None

    db_path = 'mamemerged%s.json' % mameversion
    if not Path(db_path).is_file():
        return None

    with open(db_path) as f:
        return json.load(f)

def find_all_mras(directory):
    return sorted(_find_all_mras_scan(directory), key=lambda mra: mra.name.lower())

def _find_all_mras_scan(directory):
    for entry in os.scandir(directory):
        if entry.is_dir(follow_symlinks=False):
            yield from _find_all_mras_scan(entry.path)
        elif entry.name.lower().endswith(".mra"):
            yield Path(entry.path)

def et_iterparse(mra_file, events):
    with open(mra_file, 'r') as f:
        text = f.read()

    with tempfile.NamedTemporaryFile() as temp:
        with open(temp.name, 'w') as f:
            f.write(text.lower())

        return ET.iterparse(temp.name, events=events)

def read_mra_fields(mra_path):
    mameversion = None
    zips = set()

    context = et_iterparse(str(mra_path), events=("start",))
    for _, elem in context:
        elem_tag = elem.tag.lower()
        if elem_tag == 'mameversion':
            if mameversion is not None:
                print('WARNING! Duplicated mameversion tag on file %s, first value %s, later value %s' % (str(mra_path),mameversion,elem.text))
                continue
            if elem.text is None:
                continue
            mameversion = elem.text.strip().lower()
        elif elem_tag == 'rom':
            attributes = {k.strip().lower(): v for k, v in elem.attrib.items()}
            if 'zip' in attributes and attributes['zip'] is not None:
                zips |= {z.strip().lower() for z in attributes['zip'].strip().lower().split('|')}

    return mameversion, list(zips)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(e)
        exit(-1)
