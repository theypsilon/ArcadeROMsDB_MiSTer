#!/bin/python3

import json
import subprocess
import tempfile
import os
from pathlib import Path
import xml.etree.cElementTree as ET
import sys
from zipfile import ZIP_DEFLATED, ZipFile
import time
import shlex

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
    tag_dictionary = {
        "mame": 0,
        "hbmame": 1,
        "games": 2,
    }
    
    hbmame_zips = set(load_hash_db_with_fallback(None, hash_dbs_storage, is_hbmame=True, mra=None)[0].keys())
    mame_zips = set(load_hash_db_with_fallback(None, hash_dbs_storage, is_hbmame=False, mra=None)[0].keys())

    for mra in find_all_mras(mra_dirs):
        mameversion, zips, rbf = read_mra_fields(mra)
        
        for z in zips:
            if z == 'jtbeta.zip':
                continue

            is_hbmame = 'hbmame/' in z

            zip_name = Path(z).name
            games_path = ('games/hbmame/%s' % zip_name) if is_hbmame else ('games/mame/%s' % zip_name)
            if games_path in files:
                print('WARNING! File %s tried to be redefined during mra %s' % (games_path, str(mra)))
                continue
            
            hash_db, mameversion = load_hash_db_with_fallback(mameversion, hash_dbs_storage, is_hbmame, mra)
            if zip_name not in hash_db:
                print('INFO: zip_name %s not in hash_db %s for mra %s' % (zip_name, mameversion, str(mra)))
                continue

            tags = [1 if is_hbmame else 0, 2]
            if rbf is not None:
                tags.append(tag_by_rbf(tag_dictionary, rbf))

            hash_description = hash_db[zip_name]
            files[games_path] = {
                "hash": hash_description['md5'],
                "size": hash_description['size'],
                "url": sources['hbmame' if is_hbmame else 'mame'][mameversion] + zip_name,
                "tags": tags
            }

    db = {
        "db_id": 'arcade_roms_db',
        "db_files": [],
        "files": files,
        "folders": {
            "games": {
                "tags": [2]
            },
            "games/mame": {
                "tags": [0, 2]
            },
            "games/hbmame": {
                "tags": [1, 2]
            },
        },
        "zips": {},
        "base_files_url": "",
        "default_options": {
            "downloader_timeout": 900,
            "downloader_retries": 6
        },
        "tag_dictionary": tag_dictionary,
        "timestamp":  int(time.time())
    }

    save_json(db, 'arcade_roms_db.json')

    git_push_branch = os.environ.get('GIT_PUSH_BRANCH', None)
    if git_push_branch is not None:
        try_git_push(db, 'arcade_roms_db.json.zip', git_push_branch)
    print('Done.')

def tag_by_rbf(tag_dictionary, rbf):
    rbf = rbf if rbf.startswith('jt') else ('arcade%s' % rbf)
    if rbf not in tag_dictionary:
        tag_dictionary[rbf] = len(tag_dictionary)
    return tag_dictionary[rbf]

def load_hash_db_with_fallback(old_mameversion, hash_dbs_storage, is_hbmame, mra):
    new_mameversion = old_mameversion
    hash_db = load_hash_db_from_mameversion(new_mameversion, hash_dbs_storage, is_hbmame)
    if hash_db is None:
        new_mameversion = '0220' if is_hbmame else '0217'
        if mra is not None:
            print('WARNING! mameversion "%s" missing for mra %s, falling back to %s.' % (str(old_mameversion), str(mra), new_mameversion))
        hash_db = load_hash_db_from_mameversion(new_mameversion, hash_dbs_storage, is_hbmame)
    return hash_db, new_mameversion

def load_hash_db_from_mameversion(mameversion, hash_dbs_storage, is_hbmame):
    if mameversion is None:
        return None

    db_path = ('hbmamemerged%s.json' % mameversion) if is_hbmame else ('mamemerged%s.json' % mameversion)
    if db_path not in hash_dbs_storage:
        hash_dbs_storage[db_path] = load_json_from_path(db_path)

    return hash_dbs_storage[db_path]

def load_json_from_path(db_path):
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
    rbf = None
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
        elif elem_tag == 'rbf':
            if rbf is not None:
                print('WARNING! Duplicated rbf tag on file %s, first value %s, later value %s' % (str(mra_path),rbf,elem.text))
                continue
            if elem.text is None:
                continue
            rbf = elem.text.strip().lower()

    return mameversion, list(zips), rbf

def save_json(db, json_name):
    zip_name = json_name + '.zip'
    with ZipFile(zip_name, 'w', compression=ZIP_DEFLATED) as zipf:
        with zipf.open(json_name, "w") as jsonf:
            jsonf.write(json.dumps(db, sort_keys=True).encode("utf-8"))
    with open(json_name, 'w') as f:
        json.dump(db, f, sort_keys=True, indent=4)

def load_zipped_json(zip_name, json_name):
    with ZipFile(zip_name, 'r') as zipf:
        with zipf.open(json_name, "r") as jsonf:
            return json.load(jsonf)

def try_git_push(db, file, branch):
    run('git fetch origin')
    proc = run('git show origin/%s:%s > other.json.zip' % (branch, file), shell=True, fail_ok=True)
    other_db = load_zipped_json('other.json.zip', Path(file).stem) if proc.returncode == 0 else {}

    if json.dumps(clean_db(db), sort_keys=True) == json.dumps(clean_db(other_db), sort_keys=True):
        print('No changes deteted.')
        return

    run('git checkout --orphan %s' % branch)
    run('git reset')
    run('git add %s' % file)
    run('git commit -m "-"')
    run('git push --force origin %s' % branch)

def run(cmd, fail_ok=False, shell=False, stderr=subprocess.STDOUT, stdout=None):
    print("Running command: " + cmd)
    proc = subprocess.run(cmd if shell else shlex.split(cmd), shell=shell, stderr=stderr, stdout=stdout)
    if not fail_ok and proc.returncode != 0:
        raise Exception('Command failed!')
    return proc

def clean_db(db):
    db['timestamp'] = 0
    return db

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(e)
        exit(-1)
