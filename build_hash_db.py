#!/bin/python3
# Copyright (c) 2021 José Manuel Barroso Galindo <theypsilon@gmail.com>

import json
import subprocess
import tempfile
import re
import os
import hashlib
from typing import Any, Dict, List
import zlib
import signal
import time
import sys

_print = print
def print(text=""):
    _print(text, flush=True)
    sys.stdout.flush()

skip_list = ['hapyfsh2.zip']

class InterruptHandler:
    def __init__(self, timeout: int):
        self._timeout = timeout
        self._kill_now = False
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args) -> None:
        self._kill_now = True

    def should_end(self) -> bool:
        if self._timeout > 0 and int(time.time()) > self._timeout:
            print('Time is out!')
            return True

        if self._kill_now:
            print('Signal received for termination!')
            return True
        
        return False

def main() -> None:
    print('START!')

    source = os.environ['SOURCE'].strip()
    db_file = os.environ['DB_FILE'].strip()
    timeout = int(os.environ.get('TIMEOUT_MINUTES', '-1').strip()) * 60
    if timeout >= 0:
        timeout += int(time.time())

    print('source: %s' % source)
    print('db_file: %s' % db_file)
    print('timeout: %d' % timeout)

    process(source, InterruptHandler(timeout), db_file)

    print('Done.')

def process(source: str, interrupt_handler: InterruptHandler, db_file: str) -> None:
    if re.fullmatch('https://archive[.]org/download/([-_a-z0-9.%]+)/([-_a-z0-9.%]+)[.]zip/', source.lower()):
        print('process_with_downloads')
        return process_with_downloads(source, interrupt_handler, db_file)

    if re.fullmatch('([-_a-z0-9.%]+)', source.lower()):
        print('process_with_metadata_query')
        return process_with_metadata_query(source, interrupt_handler, db_file)
    
    raise Exception('Could not process source %s' % source)

def process_with_metadata_query(source: str, interrupt_handler: InterruptHandler, db_file: str) -> None:
    proc = subprocess.run(curl(["https://archive.org/metadata/%s" % source]), stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
    if proc.returncode == 0:
        print('Ok')
    else:
        print("FAILED! %d" % proc.returncode)
        exit(-1)

    files = load_files(db_file)

    for description in json.loads(proc.stdout.decode())["files"]:
        if description["format"].strip().lower() != "zip":
            continue

        rom = description["name"]
        if rom in files:
            continue
        
        if rom in skip_list:
            print('Skipping %s' % rom)
            continue

        print(rom)
        save_progress(db_file, files, rom, {
            "md5": description["md5"].strip(),
            "size": int(description["size"].strip())
        })

        if interrupt_handler.should_end():
            return

def process_with_downloads(source: str, interrupt_handler: InterruptHandler, db_file: str) -> None:
    roms = query_roms(source)
    files = load_files(db_file)

    with tempfile.NamedTemporaryFile() as temp:
        for rom in roms:
            if rom in files:
                continue

            rom_description = try_work_on_rom_a_few_times(rom, source, temp, roms[rom], interrupt_handler)
            save_progress(db_file, files, rom, rom_description)

            if interrupt_handler.should_end():
                return

def save_progress(db_file: str, files: Dict[str, Dict[str, Any]], rom: str, rom_description: Dict[str, Any]) -> None:
    if rom_description is not None:
        files[rom] = rom_description

        with open(db_file, 'wt') as f:
            json.dump(files, f, indent=4, sort_keys=True)

def try_work_on_rom_a_few_times(rom: str, source: str, temp: Any, expected_size: int, interrupt_handler: InterruptHandler) -> Dict[str, Any]:
    for try_index in range(3):
        rom_description = work_on_rom(rom, source, temp, expected_size)
        if rom_description is not None:
            return rom_description

        print('Try %d failed!' % try_index)

        if interrupt_handler.should_end():
            return None

        print('Waiting 5 minutes until next try...')
        time.sleep(300.0)

        if interrupt_handler.should_end():
            return None

    print('Aborting execution with errors.')
    exit(-1)

def work_on_rom(rom: str, source: str, temp: Any, expected_size: int) -> Dict[str, Any]:
    print(rom)
    url = source + rom
    proc = subprocess.run(curl(['-o', temp.name, url]), stderr=subprocess.STDOUT)
    if proc.returncode != 0:
        print('Failed! %d' % proc.returncode)
        return None

    filesize = size(temp.name)
    if filesize != expected_size:
        print('File size missmatch')
        print('%s != %s' % (filesize, expected_size))
        return None

    proc = subprocess.run(['unzip', '-t', temp.name], stdout=subprocess.DEVNULL)
    if proc.returncode != 0:
        print('Wrong zip! %d' % proc.returncode)
        return None
    
    md5 = md5_calc(temp.name)
    print(md5)
    print()

    return {
        "md5": md5,
        "size": filesize
    }


def query_roms(source: str) -> Dict[str, int]:
    proc = subprocess.run(curl([source]), stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
    if proc.returncode != 0:
        print('Failed! %d' % proc.returncode)
        exit(1)

    printing = False

    regex = re.compile('.*>([-_a-zA-Z0-9.]+[.]zip)<.*"size">([0-9]+)<.*')
    roms = {}
    
    for line in proc.stdout.decode().splitlines():
        if not printing and '<main id="maincontent">' in line:
            printing = True
        elif '</main>' in line:
            printing = False

        if not printing:
            continue

        match = regex.match(line.lower())
        if not match:
            continue

        roms[match.group(1)] = int(match.group(2))

    return roms

def load_files(db_file: str) -> Dict[str, Dict[str, Any]]:
    files = {}
    if os.path.isfile(db_file):
        with open(db_file, 'r') as f:
            files = json.load(f)

    return files

def curl(params: List[str]) -> List[str]:
    curl_parameters = ['curl']
    if os.environ.get('VERBOSE', 'false') == 'true':
        curl_parameters.append('-L')
    else:
        curl_parameters.append('-sL')
    for s in os.environ.get('CURL_SECURE', '').split():
        curl_parameters.append(s)
    for p in params:
        curl_parameters.append(p)
    return curl_parameters

def md5_calc(file: str) -> str:
    with open(file, "rb") as f:
        file_hash = hashlib.md5()
        chunk = f.read(8192)
        while chunk:
            file_hash.update(chunk)
            chunk = f.read(8192)
        return file_hash.hexdigest()

def crc32_calc(file: str) -> str:
    prev = 0
    for line in open(file,"rb"):
        prev = zlib.crc32(line, prev)
    return "%X"%(prev & 0xFFFFFFFF)

def size(file: str):
    return os.path.getsize(file)

if __name__ == "__main__":
    main()
