#! encoding=utf-8

import logging
import os
import json
import subprocess
import time
from pathlib import Path
import argparse
import sys
from xml.etree import ElementTree


logger = logging.getLogger(__name__)

script_dir = Path(__file__).resolve().parent

known_chips = dict()

history_file = os.path.join(script_dir, ".mdx.pull.json")

if sys.platform == 'win32':
    ossutil = script_dir / "ossutil64.exe"
elif sys.platform == 'linux':
    ossutil = script_dir / 'ossutil64'
elif sys.platform == 'darwin':
    ossutil = script_dir / 'ossutilmac64'
else:
    raise SystemExit(f'Unsupport platform: {sys.platform}')


ossutil = f'{ossutil} --config-file {script_dir}/config.ini'


def load_history(args):
    global known_chips
    logger.info('loading history data...')
    if not os.path.exists(history_file):
        logger.info(f'no history data, mark all exists names in {args.bucket} as known')
        all_chips = get_all_chips(args)
        known_chips = {x: 0 for x in all_chips}
        with open(history_file, 'w') as f:
            json.dump(known_chips, f, indent=2)
        return
    with open(history_file) as f:
        known_chips = json.load(f)
    logger.info(f'data loaded, {len(known_chips)} chips already pulled')


def get_all_chips(args):
    cmd = f'{ossutil} ls -d oss://{args.bucket}/ '
    output = os.popen(cmd).read()
    return [get_chip(x) for x in output.split('\n') if x.startswith('oss://')]


def get_chip(name):
    return name.strip('/').split('/')[-1]


def is_valid(chip):
    if len(chip.split('_')) != 4:
        return False
    if '.' in chip:
        return False
    try:
        int(chip.split('_')[0], 10)
    except ValueError:
        return False
    return True


def find_new_chip(args):
    logger.info('finding new chip...')
    chips = get_all_chips(args)
    new_chips = [x for x in chips if x not in known_chips and is_valid(x)]
    logger.info(f'found {len(chips)} chips in {args.bucket} , {len(new_chips)} new chips')
    return new_chips


def download(name, dest_dir, bucket):
    logger.info(f'Pulling {name}...')
    cmd = f"{ossutil} cp oss://{bucket}/{name} {dest_dir}  -r -u --jobs 30 --parallel 30 "
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT, encoding='utf-8')
    for line in p.stdout:
        msg = line.strip()
        if msg:
            logger.info(line.strip())
    p.wait()
    return p.returncode


def is_file_exists(name, bucket):
    cmd = f'{ossutil} ls -d oss://{bucket}/{name}'
    output = os.popen(cmd).read()
    for line in output.split('\n'):
        if line.startswith('Object and Directory Number is: 0'):
            return False
    return True


def wait_and_download(name, dest_dir, bucket):
    off = False
    while not is_file_exists(name, bucket):
        if not off:
            logger.info(f'Wait {name}...')
            off = True
        time.sleep(60)
    download(name, dest_dir, bucket)


def get_cycle_number(xmlf):
    root = ElementTree.parse(xmlf).getroot()
    count = 0
    for read in root.findall('./Run/Reads/Read'):
        count += int(read.get('NumCycles', 0))
    return count


def download_data(chip, dest_dir, bucket):
    wait_and_download(f'{chip}/Config', dest_dir, bucket)
    wait_and_download(f'{chip}/Recipe', dest_dir, bucket)
    wait_and_download(f'{chip}/RunInfo.xml', dest_dir, bucket)
    cycles = get_cycle_number(dest_dir / chip / 'RunInfo.xml')

    for lane in range(1, 5):
        for cycle in range(1, cycles + 1):
            wait_and_download(f'{chip}/Data/Intensities/BaseCalls/L00{lane}/{str(cycle).zfill(4)}.bcl.bgzf', dest_dir, bucket)
            wait_and_download(f'{chip}/Data/Intensities/BaseCalls/L00{lane}/{str(cycle).zfill(4)}.bcl.bgzf.bci', dest_dir, bucket)
            wait_and_download(f'{chip}/InterOp', dest_dir, bucket)
    for lane in range(1, 5):
        wait_and_download(f'{chip}/Data/Intensities/L00{lane}/s_{lane}.locs', dest_dir, bucket)
        wait_and_download(f'{chip}/Data/Intensities/BaseCalls/L00{lane}/s_{lane}.bci', dest_dir, bucket)
        wait_and_download(f'{chip}/Data/Intensities/BaseCalls/L00{lane}/s_{lane}.filter', dest_dir, bucket)


def download_till_finish(name, dest_dir, bucket):
    dest_dir = Path(dest_dir)
    logger.info(f'download loop started for chip: {name}')
    download_data(name, dest_dir, bucket)
    while not is_sequencing_finisehd(dest_dir / name):
        download(name, dest_dir, bucket)
        time.sleep(30)
    logger.info('sequence finished, stop pulling')
    known_chips[name] = 1
    with open(history_file, 'w') as f:
        json.dump(known_chips, f, indent=2)


def is_sequencing_finisehd(chip_dir):
    chip_dir = Path(chip_dir)
    done_flag = chip_dir / 'RunCompletionStatus.xml'
    runinfo = chip_dir / 'RunInfo.xml'
    if not done_flag.exists():
        logger.debug('done flag not found, sequencing not finished')
        return False
    if not runinfo.exists():
        logger.debug('runinfo not found, sequencing not finished')
        return False
    cycles = find_read_length(runinfo)
    cycle_files = list((chip_dir / 'Data/Intensities/BaseCalls').glob(f'**/*.bcl.bgzf'))
    if len(cycle_files) != 4 * cycles:
        logger.debug(f'{len(cycle_files)} cycle files found, expect {4 * cycles}')
        return False
    logger.debug('sequence finished, but we still wait for 300s...')
    return time.time() - done_flag.stat().st_ctime >= 300


def find_read_length(xmlfile):
    tree = ElementTree.parse(xmlfile)
    count = 0
    for read in tree.findall('./Run/Reads/Read'):
        count += int(read.get('NumCycles', 0))
    return count


def main():
    args = arg_handle()
    if args.verbose:
        level = 'DEBUG'
        formatstr = '%(levelname)s %(asctime)s %(module)s %(process)d => %(message)s'
    else:
        level = 'INFO'
        formatstr = "%(levelname)s %(message)s"
    if args.log:
        logging.basicConfig(filename=args.log, level=level, format=formatstr)
    else:
        logging.basicConfig(level=level, format=formatstr)

    logger.info('program start')
    load_history(args)
    while True:
        logger.debug('loop start')
        new_chips = find_new_chip(args)
        for chip in new_chips:
            download_till_finish(chip, args.dest, args.bucket)
        logger.debug(f'wait {args.interval}s for next loop')
        time.sleep(args.interval)


def arg_handle():
    parser = argparse.ArgumentParser()
    parser.add_argument('dest', metavar='dest_dir', type=Path, help='dest dir to save download data')
    parser.add_argument('--log', metavar='file', help='write log to logfile')
    parser.add_argument('-v', '--verbose', dest="verbose", help='output all log info',
                        action='store_true', default=False)
    parser.add_argument('--bucket', metavar='bucket', help='bucket name', required=True)
    parser.add_argument('--interval', metavar='int', help='wait how many seconds between two loop',
                        default=300, type=int)
    return parser.parse_args()


if __name__ == "__main__":
    main()
