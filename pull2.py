#! encoding=utf-8

import logging
import os
import json
import subprocess
import time
from pathlib import Path
import argparse
import sys
from nextseq import Sequence
import hashlib
from threading import Thread
from queue import PriorityQueue
import signal
import configparser
import base64


import oss2
from oss2 import SizedFileAdapter, determine_part_size
from oss2.models import PartInfo


logger = logging.getLogger(__name__)

logging.getLogger('oss2').setLevel(logging.WARNING)

script_dir = Path(__file__).resolve().parent


if sys.platform == 'win32':
    ossutil = script_dir / "ossutil64.exe"
elif sys.platform == 'linux':
    ossutil = script_dir / 'ossutil64'
elif sys.platform == 'darwin':
    ossutil = script_dir / 'ossutilmac64'
else:
    raise SystemExit(f'Unsupport platform: {sys.platform}')


class PushTask(object):
    known_chips = {}
    queued_chips = set()
    history_file = os.path.join(script_dir, ".mdx.pull.json")
    new_chips = PriorityQueue()
    current_chip = 'NULL'
    exit_stat = False
    failed_files = []
    keyid = ""
    keysec = ""
    endpoint = ""
    auth = None
    bucket = None

    def __init__(self, dest, bucket, work_dir=".", history_file=".mdx.pull.json",
                 configfile="config.ini", dry_run=False, force=False):
        self.dest = Path(dest).resolve()
        self.bucket_name = bucket
        self.work_dir = work_dir
        self.history_file = Path(work_dir) / history_file
        self.config_file = Path(work_dir) / configfile
        self.dry_run = dry_run
        self.force = force

    def check_config(self):
        parser = configparser.ConfigParser()
        parser.read(self.config_file)
        try:
            self.keyid = parser['Credentials']['accessKeyID']
            self.keysec = parser['Credentials']['accessKeySecret']
            self.endpoint = parser['Credentials']['endpoint']
            self.auth = oss2.Auth(self.keyid, self.keysec)
            self.bucket = oss2.Bucket(self.auth, self.endpoint,
                                      self.bucket_name)
            self.bucket.list_objects()
        except Exception:
            raise SystemExit('Invalid config file')

    def load_history(self):
        logger.info('loading history data...')
        if not self.history_file.exists():
            logger.info(f'no history data, mark all names in {self.dest} as known')
            all_names = os.listdir(self.dest)
            self.known_chips = {x: 0 for x in all_names}
            with open(self.history_file, 'w') as f:
                json.dump(self.known_chips, f, indent=2)
            return
        with open(self.history_file) as f:
            self.known_chips = json.load(f)
        logger.info('data loaded, {0} chips already pulled'.format(len(self.known_chips)))

    @property
    def chip_dir(self):
        return self.dest / self.current_chip

    @property
    def seq(self):
        return Sequence(self.chip_dir)

    def find_new_chip(self):
        logger.info('finding new chip...')
        chips = os.listdir(self.dest)
        valid_chips = [x for x in chips if (self.dest / x).is_dir() and len(x.split('_')) == 4]
        count = 0
        for x in valid_chips:
            if x not in self.known_chips and x not in self.queued_chips:
                self.new_chips.put((10, x))
                self.queued_chips.add(x)
                count += 1
        logger.info(f'found {len(valid_chips)} valid chips, {count} new chips, {len(self.queued_chips)} queued chips')

    def pull_path(self, path, force=None):
        path = Path(path)
        if path.is_dir():
            self.pull_dir(path, force=force)
        else:
            self.pull_file(path, force=force)

    def pull_dir(self, path, force=None):
        logger.info(f'Pushing {path}...')
        path = Path(path)
        for sub in os.listdir(path):
            sub = path / sub
            if sub.is_dir():
                self.pull_dir(sub, force=force)
            else:
                self.pull_file(sub, force=force)

    def pull_by_piece(self, path, name):
        path = Path(path)
        total_size = path.stat().st_size
        part_size = determine_part_size(total_size, preferred_size=1024 * 1024)
        upload_id = self.bucket.init_multipart_upload(name).upload_id
        parts = []
        with open(path, 'rb') as fileobj:
            part_number = 1
            offset = 0
            while offset < total_size:
                num_to_upload = min(part_size, total_size - offset)
                # SizedFileAdapter(fileobj, size)方法会生成一个新的文件对象，重新计算起始追加位置。
                result = self.bucket.upload_part(name, upload_id, part_number,
                                                 SizedFileAdapter(fileobj,
                                                                  num_to_upload))
                parts.append(PartInfo(part_number, result.etag))
                offset += num_to_upload
                part_number += 1
        headers = {'Content-MD5': self.get_md5(path)}
        self.bucket.complete_multipart_upload(name, upload_id, parts, headers=headers)

    def check_size(self, path, name):
        obj = self.bucket.get_object(name)
        if obj.content_length != path.stat().st_size:
            raise ValueError(f'file size error, remote({obj.content_length}) != local({path.stat().st_size})')

    def pull_file(self, path, force=None):
        logger.info(f'Pushing {path}...')
        path = Path(path)
        name = path.relative_to(self.dest).as_posix()
        if force is None:
            force = self.force  # local force 有高优先级
        try:
            remoteobj = self.bucket.get_object(name)
            if not force and not self.force and remoteobj.content_length == path.stat().st_size:
                return
        except oss2.exceptions.NoSuchKey:
            pass

        try:
            oss2.resumable_upload(self.bucket, name, filename=str(path), num_threads=3)
            self.check_size(path, name)
        except Exception as e:
            logger.error(f'Push {path} error, msg: {e}')
            self.failed_files.append(path)

    @staticmethod
    def get_md5(file):
        file = Path(file)
        if Path(file).is_dir():
            return ""
        hashobj = hashlib.md5()
        hashobj.update(file.read_bytes())
        return base64.b64encode(hashobj.digest())

    def pull(self):
        logger.info(f'Push {self.current_chip}...')
        self.failed_files = []  # 清空错误列表
        if self.seq.is_file_complete() and self.seq.is_run_complete() and self.seq.is_rta_complete():
            logger.info('Sequencing finished, pull all...')
            self.pull_path(self.chip_dir)
            return
        # pull 配置文件
        self.seq.wait_file(self.seq.recipe_dir)
        self.pull_path(self.seq.recipe_dir)

        self.seq.wait_file(self.seq.config_dir)
        self.pull_path(self.seq.config_dir)
        # pull data目录
        count = 0
        for path in self.seq.iter_data_files():
            self.pull_path(path)
            count += 1
            if count % 16 == 0:
                self.pull_path(self.seq.interop_dir)  # 每2个cycle pull一次 interop

        for path in self.seq.non_important_paths():
            self.pull_path(path)

        self.pull_path(self.seq.interop_dir)  # 最后再pull一次interop

        # pull again failed files
        pull_error = self.failed_files.copy()
        self.failed_files = []
        for path in pull_error:
            self.pull_path(path, force=True)
        if len(self.failed_files) != 0:
            logger.error(f'{len(self.failed_files)} pull failed， they are: {self.failed_files}')

        self.pull_path(self.seq.run_completion_status_xml)  # 最最后pull run结束的标记

        self.known_chips[self.current_chip] = 1
        self.queued_chips.remove(self.current_chip)
        with open(self.history_file, 'w') as f:
            json.dump(self.known_chips, f, indent=2)

    def consumer(self):
        logger.info('Start consumer...')
        while True:
            _, chip = self.new_chips.get()
            if chip is None:
                break
            self.current_chip = chip
            self.pull()

    def producer(self):
        logger.info('Start producer...')
        while not self.exit_stat:
            self.find_new_chip()
            for x in range(300):
                time.sleep(1)
                if self.exit_stat:
                    break

    def loop(self):
        self.check_config()
        self.load_history()
        signal.signal(signal.SIGINT, self.signal_handle)
        signal.signal(signal.SIGTERM, self.signal_handle)
        t1 = Thread(target=self.producer, daemon=True)
        t1.start()
        t2 = Thread(target=self.consumer, daemon=True)
        t2.start()
        t2.join()
        t1.join()
        raise SystemExit('Exit loop')

    def signal_handle(self, signum, _):
        if self.exit_stat:
            raise SystemExit(f'Force exit by signal: {signum}')
        logger.warning(f'SIG {signum} received')
        logger.warning('Stop producer')
        self.exit_stat = True
        logger.warning('Warm stop consumer')
        self.new_chips.put((-1, None))
        logger.info('Wait current task finishing...')


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

    task = PushTask(args.dest, args.bucket, configfile=args.config, dry_run=args.dry_run, force=args.force)
    task.loop()


def arg_handle():
    parser = argparse.ArgumentParser()
    parser.add_argument('dest', metavar='dest_dir', type=Path, help='source dir to monitor and upload')
    parser.add_argument('--log', metavar='file', help='write log to logfile')
    parser.add_argument('-v', '--verbose', dest="verbose", help='output all log info',
                        action='store_true', default=False)
    parser.add_argument('--bucket', metavar='bucket', help='bucket name',
                        required=True)
    parser.add_argument('--config-file', metavar='file', dest='config',
                        help='Config file to use', default='config.ini')
    parser.add_argument('--dry-run', dest='dry_run', action='store_true',
                        help='Only print cmd, not exec it', default=False)
    parser.add_argument('--force', action='store_true', help='force pull, ignore existing files in server', default=False)
    return parser.parse_args()


if __name__ == "__main__":
    main()
