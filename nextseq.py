"""
    NextSeq 550 测序文件类
"""

from pathlib import Path
from xml.etree import ElementTree
import time
import logging

logger = logging.getLogger(__name__)


class Sequence(object):
    def __init__(self, seqdir, lane: int=4):
        self.seq_dir = Path(seqdir).resolve()
        self.lane_count = int(lane)
        self.chip = self.seq_dir.name

    @property
    def cycle_count(self):
        if not self.run_info_xml.exists():
            return 9999
        tree = ElementTree.parse(self.run_info_xml)
        count = 0
        for read in tree.findall('./Run/Reads/Read'):
            count += int(read.get('NumCycles', 0))
        return count

    def text_files(self):
        self.wait_file(self.rta_complete_txt)
        yield self.rta_complete_txt
        for file in self.rta_read_complete_txts:
            yield file

        yield self.run_info_xml
        yield self.run_parammeters_xml
        yield self.rta_configuration_xml

        # wait for run totally completion
        self.wait_file(self.run_completion_status_xml)
        yield self.run_completion_status_xml

    def iter_data_files(self):
        self.wait_file(self.run_info_xml)
        yield self.run_info_xml
        self.wait_file(self.run_parammeters_xml)
        yield self.run_parammeters_xml

        # lane level bci file
        for file in self.lane_bci_files:
            self.wait_file(file)
            yield file

        # first 5 cycles
        for cycle in range(1, 6):
            self.wait_cycle(cycle)
            for lane in range(1, self.lane_count + 1):
                yield self.cycle_bcl_files(cycle, lane)
                yield self.cycle_bcl_index_files(cycle, lane)

        # location files
        self.wait_file(self.cycle_bcl_files(cycle=6, lane=1))  # cycle 6出现时location文件已经生成，且不再变动
        for file in self.location_files:
            yield file

        # 6 - last cycles
        this_cycle = 6
        while self.cycle_count >= this_cycle:
            self.wait_cycle(this_cycle)
            for lane in range(1, self.lane_count + 1):
                yield self.cycle_bcl_files(this_cycle, lane)
                yield self.cycle_bcl_index_files(this_cycle, lane)
            if this_cycle == 25:  # 第25个cycle以后，出现filters文件
                for file in self.filter_files:
                    self.wait_file(file)
                    yield file
            this_cycle += 1
        # wait till RTA complete
        self.wait_file(self.rta_complete_txt)
        yield self.rta_configuration_xml
        yield self.run_info_xml
        yield self.run_parammeters_xml
        yield self.rta_complete_txt
        for x in self.rta_read_complete_txts:
            yield x

    def wait_cycle(self, cycle, interval=30):
        """等待cycle结束，bcl文件出现时会很快写完，每个bcl文件大小在50多M，第一个cycle出现时，RunInfo.xml就已经有了"""
        logger.debug(f'wait cycle {cycle} ...')
        this_cycle_file = self.cycle_bcl_files(cycle=cycle, lane=1)
        next_cycle_file = self.cycle_bcl_files(cycle=cycle + 1, lane=1)
        while not this_cycle_file.exists():
            time.sleep(interval)
        if not next_cycle_file.exists():
            time.sleep(interval)  # wait all cycle files to generated
        logger.debug(f'cycle {cycle} finished')

    def wait_file(self, file, interval=10):
        logger.debug(f'wait {file} appear...')
        while not Path(file).exists():
            time.sleep(interval)
        time.sleep(interval)  # wait file write done
        logger.debug(f'{file} appeared')

    def dynamic_paths(self):
        return [self.interop_dir]

    def non_important_paths(self):
        return [self.images_dir, self.thumbnail_images_dir, self.rtalogs_dir, self.logs_dir]

    def static_path(self):
        return [self.recipe_dir, self.config_dir]

    def is_run_complete(self):
        return self.run_completion_status_xml.exists()

    def is_rta_complete(self):
        return self.rta_complete_txt.exists()

    def is_file_complete(self):
        bcl_complete = self.all_exists(self.all_bcl_files)
        bci_complete = self.all_exists(self.all_bcl_index_files)
        lane_bci_complete = self.all_exists(self.lane_bci_files)
        locs_complete = self.all_exists(self.location_files)
        filter_complete = self.all_exists(self.filter_files)
        run_info_exists = self.run_info_xml.exists()
        return run_info_exists and locs_complete and filter_complete and lane_bci_complete and bcl_complete and bci_complete

    @staticmethod
    def all_exists(file_list):
        for x in file_list:
            if not x.exists():
                return False
        return True

    @property
    def config_dir(self):
        return self.seq_dir / 'Config'

    @property
    def recipe_dir(self):
        return self.seq_dir / 'Recipe'

    @property
    def data_dir(self):
        return self.seq_dir / 'Data'

    @property
    def intensities_dir(self):
        return self.data_dir / 'Intensities'

    @property
    def location_files(self):
        files = []
        for lane in range(1, self.lane_count + 1):
            files.append(self.intensities_dir / f'L00{lane}' / f's_{lane}.locs')
        return files

    @property
    def filter_files(self):
        files = []
        for lane in range(1, self.lane_count + 1):
            files.append(self.basecall_dir / f'L00{lane}' / f's_{lane}.filter')
        return files

    @property
    def basecall_dir(self):
        return self.intensities_dir / 'BaseCalls'

    def cycle_bcl_files(self, cycle, lane):
        return self.basecall_dir / f'L00{lane}' / f'{str(cycle).zfill(4)}.bcl.bgzf'

    def cycle_bcl_index_files(self, cycle, lane):
        return self.basecall_dir / f'L00{lane}' / f'{str(cycle).zfill(4)}.bcl.bgzf.bci'

    @property
    def all_bcl_files(self):
        files = []
        for lane in range(1, self.lane_count + 1):
            for cycle in range(1, self.cycle_count + 1):
                files.append(self.basecall_dir / f'L00{lane}' / f'{str(cycle).zfill(4)}.bcl.bgzf')
        return files

    @property
    def all_bcl_index_files(self):
        files = []
        for lane in range(1, self.lane_count + 1):
            for cycle in range(1, self.cycle_count + 1):
                files.append(
                    self.basecall_dir / f'L00{lane}' / f'{str(cycle).zfill(4)}.bcl.bgzf.bci')
        return files

    @property
    def lane_bci_files(self):
        files = []
        for lane in range(1, self.lane_count + 1):
            files.append(self.basecall_dir / f'L00{lane}' / f's_{lane}.bci')
        return files

    @property
    def interop_dir(self):
        return self.seq_dir / 'InterOp'

    @property
    def images_dir(self):
        return self.seq_dir / 'Images'

    @property
    def rtalogs_dir(self):
        return self.seq_dir / 'RTALogs'

    @property
    def logs_dir(self):
        return self.seq_dir / 'Logs'

    @property
    def run_completion_status_xml(self):
        return self.seq_dir / 'RunCompletionStatus.xml'

    @property
    def run_info_xml(self):
        return self.seq_dir / 'RunInfo.xml'

    @property
    def run_parammeters_xml(self):
        return self.seq_dir / 'RunParameters.xml'

    @property
    def thumbnail_images_dir(self):
        return self.seq_dir / 'Thumbnail_Images'

    @property
    def rta_complete_txt(self):
        return self.seq_dir / 'RTAComplete.txt'

    @property
    def rta_read_complete_txts(self):
        return self.seq_dir.glob('RTARead*Complete.txt')

    @property
    def rta_configuration_xml(self):
        return self.seq_dir / 'RTAConfiguration.xml'
