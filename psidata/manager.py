import logging
log = logging.getLogger(__name__)

import fnmatch
import json
import os
from pathlib import Path

from joblib import Parallel, delayed
from tqdm.auto import tqdm

from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
import numpy as np

from psiaudio.util import get_cb


class ProgressParallel(Parallel):
    '''
    Provide a progressbar to track status of parallel jobs
    '''
    def __init__(self, use_tqdm=True, total=None, *args, **kwargs):
        self._use_tqdm = use_tqdm
        self._total = total
        super().__init__(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        with tqdm(disable=not self._use_tqdm, total=self._total) as self._pbar:
            return Parallel.__call__(self, *args, **kwargs)

    def print_progress(self):
        if self._total is None:
            self._pbar.total = self.n_dispatched_tasks
        self._pbar.n = self.n_completed_tasks
        self._pbar.refresh()


class CallbackManager:

    def __init__(self, cb, autoclose_figures=True):
        self._cb = cb
        self._autoclose_figures = autoclose_figures

    def __enter__(self):
        self._cb(0)
        return self

    def __call__(self, value):
        return self._cb(value)

    def __exit__(self, exc_type, exc_value, exc_tb):
        self._cb(1)
        if self._autoclose_figures:
            plt.close('all')


def add_default_options(parser):
    parser.add_argument('folder', type=str, help='Folder containing data')
    parser.add_argument('-m', '--mode', choices=['process', 'reprocess', 'clear'], default='process')
    parser.add_argument('--halt-on-error', action='store_true', help='Stop on error?')
    parser.add_argument('--logging-level', type=str, help='Logging level')
    parser.add_argument('--n-jobs', type=int, default=1)


def process_files(glob_pattern, fn, folder, cb='tqdm', mode='process',
                  halt_on_error=False, logging_level=None,
                  expected_suffixes=None, n_jobs=1):

    # Override callback if we are running in parallel otherwise it's messy
    if n_jobs > 1:
        cb = None

    def _process_file(filename):
        nonlocal cb
        nonlocal mode
        nonlocal expected_suffixes

        try:
            manager = DatasetManager(filename, cb=cb)
            if mode == 'process' and manager.is_processed(expected_suffixes):
                pass
            elif mode == 'process' and not manager.is_processed(expected_suffixes):
                fn(filename, manager=manager)
                return True
            elif mode == 'reprocess':
                fn(filename, manager=manager)
                return True
            elif mode == 'clear':
                manager.clear(expected_suffixes)
                return True
        except Exception as e:
            raise
        return False

    if logging_level is not None:
        logging.basicConfig(level=logging_level.upper())

    processed = []
    skipped = []
    errors = []

    # Adds a shortcut for situations where the full path to a single data
    # folder is provided.
    folder = Path(folder).resolve()
    if fnmatch.fnmatch(folder.name, glob_pattern):
        _process_file(folder)
        return

    jobs = []
    for filename in folder.glob(glob_pattern):
        if filename.suffix == '.md5':
            # Skip the MD5 checksum files
            continue
        if filename.is_dir():
            # Make sure that it is actually a psiexperiment recording
            if not (filename / 'io.json').exists():
                continue
        elif filename.suffix != '.zip':
            continue

        if n_jobs == 1:
            try:
                if _process_file(filename):
                    processed.append(filename)
                else:
                    skipped.append(filename)
            except KeyboardInterrupt:
                # Don't capture this otherwise it just keeps continuing with the
                # next file.
                raise
            except Exception as e:
                if halt_on_error:
                    raise
                errors.append((filename, e))
                print(f'Error processing {filename}')
            finally:
                plt.close('all')
        else:
            job = delayed(_process_file)(filename)
            jobs.append(job)

    if n_jobs == 1:
        print(f'Processed {len(processed)} files with {len(errors)} errors. {len(skipped)} files were skipped.')
    else:
        result = ProgressParallel(n_jobs=n_jobs)(jobs)
        n_processed = sum(result)
        n_skipped = len(result) - n_processed
        print(f'Processed {n_processed} files. {n_skipped} files were skipped.')


class PsiDataJsonEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, np.bool_):
            return bool(obj)
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, Path):
            return str(obj)
        elif callable(obj):
            return f'{obj.__module__}.{obj.__name__}'
        else:
            return super().default(obj)


class BaseDatasetManager:

    def __init__(self, path, cb='tqdm', file_template=None):
        '''
        Manages paths of processed files given the relative path between the
        raw and processed directory structure.

        Parameters
        ----------
        path : {str, Path}
            Base path containing raw data
        cb : str
            Callback to use for showing processing status. See `get_cb` for
            options.
        file_template : {None, str}
            If None, defaults to the filename stem
        '''
        self.path = Path(path)
        self.cb = cb
        if file_template is None:
            file_template = f'{self.path.stem}'
        self.file_template = file_template

    def create_cb(self, cb=None):
        if cb is None:
            cb = self.cb
        return CallbackManager(get_cb(cb, self.path.stem))

    def get_proc_path(self):
        raise NotImplementedError

    def get_proc_filename(self, suffix, mkdir=True):
        proc_path = self.get_proc_path()
        proc_path.mkdir(exist_ok=True, parents=True)
        return proc_path / f'{self.file_template} {suffix}'

    def is_processed(self, suffixes):
        if isinstance(suffixes, str):
            suffixes = [suffixes]
        for suffix in suffixes:
            if not self.get_proc_filename(suffix).exists():
                #log.info(f'Needs reprocessing since {suffix} is missing')
                return False
        return True

    def save_dict(self, d, suffix):
        filename = self.get_proc_filename(suffix)
        filename.write_text(json.dumps(d, indent=4, cls=PsiDataJsonEncoder))

    def save_fig(self, figure, suffix, add_filename=True):
        filename = self.get_proc_filename(suffix)
        if add_filename:
            figure.suptitle(filename.stem)
        figure.savefig(filename, bbox_inches='tight')

    def save_figs(self, figures, suffix):
        filename = self.get_proc_filename(suffix).with_suffix('.pdf')
        with PdfPages(filename) as pdf:
            for figure in figures:
                pdf.savefig(figure, bbox_inches='tight')

    def save_dataframe(self, df, suffix, **kw):
        filename = self.get_proc_filename(suffix)
        df.to_csv(filename, **kw)

    save_df = save_dataframe

    def clear(self, suffixes):
        for suffix in suffixes:
            filename = self.get_proc_filename(suffix)
            if filename.exists():
                filename.unlink()


class CombinedDatasetManager(BaseDatasetManager):

    def get_proc_path(self):
        return self.path.parent


class SplitDatasetManager(BaseDatasetManager):

    def __init__(self, path, raw_dir=None, proc_dir=None, file_template=None):
        '''
        Manages paths of processed files given the relative path between the
        raw and processed directory structure.

        Parameters
        ----------
        raw_dir : {None, str, Path}
            Base path containing raw data
        proc_dir : {None, str, Path}
            Base path containing processed data
        file_template : {None, str}
            If None, defaults to the filename stem
        '''
        super().__init__(path, file_template)
        if raw_dir is None:
            raw_dir = os.environ.get('RAW_DATA_DIR', None)
        if proc_dir is None:
            proc_dir = os.environ.get('PROC_DATA_DIR', None)
        self.raw_dir = Path(raw_dir)
        self.proc_dir = Path(proc_dir)

    def get_proc_path(self):
        return self.proc_dir / self.path.parent.relative_to(self.raw_dir) / self.path.stem


# TODO: How do we make this so that it is a bit more clever about selecting the
# correct manager based on preference?
DatasetManager = CombinedDatasetManager
