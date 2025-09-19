import datetime as dt
import functools
from pathlib import Path
import re
import yaml
import zipfile

import jmespath
import pandas as pd


# Address issue where yaml was refusing to load lists with this tag.
yaml.add_constructor(
    'tag:yaml.org,2002:python/object/apply:builtins.list',
    lambda l, n: l.construct_sequence(n)
)


def get_filename_parser(experiments, include_ear):
    experiment_str = '|'.join(experiments)

    pattern = re.compile(
        r'^(?P<datetime>\d{8}-\d{6}) '
        r'(?P<experimenter>\w+) '
        r'(?P<animal_id>[-\w]+) '
        r'((?P<ear>left|right) )?'
        r'((?P<note>.*) )?'
        r'(?P<experiment_type>(?:' + experiment_str + ')(_\w+)?).*$'
    )

    return functools.partial(parse_psi_filename, include_ear=include_ear,
                             pattern=pattern)


def parse_psi_filename(filename, include_ear, pattern):
    try:
        groups = pattern.match(filename.stem).groupdict()
        groups['datetime'] = dt.datetime.strptime(groups['datetime'], '%Y%m%d-%H%M%S')
        groups['date'] = pd.to_datetime(groups['datetime'].date())
        groups['time'] = groups['datetime'].time()
        if not include_ear:
            del groups['ear']
        return groups
    except AttributeError:
        raise ValueError(f'Could not parse {filename.stem}')


def load(cb, glob, filename_parser, data_path, include_dataset=False,
         should_load_cb=None, info_as_cols=True):
    '''
    Parameters
    ----------
    cb : callable
        Callback that takes name of file and returns a DataFrame or Series.
    glob : string
        Wildcard pattern used to find files to load.
    filename_parser : {None, callable}
        Callback that returns dictionary containing keys that will be added
        as columns to the result.
    data_path : {None, string, Path}
        Path to scan for data. If not provided, defaults to the ephys path.
    include_dataset : bool
        If True, include the name of the dataset the file was found in
        (i.e., the parent folder).
    should_load_cb : {None, callable}
        Callback that returns True if the file should be loaded. If a
        callback is not provided, all files found are loaded.
    '''
    data_path = Path(data_path)
    if should_load_cb is None:
        should_load_cb = lambda x: True
    result = []
    for filename in data_path.glob(glob):
        try:
            if '_exclude' in str(filename):
                continue
            if '.imaris_cache' in str(filename):
                continue
            if not should_load_cb(filename):
                continue
            data = cb(filename)
            info = filename_parser(filename)
            if include_dataset:
                info['dataset'] = filename.parent

            if info_as_cols:
                for k, v in info.items():
                    if k in data:
                        raise ValueError('Column will get overwritten')
                    data[k] = v
            else:
                data = pd.concat([data], keys=[tuple(info.values())], names=list(info.keys()))

            result.append(data)
        except Exception as e:
            raise ValueError(f'Error processing {filename}') from e
    if len(result) == 0:
        raise ValueError('No data found')
    if isinstance(data, pd.DataFrame):
        df = pd.concat(result)
    else:
        df = pd.DataFrame(result)
    return df


def load_raw(cb, etype=None, **kwargs):
    '''
    Facilitate loading information from raw data zipfile

    Parameters
    ----------
    cb : callable
        Takes a single argument, the zip filename, and returns a dictionary
        or dataframe.
    '''
    wildcard = '**/*.zip' if etype is None else f'**/*{etype}*.zip'
    return load(cb, wildcard, **kwargs)


def load_raw_jmes(filename, query, etype=None, file_format=None, **kwargs):
    '''
    Load value from JSON or YAML file saved to the raw data zipfile

    Parameters
    ----------
    filename : str
        Name of file stored in zipfile to query (e.g., `io.json`,
        `final.preferences`).
    query : dict
        Mapping of result names to the corresponding JMES query.
    etype : {None, str}
        If specified, filter datasets by those that match the experiment
        type (e.g., abr_io).
    file_format : {None, 'json', 'yaml'}
        File format. If None, will make a best guess based on the filename
        ending.

    Examples
    --------
    Load the channel used for the starship A primary output.
    >>> ds.load_raw_jmes('io.json', {'primary_output': 'output.starship_A_primary.channel'})
    '''
    c_query = {n: jmespath.compile(q) for n, q in query.items()}
    if file_format is None:
        if filename.endswith('.json'):
            file_format = 'json'
        elif filename.endswith('.yaml'):
            file_format = 'yaml'
        elif filename.endswith('.preferences'):
            file_format = 'yaml'
        else:
            raise ValueError(f'Could not determine file format for {filename}')
    if file_format not in ('json', 'yaml'):
        raise ValueError(f'Invalid file format {file_format}')

    def cb(zip_filename):
        nonlocal c_query
        nonlocal filename
        nonlocal file_format
        with zipfile.ZipFile(zip_filename) as fh_zip:
            with fh_zip.open(filename) as fh:
                if file_format == 'json':
                    text = json.load(fh)
                elif file_format == 'yaml':
                    text = yaml.full_load(fh)
                return {n: q.search(text) for n, q in c_query.items()}
    return load_raw(cb, etype, **kwargs)
