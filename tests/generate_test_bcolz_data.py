# Script to generate bcolz data for testing. This requires Python 3.7 as that's
# the last verison of Python that bcolz was released for. You can create an
# environment to run this script as:
#
# conda create -n bcolz-test python=3.7 bcolz numpy pandas
from pathlib import Path

import bcolz
import numpy as np
import pandas as pd

DATA_PATH = Path(__file__).parent / 'data' / 'bcolz'


def generate_carray():
    rootdir = DATA_PATH / 'recording' / 'signal'
    carray = bcolz.carray([], rootdir=rootdir, mode='w', dtype='double',
                          expectedlen=10000)
    carray.attrs['fs'] = 10e3
    carray.attrs['generated_by'] = 'test_script'

    data = []
    for i in range(10):
        s = np.random.uniform(size=1000)
        data.append(s)
        carray.append(s)

    carray.flush()
    np.save(DATA_PATH / 'signal.npy', np.concatenate(data, axis=-1),
            allow_pickle=False)


def generate_ctable():
    rootdir = DATA_PATH / 'recording' / 'epoch_md'
    dtype = [
        ('t0', 'float64'),
        ('duration', 'float64'),
        ('trial_type', 'S32'),
        ('exclude', 'bool'),
        ('trial_number', 'int32'),
    ]
    ctable = bcolz.zeros(0, rootdir=rootdir, mode='w', dtype=dtype)
    data = [
        (1.32, 5, 'interleaved', False, 1),
        (2.32, 1, 'conventional', True, 2),
        (4.32, 5, 'interleaved', True, 3),
        (5.32, 1, 'conventional', False, 4),
    ]
    for d in data:
        ctable.append(d)
    ctable.flush()
    columns = [d[0] for d in dtype]
    pd.DataFrame(data, columns=columns).to_csv(DATA_PATH / 'epoch_md.csv')


if __name__ == '__main__':
    generate_carray()
    generate_ctable()
