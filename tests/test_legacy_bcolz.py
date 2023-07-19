import numpy as np
import pandas as pd

from psidata.recording import Recording


def test_load_recording(data_path):
    # See generate_test_bcolz_data.py to generate the data for this unit-test.
    fh = Recording(data_path / 'bcolz' / 'recording')
    signal = fh.signal[:]
    epoch_md = fh.epoch_md[:]

    expected_signal = np.load(data_path / 'bcolz' / 'signal.npy')
    np.testing.assert_array_equal(signal, expected_signal)
    expected_epoch_md = pd.read_csv(data_path / 'bcolz' / 'epoch_md.csv', index_col=0)
    pd.testing.assert_frame_equal(epoch_md, expected_epoch_md, check_dtype=False)
