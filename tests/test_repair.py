import json
import os
from pathlib import Path
import zipfile

import numpy as np
import pytest

zarr = pytest.importorskip('zarr')

from psidata.repair import (find_fill_blocks, repair_main, scan_main,
                            scan_recording)


BLOCK = 1000


def damage(root, name, n_blocks, bad_blocks, partial=0, seed=0,
           engine='NI_misc', fs=8000.0):
    '''
    Write a recording the way psi did, reproducing the append-retry bug on the
    blocks in `bad_blocks`: the resize from the failed attempt is committed,
    then the whole append is retried. `partial` samples of the failed write
    reach disk first, as they would if its block spanned two chunks and only
    one of them failed.
    '''
    rng = np.random.default_rng(seed)
    array = zarr.create_array(store=str(root / f'{name}.zarr'), shape=(1, 0),
                              chunks=(1, 4096), dtype='f8',
                              attributes={'fs': fs, 'engine': engine})
    signal = np.cumsum(rng.normal(size=(1, n_blocks * BLOCK)), axis=-1) + 5
    log = []
    for i in range(n_blocks):
        s0 = i * BLOCK
        length = array.shape[-1]
        if length != s0:
            log.append(f'W :: NIDAQ_DATA_GAP store=st input={name} '
                       f'expected_s0={length} got_s0={s0} '
                       f'gap_samples={s0 - length} gap_sec=0 -- x')
        data = signal[:, s0:s0 + BLOCK]
        if i in bad_blocks:
            log.append('W :: Transient PermissionError writing zarr chunk; '
                       'retrying (1/5)')
            array.resize((1, length + BLOCK))
            if partial:
                array[:, length:length + partial] = data[:, :partial]
        array.append(data, axis=1)
    return signal, log


@pytest.fixture
def recording(tmp_path, request):
    partial = getattr(request, 'param', 0)
    root = tmp_path / 'rec'
    root.mkdir()
    truth = {}
    truth['eeg'], log = damage(root, 'eeg', 60, {10, 40}, partial)
    truth['mic'], mic_log = damage(root, 'mic', 60, {25}, partial, seed=1)
    (root / 'experiment_log.txt').write_text('\n'.join(log + mic_log))
    (root / 'other.csv').write_text('a,b\n1,2\n')

    path = tmp_path / 'rec.zip'
    with zipfile.ZipFile(path, 'w', zipfile.ZIP_DEFLATED) as zf:
        for dirpath, dirnames, filenames in os.walk(root):
            rel = Path(dirpath).relative_to(root).as_posix()
            for filename in filenames:
                arcname = filename if rel == '.' else f'{rel}/{filename}'
                zf.write(Path(dirpath) / filename, arcname)
    return path, truth


def repaired_arrays(path):
    store = zarr.storage.ZipStore(path, mode='r')
    try:
        return {name: zarr.open_array(store, path=f'{name}.zarr', mode='r')[:]
                for name in ('eeg', 'mic')}
    finally:
        store.close()


def test_find_fill_blocks(recording):
    path, truth = recording
    with zipfile.ZipFile(path) as zf:
        log_text = zf.read('experiment_log.txt').decode()
    blocks, retries = find_fill_blocks(log_text)
    assert retries == 3
    assert blocks == {'eeg': [(10000, 11000), (41000, 42000)],
                      'mic': [(25000, 26000)]}


def test_scan_finds_same_ranges(recording):
    path, truth = recording
    report = scan_recording(path, min_samples=100)
    assert report['suspect']
    ranges = {name: [(r['damaged_start'], r['stop']) for r in info['fill_runs']]
              for name, info in report['arrays'].items()}
    assert ranges == {'eeg': [(10000, 11000), (41000, 42000)],
                      'mic': [(25000, 26000)]}


@pytest.mark.parametrize('recording, mode', [(0, 'log'), (0, 'scan'),
                                             (300, 'log'), (300, 'scan')],
                         indirect=['recording'])
def test_repair(recording, mode):
    '''
    Both ways of locating the ranges restore the original signal exactly,
    whether or not part of the failed write reached disk.
    '''
    path, truth = recording
    argv = [str(path), '--apply'] + (['--scan'] if mode == 'scan' else [])
    assert repair_main(argv) == 0

    for name, data in repaired_arrays(path).items():
        np.testing.assert_array_equal(data, truth[name])
    original = path.with_name(f'{path.stem} (original){path.suffix}')
    assert original.exists()
    with zipfile.ZipFile(path) as zf:
        assert json.loads(zf.read('repair_log.json'))['ranges_from'] == \
            ('data scan' if mode == 'scan' else 'log')
        assert zf.read('other.csv')      # unrelated files are carried over


def test_repair_is_dry_run_by_default(recording):
    path, truth = recording
    before = path.read_bytes()
    assert repair_main([str(path)]) == 0
    assert path.read_bytes() == before
    assert not path.with_name(f'{path.stem} (original){path.suffix}').exists()


def test_repair_refuses_twice(recording):
    path, truth = recording
    assert repair_main([str(path), '--apply']) == 0
    repaired = path.read_bytes()
    original = path.with_name(f'{path.stem} (original){path.suffix}')
    original.unlink()
    assert repair_main([str(path), '--apply']) == 0
    assert path.read_bytes() == repaired


def test_repair_refuses_misplaced_range(recording, monkeypatch):
    path, truth = recording
    before = path.read_bytes()
    monkeypatch.setattr('psidata.repair.scan_fill_blocks',
                        lambda *a, **kw: {'eeg': [(10500, 11500)]})
    assert repair_main([str(path), '--scan', '--apply']) == 1
    assert path.read_bytes() == before
    assert not path.with_name(f'{path.stem}.repairing{path.suffix}').exists()


def test_repair_without_log(recording):
    path, truth = recording
    stripped = path.with_name('nolog.zip')
    with zipfile.ZipFile(path) as zin, \
            zipfile.ZipFile(stripped, 'w', zipfile.ZIP_DEFLATED) as zout:
        for info in zin.infolist():
            if info.filename != 'experiment_log.txt':
                zout.writestr(info, zin.read(info.filename))

    with pytest.raises(SystemExit):
        repair_main([str(stripped)])
    assert repair_main([str(stripped), '--scan', '--apply']) == 0
    for name, data in repaired_arrays(stripped).items():
        np.testing.assert_array_equal(data, truth[name])


def test_ignore_excludes_array(recording):
    '''
    A counter or quadrature channel resting at zero looks exactly like a fill
    block, so it has to be possible to leave it out.
    '''
    path, truth = recording
    report = scan_recording(path, min_samples=100, ignore=['mic'])
    assert report['arrays']['mic']['ignored']
    assert report['arrays']['mic']['fill_runs'] == []
    assert report['arrays']['eeg']['fill_runs']      # still scanned

    # An ignored array is also left out of the length cross-check, so on its
    # own it cannot make a recording suspect.
    only_mic = scan_recording(path, min_samples=100, ignore=['eeg'])
    assert only_mic['suspect']       # mic really is damaged here
    both = scan_recording(path, min_samples=100, ignore=['eeg', 'mic'])
    assert not both['suspect']
    assert both['length_mismatch'] == []


def test_ignore_accepts_wildcards(recording):
    path, truth = recording
    report = scan_recording(path, min_samples=100, ignore=['m*'])
    assert report['arrays']['mic']['ignored']
    assert not report['arrays']['eeg']['ignored']


def test_repair_scan_respects_ignore(recording):
    path, truth = recording
    assert repair_main([str(path), '--scan', '--apply', '--ignore', 'mic']) == 0
    arrays = repaired_arrays(path)
    np.testing.assert_array_equal(arrays['eeg'], truth['eeg'])
    assert arrays['mic'].shape[-1] == truth['mic'].shape[-1] + BLOCK


def zip_arrays(tmp_path, name, builder):
    root = tmp_path / name
    root.mkdir()
    builder(root)
    path = tmp_path / f'{name}.zip'
    with zipfile.ZipFile(path, 'w', zipfile.ZIP_DEFLATED) as zf:
        for dirpath, dirnames, filenames in os.walk(root):
            rel = Path(dirpath).relative_to(root).as_posix()
            for filename in filenames:
                arcname = filename if rel == '.' else f'{rel}/{filename}'
                zf.write(Path(dirpath) / filename, arcname)
    return path


def test_length_check_only_compares_one_engine(tmp_path):
    '''
    Channels on separate engines are started independently and routinely end
    up different lengths (e.g. a probe and an elicitor microphone, or a
    temperature channel polled by its own task). Only arrays from one engine
    at one rate should be compared.
    '''
    def build(root):
        for name, engine, n in [('probe', 'NI_a', 20000), ('elicitor', 'NI_b', 22000),
                                ('temperature', 'NI_c', 300)]:
            array = zarr.create_array(store=str(root / f'{name}.zarr'), shape=(1, 0),
                                      chunks=(1, 4096), dtype='f8',
                                      attributes={'fs': 8000.0, 'engine': engine})
            array.append(np.random.default_rng(0).normal(size=(1, n)), axis=1)

    report = scan_recording(zip_arrays(tmp_path, 'multi', build), min_samples=100)
    assert report['length_mismatch'] == []
    assert not report['suspect']


def test_long_fill_run_is_not_damage(tmp_path):
    '''
    A quadrature or counter channel resting at zero holds the fill value for
    far longer than any single append, so it is reported but not called
    damage.
    '''
    def build(root):
        array = zarr.create_array(store=str(root / 'turntable_angle.zarr'),
                                  shape=(1, 0), chunks=(1, 4096), dtype='f8',
                                  attributes={'fs': 8000.0, 'engine': 'NI_a'})
        data = np.random.default_rng(0).normal(size=(1, 60000))
        data[:, 20000:44000] = 0.0       # 3 s at rest
        array.append(data, axis=1)

    report = scan_recording(zip_arrays(tmp_path, 'turntable', build), min_samples=100)
    run, = report['arrays']['turntable_angle']['fill_runs']
    assert run['too_long']
    assert run['duration'] == pytest.approx(3.0)
    assert not report['suspect']

    # The same run inside the allowed length is damage again.
    strict = scan_recording(zip_arrays(tmp_path, 'turntable2', build),
                            min_samples=100, max_seconds=5.0)
    assert strict['suspect']


def test_length_mismatch_alone_is_not_damage(tmp_path):
    '''
    Two inputs from one channel can stop an append apart at the end of a
    recording, so a length difference is reported but is not damage by itself.
    '''
    def build(root):
        for name, n in [('probe_microphone', 20000), ('elicitor_microphone', 32500)]:
            array = zarr.create_array(store=str(root / f'{name}.zarr'), shape=(1, 0),
                                      chunks=(1, 4096), dtype='f8',
                                      attributes={'fs': 100000.0, 'engine': 'NI_a'})
            array.append(np.random.default_rng(0).normal(size=(1, n)), axis=1)

    report = scan_recording(zip_arrays(tmp_path, 'memr', build), min_samples=100)
    assert report['length_mismatch']        # still reported
    assert not report['suspect']            # but not called damage


def test_recording_without_arrays(tmp_path):
    '''
    A calibration-only recording holds no zarr arrays. zarr's ZipStore opens
    lazily and raises when closed unread, which used to surface as a scan
    error.
    '''
    path = tmp_path / 'calibration_only.zip'
    with zipfile.ZipFile(path, 'w') as zf:
        zf.writestr('calibration.csv', 'a,b\n1,2\n')
        zf.writestr('experiment_log.txt', 'nothing to see')

    report = scan_recording(path)
    assert report['arrays'] == {}
    assert not report['suspect']


def test_summarize_prints_only_problems(recording, tmp_path, capsys):
    '''
    The summary names damaged recordings and the channels to repair, and says
    nothing about clean recordings, clean channels or notes.
    '''
    path, truth = recording
    def build_clean(root):
        array = zarr.create_array(store=str(root / 'eeg.zarr'), shape=(1, 0),
                                  chunks=(1, 4096), dtype='f8',
                                  attributes={'fs': 8000.0, 'engine': 'NI_a'})
        array.append(np.random.default_rng(0).normal(size=(1, 5000)), axis=1)

    clean = zip_arrays(tmp_path, 'clean', build_clean)
    report_file = tmp_path / 'report.json'
    assert scan_main([str(path), str(clean), '--min-samples', '100',
                      '--json', str(report_file)]) == 1
    capsys.readouterr()

    assert scan_main(['--summarize', str(report_file)]) == 1
    out = capsys.readouterr().out
    assert str(path) in out
    assert str(clean) not in out
    assert out.count('eeg:') == 2 and out.count('mic:') == 1
    assert 'note:' not in out
    assert 'Shifts by channel: eeg (2), mic (1).' in out


def test_summarize_separates_notes_and_errors(tmp_path, capsys):
    report_file = tmp_path / 'report.json'
    report_file.write_text(json.dumps([
        {'recording': 'unreadable.zip', 'error': 'boom', 'suspect': False},
        {'recording': 'length_only.zip', 'suspect': True, 'arrays': {},
         'length_mismatch': [{'engine': 'NI_a', 'fs': 100.0,
                              'lengths': {'a': 1, 'b': 2}}]},
    ]))
    assert scan_main(['--summarize', str(report_file)]) == 0    # no real damage
    out = capsys.readouterr().out
    assert 'length_only.zip' in out and 'only by a length difference' in out
    assert 'unreadable.zip' in out and 'boom' in out
    assert '0 of 2 recordings have shifted data.' in out


def test_summarize_requires_a_report_or_paths():
    with pytest.raises(SystemExit):
        scan_main([])
