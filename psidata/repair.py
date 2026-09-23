'''
Find and repair recordings damaged by psiexperiment's zarr append-retry bug.

Until psiexperiment 0.8.4, a transient PermissionError during a chunk write
(antivirus, search indexer or a sync client briefly locking the file) caused
psi's zarr store to retry `zarr.Array.append` as a whole. Append commits the
resize before writing the chunks, so the retry grew the array a second time.
The failed block was never written and stays at the array's fill value, and
every sample after it sits one block late -- about 0.125 s at the usual
acquisition settings, which shows up as a sudden latency shift partway through
a recording.

Damaged ranges are found two ways:

- From experiment_log.txt, for recordings made once psi logged NIDAQ_DATA_GAP.
  From the next append onward every block logs a gap, and gap_samples jumps by
  the length of the bad block, d. On the first such line, expected_s0 is the
  file length L just after the bad append, which wrote its data to [L - d, L),
  so the fill block is [L - 2d, L - d).
- From the data itself, for older recordings. A run of samples that are exactly
  the fill value on every channel is the signature: acquired analog data has
  noise on it, so hundreds of consecutive samples of exactly 0.0 do not happen
  by chance. If the failed write partly reached disk, those real samples appear
  again at the start of the retried block, which recovers the full range.

Both give the same answer where both are available.

Commands
--------
psidata-scan-recording
    Report recordings that look damaged. Takes recordings or directories of
    them, and `--recursive` to walk a whole data tree.

psidata-repair-recording
    Repair one recording. Reports only, unless `--apply` is passed, which keeps
    the original as "<name> (original).zip" and writes the repaired recording
    in its place. Every range is verified first: each sample in it must be the
    fill value or an exact copy of the matching sample in the retried block.
    If any range fails, nothing is written or renamed.
'''
import logging
log = logging.getLogger(__name__)

import argparse
import datetime as dt
from fnmatch import fnmatch
import json
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile
import zipfile

import numpy as np
import zarr
from zarr.storage import LocalStore, ZipStore


GAP_RE = re.compile(r'NIDAQ_DATA_GAP store=\S+ input=(\S+) expected_s0=(\d+) '
                    r'got_s0=(\d+) gap_samples=(-?\d+)')
RETRY_RE = re.compile(r'Transient PermissionError writing zarr chunk; '
                      r'retrying \((\d+)/\d+\)')

# Runs shorter than this are reported only if asked for. At 100 kHz an
# acquisition block is ~12500 samples, and 500 samples is 5 ms of the signal
# sitting at exactly 0.0 on every channel.
DEFAULT_MIN_SAMPLES = 500

# How far back to look for a partially written block ahead of a fill run.
MAX_PARTIAL_PREFIX = 2 ** 16

# Quantized channels repeat sample values often (~1e-4 of the time for a
# microphone monitor), so over a block-sized search window a one- or two-sample
# "match" is expected by chance. Only longer runs mean anything.
DEFAULT_MIN_PREFIX = 8

# Samples after each join used to estimate the typical sample-to-sample step.
JUMP_CONTEXT = 1000

DEFAULT_BLOCK = 4_000_000

# A fill block is a single failed append, i.e. one monitor period of data --
# 0.125 s for analog input, and at most ~1 s for any psi engine. A much longer
# run of the fill value is something else, typically a counter or quadrature
# channel resting at zero.
MAX_RUN_SECONDS = 2.0


def find_fill_blocks(log_text):
    '''
    Return ({input name: [(start, stop), ...]}, number of retries) from the
    experiment log. Ranges are in the coordinates of the damaged file.
    '''
    retries = 0
    for m in RETRY_RE.finditer(log_text):
        if int(m.group(1)) != 1:
            # With k failed attempts the array grew k extra times, and the
            # fill block's position depends on k. That case isn't handled.
            raise ValueError('Log contains a chunk write that failed more '
                             'than once; only single retries are handled.')
        retries += 1

    last_gap = {}
    blocks = {}
    for m in GAP_RE.finditer(log_text):
        name = m.group(1)
        expected, got, gap = (int(g) for g in m.group(2, 3, 4))
        prior = last_gap.get(name, 0)
        if gap == prior:
            continue
        d = prior - gap
        if d <= 0 or expected - got != -gap:
            raise ValueError(f'NIDAQ_DATA_GAP for {name} does not match the '
                             f'append-retry signature: {m.group(0)}')
        blocks.setdefault(name, []).append((expected - 2 * d, expected - d))
        last_gap[name] = gap
    return blocks, retries


def open_arrays(path):
    '''
    Return (store, [array name, ...]) for a recording zip or directory.

    This deliberately does not go through `Recording`, which keeps file
    handles open with no way to close them -- repairing has to release the
    recording before renaming it, which Windows enforces.
    '''
    if path.is_dir():
        names = sorted(p.stem for p in path.glob('*.zarr')
                       if (p / 'zarr.json').exists() or (p / '.zarray').exists())
        return LocalStore(path), names
    with zipfile.ZipFile(path) as zf:
        names = sorted({n.split('.zarr/')[0] for n in zf.namelist()
                        if n.endswith(('.zarr/zarr.json', '.zarr/.zarray'))})
    if not names:
        # zarr's ZipStore opens lazily, and closing one that was never read
        # raises AttributeError, so don't create it for a recording that holds
        # no arrays at all (a calibration-only recording, say).
        return None, []
    return ZipStore(path, mode='r'), names


def iter_fill_runs(array, min_samples=DEFAULT_MIN_SAMPLES, block=DEFAULT_BLOCK):
    '''
    Yield (start, stop) for each run of at least `min_samples` consecutive
    samples that equal the fill value on every channel.
    '''
    fill = 0.0 if array.fill_value is None else array.fill_value
    n = array.shape[-1]
    run_start = None
    for i in range(0, n, block):
        data = array[..., i:min(i + block, n)]
        is_fill = data == fill
        while is_fill.ndim > 1:
            is_fill = is_fill.all(axis=0)
        # Bracket the block with False so edges show up as transitions, then
        # carry an unfinished run across block boundaries via run_start.
        edges = np.diff(np.concatenate(([False], is_fill, [False])).astype(np.int8))
        starts = np.flatnonzero(edges == 1) + i
        stops = np.flatnonzero(edges == -1) + i
        if run_start is not None:
            starts = np.concatenate(([run_start], starts))
            run_start = None
        if len(stops) < len(starts):
            run_start = starts[-1]
            starts = starts[:-1]
        for start, stop in zip(starts, stops):
            if stop - start >= min_samples:
                yield int(start), int(stop)
    if run_start is not None and n - run_start >= min_samples:
        yield int(run_start), n


def find_partial_prefix(array, start, stop, min_prefix=DEFAULT_MIN_PREFIX,
                        max_prefix=MAX_PARTIAL_PREFIX):
    '''
    Return how many real samples precede a fill run as part of the same
    damaged block, i.e. how many samples just before `start` reappear at
    `stop` as the retried block's copy of them.
    '''
    n = array.shape[-1]
    limit = min(max_prefix, start, n - stop)
    if limit <= 0:
        return 0
    tail = array[..., stop:stop + limit]
    last = array[..., start - 1]
    match = tail == np.asarray(last)[..., np.newaxis]
    while match.ndim > 1:
        match = match.all(axis=0)
    for i in np.flatnonzero(match)[::-1]:
        k = int(i) + 1
        if k < min_prefix:
            break
        if np.array_equal(array[..., start - k:start], array[..., stop:stop + k]):
            return k
    return 0


def scan_recording(path, min_samples=DEFAULT_MIN_SAMPLES, block=DEFAULT_BLOCK,
                   quick=False, min_prefix=DEFAULT_MIN_PREFIX, ignore=(),
                   max_seconds=MAX_RUN_SECONDS):
    '''
    Return a report of the fill runs in each array of a recording.

    Arrays whose name matches one of the `ignore` patterns are listed but
    not scanned, and never make a recording suspect. Use this for channels
    that legitimately hold a constant value -- a counter or quadrature input
    resting at zero looks exactly like a fill block, and a near-constant
    channel such as temperature can too.

    Runs longer than `max_seconds` are reported but do not make a recording
    suspect, since no single append is that long.
    '''
    store, names = open_arrays(path)
    report = {'recording': str(path), 'arrays': {}, 'suspect': False}
    try:
        for name in names:
            array = zarr.open_array(store, path=f'{name}.zarr', mode='r')
            fs = array.attrs.get('fs')
            info = {
                'fs': fs,
                'length': array.shape[-1],
                'n_channels': int(np.prod(array.shape[:-1])) if array.ndim > 1 else 1,
                'engine': _engine_ref(array),
                'ignored': any(fnmatch(name, p) for p in ignore),
                'fill_runs': [],
            }
            report['arrays'][name] = info
            if quick or info['ignored']:
                continue
            for start, stop in iter_fill_runs(array, min_samples, block):
                prefix = find_partial_prefix(array, start, stop, min_prefix)
                duration = None if not fs else (stop - start + prefix) / fs
                too_long = duration is not None and duration > max_seconds
                info['fill_runs'].append({
                    'start': start,
                    'stop': stop,
                    'n_samples': stop - start,
                    'partial_prefix': prefix,
                    'damaged_start': start - prefix,
                    'seconds': None if not fs else (start - prefix) / fs,
                    'duration': duration,
                    'too_long': too_long,
                })
                if not too_long:
                    report['suspect'] = True
    finally:
        if store is not None:
            store.close()

    # Arrays acquired by one engine at one sample rate come off the same task,
    # so a difference in length is worth reporting. It is only a hint, never
    # proof: two inputs from one channel can stop an append apart at the end of
    # a recording (memr recordings show a steady 12500-sample difference
    # between the probe and elicitor microphones, with no fill block anywhere),
    # so this does not on its own make a recording damaged. Arrays on different
    # engines are started independently and are not comparable at all.
    groups = {}
    for name, info in report['arrays'].items():
        if info['ignored'] or not info['fs'] or info['engine'] is None:
            continue
        groups.setdefault((info['engine'], info['fs']), {})[name] = info['length']
    report['length_mismatch'] = [
        {'engine': engine, 'fs': fs, 'lengths': lengths}
        for (engine, fs), lengths in groups.items()
        if len(set(lengths.values())) > 1
    ]
    return report


def _engine_ref(array):
    '''
    Return something identifying the engine an array was acquired by, or None.
    psi serializes the engine as a nested object the first time it appears and
    as an "__obj__::<id>" reference after that; either way the value is stable
    within one recording.
    '''
    engine = array.attrs.get('engine')
    if isinstance(engine, dict):
        return engine.get('__id__', engine.get('name'))
    return engine


def scan_fill_blocks(path, min_samples=DEFAULT_MIN_SAMPLES, ignore=()):
    '''
    Return {input name: [(start, stop), ...]} found by scanning the data.
    '''
    report = scan_recording(path, min_samples=min_samples, ignore=ignore)
    blocks = {}
    for name, info in report['arrays'].items():
        ranges = [(r['damaged_start'], r['stop']) for r in info['fill_runs']
                  if not r['too_long']]
        if ranges:
            blocks[name] = ranges
    return blocks


def check_range(src, start, stop):
    '''
    Return what is known about a range before it is removed: how much of it is
    fill, whether the retried block accounts for the rest, and how big the join
    would be relative to an ordinary sample-to-sample step.
    '''
    fill = src[..., start:stop]
    is_fill = fill == src.fill_value
    zero_fraction = float(np.mean(is_fill))
    # If part of the failed write reached disk (the block spanned two chunks
    # and only one failed), those samples are real, and they equal the retry's
    # copy of the same block, which directly follows the range.
    retry = src[..., stop:stop + (stop - start)]
    verified = retry.shape == fill.shape and bool(np.all(is_fill | (fill == retry)))
    before = src[..., start - 1]
    after = src[..., stop:stop + JUMP_CONTEXT]
    typical_step = np.median(np.abs(np.diff(after, axis=-1)), axis=-1)
    jump = np.abs(after[..., 0] - before)
    jump_ratio = float(np.max(jump / np.where(typical_step > 0, typical_step, np.nan)))
    return {
        'start': start,
        'stop': stop,
        'n_samples': stop - start,
        'zero_fraction': zero_fraction,
        'verified': verified,
        'join_jump_vs_typical_step': jump_ratio,
    }


def kept_segments(n, remove):
    segments = []
    pos = 0
    for start, stop in sorted(remove):
        segments.append((pos, start))
        pos = stop
    segments.append((pos, n))
    return segments


def copy_without(src, dst, remove, batch):
    '''
    Copy `src` into `dst` along the last axis, skipping the `remove` ranges.
    Writes are aligned to `batch` (a multiple of the chunk length) so each
    output chunk is written exactly once.
    '''
    pending = []
    n_pending = 0
    out_pos = 0

    def flush(n):
        nonlocal pending, n_pending, out_pos
        data = np.concatenate(pending, axis=-1)
        dst[..., out_pos:out_pos + n] = data[..., :n]
        pending = [data[..., n:]]
        n_pending -= n
        out_pos += n

    for start, stop in kept_segments(src.shape[-1], remove):
        for i in range(start, stop, batch):
            pending.append(src[..., i:min(i + batch, stop)])
            n_pending += pending[-1].shape[-1]
            if n_pending >= batch:
                flush(batch)
    if n_pending:
        flush(n_pending)
    assert out_pos == dst.shape[-1]


def zip_copy_entry(zin, zout, info):
    new_info = zipfile.ZipInfo(info.filename, info.date_time)
    new_info.compress_type = info.compress_type
    new_info.external_attr = info.external_attr
    if info.is_dir():
        zout.writestr(new_info, b'')
        return
    with zin.open(info) as fh_in, zout.open(new_info, 'w', force_zip64=True) as fh_out:
        shutil.copyfileobj(fh_in, fh_out, 16 * 2**20)


def zip_add_tree(zout, root, arc_root, compress_type):
    now = dt.datetime.now().timetuple()[:6]
    for dirpath, dirnames, filenames in os.walk(root):
        rel = Path(dirpath).relative_to(root).as_posix()
        arc_dir = arc_root if rel == '.' else f'{arc_root}{rel}/'
        zout.writestr(zipfile.ZipInfo(arc_dir, now), b'')
        for filename in filenames:
            info = zipfile.ZipInfo(arc_dir + filename, now)
            info.compress_type = compress_type
            with open(Path(dirpath) / filename, 'rb') as fh_in, \
                    zout.open(info, 'w', force_zip64=True) as fh_out:
                shutil.copyfileobj(fh_in, fh_out, 16 * 2**20)


def write_repaired(recording, output, sources, blocks, report, batch_chunks,
                   temp_dir=None):
    # The repaired arrays are staged on local disk by default: staging them
    # beside a recording on a network share doubles the traffic, and cleanup
    # there is unreliable enough to leave directories behind.
    with tempfile.TemporaryDirectory(dir=temp_dir, ignore_cleanup_errors=True) as tmp, \
            zipfile.ZipFile(recording) as zin, \
            zipfile.ZipFile(output, 'x', allowZip64=True) as zout:
        tmp = Path(tmp)
        repaired_prefixes = tuple(f'{name}.zarr/' for name in blocks)
        compress_type = zipfile.ZIP_DEFLATED
        for info in zin.infolist():
            if info.filename.startswith(repaired_prefixes):
                if not info.is_dir():
                    compress_type = info.compress_type
                continue
            zip_copy_entry(zin, zout, info)

        for name, remove in blocks.items():
            src = sources[name]
            print(f'Writing repaired {name}.zarr')
            shape = src.shape[:-1] + (report['arrays'][name]['repaired_length'],)
            dst = zarr.create_array(
                store=str(tmp / f'{name}.zarr'), shape=shape, chunks=src.chunks,
                dtype=src.dtype, fill_value=src.fill_value,
                serializer=src.serializer, compressors=src.compressors,
                filters=src.filters, attributes=src.attrs.asdict())
            copy_without(src, dst, remove, src.chunks[-1] * batch_chunks)
            zip_add_tree(zout, tmp / f'{name}.zarr', f'{name}.zarr/', compress_type)

        info = zipfile.ZipInfo('repair_log.json', dt.datetime.now().timetuple()[:6])
        info.compress_type = zipfile.ZIP_DEFLATED
        zout.writestr(info, json.dumps(report, indent=2))


def print_scan_report(report, verbose):
    if not report['suspect'] and not verbose:
        return
    print(report['recording'])
    for name, info in report['arrays'].items():
        fs = info['fs']
        duration = f'{info["length"] / fs:.1f} s' if fs else 'unknown duration'
        if info['fill_runs'] or verbose:
            ignored = ' -- ignored' if info['ignored'] else ''
            print(f'  {name}.zarr: {info["length"]} samples ({duration}){ignored}')
        for run in info['fill_runs']:
            if run['too_long'] and not verbose:
                continue
            where = '' if run['seconds'] is None else f' at t={run["seconds"]:.3f} s'
            extra = '' if not run['partial_prefix'] else \
                f', plus {run["partial_prefix"]} partly written samples before it'
            note = '' if not run['too_long'] else \
                f' -- {run["duration"]:.1f} s is too long to be one append, so ' \
                f'this is a channel holding a constant value, not damage'
            print(f'    fill run [{run["damaged_start"]}, {run["stop"]}): '
                  f'{run["n_samples"]} fill samples{where}{extra}{note}')
    for mismatch in report['length_mismatch']:
        print(f'  note: arrays on one engine at {mismatch["fs"]} Hz have '
              f'different lengths: {mismatch["lengths"]}. Inputs can stop an '
              f'append apart, so this is only damage if a fill run says so.')


def iter_recordings(paths, recursive):
    for path in paths:
        if path.is_file() or (path / 'experiment_log.txt').exists() \
                or any(path.glob('*.zarr')):
            yield path
        elif recursive:
            yield from sorted(p for p in path.rglob('*.zip'))
        else:
            yield from sorted(p for p in path.glob('*.zip'))


def scan_main(argv=None):
    parser = argparse.ArgumentParser(
        description='Scan recordings for the fill block left by the zarr '
        'append-retry bug.')
    parser.add_argument('paths', nargs='+', type=Path,
                        help='Recordings, or directories of recordings')
    parser.add_argument('--recursive', action='store_true',
                        help='Search directories for recordings recursively')
    parser.add_argument('--min-samples', type=int, default=DEFAULT_MIN_SAMPLES,
                        help='Shortest fill run to report (default: %(default)s)')
    parser.add_argument('--min-prefix', type=int, default=DEFAULT_MIN_PREFIX,
                        help='Shortest partially written prefix to believe '
                        '(default: %(default)s)')
    parser.add_argument('--block', type=int, default=DEFAULT_BLOCK,
                        help='Samples to read at a time (default: %(default)s)')
    parser.add_argument('--ignore', action='append', default=[],
                        metavar='PATTERN',
                        help='Array to leave unscanned, e.g. a counter or '
                        'quadrature channel that rests at zero, or a '
                        'near-constant one such as temperature. Accepts '
                        'wildcards and can be repeated.')
    parser.add_argument('--max-seconds', type=float, default=MAX_RUN_SECONDS,
                        help='Fill runs longer than this are reported but not '
                        'treated as damage, since no single append is that '
                        'long (default: %(default)s)')
    parser.add_argument('--quick', action='store_true',
                        help='Only compare array lengths; do not read samples')
    parser.add_argument('--verbose', action='store_true',
                        help='Report every recording, not just suspect ones')
    parser.add_argument('--json', type=Path,
                        help='Also write the full report to this file')
    args = parser.parse_args(argv)

    reports = []
    for path in iter_recordings(args.paths, args.recursive):
        try:
            report = scan_recording(path, args.min_samples, args.block,
                                    args.quick, args.min_prefix, args.ignore,
                                    args.max_seconds)
        except Exception as e:
            print(f'{path}\n  ERROR: {e}')
            reports.append({'recording': str(path), 'error': str(e),
                            'suspect': False})
            continue
        reports.append(report)
        print_scan_report(report, args.verbose)
        sys.stdout.flush()

    if args.json:
        args.json.write_text(json.dumps(reports, indent=2))
    suspect = [r for r in reports if r['suspect']]
    print(f'\n{len(suspect)} of {len(reports)} recordings look damaged.')
    if suspect:
        print('Repair them with psidata-repair-recording.')
    return 1 if suspect else 0


def repair_main(argv=None):
    parser = argparse.ArgumentParser(
        description='Repair a recording damaged by the zarr append-retry bug.')
    parser.add_argument('recording', type=Path, help='Recording zip to repair')
    parser.add_argument('--apply', action='store_true',
                        help='Keep the original as "<name> (original).zip" '
                        'and write the repaired recording as "<name>.zip". '
                        'Without this, only report what would be removed.')
    parser.add_argument('--scan', action='store_true',
                        help='Find the ranges by scanning the data for fill '
                        'blocks instead of reading them from the log. Needed '
                        'for recordings made before NIDAQ_DATA_GAP logging.')
    parser.add_argument('--ignore', action='append', default=[],
                        metavar='PATTERN',
                        help='Array to leave unscanned when --scan is used, '
                        'e.g. a counter or '
                        'quadrature channel that rests at zero, or a '
                        'near-constant one such as temperature. Accepts '
                        'wildcards and can be repeated.')
    parser.add_argument('--temp-dir', type=Path,
                        help='Where to stage the repaired arrays (default: '
                        'the system temp directory; needs room for the '
                        'recording)')
    parser.add_argument('--batch-chunks', type=int, default=32,
                        help='Chunks to copy per write (default: %(default)s)')
    args = parser.parse_args(argv)

    recording = args.recording
    original = recording.with_name(f'{recording.stem} (original){recording.suffix}')
    partial = recording.with_name(f'{recording.stem}.repairing{recording.suffix}')
    if args.apply:
        for path in (original, partial):
            if path.exists():
                parser.error(f'{path} already exists')

    with zipfile.ZipFile(recording) as zin:
        entry_names = {i.filename for i in zin.infolist()}
        log_text = ''
        if not args.scan:
            if 'experiment_log.txt' not in entry_names:
                parser.error(f'{recording} has no experiment_log.txt; use '
                             f'--scan to find the ranges in the data')
            log_text = zin.read('experiment_log.txt').decode('utf-8', 'replace')
    if 'repair_log.json' in entry_names:
        # The log still describes the original shifts, so repairing again
        # would remove good data.
        print(f'{recording} has already been repaired; nothing to do.')
        return 0

    if args.scan:
        print(f'Scanning {recording} for fill blocks')
        blocks = scan_fill_blocks(recording, ignore=args.ignore)
        source = 'data scan'
    else:
        blocks, retries = find_fill_blocks(log_text)
        source = 'log'
        n_shifts = sum(len(r) for r in blocks.values())
        if n_shifts != retries:
            # e.g. a retry on an epoch store, which has no continuity check,
            # or on the final block, so there is no later append to log a gap.
            print(f'WARNING: log has {retries} retries but only {n_shifts} '
                  f'detected shifts. The others cannot be repaired from the '
                  f'log; --scan finds them in the data.')

    if not blocks:
        print(f'No append-retry shifts found in the {source}; '
              f'nothing to repair.')
        return 0

    src_store = ZipStore(recording, mode='r')
    report = {
        'source': original.name,
        'repaired_at': dt.datetime.now().isoformat(timespec='seconds'),
        'tool': f'psidata.repair {_version()}',
        'ranges_from': source,
        'arrays': {},
    }
    sources = {}
    for name, remove in blocks.items():
        if not any(n.startswith(f'{name}.zarr/') for n in entry_names):
            raise ValueError(f'{name}.zarr is not in the recording')
        src = zarr.open_array(src_store, path=f'{name}.zarr', mode='r')
        sources[name] = src
        checks = [check_range(src, start, stop) for start, stop in remove]
        n_removed = sum(stop - start for start, stop in remove)
        report['arrays'][name] = {
            'fs': src.attrs.get('fs'),
            'original_length': src.shape[-1],
            'repaired_length': src.shape[-1] - n_removed,
            'removed': checks,
        }
        print(f'{name}.zarr: {src.shape[-1]} -> {src.shape[-1] - n_removed} samples')
        for c in checks:
            status = 'ok' if c['verified'] else 'FAILED'
            print(f'  remove [{c["start"]}, {c["stop"]}) ({c["n_samples"]} samples): '
                  f'{status}, {c["zero_fraction"]:.1%} fill values, join jump = '
                  f'{c["join_jump_vs_typical_step"]:.1f}x typical step')

    failed = [(name, c['start'], c['stop'])
              for name, info in report['arrays'].items()
              for c in info['removed'] if not c['verified']]
    for name, start, stop in failed:
        print(f'ERROR: {name}.zarr [{start}, {stop}) is not all fill values '
              f'or copies of the retried block, so it may not be the '
              f'damaged range.')

    lengths = {r['repaired_length'] for r in report['arrays'].values()}
    if len(lengths) > 1:
        print('NOTE: repaired arrays have different lengths. That is expected '
              'only if they come from different engines or sample rates.')

    if not args.apply:
        src_store.close()
        print('Dry run only; pass --apply to repair.')
        return 0
    if failed:
        src_store.close()
        print('Refusing to repair.')
        return 1

    # Build the repaired zip under a temporary name so the recording is only
    # swapped out once the new copy is complete.
    try:
        write_repaired(recording, partial, sources, blocks, report,
                       args.batch_chunks, args.temp_dir)
    except BaseException:
        partial.unlink(missing_ok=True)
        raise
    finally:
        src_store.close()
    recording.rename(original)
    try:
        partial.rename(recording)
    except BaseException:
        original.rename(recording)
        raise
    print(f'Original kept as {original}')
    print(f'Repaired recording written to {recording}')
    return 0


def _version():
    try:
        from .version import __version__
        return __version__
    except ImportError:
        return 'unknown'


if __name__ == '__main__':
    sys.exit(repair_main())
