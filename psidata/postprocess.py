from fnmatch import fnmatch
import hashlib
from pathlib import Path
import shutil
import zipfile

from tqdm import tqdm


def archive_data(path):
    '''
    Creates a zip archive of the specified path, validates the MD5sum of each
    file in the archive, and then generates an MD5sum sidecar containing the
    MD5sum of the zip file itself.
    '''
    # Need to handle cases where path has a dot in it.
    zippath = Path(str(path) + '.zip')
    md5path = Path(str(path) + '.md5')
    path = Path(path)
    shutil.make_archive(str(path), 'zip', str(path))

    try:
        validate(zippath)
        zipmd5 = md5sum(zippath.open('rb'))
        md5path.write_text(zipmd5)
        shutil.rmtree(path)
    except IOError as e:
        print(e)


def move_files(path, dest, pattern):
    for file in path.iterdir():
        if fnmatch(file.name, pattern):
            file_dest = dest / file.parent.name / file.name
            file_dest.parent.mkdir(exist_ok=True, parents=True)
            shutil.move(file, file_dest)


def split_data():
    '''
    Main function for moving files out of experiment folder into a parallel
    experiment structure.

    For each experiment, move all files matching a pattern to a separate
    directory that parallels the experiment data.
    '''
    import argparse
    parser = argparse.ArgumentParser('psi-split-data')
    parser.add_argument('path', type=Path)
    parser.add_argument('dest', type=Path)
    parser.add_argument('pattern', type=str)
    args = parser.parse_args()

    dirs = [p for p in args.path.iterdir() if p.is_dir()]
    for path in tqdm(dirs):
        move_files(path, args.dest, args.pattern)


def zip_data():
    '''
    Main function for creating zipfiles from raw psiexperiment data

    For each psi experiment, compress into a single zipfile that is supported
    by `psidata.Recording`. Calculate MD5sum of zipfile and save alongside the
    zipfile. The MD5sum can be safely discarded once the zipfile has been
    transferred to a filesystem that uses checksums for integrity checks such
    as BTRFS or ZFS.
    '''
    import argparse
    parser = argparse.ArgumentParser('cfts-zip-data')
    parser.add_argument('path', type=Path)
    parser.add_argument('-d', '--destination', type=Path)
    args = parser.parse_args()

    # Make zip archives first
    dirs = [p for p in args.path.iterdir() if p.is_dir()]
    for path in tqdm(dirs):
        archive_data(path)

    # Now, move all zip and md5 files if a destination is specified
    if args.destination is not None:
        for zippath in tqdm(args.path.glob('*.zip')):
            md5path = zippath.with_suffix('.md5')
            for file in (zippath, md5path):
                new_file = args.destination / file.name
                shutil.move(file, new_file)


def md5sum(stream, blocksize=1024**2):
    '''
    Generates md5sum from byte stream

    Parameters
    ----------
    stream : stream
        Any object supporting a `read` method that returns bytes.
    blocksize : int
        Blocksize to use for computing md5sum

    Returns
    -------
    md5sum : str
        Hexdigest of md5sum for stream
    '''
    md5 = hashlib.md5()
    while True:
        block = stream.read(blocksize)
        if not block:
            break
        md5.update(block)
    return md5.hexdigest()


def validate(path):
    '''
    Validates contents of zipfile using md5sum

    Parameters
    ----------
    path : {str, pathlib.Path}
        Path containing data that was zipped. Zipfile is expected to have the
        same path, but ending in ".zip".

    The zipfile is opened and iterated through. The MD5 sum for each file
    inside the archive is compared with the companion file in the unzipped
    folder.
    '''
    archive = zipfile.ZipFile(path)
    for name in archive.namelist():
        archive_md5 = md5sum(archive.open(name))
        file = path / name
        if file.is_file():
            with file.open('rb') as fh:
                file_md5 = md5sum(fh)
            if archive_md5 != file_md5:
                raise IOError('{name} in zipfile for {path} is corrupted')


def merge_pdf():
    '''
    Merge PDFs matching filename pattern into a single file for review
    '''
    from pypdf import PdfReader, PdfWriter
    from pypdf.annotations import FreeText
    from pypdf.errors import PdfReadError
    import argparse

    parser = argparse.ArgumentParser('cfts-merge-pdf')
    parser.add_argument('path', type=Path)
    parser.add_argument('pattern', type=str)
    parser.add_argument('output', type=Path)
    parser.add_argument('--delete-bad', action='store_true')
    args = parser.parse_args()

    if args.output.exists():
        raise IOError('Output file exists')

    # Make sure we can create the file before we get too far along.
    args.output.touch()
    args.output.unlink()

    pattern = args.pattern
    if not pattern.endswith('.pdf'):
        pattern = pattern + '.pdf'
    if not pattern.startswith('**/'):
        pattern = '**/' + pattern

    merger = PdfWriter()
    for filename in tqdm(args.path.glob(pattern)):
        if '_exclude' in str(filename):
            # Don't process files hidden under the _exclude folders.
            continue
        try:
            fh = PdfReader(filename)
            merger.append(fh, filename.stem)
        except PdfReadError:
            if args.delete_bad:
                filename.unlink()
            print(filename)
    merger.write(args.output)
