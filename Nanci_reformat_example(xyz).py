#!/usr/bin/env python3
"""
reformat_nanci.py
Minimal, robust reformatter for Nanci-style data files.
Behavior:
- For each file in the target directory, tries to parse repeating blocks:
    <N>                # number of atoms (integer)
    <metadata line>    # whitespace-separated tokens (original script expects indices at 16,18,...)
    <atom line 1>
    ...
    <atom line N>
- Writes out a new file with suffix "_reformat" inserted in the filename, preserving most of naming
  structure if possible. If the metadata doesn't contain expected fields, the script logs a warning
  and skips that block.
"""
import os
import logging
from pathlib import Path

# ---- Logging setup ----
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("reformat_nanci")

# ---- Constants: which metadata token indices to pick (0-based) ----
# Original script referenced data[16], data[18], ..., data[34] (1-based thinking).
# Keep same indices here but check your files—if different, adjust METADATA_INDICES.
METADATA_INDICES = [16, 18, 20, 22, 24, 26, 28, 30, 32, 34]

def make_output_name(in_path: Path) -> Path:
    """
    Construct output filename similar to original scheme:
    - If original name has at least two dots (e.g. name.part.ext), produce: name.part_reformat.ext
    - Otherwise, produce: <stem>_reformat<suffix>
    """
    name = in_path.name
    parts = name.split('.')
    if len(parts) >= 3:
        # join all parts except last two for the "base" in case filename has extra dots
        pre = '.'.join(parts[:-2])
        mid = parts[-2]
        suf = parts[-1]
        out_name = f"{pre}.{mid}_reformat.{suf}"
    else:
        # safe fallback
        out_name = f"{in_path.stem}_reformat{in_path.suffix}"
    return in_path.with_name(out_name)

def process_file(file_path: Path, out_dir: Path = None):
    """
    Read the source file and write formatted output to a sibling file (or under out_dir if provided).
    - file_path: Path to input file.
    - out_dir: optional directory to place reformatted files.
    """
    logger.info("Processing file: %s", file_path)
    if out_dir is None:
        out_dir = file_path.parent
    out_path = make_output_name(file_path)
    if out_dir != file_path.parent:
        out_path = Path(out_dir) / out_path.name

    try:
        with file_path.open('r', encoding='utf-8', errors='replace') as fin, \
             out_path.open('w', encoding='utf-8') as fout:

            block_count = 0
            while True:
                # Read first line (expected integer N)
                first = fin.readline()
                if not first:
                    # EOF
                    break
                first = first.strip()
                if first == '':
                    # skip empty lines
                    continue
                try:
                    n_atoms = int(first)
                except ValueError:
                    # if line can't be parsed as int, log and try to continue scanning
                    logger.warning("Expected integer atom count but got: '%s' in file %s. Trying to continue.", first, file_path)
                    continue

                # Read metadata line
                meta_line = fin.readline()
                if not meta_line:
                    logger.warning("Unexpected EOF when reading metadata after atom count in %s", file_path)
                    break
                meta_tokens = meta_line.strip().split()
                # check we have enough tokens
                if max(METADATA_INDICES) >= len(meta_tokens):
                    logger.warning("Metadata has %d tokens but expected at least %d; skipping this block. file=%s",
                                   len(meta_tokens), max(METADATA_INDICES)+1, file_path)
                    # skip atom lines for this block to keep file pointer consistent
                    for _ in range(n_atoms):
                        _ = fin.readline()
                    continue

                # attempt to extract metadata values in the order requested
                try:
                    meta_values = [meta_tokens[i] for i in METADATA_INDICES]
                except Exception as e:
                    logger.exception("Failed to extract metadata indices from tokens: %s", e)
                    for _ in range(n_atoms):
                        _ = fin.readline()
                    continue

                # read atom lines
                atom_lines = []
                read_ok = True
                for i in range(n_atoms):
                    line = fin.readline()
                    if not line:
                        logger.warning("Unexpected EOF while reading %d atom lines in %s (block %d)", n_atoms, file_path, block_count)
                        read_ok = False
                        break
                    atom_lines.append(line.rstrip('\n'))  # keep consistent newline handling later

                if not read_ok:
                    break

                # Compose properties header similar to original pipeline
                # The original string included labels like "CCSD(T)/CBS=%s ..." - we preserve the pattern
                # but convert meta_values list to positional formatting.
                try:
                    header = "CCSD(T)/CBS={0} CCSD(T)/haTZ={1} MP2/haTZ={2} MP2/CBS={3} MP2/aTZ={4} MP2/aQZ={5} HF/haTZ={6} HF/aTZ={7} HF/aQZ={8} SAPT2+/aDZTot={9} Properties=species:S:1:pos:R:3".format(*meta_values)
                except Exception:
                    # fallback: join meta values by space if formatting fails
                    header = " ".join(["meta:"+v for v in meta_values]) + " Properties=species:S:1:pos:R:3"

                # write out reformatted block
                fout.write(f"{n_atoms}\n")
                fout.write(header + "\n")
                for al in atom_lines:
                    fout.write(al + "\n")
                block_count += 1

            logger.info("Finished file %s: wrote %d blocks to %s", file_path, block_count, out_path)
    except Exception as e:
        logger.exception("Fatal error while processing file %s: %s", file_path, e)

def process_directory(target_dir: Path, patterns=None, out_dir: Path = None):
    """
    Process all files in target_dir that match given patterns.
    - patterns: optional list of suffix patterns (e.g. ['.dat', '.txt']) or substrings to filter filenames.
                If None, process all regular files except this script itself.
    - out_dir: optional directory to write reformatted files.
    """
    script_name = Path(__file__).name
    for fname in sorted(os.listdir(target_dir)):
        fpath = Path(target_dir) / fname
        if not fpath.is_file():
            continue
        if fname == script_name:
            continue
        if patterns:
            if not any((fname.endswith(p) or p in fname) for p in patterns):
                continue
        process_file(fpath, out_dir=out_dir)

if __name__ == "__main__":
    # Example usage: process current directory and write outputs next to original files.
    base_dir = Path.cwd()  # change this to your data directory if needed
    # Optionally, restrict patterns to files you know are Nanci files, e.g. files containing 'nanci'
    patterns = None  # e.g. ['nanci', '.dat'] or None to process all regular files
    process_directory(base_dir, patterns=patterns)
