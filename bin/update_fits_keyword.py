#!/usr/bin/env python

from astropy.io import fits
import argparse
import os
import time
from tqdm import tqdm


def format_time(seconds):
    minutes, seconds = divmod(seconds, 60)
    return f"{int(minutes):02}:{int(seconds):02}"


def update_fits_keyword(fits_path, keyword, value, output_dir=None, overwrite=False, dryrun=False):
    mode = 'readonly' if dryrun or not overwrite else 'update'

    with fits.open(fits_path, mode=mode) as hdul:
        if dryrun:
            print("  [HDU 0] Skipping primary HDU")
        for i, hdu in enumerate(hdul[1:], start=1):
            original = hdu.header.get(keyword, "<not present>")
            if dryrun:
                action = "Would update" if keyword in hdu.header else "Would add"
                print(f"  [HDU {i}] {action} '{keyword}': {original} -> {value}")
            else:
                hdu.header[keyword] = value

        if not dryrun:
            if overwrite:
                hdul.flush()
            else:
                filename = os.path.basename(fits_path)
                output_path = os.path.join(output_dir or ".", filename.replace(".fits", "_updated.fits"))
                hdul.writeto(output_path, overwrite=True)
        else:
            print("Dry run mode — no changes written.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch update a keyword in all HDUs \
                                                 (excluding HDU 0) of multi-extension FITS files.")
    parser.add_argument("fits_files", nargs="+", help="List of input FITS files")
    parser.add_argument("keyword", help="Header keyword to update/add")
    parser.add_argument("value", help="New value for the keyword")
    parser.add_argument("--output-dir", help="Directory to save updated FITS files")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite original files")
    parser.add_argument("--dryrun", action="store_true", help="Preview changes without writing")

    args = parser.parse_args()
    total_files = len(args.fits_files)

    cumulative_time = 0.0

    with tqdm(total=len(args.fits_files), desc="Processing FITS files", unit="file") as pbar:
        for fits_file in args.fits_files:
            start_time = time.time()

            update_fits_keyword(
                fits_path=fits_file,
                keyword=args.keyword,
                value=args.value,
                output_dir=args.output_dir,
                overwrite=args.overwrite,
                dryrun=args.dryrun
            )

            elapsed = time.time() - start_time
            cumulative_time += elapsed
            pbar.update(1)

    print("All files processed.")
    print(f"Final total time for {len(args.fits_files)} file(s): {format_time(cumulative_time)}")
