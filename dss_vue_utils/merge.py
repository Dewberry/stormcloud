# name=mergeData
# displayinmenu=true
# displaytouser=true
# displayinselector=true

"""
Script to merge dss files in a given directory into a single DSS file
"""

import os

from hec.heclib.dss import DSSPathname, HecDss

# INPUTS
DSS_DIR = "path/to/input/dss/dir""
DSS_FN = "path/to/output/file.dss"


def get_dss_files(dir):
    dssFileNames = os.listdir(dir)
    for f in dssFileNames:
        if f.endswith(".dss"):
            fn = os.path.join(dir, f)
            yield fn


def verify_dir(dss_dir):
    if os.path.exists(dss_dir):
        if os.path.isdir(dss_dir):
            return dss_dir
        else:
            print("Input value " + dss_dir + " is not a dir")
            exit()
    else:
        print("Input value " + dss_dir + " does not exist")
        exit()


def verify_fn(dss_fn):
    if dss_fn.endswith(".dss"):
        raise ValueError("Input file name " + dss_fn + " does not end in .dss")
    if os.path.exists(dss_fn):
        print("Input file name " + dss_fn + " already exists; overwriting")
        os.remove(dss_fn)
    return dss_fn


def main(dss_dir, dss_fn):
    output_dss = HecDss.open(dss_fn)
    for f in get_dss_files(dss_dir):
        dss = HecDss.open(f)
        paths = dss.getCondensedCatalog()
        for cr in paths:
            p = DSSPathname(cr.toString())
            tsc = dss.get(p.toString())
            output_dss.put(tsc)
    return dss_fn


main(DSS_DIR, DSS_FN)
