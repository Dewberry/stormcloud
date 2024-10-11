
import os
from pydsstools.heclib.dss.HecDss import Open

DSS_FILE = "trinity_precipitation.dss"

def extract_tifs_from_dss():
    """Extract a DSS grid to a tif"""
    if not os.path.exists(DSS_FILE):
        raise FileNotFoundError(DSS_FILE)
    print("Opening DSS file...")
    with Open(DSS_FILE) as dss:
        print("Getting path name list...")
        paths = dss.getPathnameList("/*/*/*/*/*/*/", sort=1)
        for i, path in enumerate(paths):
            # only do every 168 paths (each one is an hour, so this is one per week)
            if i % 168 != 0:
                continue
            tgt_file = os.path.join(DSS_FILE + "-tifs", DSS_FILE + f"{path.replace('/', '-').replace(':', '-')}.tif")
            print(f"Reading: {path}")
            dataset = dss.read_grid(path)
            print(f"Writing: {tgt_file}")
            os.makedirs(os.path.dirname(tgt_file), exist_ok=True)
            dataset.raster.save_tiff(tgt_file)


if __name__ == "__main__":
    extract_tifs_from_dss()
