import geopandas as gpd
import pandas as pd
import argparse
import requests

parser = argparse.ArgumentParser()
parser.add_argument("-j", "--json", help="path to the GeoJSON File")
parser.add_argument("-n", "--name", help="The name to associate with the AOI")

args = parser.parse_args()

if __name__ == "__main__":

    gdf = gpd.read_file(args.json)
    gdf_wkt = gdf.dissolve().geometry.to_wkt().iloc[0]

    response = requests.post(
        "http://127.0.0.1:8000/insert_aoi_geometry_to_db/",
        json=[{
            "aoi_name": args.name,
            "aoi_wkt": gdf_wkt
        }]
    )

    print(response.content)
    print(response.json())