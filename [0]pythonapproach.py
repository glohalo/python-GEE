import json
from pathlib import Path
from typing import List, Tuple, Union
from pyproj import Transformer
import geopandas as gpd

class CoordinateTransformer:
    """
    Handles coordinate transformation from one CRS to another.
    """

    def __init__(self, input_epsg: str, output_epsg: str):
        self.transformer = Transformer.from_crs(input_epsg, output_epsg, always_xy=True)

    def transform_coordinates(self, coords: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
        """
        Transform a list of (x, y) coordinates from input CRS to output CRS.
        """
        return [tuple(self.transformer.transform(x, y)) for x, y in coords]

    def transform_feature_geometry(self, geometry_type: str, coordinates: Union[List, List[List]]) -> Union[List, List[List]]:
        """
        Applies transformation logic to geometry coordinates based on type.
        """
        if geometry_type == "Polygon":
            return [self.transform_coordinates(ring) for ring in coordinates]
        elif geometry_type == "MultiPolygon":
            return [[self.transform_coordinates(ring) for ring in polygon] for polygon in coordinates]
        else:
            raise ValueError(f"Unsupported geometry type: {geometry_type}")


def convert_geojson_coordinates(input_path: str, output_path: str, input_epsg: str = "EPSG:3116"):
    """
    Converts coordinates of a GeoJSON file from input_epsg to the EPSG of the file.
    """
    gdf = gpd.read_file(input_path)
    output_epsg = f"EPSG:{gdf.crs.to_epsg()}"

    transformer = CoordinateTransformer(input_epsg, output_epsg)

    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    for feature in data["features"]:
        geom_type = feature["geometry"]["type"]
        coords = feature["geometry"]["coordinates"]
        feature["geometry"]["coordinates"] = transformer.transform_feature_geometry(geom_type, coords)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

    print(f"Transformed GeoJSON saved to: {output_path}")


if __name__ == "__main__":
    # Resolve paths relative to project root
    project_root = Path(__file__).resolve().parents[1]  
    input_file = project_root / "data/raw/ArchivoGeojson/Buffer100m.geojson"
    output_file = project_root / "data/processed/BufferTransformado.geojson"
    convert_geojson_coordinates(input_file, output_file)
