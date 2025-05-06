import os
import json
import tempfile
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import numpy as np
import rasterio
from planetary_computer import sign_item as pc_sign_item
from pystac_client import Client
from rasterio.mask import mask
import shapely.geometry

class SentinelProcessor:
    def __init__(self, start_year, id_lines_to_process):
        self.start_year = start_year
        self.id_lines_to_process = id_lines_to_process
        self.bands = ["B04", "B08"]
        self.catalog = Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")

        self.project_root = Path(__file__).resolve().parents[1]
        self.input_file = self.project_root / "data/raw/ArchivoGeojson/Buffer100m.geojson"
        self.output_file = self.project_root / "data/processed/BufferTransformado.geojson"
        self.json_path = self.project_root / "data/raw/ArchivoGeojson/contenedor.geojson"
        self.json_path_rec = self.project_root / "data/processed/BufferTransformado.geojson"
        self.output_base_dir = self.project_root / "data/raw/satellital_image"

    def log_message(self, message, log_file):
        print(message)
        log_file.write(message + "\n")

    def process_line(self, line_idx):
        log_file_path = self.output_base_dir / f"process_log{line_idx}.txt"
        os.makedirs(self.output_base_dir, exist_ok=True)
        with open(log_file_path, "w", encoding="utf-8") as log_file:
            def log(msg):
                self.log_message(msg, log_file)

            if not self.json_path.exists():
                log(f"JSON file not found: {self.json_path}")
                raise FileNotFoundError(f"JSON file not found: {self.json_path}")

            if not self.json_path_rec.exists():
                log(f"Recorte JSON file not found: {self.json_path_rec}")
                raise FileNotFoundError(f"Recorte JSON file not found: {self.json_path_rec}")

            with open(self.json_path, "r", encoding="utf-8") as file:
                geojson_data = json.load(file)

            feature = geojson_data["features"][line_idx]
            aoi = feature["geometry"]
            id_line = feature["properties"]["original_properties"].get("ID_Linea", "Sin ID")
            log(f"Processing AOI with ID: {id_line}")

            clip_folder = self.output_base_dir / f"{id_line}"
            os.makedirs(clip_folder, exist_ok=True)

            gdf_buffer = gpd.read_file(self.json_path_rec)
            distancia_buffer = 100

            if gdf_buffer.crs and gdf_buffer.crs.to_epsg() == 4326:
                gdf_buffer = gdf_buffer.to_crs(epsg=3857)

            gdf = gdf_buffer.copy()
            gdf["geometry"] = gdf_buffer.geometry.buffer(distancia_buffer)

            if gdf_buffer.crs.to_epsg() == 3857:
                gdf = gdf.to_crs(epsg=4326)

            if 'UBITEC' not in gdf.columns:
                if 'original_properties' in gdf.columns:
                    gdf['UBITEC'] = gdf['original_properties'].apply(lambda props: json.loads(props).get('UBITEC') if isinstance(props, str) else props.get('UBITEC'))

                else:
                    log(f"'UBITEC' column and 'original_properties' not found in GeoDataFrame. Columns: {list(gdf.columns)}")
                    raise KeyError("Required data for 'UBITEC' filtering not found")
            gdf = gdf[gdf['UBITEC'] == id_line]

            if len(gdf) == 0:
                log(f"No valid geometries found for ID_Linea: {id_line}")
                raise ValueError(f"No valid geometries found for ID_Linea: {id_line}")

            aoi_polygon = shapely.geometry.shape(aoi)
            current_year = datetime.now().year

            for year in range(self.start_year, current_year + 1):
                start_date = f"{year}-07-01T00:00:00Z"
                end_date = f"{year+1}-06-30T23:59:59Z"
                daterange = {"interval": [start_date, end_date]}
                log(f"Searching images for {year}")

                try:
                    search = self.catalog.search(
                        filter_lang="cql2-json",
                        filter={
                            "op": "and",
                            "args": [
                                {"op": "intersects", "args": [{"property": "geometry"}, aoi]},
                                {"op": "anyinteracts", "args": [{"property": "datetime"}, daterange]},
                                {"op": "=", "args": [{"property": "collection"}, "sentinel-2-l2a"]},
                                {"op": "<=", "args": [{"property": "eo:cloud_cover"}, 10]},
                            ]
                        }
                    )
                    items = list(search.items())
                    if not items:
                        log(f"No images found for {year}")
                        continue

                    items.sort(key=lambda x: x.properties.get("eo:cloud_cover", 100))
                    complete_coverage = [item for item in items if aoi_polygon.within(shapely.geometry.shape(item.geometry))]
                    partial_coverage = [
                        (item, shapely.geometry.shape(item.geometry).intersection(aoi_polygon).area / aoi_polygon.area)
                        for item in items if item not in complete_coverage
                    ]

                    selected_items = complete_coverage[:1] if complete_coverage else []
                    if not selected_items and partial_coverage:
                        partial_coverage.sort(key=lambda x: (-x[1], x[0].properties.get("eo:cloud_cover", 100)))
                        coverage = shapely.geometry.shape(partial_coverage[0][0].geometry).intersection(aoi_polygon)
                        selected_items = [partial_coverage[0][0]]

                        for item, _ in partial_coverage[1:]:
                            new_geom = shapely.geometry.shape(item.geometry)
                            new_coverage = coverage.union(new_geom.intersection(aoi_polygon))
                            coverage_increase = new_coverage.area / aoi_polygon.area - coverage.area / aoi_polygon.area

                            if coverage_increase > 0.05:
                                selected_items.append(item)
                                coverage = new_coverage
                                log(f"Added image for additional {coverage_increase:.2%} coverage")

                            if coverage.area / aoi_polygon.area > 0.98:
                                break

                    if not selected_items:
                        log(f"No suitable images found for {year}")
                        continue

                    for i, item in enumerate(selected_items):
                        image_date = item.properties["datetime"].split("T")[0]
                        signed_item = pc_sign_item(item)

                        band_urls = {band: signed_item.assets[band].href for band in self.bands if band in signed_item.assets}
                        if len(band_urls) != len(self.bands):
                            log(f"Missing bands for image {i+1} in {year}")
                            continue

                        try:
                            band_data = []
                            with rasterio.open(list(band_urls.values())[0]) as first_src:
                                if gdf.crs != first_src.crs:
                                    gdf = gdf.to_crs(first_src.crs)

                            with tempfile.NamedTemporaryFile(suffix='.tif', delete=False) as tmp:
                                temp_path = tmp.name

                            for band in band_urls:
                                with rasterio.open(band_urls[band]) as src:
                                    if band == self.bands[0]:
                                        meta = src.meta.copy()
                                        meta.update({"count": len(self.bands), "compress": "lzw"})
                                    band_data.append(src.read(1))

                            stack = np.stack(band_data)
                            with rasterio.open(temp_path, 'w', **meta) as dst:
                                for idx in range(stack.shape[0]):
                                    dst.write(stack[idx], idx + 1)

                            log(f"Composite image created for {year} image {i+1}")

                            with rasterio.open(temp_path) as src:
                                out_image, out_transform = mask(src, gdf.geometry, crop=True, nodata=0)

                                if out_image.size > 0 and np.any(out_image):
                                    out_meta = src.meta.copy()
                                    out_meta.update({
                                        "driver": "GTiff",
                                        "height": out_image.shape[1],
                                        "width": out_image.shape[2],
                                        "transform": out_transform,
                                        "compress": "lzw"
                                    })

                                    out_name = f"composite_{year}_{image_date}_img{i+1}.tif"
                                    out_path = clip_folder / out_name

                                    with rasterio.open(out_path, "w", **out_meta) as dest:
                                        dest.write(out_image)

                                    log(f"Saved clipped image: {out_name}")
                                else:
                                    log(f"No data after clipping image {i+1} for {year}")

                            if os.path.exists(temp_path):
                                os.remove(temp_path)

                        except Exception as e:
                            log(f"Error processing image {i+1} for {year}: {str(e)}")
                            if 'temp_path' in locals() and os.path.exists(temp_path):
                                os.remove(temp_path)

                except Exception as e:
                    log(f"Search error for {year}: {str(e)}")

            log("Download and clipping process completed.")


if __name__ == "__main__":
    id_lines_to_process = {
        "Buffer1": 0,
        "Buffer2": 1,
    }processor = SentinelProcessor(start_year=2015, id_lines_to_process=id_lines_to_process)
    for id_name, idx in id_lines_to_process.items():
        print(f"\n--- Processing line: {id_name} (Index {idx}) ---")
        processor.process_line(idx)

