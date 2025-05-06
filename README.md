# Sentinel-2 Processing Pipeline

This project provides a multi-approach solution for processing Sentinel-2 satellite imagery to extract NDVI values over a specified region of interest (buffer zone). It includes methods using both the Microsoft Planetary Computer and Google Earth Engine.

## Features

* **Coordinate Transformation**: Converts GeoJSON coordinates between coordinate reference systems.
* **Sentinel-2 Image Processing (Planetary Computer)**:

  * Downloads and clips images with minimal cloud cover.
  * Applies buffer and filtering by properties (e.g., `UBITEC`).
  * Stacks bands (B04, B08) and exports composite images per year.
* **NDVI Extraction (Earth Engine)**:

  * Filters and masks cloud-affected pixels.
  * Applies multiple fallback strategies to create NDVI composites.
  * Exports NDVI images per semester from 2018 to 2025.

## Structure

* `pythonapproach.py`: Local processing pipeline using STAC API and Planetary Computer.
* `geeapproach.py`: Cloud-based processing with Earth Engine, focused on NDVI extraction.
* Data paths and buffer geometries are resolved relative to a consistent project root.

## Requirements

* Python 3.x
* Libraries: `geopandas`, `rasterio`, `shapely`, `pyproj`, `planetary_computer`, `pystac_client`
* Earth Engine Python API (for GEE approach)

## Usage

1. Run `pythonapproach.py` to process and download images using Planetary Computer.
2. Use `geeapproach.py` for cloud-based NDVI export via Earth Engine.
