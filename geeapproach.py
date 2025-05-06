import ee

# Authenticate and initialize Earth Engine
ee.Authenticate()
ee.Initialize(project='ee-yourprojectid')

# Load buffer shapefile asset
buffer = ee.FeatureCollection("projects/ee-gloriacarrascal53/assets/Buffer")
buffer_filter = buffer.geometry().buffer(250)  

# Define years and semesters
start_year = 2018
end_year = 2025

# Cloud masking using the SCL band
def mask_s2_sr(image):
    scl = image.select('SCL')
    # Keep classes that are not cloud, cloud shadow, cirrus, or water
    mask = scl.neq(3).And(scl.neq(8)).And(scl.neq(9)).And(scl.neq(10))
    return image.updateMask(mask)

# Get NDVI for each semester, with fallback handling
def get_ndvi_semester(year, semester):
    if semester == 1:
        start = ee.Date(f'{year}-01-01')
        end = ee.Date(f'{year}-06-30')
    else:
        start = ee.Date(f'{year}-07-01')
        end = ee.Date(f'{year}-12-31')

    name = f'ndvi_{year}_S{semester}'

    # Step 1:Try with strict cloud filter (10%)
    s2 = ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED") \
        .filterBounds(buffer_filter) \
        .filterDate(start, end) \
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 10))

    count = s2.size().getInfo()
    if count == 0:
        print(f"No images for {name} with <10% clouds, trying relaxed filter...")
        # Step 2: Relax cloud filter and apply SCL cloud mask
        s2 = ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED") \
            .filterBounds(buffer_filter) \
            .filterDate(start, end) \
            .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 60)) \
            .map(mask_s2_sr)

        count = s2.size().getInfo()
        if count == 0:
            print(f"No usable images even after relaxed filtering for {name}, skipping.")
            return None, name

    # Step 3: Composite strategies
    composite = s2.median()
    if composite.bandNames().size().getInfo() == 0:
        print(f"Median failed for {name}, trying mean...")
        composite = s2.mean()
        if composite.bandNames().size().getInfo() == 0:
            print(f"Mean failed for {name}, trying first image...")
            composite = s2.sort('system:time_start').first()
            if composite.bandNames().size().getInfo() == 0:
                print(f"First image failed for {name}, trying quality mosaic...")
                s2 = s2.map(lambda img: img.addBands(img.normalizedDifference(["B8", "B4"]).rename("NDVI")))
                composite = s2.qualityMosaic("NDVI")
                if composite.bandNames().size().getInfo() == 0:
                    print(f"All methods failed for {name}, skipping.")
                    return None, name

    # Step 4: Calculate NDVI
    ndvi = composite.normalizedDifference(["B8", "B4"]).rename("NDVI").clip(buffer)
    return ndvi, name

# Loop through years and semesters
for year in range(start_year, end_year + 1):
    for semester in [1, 2]:
        ndvi_image, name = get_ndvi_semester(year, semester)
        if ndvi_image is not None:
            print(f"Scheduling export for {name}")
            task = ee.batch.Export.image.toDrive(
                image=ndvi_image,
                description=name,
                folder='EarthEngine',
                fileNamePrefix=f'lvirsmar_{name}',
                region=buffer.geometry(),
                scale=10,
                maxPixels=1e9
            )
            task.start()
        else:
            print(f"Skipped export for {name}")
