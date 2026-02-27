# SkyCoord FITS Serialization - Quick Reference

## The Problem
Complex coordinate frames (HeliographicStonyhurst, Helioprojective, STIXImaging) cannot be directly serialized to FITS because they contain observer metadata.

## The Solution
Convert to ICRS frame and store observer position separately.

## Quick Code Template

### Writing FITS (with observer data)

```python
from astropy.io import fits
from astropy.io.fits import table_to_hdu
from astropy.table import QTable
from astropy.coordinates import SkyCoord

# 1. Get observer position (from SPICE, only needed at write time)
roll, solo_xyz, pointing = get_hpc_info(start_utc, end_utc)
# solo_xyz is a Quantity vector [x, y, z] in km

# 2. Convert coordinates to ICRS
coord_icrs = coord_stix.transform_to('icrs')

# For multiple coordinates, drop distance and create SkyCoord array using concatenate
# Distance must be dropped to make frames equivalent for concatenation
from astropy.coordinates import concatenate
coord_icrs_1_nodist = SkyCoord(ra=coord_icrs_1.ra, dec=coord_icrs_1.dec, frame='icrs')
coord_icrs_2_nodist = SkyCoord(ra=coord_icrs_2.ra, dec=coord_icrs_2.dec, frame='icrs')
coords_array = concatenate([coord_icrs_1_nodist, coord_icrs_2_nodist])  # or [coord_icrs] for single

# 3. Create table with all necessary data
data = QTable()
data["peak_UTC"] = [peak_utc]
data["flare_location"] = coords_array  # SkyCoord array, not list
# Store observer as Cartesian vector (no SPICE needed later!)
data["observer_hgs_xyz"] = [solo_xyz]  # [x, y, z] in km

# 4. Write to FITS
data_enc = fits.connect._encode_mixins(data)
data_hdu = table_to_hdu(data_enc)
data_hdu.name = "DATA"

primary_hdu = fits.PrimaryHDU()
hdul = fits.HDUList([primary_hdu, data_hdu])
hdul.writeto('output.fits', overwrite=True)
```

### Reading FITS and Converting Back (NO SPICE!)

```python
from sunpy.coordinates import HeliographicStonyhurst, Helioprojective
from astropy.table import QTable

# 1. Read from FITS
data = QTable.read('output.fits', astropy_native=True)

# 2. Extract data
coord_icrs = data['flare_location'][0]
obstime = data['peak_UTC'][0]
xyz = data['observer_hgs_xyz'][0]  # [x, y, z] vector with units

# 3. Reconstruct observer from Cartesian vector (NO SPICE!)
observer = HeliographicStonyhurst(
    x=xyz[0], y=xyz[1], z=xyz[2],
    obstime=obstime,
    representation_type='cartesian'
)

# 4. Convert to desired frame
# Add distance to the coordinate (stored coords are on unit sphere without distance)
coord_icrs_with_dist = SkyCoord(
    ra=coord_icrs.ra, dec=coord_icrs.dec,
    distance=observer.radius,  # Use Sun's distance from observer
    frame='icrs'
)
coord_hp = coord_icrs_with_dist.transform_to(
    Helioprojective(obstime=obstime, observer=observer)
)
```

## Key Benefits

✅ **No SPICE dependency for reading** - All observer data stored in FITS
✅ **Self-contained files** - Everything needed is in the FITS file
✅ **Standard approach** - Used in STIXCore flarelist code
✅ **Preserves sky position** - RA/Dec accurately maintained
✅ **Enables coordinate transformations** - Can convert to any frame

## Files

- **[SKYCOORD_FITS_SERIALIZATION.md](SKYCOORD_FITS_SERIALIZATION.md)** - Complete documentation
- **[skycoord.py](skycoord.py)** - Working example with tests
