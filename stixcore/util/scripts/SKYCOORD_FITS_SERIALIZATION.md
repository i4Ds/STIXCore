# SkyCoord FITS Serialization Guide

## Problem

When trying to serialize Astropy `SkyCoord` objects to FITS files, you may encounter two types of errors:

1. **Single coordinate with complex frame**:
   ```
   ('cannot represent an object', <HeliographicStonyhurst Coordinate (obstime=..., rsun=...) ...>)
   ```

2. **Multiple coordinates (mixed types)**:
   ```
   Column 'flare_location_stix' contains unsupported object types or mixed types: {dtype('O')}
   ```

## Root Cause

The issue occurs because:
1. FITS format doesn't natively support complex Python objects like `SkyCoord`
2. Astropy's `fits.connect._encode_mixins()` can serialize **simple** coordinate frames (ICRS, FK5, Galactic)
3. **Complex frames with observer information** (HeliographicStonyhurst, Helioprojective, STIXImaging) contain additional metadata that cannot be automatically serialized

## Solution 1: Convert to ICRS Frame (Recommended)

Convert your coordinates to the ICRS frame before storing. This is the approach used in STIXCore's flarelist.

```python
from astropy.io import fits
from astropy.io.fits import table_to_hdu
from astropy.table import QTable
from astropy.coordinates import SkyCoord

# Your original coordinates in complex frames
coord_hp = SkyCoord(0 * u.deg, 0 * u.deg,
                    frame=Helioprojective(obstime=peak_utc, observer=solo))
coord_stix = coord_hp.transform_to(STIXImaging(obstime=start_utc,
                                                obstime_end=end_utc,
                                                observer=solo))

# Convert to ICRS for serialization
coord_stix_icrs = coord_stix.transform_to('icrs')
coord_hp_icrs = coord_hp.transform_to('icrs')

# Create table with ICRS coordinates
data = QTable()
data["flare_location_stix"] = [coord_stix_icrs]
data["flare_location_hp"] = [coord_hp_icrs]

# Serialize using fits.connect._encode_mixins
data_enc = fits.connect._encode_mixins(data)
data_hdu = table_to_hdu(data_enc)
data_hdu.name = "DATA"

# Write to FITS
primary_hdu = fits.PrimaryHDU()
hdul = fits.HDUList([primary_hdu, data_hdu])
hdul.writeto('output.fits', overwrite=True)

# Read back with astropy_native=True to reconstruct SkyCoord
tr = QTable.read('output.fits', astropy_native=True)
# tr['flare_location_stix'] is now a SkyCoord in ICRS frame
```

### Why This Works

- ICRS is a simple, time-independent reference frame
- No observer position or observation time metadata to serialize
- `fits.connect._encode_mixins()` knows how to handle ICRS coordinates
- The coordinates can be fully reconstructed when reading back with `astropy_native=True`

## Solution 2: Store Components Separately

For maximum control, store RA/Dec as separate columns:

```python
# Store coordinate components
data = QTable()
data["stix_ra"] = [coord_stix_icrs.ra]
data["stix_dec"] = [coord_stix_icrs.dec]

# Write as before
data_enc = fits.connect._encode_mixins(data)
data_hdu = table_to_hdu(data_enc)
data_hdu.name = "DATA"

primary_hdu = fits.PrimaryHDU()
hdul = fits.HDUList([primary_hdu, data_hdu])
hdul.writeto('output.fits', overwrite=True)

# Read back and reconstruct
tr = QTable.read('output.fits', astropy_native=True)
coord_reconstructed = SkyCoord(ra=tr['stix_ra'][0],
                               dec=tr['stix_dec'][0],
                               frame='icrs')
```

### Pros and Cons

**Solution 1 (ICRS conversion)**:
- ✅ Preserves full SkyCoord structure
- ✅ Automatic reconstruction when reading
- ✅ Used in STIXCore flarelist code
- ⚠️ Loses original frame information (must reconvert if needed)

**Solution 2 (Separate components)**:
- ✅ Maximum control over serialization
- ✅ Simpler FITS structure
- ✅ Easy to understand and debug
- ⚠️ Manual reconstruction required
- ⚠️ Need separate columns for each coordinate

## Key Points

1. **Always use `fits.connect._encode_mixins()`** before writing QTables with complex types to FITS
2. **Convert to simple frames** (ICRS, FK5, Galactic) before serialization
3. **Use `astropy_native=True`** when reading to reconstruct SkyCoord objects
4. **Store observer/frame metadata separately** if you need to reconstruct the original frame

## Example in STIXCore

The flarelist code always uses ICRS frame for stored coordinates:

```python
# From stixcore/products/level3/flarelist.py:94
data["flare_position"] = [SkyCoord(0, 0, frame="icrs", unit="deg")
                          for i in range(0, len(data))]
```

## Converting Back from ICRS (Without SPICE!)

Yes, you can convert ICRS coordinates back to HeliographicStonyhurst or Helioprojective! The key is to **store the observer position in the FITS file** so you don't need SPICE for reconstruction.

### Storing Data (Write Time)

```python
from sunpy.coordinates import HeliographicStonyhurst, Helioprojective
from astropy.coordinates import SkyCoord

# Get observer position from SPICE (only needed once at write time)
roll, solo_xyz, pointing = get_hpc_info(start_utc, end_utc)
# solo_xyz is a Quantity vector [x, y, z] in km

# Convert coordinates to ICRS for storage
coord_icrs = coord_stix.transform_to('icrs')

# For multiple coordinates, drop distance and create a SkyCoord array using concatenate (not a list)
# Distance must be dropped to make frames equivalent for concatenation
from astropy.coordinates import concatenate
coord_icrs_1_nodist = SkyCoord(ra=coord_icrs_1.ra, dec=coord_icrs_1.dec, frame='icrs')
coord_icrs_2_nodist = SkyCoord(ra=coord_icrs_2.ra, dec=coord_icrs_2.dec, frame='icrs')
coords_array = concatenate([coord_icrs_1_nodist, coord_icrs_2_nodist])  # if you have multiple

# Store everything needed for reconstruction
data = QTable()
data["peak_UTC"] = [peak_utc]
data["flare_location_icrs"] = coords_array  # or [coord_icrs] for single coord
# Store observer position as Cartesian vector (no SPICE needed later!)
data["observer_hgs_xyz"] = [solo_xyz]  # [x, y, z] in km

# Write to FITS
data_enc = fits.connect._encode_mixins(data)
data_hdu = table_to_hdu(data_enc)
# ... write as shown above
```

### Reading and Converting Back (Read Time - NO SPICE!)

```python
from sunpy.coordinates import HeliographicStonyhurst, Helioprojective

# Read back from FITS
tr = QTable.read('output.fits', astropy_native=True)

# Extract coordinate and time
coord_icrs = tr['flare_location_icrs'][0]
obstime = tr['peak_UTC'][0]
xyz = tr['observer_hgs_xyz'][0]  # [x, y, z] vector with units

# Reconstruct observer from stored Cartesian vector
# NO SPICE REQUIRED!
observer = HeliographicStonyhurst(
    x=xyz[0], y=xyz[1], z=xyz[2],
    obstime=obstime,
    representation_type='cartesian'
)

# Convert back to Helioprojective
# Add distance to the coordinate (stored coords are on unit sphere without distance)
coord_icrs_with_dist = SkyCoord(
    ra=coord_icrs.ra, dec=coord_icrs.dec,
    distance=observer.radius,  # Use Sun's distance from observer
    frame='icrs'
)
coord_hp = coord_icrs_with_dist.transform_to(Helioprojective(obstime=obstime, observer=observer))
```

### Important Notes

- **Store observer position in FITS** - This eliminates the need for SPICE when reading
- **Store observation times** - ICRS has no time dependence, store `peak_UTC`, `start_UTC`, etc.
- **Distance information may vary** - ICRS preserves sky position; radial distance can change slightly
- **Self-contained FITS files** - Everything needed for coordinate transformations is in the file

### Best Practice: Complete Storage

Store these columns for full reconstruction without external dependencies:

```python
data = QTable()
data["peak_UTC"] = [obstime]                    # For time-dependent transformations
data["start_UTC"] = [start_time]                # Optional: integration window
data["end_UTC"] = [end_time]                    # Optional: integration window
data["flare_location_icrs"] = [coord_icrs]      # Sky position
# Observer position as Cartesian vector (eliminates SPICE dependency)
data["observer_hgs_xyz"] = [solo_xyz]           # [x, y, z] in km
# Optional: store distance if needed
data["flare_distance"] = [coord.distance]       # Optional: 3D position
```

This approach makes your FITS files **self-contained** - no SPICE kernels needed for reading and transforming coordinates!

## Working Example

See [skycoord.py](stixcore/util/scripts/skycoord.py) for a complete working example that demonstrates both methods and round-trip conversion.
