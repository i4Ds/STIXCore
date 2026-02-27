# Storing Coordinate Components Instead of SkyCoord Objects

## Overview

Instead of storing SkyCoord objects in FITS files, store the individual coordinate components (Tx, Ty for Helioprojective, or lon, lat for HeliographicStonyhurst) along with observer position and time. This approach is simpler, more transparent, and avoids many serialization issues.

## Benefits

1. **Simplicity**: No need for `fits.connect._encode_mixins()` or ICRS conversion
2. **Transparency**: FITS files contain plain columns with clear names and units
3. **No Distance Issues**: No need to handle distance/unit sphere complications
4. **Direct Reconstruction**: Create SkyCoord directly from stored components
5. **Frame Agnostic**: Works for any coordinate frame
6. **Better Inspection**: Easy to view coordinates with standard FITS tools

## What to Store

For **Helioprojective** coordinates:
- `hp_tx`: Tx coordinate (arcsec)
- `hp_ty`: Ty coordinate (arcsec)
- `observer_hgs_x`: Observer x position in HeliographicStonyhurst (km)
- `observer_hgs_y`: Observer y position in HeliographicStonyhurst (km)
- `observer_hgs_z`: Observer z position in HeliographicStonyhurst (km)
- `peak_UTC` (or relevant time): Observation time

For **HeliographicStonyhurst** coordinates:
- `hgs_lon`: Longitude (deg)
- `hgs_lat`: Latitude (deg)
- `hgs_radius`: Radius (km) - optional, can default to solar radius
- `peak_UTC`: Observation time

## Example: Writing to FITS

```python
from astropy.table import QTable
from astropy.io import fits
from astropy.io.fits import table_to_hdu
import astropy.units as u
from sunpy.coordinates import Helioprojective, HeliographicStonyhurst
from stixpy.coordinates.transforms import get_hpc_info

# Get coordinates and observer position
coord_hp = ...  # Your Helioprojective coordinate
roll, solo_xyz, pointing = get_hpc_info(start_utc, end_utc)

# Extract components
hp_tx = coord_hp.Tx
hp_ty = coord_hp.Ty
obs_x = solo_xyz[0]
obs_y = solo_xyz[1]
obs_z = solo_xyz[2]

# Create table with component columns
data = QTable()
data["peak_UTC"] = [peak_utc]
data["hp_tx"] = [hp_tx]
data["hp_ty"] = [hp_ty]
data["observer_hgs_x"] = [obs_x]
data["observer_hgs_y"] = [obs_y]
data["observer_hgs_z"] = [obs_z]

# Write to FITS - no special encoding needed!
data_enc = fits.connect._encode_mixins(data)  # Still handles Time, Quantity
data_hdu = table_to_hdu(data_enc)
hdul = fits.HDUList([fits.PrimaryHDU(), data_hdu])
hdul.writeto('coordinates.fits', overwrite=True)
```

## Example: Reading from FITS

```python
from astropy.table import QTable
from astropy.coordinates import SkyCoord
from sunpy.coordinates import Helioprojective, HeliographicStonyhurst

# Read back
data = QTable.read('coordinates.fits', astropy_native=True)

# Extract components (they have units!)
hp_tx = data['hp_tx'][0]
hp_ty = data['hp_ty'][0]
obs_x = data['observer_hgs_x'][0]
obs_y = data['observer_hgs_y'][0]
obs_z = data['observer_hgs_z'][0]
obstime = data['peak_UTC'][0]

# Reconstruct observer (NO SPICE!)
observer = HeliographicStonyhurst(
    x=obs_x, y=obs_y, z=obs_z,
    obstime=obstime,
    representation_type='cartesian'
)

# Reconstruct coordinate
coord_hp = SkyCoord(
    hp_tx, hp_ty,
    frame=Helioprojective(obstime=obstime, observer=observer)
)

# Done! coord_hp is now a proper Helioprojective coordinate
```

## Comparison with SkyCoord Storage

### Old Approach (storing SkyCoord objects):
1. Convert coordinate to ICRS
2. Drop distance to make frames compatible
3. Use `concatenate()` to create arrays
4. Encode with `fits.connect._encode_mixins()`
5. Write to FITS
6. Read back
7. Add distance back
8. Convert to desired frame

**Problems:**
- Complex serialization
- Distance handling issues
- Frame conversion complications
- Not transparent in FITS files

### New Approach (storing components):
1. Extract Tx, Ty (or lon, lat)
2. Extract observer x, y, z
3. Store in table columns
4. Write to FITS
5. Read back
6. Create SkyCoord from components

**Advantages:**
- Simple and direct
- No frame conversions needed
- No distance issues
- Transparent FITS structure
- Fewer steps

## Multiple Coordinate Frames

You can store multiple coordinate representations:

```python
# Store both Helioprojective and HeliographicStonyhurst
data["hp_tx"] = [coord_hp.Tx]
data["hp_ty"] = [coord_hp.Ty]
data["hgs_lon"] = [coord_hgs.lon]
data["hgs_lat"] = [coord_hgs.lat]
data["hgs_radius"] = [coord_hgs.radius]
data["observer_hgs_x"] = [obs_x]
data["observer_hgs_y"] = [obs_y]
data["observer_hgs_z"] = [obs_z]
```

Each can be reconstructed independently:

```python
# Reconstruct Helioprojective
coord_hp = SkyCoord(hp_tx, hp_ty, frame=Helioprojective(obstime, observer))

# Reconstruct HeliographicStonyhurst
coord_hgs = SkyCoord(hgs_lon, hgs_lat, hgs_radius,
                     frame=HeliographicStonyhurst(obstime=obstime))
```

## Column Naming Conventions

Suggested naming:
- **Helioprojective**: `hp_tx`, `hp_ty`
- **HeliographicStonyhurst**: `hgs_lon`, `hgs_lat`, `hgs_radius`
- **HeliographicCarrington**: `hgc_lon`, `hgc_lat`, `hgc_radius`
- **Observer position**: `observer_hgs_x`, `observer_hgs_y`, `observer_hgs_z`
- **Time**: `peak_UTC`, `start_UTC`, `end_UTC`

## Units

All units are automatically preserved by `astropy.table.QTable`:
- Angles: `astropy.units.arcsec`, `astropy.units.deg`
- Distances: `astropy.units.km`, `astropy.units.AU`
- Times: `astropy.time.Time` objects

When reading with `astropy_native=True`, units are automatically restored.

## Integration with Existing STIXCore Code

This approach can coexist with existing code:

```python
# If you have existing SkyCoord columns, convert to components
if 'flare_location' in data.colnames:
    # Extract components
    data['hp_tx'] = [coord.Tx for coord in data['flare_location']]
    data['hp_ty'] = [coord.Ty for coord in data['flare_location']]
    # Remove old column
    data.remove_column('flare_location')

# Or keep both for backwards compatibility
data['flare_location'] = coords_array  # Old format
data['hp_tx'] = [coord.Tx for coord in coords_array]  # New format
data['hp_ty'] = [coord.Ty for coord in coords_array]
```

## Summary

**Key Points:**
1. Store coordinate components (Tx, Ty, lon, lat) as separate columns
2. Store observer position as separate x, y, z columns
3. Store observation time
4. No SkyCoord serialization needed - just plain columns with units
5. Reconstruction is simple: `SkyCoord(Tx, Ty, frame=...)`
6. No SPICE needed when reading - all data is in the FITS file
7. FITS files are transparent and easy to inspect

This approach provides the simplest path to storing and retrieving astronomical coordinates in FITS files while maintaining full scientific accuracy.
