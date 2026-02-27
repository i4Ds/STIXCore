from pathlib import Path

import numpy as np
from stixpy.coordinates.transforms import STIXImaging, get_hpc_info
from sunpy.coordinates import HeliographicStonyhurst, Helioprojective

import astropy.units as u
from astropy.coordinates import SkyCoord
from astropy.io import fits
from astropy.io.fits import table_to_hdu
from astropy.table.table import QTable
from astropy.time import Time

from stixcore.config.config import CONFIG
from stixcore.ephemeris.manager import Spice, SpiceKernelManager

if __name__ == "__main__":
    _spm = SpiceKernelManager(Path(CONFIG.get("Paths", "spice_kernels")))
    spicemeta = _spm.get_latest_mk_and_pred()

    Spice.instance = Spice(spicemeta)

    now = Time.now()

    start_utc = now - 100 * u.d
    peak_utc = now - 98 * u.d
    end_utc = now - 96 * u.d

    start_utc_2 = now - 10 * u.d
    peak_utc_2 = now - 9 * u.d
    end_utc_2 = now - 9 * u.d

    roll_1, solo_xyz_1, pointing_1 = get_hpc_info(start_utc, end_utc)
    solo_1 = HeliographicStonyhurst(*solo_xyz_1, obstime=peak_utc, representation_type="cartesian")
    center_hpc = SkyCoord(0 * u.deg, 0 * u.deg, frame=Helioprojective(obstime=peak_utc, observer=solo_1))
    coord_stix = center_hpc.transform_to(STIXImaging(obstime=start_utc, obstime_end=end_utc, observer=solo_1))

    coord_hp = coord_stix.transform_to(Helioprojective(observer=solo_1))

    roll_2, solo_xyz_2, pointing_2 = get_hpc_info(start_utc_2, end_utc_2)
    solo_2 = HeliographicStonyhurst(*solo_xyz_2, obstime=peak_utc_2, representation_type="cartesian")
    center_hpc = SkyCoord(0 * u.deg, 0 * u.deg, frame=Helioprojective(obstime=peak_utc_2, observer=solo_2))
    coord_stix_2 = center_hpc.transform_to(STIXImaging(obstime=start_utc_2, obstime_end=end_utc_2, observer=solo_2))

    coord_hp_2 = coord_stix_2.transform_to(Helioprojective(observer=solo_2))

    # ==========================================================================
    # APPROACH: Store individual coordinate components instead of SkyCoord
    # ==========================================================================
    # This is simpler and more transparent than storing SkyCoord objects:
    # - No need for ICRS conversion or distance handling
    # - FITS files contain plain columns with units (hp_tx, hp_ty, observer_x, etc.)
    # - Reconstruction is straightforward: SkyCoord(Tx, Ty, frame=Helioprojective(...))
    # - Works for any coordinate frame (Helioprojective, HeliographicStonyhurst, etc.)
    # - No need for fits.connect._encode_mixins() complexity

    # Store Helioprojective coordinates as separate Tx, Ty columns
    hp_tx = [coord_hp.Tx, coord_hp_2.Tx]
    hp_ty = [coord_hp.Ty, coord_hp_2.Ty]

    # Store observer position as separate x, y, z columns (or as single vector)
    observer_x = [solo_xyz_1[0], solo_xyz_2[0]]
    observer_y = [solo_xyz_1[1], solo_xyz_2[1]]
    observer_z = [solo_xyz_1[2], solo_xyz_2[2]]

    data = QTable()
    data["start_UTC"] = [start_utc, start_utc_2]
    data["peak_UTC"] = [peak_utc, peak_utc_2]
    data["end_UTC"] = [end_utc, end_utc_2]
    data["counts"] = 100 * u.s
    data["duration"] = 10 * u.s
    data["lc_peak"] = [u.Quantity(range(1, 10))]

    # Store Helioprojective coordinates as separate columns with units
    data["hp_tx"] = hp_tx
    data["hp_ty"] = hp_ty

    # Store observer position as separate columns with units
    data["observer_hgs_x"] = observer_x
    data["observer_hgs_y"] = observer_y
    data["observer_hgs_z"] = observer_z

    # Using fits.connect._encode_mixins (recommended approach used in STIXCore)
    # This properly serializes complex types like SkyCoord
    data_enc = fits.connect._encode_mixins(data)
    data_hdu = table_to_hdu(data_enc)
    data_hdu.name = "DATA"

    primary_hdu = fits.PrimaryHDU()
    hdul = fits.HDUList([primary_hdu, data_hdu])
    hdul.writeto("test_sky_coord.fits", overwrite=True)

    # Read back - everything needed is stored in the FITS file
    # No SkyCoord objects involved - just plain columns with units!
    tr_conv = QTable.read("test_sky_coord.fits", astropy_native=True)

    print("\n" + "=" * 60)
    print("READING BACK FROM FITS - NO SKYCOORD OBJECTS")
    print("=" * 60)

    # Get original coordinates for comparison
    original_coords_hp = [coord_hp, coord_hp_2]

    # Process all rows
    for i in range(len(tr_conv)):
        # Read stored Helioprojective coordinates (just plain columns with units!)
        hp_tx_stored = tr_conv["hp_tx"][i]
        hp_ty_stored = tr_conv["hp_ty"][i]
        obstime = tr_conv["peak_UTC"][i]

        # Read observer position from separate columns
        obs_x = tr_conv["observer_hgs_x"][i]
        obs_y = tr_conv["observer_hgs_y"][i]
        obs_z = tr_conv["observer_hgs_z"][i]

        # Reconstruct observer from stored components (NO SPICE!)
        observer = HeliographicStonyhurst(x=obs_x, y=obs_y, z=obs_z, obstime=obstime, representation_type="cartesian")
        observer_spherical = observer.represent_as("spherical")

        # Reconstruct Helioprojective SkyCoord from stored Tx, Ty
        # This is the only place we create a SkyCoord - from plain stored values!
        coord_hp_reconstructed = SkyCoord(
            hp_tx_stored, hp_ty_stored, frame=Helioprojective(obstime=obstime, observer=observer)
        )

        print(f"\nRow {i + 1}:")
        print(f"  Stored Tx, Ty: {hp_tx_stored:.2f}, {hp_ty_stored:.2f}")
        print(
            f"  Reconstructed Helioprojective: Tx={coord_hp_reconstructed.Tx:.2f}, Ty={coord_hp_reconstructed.Ty:.2f}"
        )
        print(f"  Observer: x={obs_x:.2e}, y={obs_y:.2e}, z={obs_z:.2e}")
        print(
            f"  Observer (Spherical): Lon={observer_spherical.lon:.2f}, Lat={observer_spherical.lat:.2f}, "
            f"R={observer_spherical.distance.to(u.AU):.3f}"
        )

        # Verify round-trip accuracy
        original_hp = original_coords_hp[i]

        # Tolerance for angular comparison (1 arcsecond is reasonable for stored Tx/Ty)
        tolerance = 1 * u.arcsec

        # Compare Tx/Ty directly
        assert np.abs(coord_hp_reconstructed.Tx - original_hp.Tx) < tolerance, (
            f"Row {i + 1} HP Tx mismatch: {coord_hp_reconstructed.Tx} vs {original_hp.Tx}"
        )
        assert np.abs(coord_hp_reconstructed.Ty - original_hp.Ty) < tolerance, (
            f"Row {i + 1} HP Ty mismatch: {coord_hp_reconstructed.Ty} vs {original_hp.Ty}"
        )

        print("  ✓ Round-trip verification passed:")
        print(f"    - Original HP: Tx={original_hp.Tx:.2f}, Ty={original_hp.Ty:.2f}")
        print(f"    - Reconstructed HP: Tx={coord_hp_reconstructed.Tx:.2f}, Ty={coord_hp_reconstructed.Ty:.2f}")
        print(
            f"    - Difference: ΔTx={np.abs(coord_hp_reconstructed.Tx - original_hp.Tx):.4f}, "
            f"ΔTy={np.abs(coord_hp_reconstructed.Ty - original_hp.Ty):.4f}"
        )

    print("\n" + "=" * 60)
    print("✓ All conversions completed without SPICE!")
    print("=" * 60)
    print("\nBenefits of storing components instead of SkyCoord:")
    print("  • No need for SkyCoord serialization - just plain columns with units")
    print("  • FITS files are more transparent and easier to inspect")
    print("  • Simple reconstruction: just create SkyCoord from stored Tx, Ty")
    print("  • Works with any coordinate frame (Helioprojective, HeliographicStonyhurst, etc.)")
    print("  • Observer position stored as separate x, y, z columns")
    print("  • All units are preserved automatically by astropy.table")
    print("\nWhat's stored in FITS:")
    print("  • hp_tx, hp_ty: Helioprojective coordinates (arcsec)")
    print("  • observer_hgs_x, observer_hgs_y, observer_hgs_z: Observer position (km)")
    print("  • peak_UTC: Observation time")
    print("\nReconstruction is simple:")
    print("  1. Read Tx, Ty from columns (they have units!)")
    print("  2. Read observer x, y, z from columns")
    print("  3. Create observer: HeliographicStonyhurst(x=x, y=y, z=z, obstime=obstime)")
    print("  4. Create coordinate: SkyCoord(Tx, Ty, frame=Helioprojective(obstime, observer))")
    print("  5. Done! No SPICE, no ICRS conversion, no distance issues.")
