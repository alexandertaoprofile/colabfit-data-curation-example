import logging
import os
from pathlib import Path

import pandas as pd
from ase.io import read
from colabfit.tools.database import MongoDatabase, load_data
# from colabfit.tools.property_settings import PropertySettings 
from colabfit.tools.configuration import AtomicConfiguration

# ---- logging ----
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

def reader_OrbNet_from_df(df, base_xyz_dir):
    """
    Build structures list based on a pre-loaded dataframe (df) and base directory for xyz files.
    - df: pandas DataFrame with columns at least ['mol_id', 'sample_id', 'dft_energy', 'xtb1_energy', 'charge']
    - base_xyz_dir: directory containing subfolders named by mol_id and xyz files named <sample_id>.xyz
    Returns: list of ASE Atoms objects with info fields set.
    NOTE: we intentionally skip missing/invalid files and log them.
    """
    structures = []
    missing = 0
    for idx in df.index:
        try:
            mol_id = str(df.at[idx, 'mol_id'])
            sample_id = str(df.at[idx, 'sample_id'])
            # Build path robustly
            fpath = os.path.join(base_xyz_dir, mol_id, sample_id + '.xyz')
            if not os.path.exists(fpath):
                logger.warning("XYZ file not found, skipping: %s", fpath)
                missing += 1
                continue
            # ASE.read auto-detects format by extension; if format differs pass format argument
            structure = read(fpath)
            # Make sure info exists
            if not hasattr(structure, 'info') or structure.info is None:
                structure.info = {}
            # Use float() to ensure Python float, guard against pandas types
            try:
                structure.info['energy'] = float(df.at[idx, 'dft_energy'])
            except Exception:
                structure.info['energy'] = df.at[idx, 'dft_energy']
            # Optional extra fields
            if 'xtb1_energy' in df.columns:
                try:
                    structure.info['xtb1_energy'] = float(df.at[idx, 'xtb1_energy'])
                except Exception:
                    structure.info['xtb1_energy'] = df.at[idx, 'xtb1_energy']
            if 'charge' in df.columns:
                try:
                    structure.info['charge'] = float(df.at[idx, 'charge'])
                except Exception:
                    structure.info['charge'] = df.at[idx, 'charge']

            structures.append(structure)
        except Exception as e:
            logger.exception("Failed to read/attach info for row %s: %s", idx, e)
            # continue with next row
            continue

    logger.info("Finished building structures: total=%d, missing_files=%d", len(structures), missing)
    return structures


def main():
    # Initialize DB client. If you want to preserve previous db, set drop_database=False
    client = MongoDatabase('new_data_test_alexander', configuration_type=AtomicConfiguration, nprocs=4, drop_database=True)

    # Paths (tweak if your dataset is in a different location)
    base_dir = Path('/large_data/new_raw_datasets_2.0/OrbNet_Denali')
    labels_csv = base_dir / 'denali_labels.csv'
    xyz_base = base_dir / 'xyz_files'

    if not labels_csv.exists():
        logger.error("Labels CSV not found: %s", str(labels_csv))
        return

    # Read CSV once (avoid reading inside reader for each call)
    try:
        df = pd.read_csv(labels_csv, index_col=0)
    except Exception:
        logger.exception("Failed to read labels CSV: %s", str(labels_csv))
        return

    # If df is large, consider chunked processing instead of building full structures list
    logger.info("Labels CSV loaded, rows=%d", len(df))

    # Build structures list from df (this reads xyz files referenced by CSV)
    structures = reader_OrbNet_from_df(df, str(xyz_base))

    if not structures:
        logger.error("No structures were created from CSV; aborting.")
        return

    # Option A: Use load_data with custom reader to wrap the already-created structures
    # However original load_data expects to load from file; simplest is to pass 'structures' directly to insert_data.
    # But to keep similar style to your original pipeline, we'll call insert_data directly with configurations list.

    # Insert property definition(s)
    try:
        client.insert_property_definition('/home/ubuntu/notebooks/potential-energy.json')
    except Exception:
        logger.warning("Could not insert potential-energy.json; check path/permissions.")

    # Map properties: using kcal/mol as in original. Consider standardizing units across datasets (e.g., eV).
    property_map = {
        'potential-energy': [{
            'energy': {'field': 'energy', 'units': 'kcal/mol'},
            'per-atom': {'field': 'per-atom', 'units': None},
            '_metadata': {
                # 'software': {'value': 'ENTOS QCORE 0.8.17'},  # optional
                'method': {'value': 'DFT/ωB97X-D3/def2-TZVP'},
            }
        }],
    }

    # Transform function must return the (possibly modified) configuration
    def tform(c):
        if not hasattr(c, 'info') or c.info is None:
            c.info = {}
        # Indicate global energy rather than per-atom
        c.info['per-atom'] = False
        return c

    # Insert into DB
    try:
        # insert_data accepts a list of ASE Atoms (configurations) directly
        inserted_iter = client.insert_data(
            structures,
            property_map=property_map,
            generator=False,  # consider True for streaming large lists
            transform=tform,
            verbose=True
        )
        ids = list(inserted_iter)
    except Exception:
        logger.exception("Error during client.insert_data()")
        return

    if not ids:
        logger.error("No ids returned from insert_data(); abort.")
        return

    # ids expected to be iterable of (co_id, pr_id)
    try:
        co_ids, pr_ids = zip(*ids)
        all_co_ids = list(co_ids)
        all_pr_ids = list(pr_ids)
    except Exception:
        logger.exception("Unexpected return format from insert_data(); expected pairs")
        return

    logger.info("Inserted configurations: %d ; properties: %d", len(all_co_ids), len(all_pr_ids))

    # Insert dataset bundle describing OrbNet
    try:
        ds_id = client.insert_dataset(
            do_hashes=all_pr_ids,
            name='Orbnet',
            authors=['Anders S. Christensen', 'Sai Krishna Sirumalla', 'Zhuoran Qiao',
                     "Michael B. O’Connor", 'Daniel G. A. Smith', 'Feizhi Ding',
                     'Peter J. Bygrave', 'Animashree Anandkumar', 'Matthew Welborn',
                     'Frederick R. Manby', 'Thomas F. Miller III'],
            links=[
                'https://aip.scitation.org/doi/10.1063/5.0061990',
                'https://figshare.com/articles/dataset/OrbNet_Denali_Training_Data/14883867',
            ],
            description='All DFT single-point calculations for the OrbNet Denali '
                        'training set were carried out in ENTOS QCORE version 0.8.17 '
                        'at the ωB97X-D3/def2-TZVP level of theory using in-core '
                        'density fitting with the neese=4 DFT integration grid.',
            resync=True,
            verbose=True
        )
        logger.info("Inserted dataset id: %s", str(ds_id))
    except Exception:
        logger.exception("Failed to insert dataset 'Orbnet'.")


if __name__ == '__main__':
    main()

#############################################################################################
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_settings import PropertySettings
from colabfit.tools.configuration import AtomicConfiguration

import pandas as pd
from ase import Atoms
from ase.io import read
from ase.io.vasp import read_vasp
from ase.db import connect
from tqdm import tqdm
import numpy as np

#call database using its name
#drop_database=True means to start with fresh database
client = MongoDatabase('new_data_test_alexander', configuration_type=AtomicConfiguration, nprocs=4, drop_database=True)

# In[ ]:

def reader_OrbNet(p):
    df=pd.read_csv('/large_data/new_raw_datasets_2.0/OrbNet_Denali/denali_labels.csv',index_col=0)
    structures=[]
    #atoms=read(p,index=',')
    for row in tqdm(df.index):
        f='/large_data/new_raw_datasets_2.0/OrbNet_Denali/xyz_files/'+str(df.loc[row,'mol_id']+'/'+str(df.loc[row,'sample_id']+'.xyz'))
        structure=read(f)
        #structures_FM.append(structure_FM)
        #print(structure_FM)
        structure.info['energy']=df.loc[row,'dft_energy'].item()
        structure.info['xtb1_energy']=df.loc[row,'xtb1_energy'].item()
        structure.info['charge']=df.loc[row,'charge'].item()
        structures.append(structure)
        #print(type(structure_FM.info['energy_FM']))
        #file_AFM='/large_data/new_raw_datasets_2.0/Co_dimer/Co_dimer_data/structures_xyz/'+str(df.loc[row,'xyz_filename_AFM'])
        #structure_AFM=read(file_AFM)
        #structure_AFM.info['energy_AFM']=df.loc[row,'E-AFM(a.u.)'].item()
        #structures_AFM.append(structure_AFM)
        #print(structures_AFM)

    return structures


#Loads data, specify reader function if not "usual" file format
'''
configurations = load_data(
    file_path='/large_data/new_raw_datasets_2.0/Co_dimer/Co_dimer_data/',
    file_format='folder',
    name_field=None,
    elements=['Co', 'C', 'O', 'H', 'Cl', 'P', 'N','S'],
    default_name='Codimer',
    reader=reader_Codimer,
    glob_string='*.xyz',
    verbose=True,
    generator=False
)
'''
configurations = load_data(
    file_path='/large_data/new_raw_datasets_2.0/OrbNet_Denali/',
    file_format='folder',
    name_field=None,
    elements=None,
    default_name='OrbNet',
    reader=reader_OrbNet,
    glob_string='denali_labels.csv',
    verbose=True,
    generator=False
)


client.insert_property_definition('/home/ubuntu/notebooks/potential-energy.json')
#client.insert_property_definition('/home/ubuntu/notebooks/atomic-forces.json')
#client.insert_property_definition('/home/ubuntu/notebooks/cauchy-stress.json')

# property included electronic and dispersion energies, highest occupied molecular orbital (HOMO) and lowest unoccupied molecular orbital (LUMO) energies, HOMO/LUMO gap, dipole moment, and natural charge of the metal center; GFN2-xTB polarizabilities are also provided. Need to decide what to add in the property setting

property_map = {
    'potential-energy': [{
        'energy':   {'field': 'energy',  'units': 'kcal/mol'},
        'per-atom': {'field': 'per-atom', 'units': None},
# For metadata want: software, method (DFT-XC Functional), basis information, more generic parameters
        '_metadata': {
           # 'software': {'value':'ENTOS QCORE 0.8.17'},
            'method':{'value':'DFT/ωB97X-D3/def2-TZVP'},
        }
    }],
    
 }
 
def tform(c):
    c.info['per-atom'] = False

# In[ ]:

ids = list(client.insert_data(
    configurations,
    property_map=property_map,
    generator=False,
    transform=tform,
    verbose=True
))

all_co_ids, all_pr_ids = list(zip(*ids))


ds_id = client.insert_dataset(
    do_hashes=all_pr_ids,
    name='Orbnet',
    authors=['Anders S. Christensen', 'Sai Krishna Sirumalla', 'Zhuoran Qiao', 'Michael B. O’Connor', 'Daniel G. A. Smith', 'Feizhi Ding', 'Peter J. Bygrave', 'Animashree Anandkumar', 'Matthew Welborn', 'Frederick R. Manby', 'Thomas F. Miller III'],
    links=[
        'https://aip.scitation.org/doi/10.1063/5.0061990',
        'https://figshare.com/articles/dataset/OrbNet_Denali_Training_Data/14883867',
    ],
    description ='All DFT single-point calculations for the OrbNet Denali '\
    'training set were carried out in ENTOS QCORE version 0.8.17 '\
    'at the ωB97X-D3/def2-TZVP level of theory using in-core '\
    'density fitting with the neese=4 DFT integration grid.',
    resync=True,
    verbose=True,
)

