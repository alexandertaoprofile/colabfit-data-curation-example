import logging
from pathlib import Path

import numpy as np
from tqdm import tqdm
from ase import Atoms

from colabfit.tools.database import MongoDatabase, load_data
# from colabfit.tools.property_settings import PropertySettings
from colabfit.tools.configuration import AtomicConfiguration

# ---- logging ----
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

def reader_ws22(p):
    """
    Reader for a single .npz/.np file (WS22 style). The function tries to be flexible:
    - p: path to a .npz or .np file containing arrays like Z, E, R, F, HL, DP
    - Expected array shapes (typical):
        Z: (n_configs, n_atoms) or (n_atoms,)      (atomic numbers)
        R: (n_configs, n_atoms, 3)                  (positions)
        E: (n_configs,)                             (energies)
        F: (n_configs, n_atoms, 3)                  (forces)
        HL: (n_configs,) or (n_configs, k)          (homo/lumo or scalar)
        DP: (n_configs,) or (n_configs, 3)          (dipole)
    Returns:
        list of ASE Atoms objects with .info and .arrays populated
    Notes:
        - This routine may consume significant memory for large n_configs.
        - If data files are huge, consider using generator=True in load_data and streaming.
    """
    atoms = []
    p = Path(p)
    if not p.exists():
        logger.error("File does not exist: %s", str(p))
        return atoms

    try:
        a = np.load(str(p), allow_pickle=True)
    except Exception:
        logger.exception("Failed to load numpy archive: %s", str(p))
        return atoms

    # try to read arrays safely
    # Accept multiple possible keys depending on how file was saved
    def _get_any(keys):
        for k in keys:
            if k in a:
                return a[k]
        return None

    z = _get_any(['Z', 'z', 'atomic_numbers', 'numbers'])
    r = _get_any(['R', 'r', 'positions'])
    e = _get_any(['E', 'e', 'energy', 'energies'])
    f = _get_any(['F', 'f', 'forces'])
    hl = _get_any(['HL', 'hl', 'homo_lumo', 'homolumo'])
    d = _get_any(['DP', 'dp', 'dipole'])

    # Basic checks
    if r is None:
        logger.error("Positions array not found in %s; keys: %s", str(p), list(a.keys()))
        return atoms
    # Determine number of configurations from the first dimension of r or e
    try:
        if hasattr(r, 'ndim') and r.ndim == 3:
            n_configs = r.shape[0]
        elif hasattr(r, 'ndim') and r.ndim == 2:
            # positions shape (n_atoms, 3) -> treat as single configuration
            n_configs = 1
            r = r.reshape((1, r.shape[0], r.shape[1]))
        else:
            # fallback: try E length
            n_configs = len(e) if e is not None else 0
    except Exception:
        logger.exception("Failed to determine number of configurations from positions/energies.")
        return atoms

    logger.info("Loaded np data from %s: detected %d configurations", str(p), n_configs)

    # Normalize shapes for z/f/hl/d/e so indexing works uniformly
    # If z has shape (n_atoms,) and n_configs>1, assume same atom types across frames
    if z is not None:
        if getattr(z, 'ndim', 0) == 1 and n_configs > 1:
            z_rep = np.repeat(z[np.newaxis, :], n_configs, axis=0)
        else:
            z_rep = z
    else:
        z_rep = None

    if f is not None and getattr(f, 'ndim', 2) == 2:
        # forces shape (n_atoms,3) -> treat as single config
        f = f.reshape((1,) + f.shape)

    # iterate configs
    for i in tqdm(range(n_configs), desc=f"processing {p.name}"):
        try:
            # determine numbers for this frame
            if z_rep is not None:
                if z_rep.ndim == 2:
                    numbers = z_rep[i] if i < z_rep.shape[0] else z_rep[0]
                else:
                    numbers = z_rep
            else:
                # cannot determine atomic numbers; try to infer from positions length later (not ideal)
                numbers = None

            # positions
            if r.ndim == 3:
                positions = r[i]
            elif r.ndim == 2:
                positions = r[0]
            else:
                logger.error("Unsupported positions ndim: %s", r.ndim)
                continue

            # build Atoms; if numbers is None, create Atoms with guessed numbers=ones -> user should fix later
            if numbers is None:
                logger.warning("Atomic numbers missing for frame %d in %s; using placeholder atomic numbers.", i, str(p))
                numbers_local = [1] * positions.shape[0]
            else:
                numbers_local = list(map(int, numbers))

            atom = Atoms(numbers=numbers_local, positions=positions)

            # energy
            if e is not None:
                try:
                    atom.info['energy'] = float(e[i]) if getattr(e, 'ndim', 0) >= 1 else float(e)
                except Exception:
                    atom.info['energy'] = e[i] if i < len(e) else e

            # forces
            if f is not None:
                try:
                    atom.arrays['forces'] = np.array(f[i], dtype=float)
                except Exception:
                    logger.warning("Forces for frame %d couldn't be assigned exactly; assigning what exists.", i)
                    atom.arrays['forces'] = np.array(f[i])

            # extra properties
            if hl is not None:
                try:
                    atom.info['homolumo'] = float(hl[i]) if getattr(hl, 'ndim', 0) >= 1 else float(hl)
                except Exception:
                    atom.info['homolumo'] = hl[i]

            if d is not None:
                try:
                    atom.info['dipole_moment'] = float(d[i]) if getattr(d, 'ndim', 0) >= 1 else float(d)
                except Exception:
                    atom.info['dipole_moment'] = d[i]

            # mark default per-atom flag (may be overridden in tform)
            if 'per-atom' not in atom.info:
                atom.info['per-atom'] = False

            atoms.append(atom)
        except Exception:
            logger.exception("Failed to process frame %d in file %s", i, str(p))
            continue

    logger.info("reader_ws22: produced %d ASE Atoms", len(atoms))
    return atoms


def main():
    # Initialize DB client (drop_database True resets DB)
    client = MongoDatabase('new_data_test_alexander', configuration_type=AtomicConfiguration, nprocs=4, drop_database=True)

    # ---- Load multiple WS22 subsets ----
    # We accumulate all configurations. For large totals, use generator streaming instead to avoid OOM.
    configurations = []

    base_path = '/large_data/new_raw_datasets_2.0/WS22_database'
    # for each dataset, we pass path to load_data with a glob_string that matches filenames (or directories)
    # Here we assume files are under the folder and reader will be called with a .npz path matching glob_string.

    datasets = [
        ('acrolein', 'acrolein'),
        ('nitrophenol', 'nitrophenol'),
        ('dmabn', 'dmabn'),
        ('sma', 'sma'),
        ('urea', 'urea'),
        ('thymine', 'thymine'),
        ('urocanic', 'urocanic'),
        ('alanine', 'alanine'),
        ('o-hbdi', 'o-hbdi'),
        ('toluene', 'toluene'),
    ]

    # Use loop to reduce copy-paste errors; keep same API signature as original
    for default_name, globstr in datasets:
        try:
            cfgs = load_data(
                file_path=base_path,
                file_format='folder',
                name_field=None,
                elements=['C', 'N', 'O', 'H'],
                default_name=f'ws22_{default_name}',
                reader=reader_ws22,
                glob_string=globstr,
                verbose=True,
                generator=False
            )
            # load_data may return a list or generator; coerce to list
            configurations += list(cfgs)
            logger.info("Loaded %d configurations for %s", len(cfgs), default_name)
        except Exception:
            logger.exception("Failed to load dataset %s (glob=%s)", default_name, globstr)
            continue

    logger.info("Total configurations collected: %d", len(configurations))

    # ---- insert property definitions ----
    try:
        client.insert_property_definition('/home/ubuntu/notebooks/potential-energy.json')
        client.insert_property_definition('/home/ubuntu/notebooks/atomic-forces.json')
    except Exception:
        logger.warning("Could not insert one or more property definition files; check paths.")

    property_map = {
        'potential-energy': [{
            'energy': {'field': 'energy', 'units': 'kcal/mol'},
            'per-atom': {'field': 'per-atom', 'units': None},
            '_metadata': {
                'software': {'value': 'ORCA 4.0.1'},
                'method': {'value': 'PBE0/6-311G*'},
            }
        }],
        'atomic-forces': [{
            'forces': {'field': 'forces', 'units': 'kcal/mol/A'},
            '_metadata': {
                'software': {'value': 'ORCA 4.0.1'},
                'method': {'value': 'PBE0/6-311G*'},
            }
        }],
    }

    # transform: ensure per-atom flag exists, return c
    def tform(c):
        if not hasattr(c, 'info') or c.info is None:
            c.info = {}
        c.info['per-atom'] = False
        return c

    # ---- Insert data ----
    try:
        inserted_iter = client.insert_data(
            configurations,
            property_map=property_map,
            generator=False,   # consider True for streaming/batching in production
            transform=tform,
            verbose=True
        )
        ids = list(inserted_iter)
    except Exception:
        logger.exception("Error occurred during client.insert_data()")
        return

    if not ids:
        logger.error("insert_data returned empty result; aborting.")
        return

    # unzip into lists
    try:
        co_ids, pr_ids = zip(*ids)
        all_co_ids = list(co_ids)
        all_pr_ids = list(pr_ids)
    except Exception:
        logger.exception("Unexpected format returned from insert_data(); expected iterable of (co_id, pr_id) pairs.")
        return

    logger.info("Inserted %d configurations and %d properties", len(all_co_ids), len(all_pr_ids))

    # ---- configuration sets creation ----
    cs_info = [
        {"name": "acrolein", "description": "Configurations with acrolein structure"},
        {"name": "nitrophenol", "description": "Configurations with nitrophenol structure"},
        {"name": "dmabn", "description": "Configurations with dmabn structure"},
        {"name": "sma", "description": "Configurations with sma structure"},
        {"name": "urea", "description": "Configurations with urea structure"},
        {"name": "thymine", "description": "Configurations with thymine structure"},
        {"name": "urocanic", "description": "Configurations with urocanic structure"},
        {"name": "alanine", "description": "Configurations with alanine structure"},
        {"name": "o-hbdi", "description": "Configurations with o-hbdi structure"},
        {"name": "toluene", "description": "Configurations with toluene structure"},
    ]

    cs_ids = []
    for entry in cs_info:
        name = entry['name']
        desc = entry.get('description', '')
        regex = f'^{name}'
        try:
            cs_id = client.query_and_insert_configuration_set(
                co_hashes=all_co_ids,
                query={'names': {'$regex': regex}},
                name=name,
                description=desc
            )
            cs_ids.append(cs_id)
            logger.info("Inserted/queried configuration set %s -> %s", name, str(cs_id))
        except Exception:
            logger.exception("Failed to create/query configuration set for %s", name)
            continue

    if not cs_ids:
        logger.error("No configuration sets were created; abort dataset insertion.")
        return

    # ---- Insert dataset bundle ----
    try:
        ds_id = client.insert_dataset(
            cs_ids=cs_ids,
            do_hashes=all_pr_ids,
            name='WS22',
            authors=['Pinheiro Jr, M.', 'Zhang, S.', 'Dral, P. O.', 'Barbatti, M.'],
            links=[
                'https://www.nature.com/articles/s41597-023-01998-3#code-availability',
                'https://zenodo.org/record/7032334#.ZEDJes7MJEY',
            ],
            description='The WS22 database combines Wigner sampling with geometry interpolation to generate 1.18 '
                        'million molecular geometries equally distributed into 10 independent datasets of flexible '
                        'organic molecules with varying sizes and chemical complexity. In addition to the potential '
                        'energy and forces required to construct potential energy surfaces, the WS22 database provides '
                        'several other quantum chemical properties.',
            resync=True,
            verbose=True,
        )
        logger.info("Dataset inserted with id %s", str(ds_id))
    except Exception:
        logger.exception("Failed to insert dataset 'WS22'.")


if __name__ == '__main__':
    main()

#############################################################
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_settings import PropertySettings
from colabfit.tools.configuration import AtomicConfiguration


from tqdm import tqdm
import numpy as np
from ase import Atoms

#call database using its name
#drop_database=True means to start with fresh database
client = MongoDatabase('new_data_test_alexander', configuration_type=AtomicConfiguration, nprocs=4, drop_database=True)

#multiple properties
# In[ ]:
def reader_ws22(p):
    atoms=[]
    a=np.load(p)
    #na=a['N']
    z=a['Z']
    e=a['E']
    r=a['R']
    f=a['F']
    hl=a['HL']
    d=a['DP']
    #q=a['nuclear_charges']
    #for i in tqdm(range(len(na))):  #need to change it
    for i in tqdm(1200):
        #n=na[i]
        #atom = Atoms(numbers=z[i, :], positions=r[i, :n, :])
        atom=Atoms(numbers=z,positions=r[i])
        #atom.info['energy']=e[i]
        atom.info['energy']=float(e[i])
        atom.arrays['forces']=f[i]
        atom.info['dipole_moment']=d[i]
        atom.info['homolumo']=hl[i]
        #atom.info['charge']=float(q[i])
        #print(atom.info['charge'])
        atoms.append(atom)
        #print(type (atom.info['charge']))
    return atoms
    
 #Loads data, specify reader function if not "usual" file format
configurations = load_data(
    file_path='/large_data/new_raw_datasets_2.0/WS22_database',
    file_format='folder',
    name_field=None,
    elements=['C','N','O','H'],
    default_name='ws22_acrolein',
    reader=reader_ws22,
    glob_string='acrolein',
    verbose=True,
    generator=False
)

configurations += load_data(
    file_path='/large_data/new_raw_datasets_2.0/WS22_database',
    file_format='folder',
    name_field=None,
    elements=['C','N','O','H'],
    default_name='ws22_nitrophenol',
    reader=reader_ws22,
    glob_string='nitrophenol',
    verbose=True,
    generator=False
)

configurations += load_data(
    file_path='/large_data/new_raw_datasets_2.0/WS22_database',
    file_format='folder',
    name_field=None,
    elements=['C','N','O','H'],
    default_name='ws22_dmabn',
    reader=reader_ws22,
    glob_string='dmabn',
    verbose=True,
    generator=False
)

configurations += load_data(
    file_path='/large_data/new_raw_datasets_2.0/WS22_database',
    file_format='folder',
    name_field=None,
    elements=['C','N','O','H'],
    default_name='ws22_sma',
    reader=reader_ws22,
    glob_string='sma',
    verbose=True,
    generator=False
)

configurations += load_data(
    file_path='/large_data/new_raw_datasets_2.0/WS22_database',
    file_format='folder',
    name_field=None,
    elements=['C','N','O','H'],
    default_name='ws22_urea',
    reader=reader_ws22,
    glob_string='urea',
    verbose=True,
    generator=False
)

configurations += load_data(
    file_path='/large_data/new_raw_datasets_2.0/WS22_database',
    file_format='folder',
    name_field=None,
    elements=['C','N','O','H'],
    default_name='ws22_thymine',
    reader=reader_ws22,
    glob_string='thymine',
    verbose=True,
    generator=False
)

configurations += load_data(
    file_path='/large_data/new_raw_datasets_2.0/WS22_database',
    file_format='folder',
    name_field=None,
    elements=['C','N','O','H'],
    default_name='ws22_urocanic',
    reader=reader_ws22,
    glob_string='urocanic',
    verbose=True,
    generator=False
)

configurations += load_data(
    file_path='/large_data/new_raw_datasets_2.0/WS22_database',
    file_format='folder',
    name_field=None,
    elements=['C','N','O','H'],
    default_name='ws22_alanine',
    reader=reader_ws22,
    glob_string='alanine',
    verbose=True,
    generator=False
)

configurations += load_data(
    file_path='/large_data/new_raw_datasets_2.0/WS22_database',
    file_format='folder',
    name_field=None,
    elements=['C','N','O','H'],
    default_name='ws22_o-hbdi',
    reader=reader_ws22,
    glob_string='o-hbdi',
    verbose=True,
    generator=False
)

configurations += load_data(
    file_path='/large_data/new_raw_datasets_2.0/WS22_database',
    file_format='folder',
    name_field=None,
    elements=['C','N','O','H'],
    default_name='ws22_toluene',
    reader=reader_ws22,
    glob_string='toluene',
    verbose=True,
    generator=False
)



client.insert_property_definition('/home/ubuntu/notebooks/potential-energy.json')
client.insert_property_definition('/home/ubuntu/notebooks/atomic-forces.json')
#client.insert_property_definition('/home/ubuntu/notebooks/cauchy-stress.json')


# In[ ]:


property_map = {

    'potential-energy': [{
        'energy':   {'field': 'energy',  'units': 'kcal/mol'},
        'per-atom': {'field': 'per-atom', 'units': None},
# For metadata want: software, method (DFT-XC Functional), basis information, more generic parameters
        '_metadata': {
            'software': {'value':'ORCA 4.0.1'},
            'method':{'value':'PBE0/6-311G*'},
        }
    }],

    'atomic-forces': [{
        'forces':   {'field': 'forces',  'units': 'kcal/mol/A'},
        '_metadata': {
            'software': {'value':'ORCA 4.0.1'},
            'method':{'value':'PBE0/6-311G*'},

        }

    }],

#    'cauchy-stress': [{
#    'stress':   {'field': 'virial',  'units': 'GPa'},

#                '_metadata': {
#            'software': {'value':'VASP'},
#        }

#    }],

    }

# In[ ]:

def tform(c):
    c.info['per-atom'] = False


# In[ ]:


ids = list(client.insert_data(
    configurations,
    property_map=property_map,
    #generator=False,
    transform=tform,
    verbose=True
))

#all_co_ids, all_pr_ids = list(zip(*ids))
all_cos, all_dos = list(zip(*ids))


cs_info = [

    {"name":"acrolein",
    "description": "Configurations with acrolein structure)"},

    {"name": "nitrophenol",
    "description": "Configurations with nitrophenol structure"},

    {"name": "dmabn",
    "description": "Configurations with dmabn structure"},

    {"name": "sma",
    "description": "Configurations with sma structure"},

    {"name": "urea",
    "description": "Configurations with urea structure"},

    {"name":"thymine",
    "description": "Configurations with thymine structure"},

    {"name": "urocanic",
    "description": "Configurations with urocanic structure"},

    {"name": "alanine",
    "description": "Configurations with alanine structure"},

    {"name": "o-hbdi",
    "description": "Configurations with o-hbdi structure"},

    {"name": "toluene",
    "description": "Configurations with toluene structure"},
]

cs_ids = []


for i in cs_info:
    cs_id = client.query_and_insert_configuration_set(
        co_hashes=all_cos,
        query={'names':{'$regex':i['name']+'_*'}},
        name=i['name'],
        description=i['description']
    )

    cs_ids.append(cs_id)



# In[ ]:


ds_id = client.insert_dataset(
    #cs_ids=cs_ids,
    do_hashes=all_dos,
    name='WS22',
    authors=['Pinheiro Jr', 'M., Zhang', 'S., Dral', 'P. O.','Barbatti, M.'],
    links=[
        'https://www.nature.com/articles/s41597-023-01998-3#code-availability',
        'https://zenodo.org/record/7032334#.ZEDJes7MJEY',
    ],
    description='The WS22 database combines Wigner sampling with geometry interpolation to generate 1.18 '\
    'million molecular geometries equally distributed into 10 independent datasets of flexible '\
    'organic molecules with varying sizes and chemical complexity. '\
    'In addition to the potential energy and forces required to construct potential energy surfaces, the WS22 '\
    'database provides several other quantum chemical properties, all obtained via single-point calculations '\
    'for each molecular geometry. All quantum chemical calculations were performed with the Gaussian09 program.',

    resync=True,
    verbose=True,

)
