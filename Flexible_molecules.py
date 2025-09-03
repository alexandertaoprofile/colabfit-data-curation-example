
import logging
from colabfit.tools.database import MongoDatabase, load_data
# PropertySettings not used, keep import only if needed later
# from colabfit.tools.property_settings import PropertySettings
from colabfit.tools.configuration import AtomicConfiguration

# ---- logging ----
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def main():
    # Initialize DB client (drop_database=True starts fresh)
    client = MongoDatabase('new_data_test_alexander',
                           configuration_type=AtomicConfiguration,
                           nprocs=4,
                           drop_database=True)

    # ---- Load configurations ----
    configurations = []

    try:
        configurations += load_data(
            file_path='/large_data/new_raw_datasets_2.0/flexible_molecules/Datasets/Datasets/Azobenzene_inversion_reformat.xyz',
            file_format='xyz',
            name_field=None,
            elements=['C', 'N', 'H', 'O'],
            default_name='Azobenzene_inversion',
            verbose=True,
            generator=False
        )

        configurations += load_data(
            file_path='/large_data/new_raw_datasets_2.0/flexible_molecules/Datasets/Datasets/Azobenzene_rotation_and_inversion_reformat.xyz',
            file_format='xyz',
            name_field=None,
            elements=['C', 'N', 'H', 'O'],
            default_name='Azobenzene_rotation_and_inversion',
            verbose=True,
            generator=False
        )

        configurations += load_data(
            file_path='/large_data/new_raw_datasets_2.0/flexible_molecules/Datasets/Datasets/Azobenzene_rotation_reformat.xyz',
            file_format='xyz',
            name_field=None,
            elements=['C', 'N', 'H', 'O'],
            default_name='Azobenzene_rotation',
            verbose=True,
            generator=False
        )

        configurations += load_data(
            file_path='/large_data/new_raw_datasets_2.0/flexible_molecules/Datasets/Datasets/Glycine_reformat.xyz',
            file_format='xyz',
            name_field=None,
            elements=['C', 'N', 'H', 'O'],
            default_name='Glycine',
            verbose=True,
            generator=False
        )
    except Exception:
        logger.exception("Failed to load one or more data files via load_data()")
        raise

    logger.info("Total configurations loaded: %d", len(configurations))

    # ---- Insert property definition(s) ----
    try:
        client.insert_property_definition('/home/ubuntu/notebooks/potential-energy.json')
        # client.insert_property_definition('/home/ubuntu/notebooks/atomic-forces.json')
    except Exception:
        logger.warning("Could not insert potential-energy.json; verify file exists and is valid.")

    # ---- property_map (map source fields to ColabFit properties) ----
    # NOTE: units 'Kcal/Mol' kept from original, standardize to lowercase or consistent units if necessary.
    property_map = {
        'potential-energy': [{
            'energy': {'field': 'energy', 'units': 'Kcal/Mol'},
            'per-atom': {'field': 'per-atom', 'units': None},
            '_metadata': {
                'software': {'value': 'FHI-aims'},
                'method': {'value': 'DFT-PBE'},
            }
        }],
        # add forces/stress mapping if present in source
    }

    # ---- transform function ----
    def tform(c):
        """
        Ensure configuration.info exists and set per-atom flag.
        Must return the (possibly modified) configuration.
        """
        if not hasattr(c, 'info') or c.info is None:
            c.info = {}
        c.info['per-atom'] = False
        return c

    # ---- Insert data into DB ----
    try:
        inserted_iter = client.insert_data(
            configurations,
            property_map=property_map,
            generator=False,
            transform=tform,
            verbose=True
        )
        # Convert to list to consume iterator (insert_data may return generator)
        ids = list(inserted_iter)
    except Exception:
        logger.exception("Error during insert_data()")
        raise

    if not ids:
        logger.error("No ids returned from insert_data(); aborting further steps.")
        return

    # ids should be an iterable of (co_id, pr_id) pairs; robustly unzip
    try:
        co_ids, pr_ids = zip(*ids)
        all_co_ids = list(co_ids)
        all_pr_ids = list(pr_ids)
    except Exception:
        logger.exception("Unexpected format returned from insert_data(); expected iterable of (co_id, pr_id) pairs.")
        raise

    logger.info("Inserted %d configurations and %d properties", len(all_co_ids), len(all_pr_ids))

    # ---- Define configuration sets info ----
    cs_info = [
        {"name": "Azobenzene_inversion", "description": "Configurations with Azobenzene inversion structure"},
        {"name": "Azobenzene_rotation_and_inversion", "description": "Configurations with Azobenzene rotation and inversion structure"},
        {"name": "Azobenzene_rotation", "description": "Configurations with Azobenzene rotation structure"},
        {"name": "Glycine", "description": "Configurations with Glycine structure"},
    ]

    # Insert each configuration set by querying for names that match. Collect cs_ids.
    cs_ids = []
    for entry in cs_info:
        name = entry['name']
        desc = entry.get('description', '')
        # Use anchored regex to match names that start with this token (tweak as needed)
        regex = f'^{name}'
        try:
            cs_id = client.query_and_insert_configuration_set(
                co_hashes=all_co_ids,
                query={'names': {'$regex': regex}},
                name=name,
                description=desc
            )
            logger.info("Config set '%s' -> id: %s", name, str(cs_id))
            cs_ids.append(cs_id)
        except Exception:
            logger.exception("Failed to create/query configuration set for %s", name)
            # continue to next entry

    if not cs_ids:
        logger.error("No configuration sets created; aborting dataset insert.")
        return

    # ---- Insert dataset ----
    try:
        ds_id = client.insert_dataset(
            cs_ids=cs_ids,
            do_hashes=all_pr_ids,
            name='flexible_molecules_JCP2021',
            authors=[
                'Valentin Vassilev-Galindo', 'Gregory Fonseca', 'Igor Poltavsky', 'Alexandre Tkatchenko'
            ],
            links=[
                'https://pubs.aip.org/aip/jcp/article/154/9/094119/313847/Challenges-for-machine-learning-force-fields-in',
            ],
            description='All calculations were performed in FHI-aims software using the Perdew–Burke–Ernzerhof (PBE) '
                        'exchange–correlation functional with tight settings and the Tkatchenko–Scheffler (TS) method to '
                        'account for van der Waals (vdW) interactions.',
            resync=True,
            verbose=True,
        )
        logger.info("Inserted dataset id: %s", str(ds_id))
    except Exception:
        logger.exception("Failed to insert dataset 'flexible_molecules_JCP2021'.")


if __name__ == '__main__':
    main()

#####################################################################
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_settings import PropertySettings
from colabfit.tools.configuration import AtomicConfiguration

# call database using its name
# drop_database=True means to start with fresh database
client = MongoDatabase('new_data_test_alexander', configuration_type=AtomicConfiguration, nprocs=4, drop_database=True)


# Loads data, specify reader function if not "usual" file format


configurations = load_data(
    file_path='/large_data/new_raw_datasets_2.0/flexible_molecules/Datasets/Datasets/Azobenzene_inversion_reformat.xyz',
    file_format='xyz',
    name_field=None,
    elements=['C', 'N', 'H', 'O'],
    default_name='Azobenzene_inversion',
    verbose=True,
    generator=False
)

configurations = load_data(
    file_path='/large_data/new_raw_datasets_2.0/flexible_molecules/Datasets/Datasets/Azobenzene_rotation_and_inversion_reformat.xyz',
    file_format='xyz',
    name_field=None,
    elements=['C', 'N', 'H', 'O'],
    default_name='Azobenzene_rotation_and_inversion',
    verbose=True,
    generator=False
)

configurations = load_data(
    file_path='/large_data/new_raw_datasets_2.0/flexible_molecules/Datasets/Datasets/Azobenzene_rotation_reformat.xyz',
    file_format='xyz',
    name_field=None,
    elements=['C', 'N', 'H', 'O'],
    default_name='Azobenzene_rotation',
    verbose=True,
    generator=False
)

configurations = load_data(
    file_path='/large_data/new_raw_datasets_2.0/flexible_molecules/Datasets/Datasets/Glycine_reformat.xyz',
    file_format='xyz',
    name_field=None,
    elements=['C', 'N', 'H', 'O'],
    default_name='Glycine',
    verbose=True,
    generator=False
)


'''
cs_list = set()
for c in configurations:
    cs_list.add(*c.info['_name'])
print(cs_list)
'''
# In[ ]:

client.insert_property_definition('/home/ubuntu/notebooks/potential-energy.json')
# client.insert_property_definition('/home/ubuntu/notebooks/atomic-forces.json')
#client.insert_property_definition('/home/ubuntu/notebooks/cauchy-stress.json')
'''
free_property_definition = {
    'property-id': 'free-energy',
    'property-name': 'free-energy',
    'property-title': 'molecular reference energy',
    'property-description': 'enthalpy of formation',
    'energy': {'type': 'float', 'has-unit': True, 'extent': [], 'required': True,
               'description': 'enthalpy of formation'}}

client.insert_property_definition(free_property_definition)
'''

property_map = {
        'potential-energy': [{
            'energy':   {'field': 'energy',  'units': 'Kcal/Mol'},
            'per-atom': {'field': 'per-atom', 'units': None},
     #For metadata want: software, method (DFT-XC Functional), basis information, more generic parameters
            '_metadata': {
                'software': {'value':'FHI-aims'},
                'method':{'value':'DFT-PBE'},
                #'ecut':{'value':'700 eV for GPAW, 900 eV for VASP'},
            }
        }],

#    'atomic-forces': [{
#        'forces':   {'field': 'forces',  'units': 'eV/Ang'},
#            '_metadata': {
#            'software': {'value':'VASP'},
#        }
#    }],

#     'cauchy-stress': [{
#         'stress':   {'field': 'virials',  'units': 'GPa'}, #need to check unit for stress
#
#         '_metadata': {
#             'software': {'value':'GPAW and VASP'},
#             'method':{'value':'DFT'},
#             'ecut':{'value':'700 eV for GPAW, 900 eV for VASP'},
#         }
#
#}],

}

def tform(c):
    c.info['per-atom'] = False

ids = list(client.insert_data(
    configurations,
    property_map=property_map,
    generator=False,
    transform=tform,
    verbose=True
))

all_cos, all_dos = list(zip(*ids))

cs_info = [

    {"name":"Azobenzene_inversion",
    "description": "Configurations with Azobenzene inversion structure"},

    {"name": "Azobenzene_rotation_and_inversiont",
    "description": "Configurations with Azobenzene rotation and inversion structure"},

    {"name": "Azobenzene_rotation",
    "description": "Configurations with Azobenzene rotation structure"},

    {"name": "Glycine",
    "description": "Configurations with Glycine structure"},

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

ds_id = client.insert_dataset(
    cs_ids=cs_ids,
    do_hashes=all_pr_ids,
    name='flexible_molecules_JCP2021',
    authors=[
        'Valentin Vassilev-Galindo', 'Gregory Fonseca', 'Igor Poltavsky', 'Alexandre Tkatchenko'
    ],
    links=[
        'https://pubs.aip.org/aip/jcp/article/154/9/094119/313847/Challenges-for-machine-learning-force-fields-in',
    ],
    description='All calculations were performed in FHI-aims software using the Perdew–Burke–Ernzerhof (PBE) '\
                'exchange–correlation functional with tight settings and the Tkatchenko–Scheffler (TS) method to '\
                'account for van der Waals (vdW) interactions.',
    resync=True,
    verbose=True,
)
