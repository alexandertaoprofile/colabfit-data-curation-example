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
