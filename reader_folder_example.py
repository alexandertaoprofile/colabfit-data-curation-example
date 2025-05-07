from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_settings import PropertySettings
from colabfit.tools.configuration import AtomicConfiguration
import ase
# call database using its name
# drop_database=True means to start with fresh database
client = MongoDatabase('new_data_test_alexander', configuration_type=AtomicConfiguration, nprocs=4, drop_database=True)


# Loads data, specify reader function if not "usual" file format

def reader(file_path):
    file_name=file_path.stem
    atom=ase.io.read(file_path)
    atom.info['name'] = file_name
    yield atom

configurations = load_data(
    file_path='/large_data/new_raw_datasets_2.0/flexible_molecules/Datasets/Datasets',
    file_format='folder',
    name_field='name',
    elements=['C', 'N', 'H', 'O'],
    reader=reader,
    glob_string='*.xyz',
    default_name='flexiblemolecules',
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

all_co_ids, all_pr_ids = list(zip(*ids))
'''
#matches to data CO "name" field
cs_regexes = {
    '.*':
        'Silica datasets. For DFT computations, the GPAW (in combination with ASE) and VASP codes employing '\
        'the projector augmented-wave method were used. Early versions of the GAP were based '\
        'on reference data computed using the PBEsol functional. For GPAW, an energy cut-off '\
        'of 700 eV and a k-spacing of 0.279 Å−1 were used, for VASP, a higher energy cut-off '\
        'of 900 eV and a denser k-spacing of 0.23 Å−1 were used.',
        }
cs_names=['all']
for i in cs_list:
    cs_regexes[i]='Configurations with the %s structure.' %i
    cs_names.append(i)
#print (cs_regexes)


cs_ids = []


for i, (regex, desc) in enumerate(cs_regexes.items()):
    co_ids = client.get_data(
        'configurations',
        fields='hash',
        query={'hash': {'$in': all_co_ids}, 'names': {'$regex': regex}},
        ravel=True
    ).tolist()

    print(f'Configuration set {i}', f'({regex}):'.rjust(22), f'{len(co_ids)}'.rjust(7))

    cs_id = client.insert_configuration_set(co_ids, description=desc,name=cs_names[i])

    cs_ids.append(cs_id)
'''

ds_id = client.insert_dataset(
    #cs_ids=cs_ids,
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



cs_info = [

    {"name":"Ac-Ala3-NHMe",
    "description": "Configurations with Ac-Ala3-NHMe structure(C12H22N4O4)"},

    {"name": "double-walled_nanotube",
    "description": "Configurations with double walled nanotube structure(C326H44)"},

    {"name": "DHA",
    "description": "Configurations with DHA Docosahexaenoic acid structure(C22H32O2)"},

    {"name": "stachyose",
    "description": "Configurations with stachyose structure (C24H42O21)"},

    {"name": "AT-AT",
    "description": "Configurations with DNA base pair AT-AT structure (C20H22N14O4)"},

    {"name":"AT-AT-CG-CG",
    "description": "Configurations with DNA base pair AT-AT-CG-CG structure (C38H42N30O8)"},

    {"name": "buckyball-catcher",
    "description": "Configurations with buckyball-catcher structure(C120H28)"},
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
    do_hashes=all_dos,
    cs_ids=cs_ids,
    #pr_hashes=all_pr_ids,
    name='MD22',
    authors=[
        'Stefan Chmiela', 'Valentin Vassilev-Galindo', 'Oliver T. Unke', 'Adil Kabylda', 'Huziel E. Sauceda', 'Alexandre Tkatchenko', 'Klaus-Robert Müller'],
    links=[
        'https://www.science.org/doi/10.1126/sciadv.adf0873',
        'http://sgdml.org/',
    ],
    description =  'MD22 includes examples of four major '\
    'classes of biomolecules and supramolecules, ranging from '\
    'a small peptide with 42 atoms, all the way up to a double-walled '\
    'nanotube with 370 atoms. The trajectories were sampled at temperatures '\
    'between 400 K and 500 K at a resolution of 1 fs. All calculation were '\
    'performed using the FHI-aims electronic structure software, in '\
    'combination with i-PI for the MD simulations. The potential energy and '\
    'atomic force labels were calculated at the PBE+MBD level of theory. The '\
    'keywords light and tight denote different basis set options in FHI-aims.',
    resync=True,
    verbose=True,
)