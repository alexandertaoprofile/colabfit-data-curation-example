from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_settings import PropertySettings
from colabfit.tools.configuration import AtomicConfiguration
import ase
# call database using its name
# drop_database=True means to start with fresh database
client = MongoDatabase('new_data_test_alexander', configuration_type=AtomicConfiguration, nprocs=4, drop_database=True)


def reader(file_path):
    file_name=file_path.stem
    atom=ase.io.read(file_path)
    atom.info['name'] = file_name
    yield atom

# Loads data, specify reader function if not "usual" file format
configurations = load_data(
    file_path='/large_data/new_raw_datasets_2.0/nenci2021/nenci2021/xyzfiles/',
    file_format='folder',
    name_field='name',
    elements=['C','H','N','O','F','Cl','Br','S','P'],
    reader=reader,
    glob_string='*reformat.xyz',
    #default_name='nanci',
    verbose=True,
    generator=False
)

# In[ ]:

client.insert_property_definition('/home/ubuntu/notebooks/potential-energy.json')
#client.insert_property_definition('/home/ubuntu/notebooks/atomic-forces.json')
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
#still need to add more properties
property_map = {
        'potential-energy': [{
            'energy':   {'field': 'CCSD(T)/CBS',  'units': 'kcal/mol'},
            'per-atom': {'field': 'per-atom', 'units': None},
     #For metadata want: software, method (DFT-XC Functional), basis information, more generic parameters
            '_metadata': {
                #'software': {'value':'GPAW and VASP'},
                'method':{'value':'CCSD(T)/CBS'},
                #'ecut':{'value':'700 eV for GPAW, 900 eV for VASP'},
            }
        }],

#    'free-energy': [{
#        'energy': {'field': 'free_energy', 'units': 'eV'},
#        '_metadata': {
#            'software': {'value': 'GPAW and VASP'},
#            'method': {'value': 'DFT'},
#            'ecut':{'value':'700 eV for GPAW, 900 eV for VASP'},
#        }
#    }],


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
#     }],
#
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

#more cs_info to be updated
cs_info = [

    {"name":"Water",
    "description": "Configurations with water structure"},

    {"name": "MeOH",
    "description": "Configurations with MeOH structure"},
]
cs_ids = []

for i in cs_info:
    cs_id = client.query_and_insert_configuration_set(
        co_hashes=all_co_ids,
        query={'names':{'$regex':i['name']+'_*'}},
        name=i['name'],
        description=i['description']
    )

    cs_ids.append(cs_id)

ds_id = client.insert_dataset(
    cs_ids=cs_ids,
    do_hashes=all_pr_ids,
    name='NENCI-2021',
    authors=[
        'Zachary M. Sparrow','Brian G. Ernst','Paul T. Joo','Ka Un Lao' ,'Robert A. DiStasio, Jr.'
    ],
    links=[
        'https://pubs.aip.org/aip/jcp/article/155/18/184303/199609/NENCI-2021-I-A-large-benchmark-database-of-non',
    ],
    description ='A single file containing the Cartesian coordinates of the 7763 intermolecular complexes in NENCI-2021 '\
                 '(in xyz format) and a csv file containing all the CCSD(T)/CBS and SAPT energetic components (in kcal/mol) are provided',
    resync=True,
    verbose=True,
)
