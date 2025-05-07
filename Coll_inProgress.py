from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_settings import PropertySettings
from colabfit.tools.configuration import AtomicConfiguration

# call database using its name
# drop_database=True means to start with fresh database
client = MongoDatabase('new_data_test_alexander', configuration_type=AtomicConfiguration, nprocs=4, drop_database=True)


# Loads data, specify reader function if not "usual" file format
configurations = load_data(
    file_path='/large_data/new_raw_datasets_2.0/Coll/Coll_test.xyz',
    file_format='xyz',
    name_field='config_type',
    elements=['Si', 'O','C','H'],
    default_name='Coll_test',
    verbose=True,
    generator=False
)

configurations += load_data(
    file_path='/large_data/new_raw_datasets_2.0/Coll/Coll_train.xyz',
    file_format='xyz',
    name_field='config_type',
    elements=['Si', 'O','C','H'],
    default_name='Coll_train',
    verbose=True,
    generator=False
)

configurations += load_data(
    file_path='/large_data/new_raw_datasets_2.0/Coll/Coll_validation.xyz',
    file_format='xyz',
    name_field='config_type',
    elements=['Si', 'O','C','H'],
    default_name='Coll_validation',
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
#client.insert_property_definition('/home/ubuntu/notebooks/atomic-forces.json')
#client.insert_property_definition('/home/ubuntu/notebooks/cauchy-stress.json')

atomization_property_definition = {
    'property-id': 'atomization-energy',
    'property-name': 'atomization-energy',
    'property-title': 'the extra energy needed to break up a molecule into separate atoms',
    'property-description': 'the extra energy needed to break up a molecule into separate atoms',
    'energy': {'type': 'float', 'has-unit': True, 'extent': [], 'required': True,
               'description': 'enthalpy of formation'}}

client.insert_property_definition(atomization_property_definition)


property_map = {
    'potential-energy': [{
        'energy':   {'field': 'energy',  'units': 'eV'},
        'per-atom': {'field': 'per-atom', 'units': None},
    #For metadata want: software, method (DFT-XC Functional), basis information, more generic parameters
        '_metadata': {
            'software': {'value':'GPAW and VASP'},
            'method':{'value':'DFT'},
            'ecut':{'value':'700 eV for GPAW, 900 eV for VASP'},
           }
       }],

    'atomization-energy': [{
        'energy': {'field': 'atomization_energy', 'units': 'eV'},
        '_metadata': {
            'software': {'value': 'GPAW and VASP'},
            'method': {'value': 'DFT'},
            'ecut':{'value':'700 eV for GPAW, 900 eV for VASP'},
        }
    }],


   # 'atomic-forces': [{
   #     'forces':   {'field': 'forces',  'units': 'eV/Ang'},
   #         '_metadata': {
   #         'software': {'value':'VASP'},
   #     }
   # }],

    # 'cauchy-stress': [{
    #     'stress':   {'field': 'virials',  'units': 'GPa'}, #need to check unit for stress
    #
    #     '_metadata': {
    #         'software': {'value':'GPAW and VASP'},
    #         'method':{'value':'DFT'},
    #         'ecut':{'value':'700 eV for GPAW, 900 eV for VASP'},
    #     }

    # }],

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
'''

cs_info = [
    {"name":"Coll_test",
     "description": "test sets of Coll"},

    {"name": "Coll_train",
     "description": "training sets of Coll"},

    {"name": "Coll_validation",
     "description": "validation sets of Coll"},
]

cs_id = client.query_and_insert_configuration_set(
    co_hashes=all_co_ids,
    #query={'names':cs_info['name']}, # find all COs with name=="Graphene"
    query={'names':{'$regex':i['name']+'_*'}},
    name=cs_info['name'],
    description=cs_info['description']
)


'''
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
    cs_ids=cs_ids,
    do_hashes=all_pr_ids,
    name='Coll',
    authors=[
        'Johannes Gasteiger', 'Florian Becker', 'Stephan Günnemann'
    ],
    links=[
        'https://openreview.net/forum?id=HS_sOaxS9K-',
        'https://figshare.com/articles/dataset/COLL_Dataset_v1_2/13289165',
    ],
    description='Consists of configurations taken from molecular collisions of different small organic '\
                'molecules. Energies and forces for 140 000 random snapshots taken from these trajectories '\
                'were recomputed with density functional theory (DFT). These calculations were performed with '\
                'the revPBE functional and def2-TZVP basis, including D3 dispersion corrections',
    resync=True,
    verbose=True,
)
