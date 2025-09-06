import pandas as pd

a = [
    {'coluna_um': 'coluna_um', 'coluna_dois': 'coluna_dois'},
    {'coluna_um': 'coluna_um', 'coluna_dois': 'coluna_dois'}
]

dataframe = pd.DataFrame(a, columns=['coluna_um', 'coluna_dois'])

print([(row['coluna_um'], row['coluna_dois']) for _, row in dataframe.iterrows()])
