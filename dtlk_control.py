from  os import  listdir
from datetime import datetime

path_f4 = 'C:/Users/ext_maperezr/OneDrive - Falabella/General/DATA/F4'
path_f3 = 'C:/Users/ext_maperezr/OneDrive - Falabella/General/DATA/F3'
path_f11 = 'C:/Users/ext_maperezr/OneDrive - Falabella/General/DATA/F11'
path_f5 = 'C:/Users/ext_maperezr/OneDrive - Falabella/General/DATA/F5'

def get_last_file(path):
    list_archive = listdir(path)
    date_archive = [ datetime.strptime(name[0:6], '%y%m%d') for name in list_archive]
    last = max(date_archive)
    indice = list_archive[date_archive.index(last)]
    hoy =  datetime.now().strftime('%y%m%d')
    if last.strftime('%y%m%d') == hoy:
        return f'{path}/{indice}', True
    else:
        return f'{path}/{indice}', False


print(get_last_file(path_f4))
