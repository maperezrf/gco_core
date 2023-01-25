from  os import  listdir
from datetime import datetime
# from gcloud import GCLOUD
from datetime import datetime

class DTLKCONTROL():
    dt_string = datetime.now().strftime('%y%m%d')
    fs = ['F4', 'F5'] 
    pname = 'txd-ops-control-faco-dtlk'
    tables = ['f4_gco.f4_productos', 'f5_reports.f5_cf11']

    def __init__(self) -> None:
        print('control')
        # self.gcp = GCLOUD()
        self.gdlines = load_text_file()

    def update_files(self):
        for i, f in enumerate(self.fs):
            lasfile = get_last_file(f'{self.gdlines[0]}/{f}')
            if lasfile[1]:
                print(f'Archivo de {f} está actualizado!')
                self.gdlines[i+2] = lasfile[0]
            else:
                print(f'Actualizando {f} ...')
                query = f'SELECT * FROM {self.pname}.{self.tables[i]}'
                df = self.gcp.get_query(query)
                path = f'{self.gdlines[0]}/{f}/{self.dt_string}_{f}.csv'
                df.to_csv(path, index=False)
                print(f'-- Archivo de {f} guardado en {path}')
                self.gdlines[i+2] = path
        return self.gdlines[1:]

def load_text_file():
        config = open('config/get_data_config.txt', 'r', encoding='ISO-8859-1')
        gdlines = [line.strip() for line in config.readlines()]
        config.close()
        return gdlines

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

def init_commandline():
    dtlkcon = DTLKCONTROL()
    gdlines = dtlkcon.update_files()

if __name__=='__main__':
    init_commandline()
