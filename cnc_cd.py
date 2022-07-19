from os import name
from numpy import multiply
import pandas as pd
from datetime import datetime
from ica_core.ica_nc import CierresNC
from etl_core.etl import ETL

cts = ['9913','9917','9919','9920','9918','9912','9914', '9902']
preventas = ['9904','9905','9908','9909','9915','9916']
tiendas = ['139','141','142','143','5','37','35','43','53','36','38','98','138','45','72','183','25',
'123','19','50','85','101','321','323','324','108','6','18','82','93','322','13','56','96','131','60']

class CNC_CD():

    def __init__(self, names, fcols, pcols) -> None:
        input_folder = 'input/cierres_nc/'
        self.names = names
        self.fcols = fcols
        self.pcols = pcols
        self.etl = ETL(input_folder)

    def run_test(self):
        self.data = self.etl.load_data(self.names) # Load files 
        self.set_index() # Set index 
        self.convert_dtypes() # Convert data types 
        self.set_dates() # Set date columns 
        self.cnc = CierresNC(self.data[5])
        self.cnc.set_fcols(self.fcols, self.pcols)
        #self.cnc.starting([self.pcols]) # TODO revisar duplicados en data 
        self.test_call()
        self.cnc.finals()
        self.data[5] = self.cnc.ica.get_db()
        self.print_results()
        self.save_selection()

    def set_index(self) -> None:
        self.data[5] = self.data[5].reset_index()
        self.data[5].rename(columns={'index': self.pcols[0]}, inplace=True)

    def convert_dtypes(self): # TODO unificar con cf11_cd dado que son iguales 
        # TODO boost performance 
        # Convertir columnas a número 
        self.data[0].loc[:,'cantidad'] = pd.to_numeric(self.data[0].loc[:,'cantidad'])
        self.data[1].loc[:,'qf04_ship'] = pd.to_numeric(self.data[1].loc[:,'qf04_ship'])
        self.data[1].loc[:,'fecha_registro'] = pd.to_datetime(self.data[1].loc[:,'fecha_registro'].str.replace('UTC', ''))
        self.data[2].loc[:,'trf_rec_to_date'] = pd.to_numeric(self.data[2].loc[:,'trf_rec_to_date'])
        self.data[5].loc[:,[self.pcols[4],self.pcols[3]]] = self.data[5][[self.pcols[4],self.pcols[3]]].apply(pd.to_numeric)

    def set_dates(self): # TODO unificar con cf11_cd dado que son iguales 
        # TODO delete this method 
        # Convertir columnas a fecha 
        self.data[3]['fecha_paletiza'] = pd.to_datetime(self.data[3]['fecha_paletiza'])

        # TODO ---- revisar desde aquí  
        colsf5 = ['trf_entry_date', 'trf_rec_date']
        newcolsf5 = ['year_res', 'year_rec']
        self.data[2][colsf5] = self.data[2][colsf5].replace('utc', '')
        self.data[2][newcolsf5] = self.data[2][colsf5].apply(lambda x: x.str.extract('(\d{4})', expand=False))
        # Obtener el año de la reserva, el envío y la recepción
        # datecolsf4 = ['fecha creacion',  'fecha reserva', 'fecha envio']
        # newdatecolf4 = ['aa creacion',  'aa reserva', 'aa envio']
        # f4[newdatecolf4] = f4[datecolsf4].apply(lambda x: x.str.extract('(\d{2})', expand=False))
        # TODO Pasar esto a limpieza F4 
        colsf3 = ['fecha_reserva', 'fecha_envio', 'fecha_anulacion','fecha_confirmacion']
        newcolsf3 = ['aaaa reserva', 'aaaa envio', 'aaaa anulacion','aaaa confirmacion']
        self.data[0][newcolsf3] = self.data[0][colsf3].apply(lambda x: x.str.extract('(\d{4})', expand=False))

        self.data[1]['aa creacion'] = self.data[1]['fecha_registro'].dt.strftime('%Y')

    def multi_test(self, test_id, tlist):
        for tlist_desc in tlist:
            self.single_test(test_id, tlist_desc)

    def single_test(self, test_id, type_data):
        if test_id == 0: 
            self.cnc.f3_verify(self.data[0], type_data[0], type_data[1])
        elif test_id == 1:
            self.cnc.f4_verify(self.data[1], type_data[0], type_data[1])
        elif test_id == 2: 
            self.cnc.f5_verify(self.data[2], type_data[0], type_data[1])
        elif test_id == 3:
            self.cnc.f5_verify_local(self.data[2], type_data[0], type_data[1], type_data[2])
        elif test_id == 4:
            self.cnc.f5_verify_local_list(self.data[2], type_data[0], type_data[1], type_data[2], type_data[3])
        elif test_id == 5:
            self.cnc.no_carga_verify(type_data[0])
            

    def test_call(self):

        lista_f3 = ['se asocia f3-devuelto a proveedor', '2022']
        lista_f4 = [['se asocia f4 dado de baja por producto entegado a cliente con nc', '2022'], ['se asocia f4 por producto no ubicado', '2022'],
                    ['se asocia f4-baja de inventario-menaje', '2022'],['baja con cargo a linea por costos', '2022'], 
                    ['baja con cargo a dependencia por politicasdefiniciones', '2022'], ['error en generacion de nota credito', '2022'], 
                    ['se asocia f4-baja de inventario-fast track', '2022'], ['se asocia f4 por producto no ubicado - postventa', '2022'], 
                    ['asociado a f4 facturacion por transportes', '2022']]
        lista_f5 = [['con mc asociada', '2022'],['con ro asociado', '2022'],['compensacion con ct verde', '2022'], ['con quiebre asociado', '2022'], ['f5 en revision', '2022'],
                    ['se asocia f11-conciliacion con transportadora', '2022'],['con f11 tipo cliente asociado', '2022'],
                    ['compensa con local de ventaanulado x user', '2022'], ['f12 en digitado sin salida', '2022']]
        lista_f5_local = ['compensacion con dvd administrativo', '2022', '3001']
        lista_f5_local_lista = [['compensacion con ct ciudades', '2022', 'CTs', cts], ['compensacion con preventas', '2022','preventas', preventas], ['compensacion con tienda', '2022','tiendas', tiendas]]
        lista_nac = ['local venta 3000no aplica carga']

        self.single_test(0, lista_f3)
        self.multi_test(1, lista_f4)
        self.multi_test(2, lista_f5)
        self.single_test(3, lista_f5_local)
        self.multi_test(4, lista_f5_local_lista)
        self.single_test(5, lista_nac)

    def save_test(self):
        dt_string = datetime.now().strftime('%y%m%d-%H%M')
        self.data[5].to_excel(f'output/cierres_nc/{dt_string}-cnc_21-output.xlsx', sheet_name=f'{dt_string}_cnc', index=False)
        nc2 = self.data[5].merge(self.data[2], how='left', left_on=[self.fcols[2],self.pcols[2]], right_on=['trf_number','prd_upc'], validate='many_to_one')
        nc3 = nc2.merge(self.data[1], how='left',  left_on=[self.fcols[1],self.pcols[2]], right_on=['ctech_key','prd_upc'],validate='many_to_one')
        path = f'output/cierres_nc/{dt_string}-cnc_21-all.xlsx'
        nc3.to_excel(path, sheet_name=f'{dt_string}_cnc', index=False) 
        return path

    def save_selection(self): #TODO unificar dado que igual que en cf11_cd 
        print('Desea guardar los resultados? (y/n)')
        save_res = input('//:')
        if save_res=='y':
            path = self.save_test()
            print(f'Guardado en: {path}')
        else:
            print('Ok')

    def print_results(self): #TODO unificar dado que igual que en cf11_cd 
        #print(self.data[5].groupby('gco_dup')[self.pcols[2]].sum()) # TODO activar cuando duplicados en data 
        #print(self.data[5].groupby('gco_dupall')[self.pcols[2]].sum())
        res = self.data[5].groupby([self.pcols[1],'GCO']).agg({self.pcols[3]:['sum', 'size']}).sort_values(by=[self.pcols[1],(self.pcols[3],'sum')], ascending=False)
        print(res)

def innit_condition():
    names = ['f3', 'f4', 'f5', 'kpi','refact', 'cierres_nc_21']
    fcols = ['f3','f4','f5','f11', '', 'cod_aut_nc']
    pcols = ['indice_cnc', 'tipificacion_final', 'upc', 'ct', 'cantidad_trx_actual', 'estado_final']
    cnc = CNC_CD(names, fcols, pcols)
    cnc.run_test()
        
if __name__=='__main__':
    innit_condition()