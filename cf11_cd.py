from multiprocessing import set_forkserver_preload
from os import name
import pandas as pd
from datetime import datetime
from ica_core.ica_cierres import CierresF11
from etl_core.etl import ETL

class CF11_CD():

    def __init__(self, year, names, fcols, pcols) -> None:
        self.data = []
        self.year = year
        self.names = names
        self.fcols = fcols
        self.pcols = pcols
        self.etl = ETL('input/cierres_f11/cd/')

    def run_test(self):
        self.data = self.etl.load_data(self.names) # Load files 
        self.set_index() # Set index 
        self.convert_dtypes() # Convert data types 
        self.set_dates() # Set date columns 
        self.cierres = CierresF11(self.data[7])
        self.cierres.set_fcols(self.fcols, self.pcols)
        self.cierres.starting([self.fcols[4],self.pcols[2], self.pcols[4]]) 
        self.test_call_selection() # Select test call based on the year 
        self.cierres.finals()
        self.data[7] = self.cierres.ica.get_db()
        self.print_results() # Print result in command line 
        self.save_selection() 

    def test_call_selection(self):
        if self.year == '2020':
            self.test_call_20()
        elif self.year == '2021':
            self.test_call_21()
        elif self.year == '2022':
            self.test_call_22()
        elif self.year == '2023':
            self.test_call_23()


    def convert_dtypes(self):
        # TODO boost performance 
        # Convertir columnas a número 
        self.data[0].loc[:,'cantidad'] = pd.to_numeric(self.data[0].loc[:,'cantidad'])
        self.data[1].loc[:,'qf04_ship'] = pd.to_numeric(self.data[1].loc[:,'qf04_ship'])
        self.data[1].loc[:,'fecha_registro'] = pd.to_datetime(self.data[1].loc[:,'fecha_registro'].str.replace('UTC', ''))
        self.data[2].loc[:,'trf_rec_to_date'] = pd.to_numeric(self.data[2].loc[:,'trf_rec_to_date'])
        self.data[3]['fcreareg'] = pd.to_datetime(self.data[3]['fcreareg'])
        self.data[5]['fentrada'] = pd.to_datetime(self.data[5]['fentrada'])
        self.data[7].loc[:,[self.pcols[4],self.pcols[3]]] = self.data[7].loc[:,[self.pcols[4],self.pcols[3]]].apply(pd.to_numeric)


    def set_dates(self):
        # TODO delete this method 
        # Convertir columnas a fecha 

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

    def set_index(self) -> None:
        self.data[7] = self.data[7].reset_index()
        self.data[7].rename(columns={'index': self.pcols[0]}, inplace=True)

    def multi_test(self, test_id, tlist):
        for tlist_desc in tlist:
            self.single_test(test_id, tlist_desc)

    def single_test(self, test_id, type_data):
        if test_id == 0: 
            self.cierres.f3_verify(self.data[0], type_data[0], type_data[1])
        elif test_id == 1:
            self.cierres.f4_verify(self.data[1], type_data[0], type_data[1])
        elif test_id == 2: 
            self.cierres.f5_verify(self.data[2], type_data[0], type_data[1])
        elif test_id == 3:
            self.cierres.kpi_verify(self.data[3], type_data[0], type_data[1], type_data[2])
        elif test_id == 4: 
            self.cierres.ro_verify(self.data[5], self.data[4], type_data[0], type_data[1], type_data[2])
        elif test_id == 5: 
            self.cierres.f11_verify(self.data[6], type_data[0])

    def test_call_20(self):
        lista_f4 = [ ['f4 de merma', '2021'], ['cierre x f4 cobrado a terceros', '2021'], 
        ['f4 de merma - por duplicidad f12 + upc + cantidad', '2021'], 
        ['f4 de merma - no se puede generar producto no existe', '2021'], 
        ['registro duplicado en base de datos', '2021']] # 'error en cierre de f11', 'error en creacion de f11','politica cambio agil','cierre x f4 dado baja crate prestamos', 'f4 de mermaf4 2020 cierre f11 2021', 'f4 en revision',
        lista_f3 = [['f3 en revision', '2021'],['cierre x f3 devuelto a proveedor', '2021']]
        lista_f5 = ['producto en tienda', '2021']
        lista_kpi = [['cierre x producto guardado despues de inventario', '2021', 'Recibido con fecha anterior al 21/01/2021'], 
        ['cierre x producto guardado despues de inventario - no aplica f12', '2021', 'Recibido con fecha anterior al 21/01/2021']]
        lista_refact = 'cierre x recupero con cliente - refacturacion - base fal.com'

        self.multi_test(0, lista_f3) # F3 
        self.multi_test(1, lista_f4) # F4 
        self.single_test(2, lista_f5) # F5 
        self.multi_test(3, lista_kpi) # KPI 
        self.single_test(4, lista_refact ) # Refacturación 

    def test_call_21(self):
        lista_f3 = [['cierre x f3 devuelto a proveedor', '2021']]
        lista_f4 = [['f4 en revision', '2021'],['cierre x f4 cobrado a terceros', '2021'], ['f4 de merma', '2021'], 
                    ['entregado a cliente por politica (fast track)', '2021'], ['entregado a cliente', '2021']] 
        lista_f5 = [['producto en tienda', '2021'], ['cierre con f5 administrativo cd', '2021'], 
        ['producto compensado con ctverde-recibido', '2021'], ['producto compensado con tienda - recibido', '2021']]
        lista_kpi = [['cierre x producto guardado despues de inventario', '2021', 'Recibido con fecha anterior al 21/01/2021'], 
        ['cierre x producto guardado antes de inventario', '2020', 'Recibido con fecha posterior al 20/01/2021']]
        lista_refact = 'cierre x recupero con cliente - refacturacion - base fal.com'

        self.multi_test(0, lista_f3) # F3 
        self.multi_test(1, lista_f4) # F4 
        self.multi_test(2, lista_f5) # F5 
        self.multi_test(3, lista_kpi) # KPI
        self.single_test(4, lista_refact) # Refacturación 

    def test_call_22(self):
        lista_f3 = [['cierre por f3 devuelto a proveedor', '2022']]
        lista_f4 = [['f4 en revision', '2022'],['cierre por f4 cobrado a terceros', '2022'], ['f4 merma', '2022'], 
                    ['entregado a cliente por politica (fast track)', '2022'], ['entregado a cliente', '2022']] 
        lista_f5 = [['producto en tienda', '2022'], ['cierre con f5 administrativo cd', '2022'], 
        ['producto compensado con ctverde-recibido', '2022'], ['producto compensado con tienda - recibido', '2022']]
        lista_kpi = [['cierre por producto guardado despues de inventario', '2023', 'Recibido con fecha anterior al 14/01/2022'], 
        ['cierre por producto guardado antes de inventario', '2021', 'Recibido con fecha posterior al 14/01/2022'],
        ['cierre por producto guardado antes de inventario-ingresa con skutro', '2021', 'Recibido con fecha posterior al 14/01/2022']]
        lista_refact = 'cierre x recupero con cliente - refacturacion - base fal.com'

        self.multi_test(0, lista_f3) # F3 
        self.multi_test(1, lista_f4) # F4 
        self.multi_test(2, lista_f5) # F5 
        self.multi_test(3, lista_kpi) # KPI
        # self.single_test(4, lista_refact) # Refacturación 
    
    def test_call_23(self):
        lista_f3 = [['cierre por f3 devuelto a proveedor', '2023']]
        lista_f4 = [['f4 en revision', '2022'],['cierre por f4 cobrado a terceros', '2023'], ['f4 merma', '2023'], 
                    ['entregado a cliente por politica (fast track)', '2023'], ['entregado a cliente', '2023']] 
        lista_f5 = [['producto en tienda', '2023'], ['cierre con f5 administrativo cd', '2023'], 
        ['producto compensado con ctverde-recibido', '2023'], ['cierre por producto compensado con tienda-recibido', '2023']]
        lista_kpi = [['cierre por producto guardado despues de inventario', '2023', 'Recibido con fecha anterior al 13/01/2023'], 
        ['cierre por producto guardado antes de inventario', '2023', 'Recibido con fecha posterior al 13/01/2023'],
        ['cierre por producto guardado antes de inventario-ingresa con skutro', '2023', 'Recibido con fecha posterior al 13/01/2023'],
        ['cierre por producto guardado despues de inventario-ingresa con skutro', '2023', 'Recibido con fecha anterior al 13/01/2023'],
        ['cierre por producto guardado antes de inventario-ingresa con f12tro', '2023', 'Recibido con fecha posterior al 13/01/2023']]
        lista_f11 = ['cierre por nuevo f11 en estado despachado']

        self.multi_test(0, lista_f3) # F3 
        self.multi_test(1, lista_f4) # F4 
        self.multi_test(2, lista_f5) # F5 
        self.multi_test(3, lista_kpi) # KPI
        self.multi_test(4, lista_kpi) # ro 
        self.single_test(5, lista_f11) # ro 

    def save_test(self):
        dt_string = datetime.now().strftime('%y%m%d-%H%M')
        self.data[7].to_excel(f'output/cierres_f11/cd/{dt_string}-{self.names[7]}-output.xlsx', sheet_name=f'{dt_string}_{self.names[4]}', index=False, encoding='utf-8') # Guarda el archivo 
        # bdcia = self.data[7].merge(self.data[0], how='left', left_on=[self.fcols[0],self.pcols[2]], right_on=['nro_devolucion','upc'], validate='many_to_one')
        # bdcia2 = bdcia.merge(self.data[1], how='left',  left_on=[self.fcols[1],self.pcols[2]], right_on=['ctech_key','prd_upc'],validate='many_to_one')
        # bdcia3 = bdcia2.merge(self.data[2], how='left', left_on=[self.fcols[2],self.pcols[2]], right_on=['trf_number','prd_upc'], validate='many_to_one')
        # bdcia4 = bdcia3.merge(self.data[3], how='left',left_on=[self.fcols[3]], right_on=['centrada'],validate='many_to_one')
        # bdcia5 = bdcia4.merge(self.data[3], how='left',left_on=[self.fcols[4]], right_on=['centrada'],validate='many_to_one')
        #bdcia6 = bdcia4.merge(refact, how='left',left_on=[fcols[4]], right_on=['f12cod'],validate='many_to_one')
        path = f'output/cierres_f11/cd/{dt_string}-{self.names[7]}-all.xlsx'
        # bdcia5.to_excel(path, sheet_name=f'{dt_string}_{self.names[4]}',index=False) 
        return path

    def save_selection(self):
        flag = True
        while flag:
            print('Desea guardar los resultados? (y/n)')
            save_res = input('//:')
            if save_res.lower() == 'y':
                path = self.save_test()
                print(f'Guardado en: {path}')
                flag =False
            elif save_res.lower() == 'n':
                print('Ok')
                flag = False

    def print_results(self):
        print(self.data[7].groupby('gco_dup')[self.pcols[3]].sum())
        print(self.data[7].groupby('gco_dupall')[self.pcols[3]].sum())
        res = self.data[7].groupby([self.pcols[1],'Comentario GCO']).agg({self.pcols[3]:['sum', 'size']}).sort_values(by=[self.pcols[1],(self.pcols[3],'sum')], ascending=False)
        print(res)


def innit_commandline():

    year = input('Ingrese año (2020 | 2021): ')
    names = []
    fcols = []
    pcols = []
    if year =='2020':
        names = ['f3', 'f4', 'f5', 'kpi','refact', 'cf11_cd_20']
        fcols = ['f3nuevo','f4_nuevo','f5','nfolio','f12']
        pcols = ['indice_cf11','status_nuevo', 'prd_upc', 'total_costo_promedio', 'qproducto']
    elif year =='2021':
        names = ['f3', 'f4', 'f5', 'kpi','refact', 'cf11_cd_21']
        fcols = ['f3','f4','f5','nfolio','f12']
        pcols = ['indice_cf11','status_final', 'prd_upc', 'costo_total', 'qproducto']
    
    cf11 = CF11_CD(year, names, fcols, pcols)
    cf11.run_test()

if __name__=='__main__':
    innit_commandline()