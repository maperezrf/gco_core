import pandas as pd
from ica_core.ica_raw import InternalControlAnalysis

class CierresF11:

    def __init__(self, db) -> None:
        self.db = db 
        self.pcols = [ 'index-0', 'status-1', 'upc-2', 'cost-3', 'quantity-4']
        self.fcols = ['f3col-0', 'f4col-1', 'f5col-2', 'f11col-3', 'f12col-4']
        self.ica = InternalControlAnalysis(self.db, 'indice_cf11')

    def set_fcols(self, fcols, pcols ):
        self.fcols = fcols
        self.pcols = pcols

    def f3_verify(self, f3, status, yyyy):
        df1 = self.db[self.db[self.pcols[1]]==status]
        df2= self.ica.get_fnan( df1, self.fcols[0], 'F3')
        if df2.empty == False: 
            df3 = self.ica.get_duplicates( df2,[self.fcols[4],self.pcols[2], self.pcols[4]], 'F12 + UPC + Cantidad')
            ne = self.ica.get_notfound( df3, f3, [self.fcols[0],self.pcols[2]], ['nro_devolucion','upc'], 'nro_devolucion', 'F3|UPC|QTY')
            df4 = pd.merge(df3, f3, left_on=[self.fcols[0],self.pcols[2]], right_on=['nro_devolucion','upc'])
            if df4.empty ==False: 
                df5 = self.ica.get_equalvalue(df4, 'descripcion6', 'anulado', 'ANU', 'Registro anulado')
                df6 = self.ica.get_diffqty_pro(df5, self.pcols[4], 'cantidad',self.fcols[3], 'nro_devolucion' ,'Cantidad de los F11s de un F3 > cantidad del F3')
                iokf3 = df6[self.pcols[0]].values
                self.ica.update_db(iokf3,'GCO', 'OKK')
                self.ica.update_db(iokf3,'Comentario GCO', 'Coincidencia exacta F3+UPC+QTY')
                df7 = df6[df6['descripcion6']=='confirmado']
                df8= self.ica.get_diffvalue(df7, 'aaaa anulacion', yyyy, 'NAA', f'Registro con año de confirmación diferente a {yyyy}')
                self.ica.get_okk_dup(iokf3, 'Comentario GCO', 'F3+UPC+QTY')
                self.ica.get_dup_i(iokf3, 'F3+UPC+QTY') # TODO llave como variable 

    def f4_verify(self, f4, status, yyyy):
        df1 = self.db[self.db[self.pcols[1]]==status]
        df2 = self.ica.get_fnan( df1, self.fcols[1], 'F4')
        if df2.empty == False:
            df3 = self.ica.get_duplicates( df2, [self.fcols[4], self.pcols[2], self.pcols[4]], 'F12 + UPC + Cantidad')
            ne = self.ica.get_notfound( df3, f4, [self.fcols[1], self.pcols[2]], ['ctech_key','prd_upc'], 'ctech_key', 'F4|UPC|QTY')
            df4 = pd.merge(df3, f4, left_on=[self.fcols[1], self.pcols[2]], right_on=['ctech_key','prd_upc'])
            if df4.empty ==False: 
                auxdf4 = self.ica.get_diffvalue(df4, 'ctipo', '4', 'NDB', 'El tipo de F4 es diferente a dado de baja')
                df5 = self.ica.get_equalvalue(auxdf4, 'cestado', '4', 'ANU', 'Registro anulado')
                df6 = self.ica.get_diffvalue(df5, 'aa creacion', yyyy, 'NAA', f'Registro con año de creación diferente a {yyyy}') # TODO resolver año 
                df7 = self.ica.get_diffqty_pro(df6, self.pcols[4], 'qf04_ship',self.fcols[3],'ctech_key', 'Cantidad de los F11s de un F4 > cantidad del F4')
                iokf4 = df7[self.pcols[0]].values
                self.ica.update_db(iokf4,'GCO', 'OKK')
                self.ica.update_db(iokf4,'Comentario GCO', 'Coincidencia exacta F4+UPC+QTY')
                self.ica.get_okk_dup(iokf4, 'Comentario GCO', 'F4+UPC+QTY')
                self.ica.get_dup_i(iokf4, 'F4+UPC+QTY')

    def f5_verify(self, f5, status, yyyy):
        df1 = self.db[self.db[self.pcols[1]]==status]
        df2 = self.ica.get_fnan( df1, self.fcols[2], 'F5')
        if df2.empty ==False: 
            df3 = self.ica.get_duplicates( df2, [self.fcols[4], self.pcols[2], self.pcols[4] ], 'F12 + UPC + Cantidad')
            ne = self.ica.get_notfound( df3, f5, [self.fcols[2], self.pcols[2]], ['trf_number','prd_upc'], 'trf_number', 'F5|UPC|Qty') # TODO desde aquí modificaciones
            df4 = pd.merge(df3, f5, left_on=[self.fcols[2], self.pcols[2]], right_on=['trf_number','prd_upc']) # TODO Update datalake  
            if df4.empty ==False: 
                df5 = self.ica.get_diffvalue(df4, 'trf_status', '5', 'NRE', 'Registro con estado diferente a recibido') # TODO Update datalake  
                #df6 = self.ica.get_equalvalue(df5, 'motivo_discrepancia', 'f5 no recibido', 'MDI', 'Registro con motivo de disc: F5 no recibido')
                # df7 = self.ica.get_diffvalue(df5, 'year_res', yyyy, 'NAA', f'Registro con año de reserva diferente a {yyyy}')
                comment = 'Cantidad de las F11s de un F5 > cantidad del F5'
                df8 = self.ica.get_diffqty_pro(df5,  self.pcols[4], 'trf_rec_to_date', self.fcols[3], 'trf_number', comment) # TODO Update datalake  
                iokf5 = df8[self.pcols[0]].values
                self.ica.update_db(iokf5, 'GCO','OKK')
                self.ica.update_db(iokf5, 'Comentario GCO', 'Coincidencia exacta F5+UPC+QTY')
                self.ica.get_okk_dup(iokf5, 'Comentario GCO', 'F5+UPC+QTY')
                self.ica.get_dup_i(iokf5, 'F5+UPC+QTY')

    def kpi_verify(self, kpi, status, yyyy, commenty):
        df1 = self.db[self.db[self.pcols[1]]==status]
        df2= self.ica.get_fnan_cols(df1, [self.fcols[4],self.fcols[3]], 'KPID')
        if df2.empty == False:
            df3 = self.ica.get_duplicates( df2, [self.fcols[4],'prd_upc', 'qproducto'], 'F12 + UPC + Cantidad')
            index_ne_kpi_di = self.ica.get_notfound( df3, kpi, [self.fcols[3]], ['centrada'], 'centrada', '(F12|F11)')
            index_ne_kpi_di2 = self.ica.get_notfound( self.db.loc[index_ne_kpi_di], kpi, [self.fcols[4]], ['centrada'], 'centrada', '(F12|F11)')
            pgdim1 = pd.merge(df3, kpi, left_on=[self.fcols[3]], right_on=['centrada'])
            pgdim2 = pd.merge(df3.loc[index_ne_kpi_di], kpi, left_on=[self.fcols[4]], right_on=['centrada'])
            lpgdi = [pgdim1, pgdim2]
            pgdim = pd.concat(lpgdi, axis=0)
            pgdimdyear = '' 
            if yyyy == '2022': 
                pgdimdyear = self.ica.get_lvalue(pgdim, 'fcreareg', pd.Timestamp(2022,1,14), 'NAA',commenty)
            elif yyyy =='2021':
                pgdimdyear = self.ica.get_gvalue(pgdim, 'fcreareg', pd.Timestamp(2022,1,14), 'NAA', commenty)
            elif yyyy =='2023':
                if status.startswith('cierre por producto guardado despues de inventario'):
                    pgdimdyear = self.ica.get_mvalue(pgdim, 'fcreareg', pd.Timestamp(2023,1,13), 'NAA', commenty)
                else :
                    pgdimdyear = self.ica.get_gvalue(pgdim, 'fcreareg', pd.Timestamp(2023,1,13), 'NAA', commenty)
                iokkpid = pgdimdyear[self.pcols[0]].values
                self.ica.update_db(iokkpid,'GCO', 'OKK')
                self.ica.update_db(iokkpid,'Comentario GCO', 'Coincidencia exacta (F12|F11)')
                self.ica.get_okk_dup(iokkpid, 'Comentario GCO', '(F12|F11)')
                self.ica.get_dup_i(iokkpid, '(F12|F11)')

    def ro_verify(self, en, ro, status, yyyy, commenty):
            df1 = self.db[self.db[self.pcols[1]]==status]
            ro_index = ro.loc[ro['estado_ro'] == 'receiving verified']['ro'].unique()
            df2= self.ica.get_fnan_cols(df1, ['ro'], 'ro', False)
            df3 = df2.loc[df2['ro'].isin(ro_index)]
            if df3.empty == False:
                index_ne_en_di = self.ica.get_notfound( df3, en, ['ro'], ['centrada'], 'centrada', 'ro')
                pgdim1 = pd.merge(df3, en, left_on=['ro'], right_on=['centrada'])
                pgdim2 = pd.merge(df3.loc[index_ne_en_di], en, left_on=['ro'], right_on=['centrada'])
                lpgdi = [pgdim1, pgdim2]
                pgdim = pd.concat(lpgdi, axis=0)
                pgdimdyear = '' 
                if yyyy == '2022': 
                    pgdimdyear = self.ica.get_lvalue(pgdim, 'fcreareg', pd.Timestamp(2022,1,14), 'NAA',commenty)
                elif yyyy =='2021':
                    pgdimdyear = self.ica.get_gvalue(pgdim, 'fcreareg', pd.Timestamp(2022,1,14), 'NAA', commenty)
                elif yyyy =='2023':
                    pgdimdyear = self.ica.get_gvalue(pgdim, 'fentrada', pd.Timestamp(2023,1,13), 'NAA', commenty)
                iokend = pgdimdyear[self.pcols[0]].values
                self.ica.update_db(iokend,'GCO', 'OKK')
                self.ica.update_db(iokend,'Comentario GCO', 'Coincidencia exacta (ro)')
                self.ica.get_okk_dup(iokend, 'Comentario GCO', '(ro)')
                self.ica.get_dup_i(iokend, '(ro)')

    def f11_verify(self, f11, status):
        df1 = self.db[self.db[self.pcols[1]]==status]
        df2= self.ica.get_fnan_cols(df1, ['f11_nuevo'], 'nro f11 nuevo')
        if df2.empty == False:
            df3 = self.ica.get_duplicates( df2,['f11_nuevo'], 'F11')
            index_ne_en_di = self.ica.get_notfound( df2, f11, ['f11_nuevo'], ['nro_f11'], 'nro_f11', 'nro f11 nuevo')
            df4 = df2.merge(f11, how = 'left', left_on = 'f11_nuevo', right_on = 'nro_f11')
            iokf11 = df4.loc[(df4['nro_f11'].notna()) & (df4['estado'].isin(['despachado', 'entrega total'])), self.pcols[0]]
            self.ica.update_db(iokf11,'GCO', 'OKK')
            self.ica.update_db(iokf11,'Comentario GCO', 'Coincidencia exacta F11')
            
    def refact_verify(self, refact, status):
        df1 = self.db[self.db[self.pcols[1]]==status]
        df2= self.ica.get_fnan( df1, self.fcols[4], 'F12')
        df3 = self.ica.get_duplicates( df2,[self.fcols[4],'prd_upc', 'qproducto'], 'F12+UPC+Cantidad')
        ne = self.ica.get_notfound( df3, refact, [self.fcols[4]], ['f12cod'], 'f12cod', 'F12')
        df4 = pd.merge(df3, refact, left_on=[self.fcols[4]], right_on=['f12cod'])
        df5 = self.ica.get_equalvalue(df4, 'confirmacion_tesoreria', 'no reintegrado  trx declinada', 'ANU', 'Registro con TRX declinada')
        # df5 = cierres.ica.get_diffvalue(df4, 'estado', 'APPROVED', 'ANU', 'Registro con transacción anulada')
        # df6 = cierres.ica.get_diffqty_pro(df5, 'qproducto', 'cantidad',f11_col, f3_col,'La cantidad sumada de los f11s de un f3 es mayor que la cantidad del f3')
        iokf12 = df5[self.pcols[0]].values
        self.ica.update_db(iokf12,'GCO', 'OKK')
        self.ica.update_db(iokf12,'Comentario GCO', 'Coincidencia exacta F12')
        self.ica.get_okk_dup(iokf12, 'Comentario GCO', 'F12')
        self.ica.get_dup_i(iokf12, 'F12')

    def starting(self, cols):
        # verificar duplicidad en toda la base 
        self.ica.get_dup_all_db(cols)
        self.ica.dupall(self.pcols[1])
        
    def finals(self):
        # verificar registros revisados
        self.ica.get_checked()