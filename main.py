from io import SEEK_SET
from os import pipe
import os
from unidecode import unidecode
from cf11_cd import CF11_CD
from cnc_cd import CNC_CD

select_var = ''

def clean():
    os.system('cls' if os.name=='nt' else 'clear')

def menu():
    print('-----------------------')
    print(' ### Menú inicial')
    print('1. Obtener data')
    print('2. Cierres de F11s')
    print('3. Cierres de NC')
    print('4. Salir')
    print('-----------------------')

def menu_cl():
    print('  0. Actualizar archivos dtlk')
    print('  1. Limpiar data')
    print('  2. Procesar data')
    print('  3. Regresar al menú')

def menu_cf11():
    print('  1. Cierres de F11 CD 2023')
    print('  2. Cierres de F11 Tienda 2023')
    print('  3. Regresar al menú')

def menu_cnc():
    print('  1. Cierres de NC 2022')
    print('  2. Regresar al menú')

while select_var!='4':
    menu()
    select_var = input('Seleccione una tarea: ')
    
    if select_var =='1':
        menu_cl()
        sv_cl = input('  Rta: ')
        if sv_cl =='0':
            exec(open('dtlk_control.py').read())
        elif sv_cl=='1':
            exec(open('cl_fs.py').read())
        elif sv_cl=='2':
            exec(open('get_data.py').read())
        elif sv_cl=='3':
            clean()
        else:
            print('    Por favor seleccione una opción valida!')

    elif select_var=='2':
        menu_cf11()
        sv_cf11= input('  Rta: ')
        if sv_cf11=='1':
            names = ['f3', 'f4', 'f5', 'kpi','ro','en','f11','cf11_cd_21']
            fcols = ['f3','f4','f5','nfolio','f12']
            pcols = ['indice_cf11','status_final', 'prd_upc', 'costo_total', 'qproducto']
            cf11 = CF11_CD('2023', names, fcols, pcols)
            cf11.run_test()
        elif sv_cf11=='2':
            exec(open('cf11_tienda_21.py').read())
        elif sv_cf11 =='3':
            clean()
        else:
            print('    Por favor seleccione una opción valida!')

    elif select_var=='3':
        menu_cnc()
        sv_nc= input('  Rta: ')
        if sv_nc=='1':
            names = ['f3', 'f4', 'f5', 'kpi','refact', 'cierres_nc_21']
            fcols = ['f3','f4','f5','f11', '', 'cod_aut_nc']
            pcols = ['indice_cnc', 'tipificacion_final', 'upc', 'ct', 'cantidad_trx_actual', 'estado_final']
            cnc = CNC_CD(names, fcols, pcols)
            cnc.run_test()
        elif sv_nc=='2':
            clean()
        else:
            print('    Por favor seleccione una opción valida!')
        
    elif select_var=='4':
        print('# Hasta luego!')
    else:
        print('  Por favor seleccione una opción valida!')
