import pandas as pd
import numpy as np
import pyodbc
import pandas.io.sql
from sqlalchemy import create_engine
import matplotlib.pyplot as plt #Charts
import seaborn as sns #REAL charts
from matplotlib.ticker import PercentFormatter
from matplotlib.ticker import FuncFormatter
import glob
from dateutil import parser
import dateutil.relativedelta
from datetime import datetime
from datetime import timedelta
from datetime import date
from tkcalendar import Calendar
from pymongo import MongoClient
try:
    import tkinter as tk
    from tkinter import ttk
except ImportError:
    import Tkinter as tk
    import ttk
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import os.path
import os
import time
import getpass
pd.options.display.max_columns = 20
import tabulate

class Security_Master():
    def __init__(self, date_ini, date_fin, dpi = 1000, alpha = 0.7, title_fontsize = 10, axislabel_fontsize = 8):
        '''
        Parameters
        ----------
        date_ini : string de la forma 'aaaa-mm-dd'
            Fecha desde la cual queremos la data
        date_fin : string de la forma 'aaaa-mm-dd'
            Fecha hasta la cual queremos la data
        engine : cadena necesaria de conexión para insertar al Security_Master
            Trae un default que se debe cambiar cuando el Security_Master migre de servidor
        dpi : entero para resolución de gráficas
            Default es 1000
        alpha : decimal, transparencia de las gráficas
            Default es 0.7
        title_fontsize : entero. Tamaño de fuente para títulos en gráficas
            Default trae 10
        axislabel_fontsize : entero. Tamaño de fuente para labels de los ejes.
            Default trae 8
        rojo_compass : el rojo para todas las gráficas. En realidad deberíamos definir la paleta de colores.
            Default es '#C00000'
        gris1 : uno de los grises cuando necesitamos otro color que no sea el rojo compass. De nuevo, deberíamos definir una paleta de colores
            Default es '#BFBFBF'
        '''
        self.date_ini = date_ini
        self.date_fin = date_fin
        self.dpi = dpi
        self.alpha = alpha
        self.title_fontsize = title_fontsize
        self.axislabel_fontsize = axislabel_fontsize
        self.rojo_compass = '#C00000'
        self.gris1 = '#BFBFBF'       
        self.engine = create_engine("mssql+pyodbc://usr_smaster:XB5D#LhB@CGQ-BI")
    def control_cargas_securities(self):
        '''
        Method para ver toda la info que se cargó a una cierta fecha en cuanto a securities, issuers y coverage
        OUT:
            df.- pandas DataFrame con las características de los securities, issuers, corporate id, coverage, sectores... etc
        '''
        df_newsec = pd.read_csv(r'Compass Group\Riesgo Financiero - Documentos\Data Bases\Security Master\Scripts\Uploads\historico_cargas_securities.csv', dayfirst=True,  parse_dates=['date'])
        df_newsec = df_newsec.rename(columns = {'id':'securityid_id'})
        df_newsec['date'] = pd.to_datetime(df_newsec['date'])
        df_newsec = df_newsec[df_newsec['date'] == parser.parse(self.date_ini)]
        lista_nuevas_cargas = df_newsec['securityid_id'].tolist()
        df = self.get_all_from_id(lista_nuevas_cargas, by = 'securityid_id')
        return df
    def get_hist_ratings(self, rating_source_id = 1, grade = 'grades_min'):
        '''
        Method para sacar la historia de ratings por issuer. Opté por tener esta función como un catch all de internal, external etc.
        Habrá otra función que llame esta con id = 3 para sacar los internal. Comparten tabla entonces tiene sentido que compartan función.
        IN:
            rating_source_id:
                1 para BDP
                2 ValmerMX (Local)
                3 Internal ratings.
            grade:
                grades_min
                grades_prom
                grades_max
                default: grades_min para no cambiar funciones anteriores
        OUT:
            df_ratings: pandas DataFrame con issuerid_id, compass_issuer_name, ...
        '''
        #conn = self.conexion_sm()
        str_rating_source_id = str(rating_source_id)
        query_str = "SELECT SecurityId.issuerid_id, Rating.securityid_id," +  grade + ", date_r FROM Rating INNER JOIN SecurityId on Rating.securityid_id = SecurityId.id WHERE rating_source_id = "+str_rating_source_id
        #df_ratings = pd.read_sql_query(query_str, conn)
        df_ratings = pd.read_sql_query(query_str, self.engine)
        sec_iss_all = self.get_security_issuer_all()[['issuerid_id', 'compass_issuer_name']].drop_duplicates()
        dict_rating = self.dict_rating_score_letter()
        df_ratings = df_ratings.fillna(-1)
        df_ratings = df_ratings.merge(sec_iss_all, how = 'left', on = 'issuerid_id')
        df_ratings = df_ratings.merge(dict_rating, how = 'left', left_on =grade, right_on = 'score')
        return df_ratings
    def get_latest_ratings(self, rating_source_id = 1):
        '''
        Method para sacar solo los ratings más recientes.
        Literalmente usa get_hist_ratings, ordena por fecha y quita duplicados conservando el más reciente.
        IN:
            rating_source_id: opcional
                1 para BDP
                2 ValmerMX (Local)
                3 Internal ratings.
        '''
        df_ratings_all = self.get_hist_ratings(rating_source_id)
        df_ratings = df_ratings_all.sort_values(by = 'date_r', ascending = False).drop_duplicates(subset = ['securityid_id'], keep = 'first')
        return df_ratings
    def tp_vs_gscore(self, portfolio, chart = True):
        '''
        Method para generar la tabla de Target Price vs. GScore en USD
        IN: 
            portfolio: Vector con las cuentas of interest
                portfolio = ['INVLAE', 'INVLASC',...] o
                portfolio = ['INVLAE']
            chart: boolean
                Define si queremos o no que grafique. El default es que sí.
        OUT:
            df_tp_vs_g: pandas DataFrame con fondo, security, tp, gscore, upside, fecha, mkt value
        '''
        mkt = self.mkt_value(portfolio)
        df_tp = self.get_latest_TP()[['securityid_id', 'recommendation', 'target_price', 'date_e']].rename(columns = {'date_e': 'date_tp'})
        df_g = self.get_latest_gscore()[['issuerid_id', 'g_score', 'date_e', 'compass_issuer_name']].rename(columns = {'date_e': 'date_g'})
        lista_secs_tp = self._port_list_queries(df_tp['securityid_id'])
        characteristics_tp = self.generic_get_from_sm("SELECT * FROM Equity_characteristics WHERE securityid_id IN " + lista_secs_tp + " AND adr_per_share IS NOT NULL")
        characteristics_tp = characteristics_tp[['securityid_id', 'adr_per_share', 'currency_underlying']]
        characteristics_tp = characteristics_tp.astype({'currency_underlying':'string'})
        currency_values = self.get_currency_values()
        characteristics_tp = characteristics_tp.merge(currency_values, how = 'left', left_on = 'currency_underlying', right_on = 'currency_id')
        characteristics_tp = characteristics_tp[['securityid_id', 'adr_per_share', 'value']].rename(columns = {'value':'currency_value_adr'})
        df_tp = df_tp.merge(characteristics_tp, on = 'securityid_id', how = 'left').fillna(1)
        df_tp['target_price'] = df_tp['target_price']*df_tp['currency_value_adr']/df_tp['adr_per_share']
        df_tp = df_tp[['securityid_id', 'recommendation', 'target_price', 'date_tp']]
        df_tp_vs_g = mkt.merge(df_tp, how = 'left', on = 'securityid_id').merge(df_g, how = 'left', on = 'issuerid_id')
        sin_tp = df_tp_vs_g[df_tp_vs_g['target_price'].isnull()]#Guardaré los que no traen TP, son los que debo trabajar
        df_tp_vs_g = df_tp_vs_g.dropna(subset = ['target_price'])#Hasta acá solo trae lo que está bien cuadrado y no tiene mayor tema
        sin_tp = sin_tp[sin_tp['securityid_id'].str.contains("EQ")]#Solo los de Equity, cash me vale pito
        sec_iss_all = self.get_security_issuer_all()[['securityid_id', 'issuerid_id']]
        df_tp = df_tp.merge(sec_iss_all, how = 'left', on = 'securityid_id')
        sin_tp = sin_tp.merge(df_tp, how = 'left', on = 'issuerid_id')#Sacar los TP tratando de pivotear con el issuerid
        sin_tp_def = sin_tp[sin_tp['target_price_y'].isnull()]#Definitivamente no hay ni una coincidencia
        sin_tp_def = sin_tp_def.drop(['securityid_id_y', 'recommendation_x', 'target_price_x', 'date_tp_x'], axis=1)
        sin_tp_def = sin_tp_def.rename(columns = {'securityid_id_x': 'securityid_id', 'recommendation_y': 'recommendation', 'target_price_y': 'target_price', 'date_tp_y': 'date_tp'})
        df_tp_vs_g = pd.concat([df_tp_vs_g, sin_tp_def], ignore_index = True)#Al dataframe que estaba bien, le pego lo que definitivamente sé que no traen TP de nuestro SM. Son los que entran a controversias por faltante
        sin_tp = sin_tp.dropna(subset = ['target_price_y'])#Me quedo con lo que tenga ALGO de TP
        sin_tp = sin_tp.drop(['securityid_id_y', 'recommendation_x', 'target_price_x', 'date_tp_x'], axis=1)
        sin_tp = sin_tp.rename(columns = {'securityid_id_x': 'securityid_id', 'recommendation_y': 'recommendation', 'target_price_y': 'target_price', 'date_tp_y': 'date_tp'})
        try:
            eq_char_sin_tp = self.get_equity_characteristics(sin_tp['securityid_id'].tolist())[['securityid_id', 'adr_per_share']]
        except:
            eq_char_sin_tp = pd.DataFrame(columns = ['securityid_id', 'adr_per_share'])
        sin_tp = sin_tp.merge(eq_char_sin_tp, on = 'securityid_id')
        df_tp_vs_g = pd.concat([df_tp_vs_g, sin_tp], ignore_index = True)      
        df_tp_vs_g['upside'] = df_tp_vs_g['target_price']/df_tp_vs_g['price']-1
        if(chart):
            funds_chart_list = df_tp_vs_g['funds_id'].drop_duplicates()
            funds_id_name_list = df_tp_vs_g[['funds_id', 'aim_account']].drop_duplicates()
            for i in funds_chart_list:
                fondo_para_graficar = i
                fund_name_string = funds_id_name_list[funds_id_name_list['funds_id']==i]['aim_account'].iloc[0]
                df_grafica = df_tp_vs_g[df_tp_vs_g['funds_id']==fondo_para_graficar]
                df_grafica = df_grafica[['upside', 'g_score', 'compass_issuer_name', 'mkt_value_usd']]
                alpha = self.alpha
                fig, ax = plt.subplots()
                plt.rcParams['figure.dpi'] = self.dpi
                ax.set_title('Upside vs. Quality ' + fund_name_string, fontsize = self.title_fontsize)
                ax.set_xlabel('Upside (%)', fontsize = self.axislabel_fontsize)
                ax.set_ylabel('Quality (%)', fontsize = self.axislabel_fontsize)
                ax.yaxis.set_major_formatter(PercentFormatter(1))
                ax.xaxis.set_major_formatter(PercentFormatter(1))
                ax.axhline(0.75, ls='--', color = self.gris1)
                ax.axvline(0, ls='--', color = self.gris1)
                plt.xlim([-0.4,1.2])
                sns.scatterplot(data = df_grafica, x = 'upside', y = 'g_score', size = 'mkt_value_usd', sizes = (50, 2000),legend = False, color = self.rojo_compass, alpha = alpha)#hue = 'class', 
                plt.show()
        return df_tp_vs_g  
    def mkt_value(self, portfolio, price_source_id = 9, price_type_id = 6, position_source_id = 4):
        '''
        Method para obtener market value por posicion de cada cuenta de interÃ©s entre dos fechas.
        IN:
            portfolio: Vector con las cuentas de interÃ©s 
                portfolio = ['INVLAE', 'INVLASC',...] o
                portfolio = ['INVLAE']
            position_source: de donde queremos posiciones. El default es 4 por el U_compass. Luego sacaremos con back office
            price_source_id: por default que sea BBG, pero a veces serÃ¡ de interÃ©s tener precios de backoffice, state street...
            price_type_id: por default estÃ¡ en px_last = 6. Puede ser de interÃ©s volumen, clean or dirty
        OUT:
            mkt_value: pandas DataFrame con date_p, aim_account, securityid_id, mkt_value_usd, weight
        '''
        
        positions = self.positions(portfolio, position_source_id = position_source_id)
        lista_securities = positions['securityid_id']
        prices = self.prices(lista_securities, price_source_id = price_source_id, price_type_id = price_type_id)
        currency_values = self.get_currency_values()
        mkt_val_df = positions.merge(prices, how = 'left', left_on = ['securityid_id', 'date_p'], right_on = ['securityid_id', 'date_p'])
        mkt_val_df = mkt_val_df.merge(currency_values, how = 'left', left_on = ['date_p', 'currency_id'], right_on = ['date_c', 'currency_id'])
        mkt_val_df['mkt_value_usd'] = mkt_val_df['position']*mkt_val_df['price']/mkt_val_df['value']
        mkt_fund = mkt_val_df[['aim_account','mkt_value_usd', 'date_p']].groupby(by = ['aim_account', 'date_p']).sum().reset_index().rename(columns = {'mkt_value_usd':'fund_aum'})
        mkt_val_df = mkt_val_df.merge(mkt_fund, on = ['aim_account', 'date_p'])
        mkt_val_df['weight'] = mkt_val_df['mkt_value_usd']/mkt_val_df['fund_aum']
        return mkt_val_df
    def prices(self, lista_securities, price_source_id = 9, price_type_id = 6, fecha_ini = '', fecha_fin = '', in_usd = False):
        '''
        Obtiene precios de todos los securities cargados entre dos fechas
        IN:
            price_source_id: por default que sea BBG, pero a veces serÃ¡ de interÃ©s tener precios de backoffice, state street...
            price_type_id: por default estÃ¡ en px_last = 6. Puede ser de interÃ©s volumen, clean or dirty
            fecha_ini : opcional para ver si necesito precios hacia atrás para comparar con cartera de hoy. Pensando en una matriz de covarianzas o un capacity.
                        Por default está en vacío.
            fecha_fin : opcional para ver si necesito precios hacia atrás para comparar con cartera de hoy. Pensando en una matriz de covarianzas o un capacity.
                        Por default está en vacío.
            in_usd :    String opcional. Indica si se quiere que los precios de una vez sean convertidos a dólares.
                        Esto en contexto de una matriz de varianzas y covarianzas o un Performance Attribution.
        OUT:
            prices: pandas DataFrame con los precios de los securities. Trae securityid_id, price, price_type_id, date_p
        '''
        if(fecha_ini == ''):
            fecha_ini = self.date_ini
        if(fecha_fin == ''):
            fecha_fin = self.date_fin
        #conn = self.conexion_sm()
        lista_securities = self._port_list_queries(lista_securities)
        query_str = "SELECT Prices.securityid_id, price, price_type_id, price_source_id, date_p, currency_id FROM Prices LEFT JOIN (SELECT securityid_id, currency_id FROM Equity_characteristics UNION SELECT securityid_id, currency_id FROM Fixed_characteristics) AS Characteristics ON Characteristics.securityid_id = Prices.Securityid_id WHERE price_type_id = " + str(price_type_id) + " AND price_source_id = " + str(price_source_id) + " AND (date_p BETWEEN '" + fecha_ini + "' AND '" + fecha_fin + "')" + "AND Prices.securityid_id IN " + lista_securities + "ORDER BY date_p DESC"
        #df_prices = pd.read_sql_query(query_str, conn)
        df_prices = pd.read_sql_query(query_str, self.engine)
        df_prices['currency_id'] = df_prices['currency_id'].fillna(0).astype(int)
        df_prices['currency_id'] = df_prices['currency_id'].astype(str)
        if(in_usd == True):
            currency_values = self.get_currency_values()
            df_prices = df_prices.merge(currency_values, how = 'left', left_on = ['date_p', 'currency_id'], right_on = ['date_c', 'currency_id'])
            df_prices['price_usd'] = df_prices['price']/df_prices['value']
        return df_prices
    def positions(self, portfolio, position_source_id = 4):
        '''
        Obtiene positions de una lista de portafolios entre dos fechas.
        IN:
            portfolio: Vector con las cuentas of interest 
                portfolio = ['INVLAE', 'INVLASC',...] o
                portfolio = ['INVLAE']
            position_source: de dÃ³nde queremos posiciones. El default es 4 por el U_compass. Luego sacaremos con back office
        OUT:
            df_positions: pandas DataFrame con funds_id, securityid_id, position, date_p, aim_account
        '''
        #conn = self.conexion_sm()
        accounts_string = self._port_list_queries(portfolio)
        # Aqui el cambio puse el Position.
        query_str = "SELECT funds_id, Position.securityid_id, position, date_p, aim_account, issuerid_id FROM Position INNER JOIN Funds ON Position.funds_id = Funds.id INNER JOIN SecurityId on Position.securityid_id = SecurityId.id INNER JOIN IssuerId ON SecurityId.issuerid_id = IssuerId.ID WHERE Position.position_source_id = " + str(position_source_id) + " AND (date_p BETWEEN '" + self.date_ini + "' AND '" + self.date_fin + "')" + " AND aim_account IN " + accounts_string
        #df_positions = pd.read_sql_query(query_str, conn)
        df_positions = pd.read_sql_query(query_str, self.engine)
        return df_positions
    def carga_new_gscore(self, tol = 0.005):
        '''
        Method para cargar los g_scores que manipula Ricardo Miranda con su macro tras mandarle la descarga del sitio de ESG.
        Necesita tener en r"Compass Group->Riesgo Financiero - Documents->Data Bases->Security Master->Scripts->Uploads->Esg"
        Un archivo csv del tipo g_score_aaaammdd.csv
        IN :
        tol : decimal, optional
            tolerancia a partir de la cual considero que un gscore es nuevo para cargarlo
        '''
        df_gscore = self.get_latest_gscore()[['issuerid_id', 'g_score', 'date_e']].rename(columns = {'g_score':'g_score_sm', 'date_e': 'date_e_sm'})
        df_new_all = pd.read_csv(r'Compass Group\Riesgo Financiero - Documents\Data Bases\Security Master\Scripts\Uploads\Esg\gscore_20220331.csv')[['issuerid_id', 'g_score', 'date_e']].rename(columns = {'g_score':'g_score_new'})
        df_new_gscore = df_new_all
        df_new_gscore['g_score_new'] = df_new_gscore['g_score_new']/100
        df_new_gscore = df_new_gscore.merge(df_gscore, on = 'issuerid_id', how = 'left')
        df_new_gscore['diff'] = np.abs(df_new_gscore['g_score_new']-df_new_gscore['g_score_sm'])
        g_score_act = df_new_gscore[df_new_gscore['diff']>0.005]
        g_score_act = g_score_act.rename(columns = {'g_score_new':'g_score'})
        #g_score_act['date_e'] = self.date_fin
        g_score_act = g_score_act[['issuerid_id', 'g_score', 'date_e']]
        
        gscore_newiss = df_new_all.merge(df_gscore, on = 'issuerid_id', how = 'left')
        gscore_newiss = gscore_newiss[gscore_newiss['g_score_sm'].isna()]
        gscore_newiss = gscore_newiss.rename(columns = {'g_score_new':'g_score'})
        #gscore_newiss['date_e'] = self.date_fin
        gscore_newiss = gscore_newiss[['issuerid_id', 'g_score', 'date_e']]
        g_score_act = pd.concat([g_score_act, gscore_newiss], ignore_index = True)
        reporte_nuevos = g_score_act.copy()
        secissall = self.get_security_issuer_all()[['issuerid_id', 'compass_issuer_name']].drop_duplicates()
        reporte_nuevos = reporte_nuevos.merge(secissall, how = 'left', on = 'issuerid_id')
        print(self.insert_into_security_master('Esg', g_score_act))
        return reporte_nuevos
    def carga_new_tp(self, tol = 0.001):
        '''
        Method para mostrar el DataFrame que se 'propone' cargar al Security Master.
        Toma los target que están en Mongo, los compara con los target de nuestro security master y propone un dataframe a cargar
        IN:
            tol.- tolerancia para considerar una diferencia en TP como suficiente para considerarse nuevo. Default es 0.5%
        OUT:
            df.- pandas DataFrame con los targets nuevos
        '''
        new_tp_mongo = self.get_new_tp_mongo().rename(columns = {'target_price': 'tp_mongo', 'date_e':'date_e_mongo'})
        old_tp = self.get_latest_TP().rename(columns = {'target_price': 'tp_sm', 'date_e':'date_e_sm'})
        secid_issid = self.get_security_issuer_all()
        old_tp = old_tp.merge(secid_issid, on = 'securityid_id', how = 'left')
        df = new_tp_mongo.merge(old_tp, on = 'issuerid_id', how = 'left')
        df = df[df['tp_mongo']>0]
        df['diff_tp'] = (np.abs(df['tp_mongo'] - df['tp_sm']))/df['tp_mongo']
        
        
        
        type_to_string = {-1.0: 'sell', 0.0: 'hold', 1.0:'buy'}
        type_to_string = pd.DataFrame.from_dict(type_to_string, orient = 'index', columns = ['recommendation_string']).reset_index()
        type_to_string.rename(columns = {'index':'type_number'}, inplace = True)
        df = df.merge(type_to_string, how = 'left', left_on = 'type_recommendation_id', right_on = 'type_number')
        df.drop('type_number', axis = 1, inplace = True)
        df.rename(columns = {'recommendation_string':'recommendation_mongo'}, inplace = True)
        
        df = df.merge(type_to_string, how = 'left', left_on = 'recommendation', right_on = 'recommendation_string')
        df.drop('recommendation_string', axis = 1, inplace = True)
        df.rename(columns = {'type_number':'type_recommendation_sm'}, inplace = True)

        df_new_tp = df[df['diff_tp'].isnull()]    
        df = df[df['diff_tp']>tol]

        return df, df_new_tp
    def get_currency_values(self):
        '''
        Method para obtener valores de las monedas con respecto al dÃ³lar estadounidense.
        Además pivotea por el 'otro' currency_id.
        IN:
            Nada, toma los dates de los atributos del objeto
        OUT:
            df_currency: pandas DataFrame con date_c, currency_id, value
        '''
        #conn = self.conexion_sm()
        query_str = "SELECT currency_id, value, date_c FROM Currency_values WHERE (date_c BETWEEN '" + self.date_ini + "' AND '" + self.date_fin + "')"
        #currency_values = pd.read_sql_query(query_str, conn)
        currency_values = pd.read_sql_query(query_str, self.engine)
        #Condition list trae los que vienen en la tabla de Currency_values, son los de la carga
        conditionlist = [
            (currency_values['currency_id'] == 14) ,
            (currency_values['currency_id'] == 18),
            (currency_values['currency_id'] == 15),
            (currency_values['currency_id'] == 19),
            (currency_values['currency_id'] == 17),
            (currency_values['currency_id'] == 16),
            (currency_values['currency_id'] == 25),
            (currency_values['currency_id'] == 26),
            (currency_values['currency_id'] == 27),
            (currency_values['currency_id'] == 13)]
        #Choicelist trae los currencyid de Equity_characteristics
        choicelist = ["7","12","8","9","10","11","22","20","21","13"]
        currency_values['currency_id'] = np.select(conditionlist, choicelist, default='Not Specified').astype(str)
        dates = currency_values['date_c'].drop_duplicates()
        for date_iter in dates:
            #currency_values = currency_values.append({'currency_id': str(13), 'value': 1, 'date_c': date_iter}, ignore_index = True)
            curr_temp_append = pd.DataFrame({'currency_id': [str(13)], 'value': [1], 'date_c': [date_iter]})
            currency_values = pd.concat([currency_values, curr_temp_append], ignore_index = True, axis = 0)
        return currency_values
    def get_transactions(self, fecha_prev = ''):
        '''
        Obtiene tabla con las transacciones de los fondos de interés
        IN:
            portfolio: lista de la forma ['aim_account1', 'aim_account2', ..., 'aim_accountk']
                Los portafolios para los cuales es de interés sacar las transacciones
        OUT:
            df_transactions: pandas DataFrame con aim_account, securityid y diversas cosas de transacciones
        '''
        #conn = self.conexion_sm()
        if fecha_prev == '':
            fecha_prev = self.date_ini
        query_str = '''SELECT 
                        Transactions.securityid_id as securityid_id, Transactions.funds_id  as funds_id, quantity, net_amount_usd,  trade_date, Transaction_type.type
                        FROM Transactions
                        INNER JOIN Funds
                        ON Transactions.funds_id = Funds.id
                        INNER JOIN Transaction_type
                        ON Transactions.transaction_type_id = Transaction_type.id'''
                        #WHERE Funds.aim_account IN '''
        #query_str = query_str + lista_ports_query
        query_str = query_str + " AND (Transactions.trade_date BETWEEN '" + fecha_prev + "' AND '" + self.date_fin + "')"
        #df_transactions = pd.read_sql_query(query_str, conn)
        df_transactions = pd.read_sql_query(query_str, self.engine)
        return df_transactions
    def get_latest_TP(self):
        '''
        Method para obtener un dataframe con los últimos target prices y recomendaciones de equity
        OUT: pandas DataFrame con securityid_id, tp, recommendation, target_price, date_e
        '''
        #conn = self.conexion_sm()
        query_string = "SELECT securityid_id, recommendation, target_price, date_e FROM Equity_recommendation INNER JOIN Type_recommendation ON Equity_recommendation.type_recommendation_id = Type_recommendation.id ORDER BY date_e DESC"
        #df = pd.read_sql_query(query_string, conn)
        df = pd.read_sql_query(query_string, self.engine)
        df = df.drop_duplicates(subset = ['securityid_id'], keep = 'first')
        return df
    def get_all_TP(self):
        '''
        Method para obtener un dataframe con todos los target prices y recomendaciones de equity de la historia
        OUT: 
            df.- pandas DataFrame con securityid_id, tp, recommendation, target_price, date_e
        '''
        #conn = self.conexion_sm()
        query_string = "SELECT securityid_id, recommendation, target_price, date_e FROM Equity_recommendation INNER JOIN Type_recommendation ON Equity_recommendation.type_recommendation_id = Type_recommendation.id ORDER BY date_e DESC"
        #df = pd.read_sql_query(query_string, conn)
        df = pd.read_sql_query(query_string, self.engine)
        return df
    def get_latest_gscore(self):
        '''
        Method para obtener un dataframe con los últimos gscores
        OUT:
            df.- pandas DataFrame con issuerid_id, g_score, date_e, compass_issuer_alias, compass_issuer_name
        '''
        #conn = self.conexion_sm()
        query_string = "SELECT IssuerId.id as issuerid_id, g_score, date_e, compass_issuer_alias, compass_issuer_name FROM IssuerId LEFT JOIN Esg ON Esg.issuerid_id = IssuerId.id ORDER BY date_e DESC"
        #df = pd.read_sql_query(query_string, conn)
        df = pd.read_sql_query(query_string, self.engine)
        df = df.drop_duplicates(subset = ['issuerid_id'], keep = 'first')
        return df
    def get_hist_internal_ratings(self):
        '''
        Method para sacar internal ratings de toda la historia. Se extiende del "get_hist_ratings"
        '''
        return self.get_hist_ratings(rating_source_id = 3)
    def get_latest_internal_ratings(self):
        '''
        Method para sacar internal ratings más recientes. Se extiende del "get_latest_ratings"
        '''
        return self.get_latest_ratings(rating_source_id = 3)
    def get_all_gscore(self):
        '''
        Method para obtener un dataframe con los todos los g_scores
        OUT:
            df.- pandas DataFrame con issuerid_id, g_score, date_e, compass_issuer_alias, compass_issuer_name
        '''
        #conn = self.conexion_sm()
        query_string = "SELECT IssuerId.id as issuerid_id, g_score, date_e, compass_issuer_alias, compass_issuer_name FROM Esg INNER JOIN IssuerId ON Esg.issuerid_id = IssuerId.id ORDER BY date_e DESC"
        #df = pd.read_sql_query(query_string, conn)
        df = pd.read_sql_query(query_string, self.engine)
        return df
    def get_all_from_id(self, identif, by = 'issuerid_id', with_position = False):
        '''
        Method para sacar todas las características de un identificador
        IN:
            identif: lista con identificadores que se van a emplear para la búsqueda. Deben ser el mismo tipo de identificador (no mezclar issuerid con isin)
                Ejemplo:
                    identif = ['MX001860'] (esto es issuerid_id)
                    identif = ['MX001860EQ00001'] (securityid_id)
                    identif = ['Qualitas'] (es parte del compass_issuer_name, la coincidencia se hace con "LIKE '%XXX%' ")
                    identif = ['BRAMERACNOR6', 'BRCSANACNOR6'] (con isin)
            with_position: boolean (True o False) para indicar si se quiere pivotear con los holdings. Esto es principalmente para control de cargas, ya que si no está en los portafolios no aparecería en la búsqueda
                Si lo tenemos entonces nos arriesgamos a que el Query demore demasiado al jalar toda la posición por darle un "OR" a la fecha de posición
            by: tipo de identificador para la búsqueda. Puede ser de interés por nombre, issuerid_id o securityid_id.
                Esto es muy modular entonces más tarde podemos hacer la búsqueda por otros criterios como obtener todo de un sector, con ticker, con isin...
                Opciones:
                    by = 'issuerid_id'
                    by = 'securityid_id'
                    by = 'compass_issuer_name'
                    by = 'isin'
                    by = 'compass_issuer_alias'
                    by = 'ticker_issuer'
                    by = 'analyst_name'
        OUT:
            df: pandas DataFrame con todos los campos relacionados
        '''
        #conn = self.conexion_sm()
        lista_exacta = ['issuerid_id', 'securityid_id', 'isin']
        lista_parcial = ['compass_issuer_name', 'compass_issuer_alias', 'ticker_issuer', 'analyst_name']
        query_string_base = '''SELECT
        IssuerId.id as issuerid_id, compass_issuer_name, compass_issuer_alias, SecurityId.id as securityid_id, ticker_issuer, isin, local_id, sedol, figi, Country.country, Compass_sectors.sector, Compass_industry.industry, Analyst.analyst_name, Coverage.date_in, Currency.currency'''
        if(with_position):
            query_string_base = query_string_base + ''', Funds.aim_account'''
        query_string_base = query_string_base + ''' FROM IssuerId
        INNER JOIN SecurityId
        ON IssuerId.id = SecurityId.issuerid_id
        LEFT JOIN Issuer_characteristics
        ON IssuerId.id = Issuer_characteristics.issuerid_id
        LEFT JOIN Country
        ON Country.id = Issuer_characteristics.country_id
        LEFT JOIN Compass_sectors
        ON Compass_sectors.id = Issuer_characteristics.compass_sectors_id
        LEFT JOIN Compass_industry
        ON Compass_industry.id = Issuer_characteristics.compass_industry_id
        LEFT JOIN Coverage
        ON Coverage.issuerid_id = IssuerId.id
        LEFT JOIN Analyst
        ON Analyst.id = Coverage.analyst_id'''
        if(with_position):
            query_string_base = query_string_base + ''' LEFT JOIN Position
            ON SecurityId.id = Position.securityid_id
            LEFT JOIN Funds
            ON Position.funds_id = Funds.id'''
        query_string_base = query_string_base + ''' LEFT JOIN (SELECT securityid_id, currency_id
			FROM Equity_characteristics
			UNION ALL
			SELECT securityid_id, currency_id
			FROM Fixed_characteristics) AS Characteristics ON Characteristics.securityid_id = SecurityId.id
        LEFT JOIN Currency ON Characteristics.currency_id = Currency.id
        WHERE 
        '''
        
        if (with_position):
            query_string_base = query_string_base + "Position.date_p = '" + self.date_ini + "' AND "
        if by in lista_parcial:            
            res = self._switch_by_get_all_from_id(identif[0], by)
            query_string_iter = query_string_base + res 
            #df = pd.read_sql_query(query_string_iter, conn)
            df = pd.read_sql_query(query_string_iter, self.engine)
            for i in range(1,len(identif)):
                res = self._switch_by_get_all_from_id(identif[i], by)
                query_string_iter = query_string_base + res
                #df = df.append(pd.read_sql_query(query_string_iter, conn))
                #df = pd.concat([df, pd.read_sql_query(query_string_iter, conn)], ignore_index = True, axis = 0)
                df = pd.concat([df, pd.read_sql_query(query_string_iter, self.engine)], ignore_index = True, axis = 0)
        else:
            
            search_exacta_list = self._port_list_queries(identif)
            res = self._switch_by_get_all_from_id(search_exacta_list, by)
            
            query_string_iter = query_string_base + res
            #df = pd.read_sql_query(query_string_iter, conn)
            df = pd.read_sql_query(query_string_iter, self.engine)
        df = df.drop_duplicates()
        df = df.sort_values(by = 'date_in', ascending = False)
        return df
    def get_security_issuer_all(self):
        '''
        Method para obtener todos los securities con issuers y sus distintos nombres/aliases
        OUT:
            df.- pandas DataFrame con básicamente SecurityId e IssuerId con el inner join
        '''
        #conn = self.conexion_sm()
        query_string = "SELECT SecurityId.id as securityid_id, IssuerId.id as issuerid_id, compass_issuer_name, SecurityId.ticker_issuer FROM SecurityId INNER JOIN IssuerId ON SecurityId.issuerid_id = IssuerId.id"
        #df = pd.read_sql_query(query_string, conn)
        df = pd.read_sql_query(query_string, self.engine)
        return df
    def get_equity_characteristics(self, ids):
        '''
        Method para sacar Equity_characteristics de una lista
        Parameters
        ----------
        ids : lista con security_id de equity
            Piensa en: ['BR000250EQ00010', 'BR000250EQ00011']
        Returns
        -------
        df.- pandas DataFrame con adr per share y currency
        '''
        #conn = self.conexion_sm()
        lista_ids = self._port_list_queries(ids)
        query_string = "SELECT * FROM Equity_characteristics WHERE securityid_id IN " + lista_ids
        #df = pd.read_sql_query(query_string, conn)
        df = pd.read_sql_query(query_string, self.engine)
        return df
    def insert_into_security_master(self, target, dataframe):
        '''
        Method para insertar una tabla al Security_Master
        IN:
            target.- tabla objetivo. Digamos:
                SecurityId, IssuerId, Prices, Equity_recommendation...
            dataframe.- pandas DataFrame a cargar.
        '''
        try:
            dataframe.to_sql(target, con= self.engine, if_exists= 'append', index=False, chunksize=300, method='multi')
            print('Se insertó correctamente.')
            ban = True
        except:
            print("Error insertando al SM. Try again.")
            ban = False
        return ban
    def _aimaccount2id(self, aim_accounts):
        '''
        Function privada que pivotea la clave de aim_account a funds_id
        IN:
            aim_accounts: lista con las cuentas de interÃ©s de la forma ['INVLAE', 'INVLASC']
        OUT:
            df_aim_id: pandas DataFrame con funds_id, fund_name, aim_account
        '''
        df_ports = self._portfolio_dict()
        accounts = pd.DataFrame(aim_accounts, columns = 'aim_account')
        df_aim_id = accounts.merge(df_ports, on = 'aim_account', how = 'left')
        return df_aim_id
    def _portfolio_dict(self):
        '''
        Method privado para seleccionar diccionario de funds_id y aim_account
        OUT:
            df_ports: pandas DataFrame con id, fund_alias
        '''
        #conn = self.conexion_sm()
        #df_ports = pd.read_sql_query('SELECT id as funds_id, fund_name, aim_account FROM Funds', conn)
        df_ports = pd.read_sql_query('SELECT id as funds_id, fund_name, aim_account FROM Funds', self.engine)
        return df_ports
    def dict_rating_score_letter(self):
        '''
        Method que entrega el diccionario de rating score con el rating de letra.
        OUT:
            dict_rating.- pandas DataFrame con ['score', 'compass_rating', 'compass_subrating']
        '''
        score = [-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]
        compass_subrating = ['NR', 'Cash', 'AAA', 'AA+', 'AA', 'AA-', 'A+', 'A', 'A-', 'BBB+', 'BBB', 'BBB-', 'BB+', 'BB', 'BB-', 'B+', 'B', 'B-', 'CCC+', 'CCC', 'CCC-', 'CC', 'C', 'D']
        compass_rating = ['NR', 'Cash', 'AAA', 'AA', 'AA', 'AA', 'A', 'A', 'A', 'BBB', 'BBB', 'BBB', 'BB', 'BB', 'BB', 'B', 'B', 'B', 'CCC', 'CCC', 'CCC', 'CC', 'C', 'D']
        dict_rating = pd.DataFrame({'score': score, 'compass_rating':compass_rating, 'compass_subrating':compass_subrating})
        return dict_rating
    def internal_external_conversion(self):
        '''
        Method que regresa un DataFrame para pivotear score de external ratings con compass_rating, compass_subrating, score_internal_rating e internal_rating en letra
        OUT:
            pandas DataFrame cuyas columnas son:
                ['score', 'compass_rating', 'compass_subrating', 'score_ir', 'internal_rating']
        '''
        internal_rating_conversion = {0: 'NR', 1: 'CCC+', 2: 'B-', 3:'B', 4: 'B+', 5: 'BB-', 6: 'BB', 7: 'BB+', 8:'BBB-', 9:'BBB'}
        ig_ir = {n:'BBB+' for n in range(10, 30)}
        internal_rating_conversion.update(ig_ir)
        df_ir = pd.DataFrame.from_dict(internal_rating_conversion, orient='index').reset_index()
        df_ir.columns = ['score_ir', 'internal_rating']
        escala_udcompass_rating = self.dict_rating_score_letter()
        full_rating_conversion = escala_udcompass_rating.merge(df_ir, how = 'left', left_on = 'compass_subrating', right_on = 'internal_rating')
        return full_rating_conversion
    def conexion_sm(self, server = 'tcp:cgp-db-bi,64468', database = 'Security_Master', username = 'usr_smaster', password = 'XB5D#LhB'):
        '''
        Crea conection a SQL Server para el Security_Master
        IN:
            server.- Nombre del servidor. Default ya estÃ¡ definido para no andar arrastrando variables
            database.- Por default es SecurityMaster
            username.- Usuario de Python. Afterwards se vuelve relevante para usuarios de solo consulta
            password.- Self explanatory.
        OUT:
            conn.- connection a SQL. Es auxiliar, de uso interno a la clase pero se puede emplear para custom queries.
        '''
        #tcp:cgp-db-bi,64468
        #conn = create_engine('mssql://*username*:*password*@*server_name*/*database_name*')
        #engine_string = 'mssql://' + username + ':' + 'password' + '@'+server+'/'+database
        #conn = create_engine(engine_string)
        conn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        #
        return conn 
    def _port_list_queries(self, lista):
        '''
        Toma una lista de lo que sea y transforma el input para que quede de la forma
        SELECT ... WHERE variable_de_interes IN ('acc1', 'acc2', ... 'accn')
        IN:
            lista: lista con los identificadores
                ['INVLASC', 'INVLAE']
                ['BR000380', ...]
        OUT:
            accounts_string: string con el formato deseado
            ('acc1', 'acc2', ... 'accn')
        '''
        accounts_string = "('"
        n = len(lista)
        for index, elem in enumerate(lista):
            if (index+1<n):
                accounts_string = accounts_string + elem + "', '"
            else:
                accounts_string = accounts_string + elem + "')"
        return accounts_string
    def _switch_by_get_all_from_id(self, identif, by):
        '''
        Method privado para obtener la cadena que completa el query para traer todos los atributos de interés.
        IN:
            identif: identificador que se va a emplear para la búsqueda.
                Ejemplo:
                    identif = 'MX001860' (esto es issuerid_id)
                    identif = 'MX001860EQ00001' (securityid_id)
                    identif = 'Qualitas' (es parte del compass_issuer_name, la coincidencia se hace con "LIKE '%XXX%' ")
                    identif = 'BRAMERACNOR6' (isin)
                    identif = 'GMEXICO' (el compass_issuer_alias)
            by: tipo de identificador para la búsqueda. Puede ser de interés por nombre, issuerid_id o securityid_id.
                Esto es muy modular entonces más tarde podemos hacer la búsqueda por otros criterios como obtener todo de un sector, con ticker, con isin...
                Opciones:
                    by = 'issuerid_id'
                    by = 'securityid_id'
                    by = 'compass_issuer_name'
                    by = 'isin'
                    by = 'compass_issuer_alias'
                    by = 'ticker_issuer'
                    by = 'analyst_name'
        OUT:
            Lo que completa el query de coincidencias
        '''
        switcher = {
            'issuerid_id': "IssuerId.id IN " + identif + "",
            'securityid_id': "SecurityId.id IN " + identif + "",
            'compass_issuer_name': "IssuerId.compass_issuer_name LIKE '%" + identif + "%'",
            'isin': "SecurityId.isin IN " + identif + "",
            'compass_issuer_alias': "IssuerId.compass_issuer_alias LIKE'%" + identif + "%'",
            'ticker_issuer': "SecurityId.ticker_issuer LIKE '%" + identif + "%'",
            'analyst_name': "Analyst.analyst_name LIKE '%" + identif + "%'"
        }
        res = switcher.get(by, "ERROR")
        return res     
    def _switch_cascada_precios(self, uso):
        
        '''
        Method para generar la lista de cascada de precios 
        IN:
            uso : string que trae el objetivo de la cascada.
            Por ejemplo:
                'equity_regional': fondos tipo LAE, LASC, etc. Privilegiaré DataLicense y Bloomberg sobre otras fuentes
                'equity_mx': fondos locales de México. Privilegiaré Valmer y Jonima sobre Bloomberg
                'fixed_income_regional': fondos tipo LACD, LAIG, LACDHY, LAHY2USD
                'fixed_income_mx': fondos como I+CORP, I+PLAZO donde privilegio ValmerMX y Jonima
                'fixed_income_argentina': todas las carteras argentinas. Privilegiaré ARG-ARS, ARG-USD
        OUT:
            lista: lista de enteros de la forma [source1, source2, ..., sourcen]
            Va a traer el orden de la cascada de precios
        '''
        switcher = {
            'equity_regional': [10, 9, 2],#data license, Universo, yahoofinance
            'equity_mx': [6, 10, 9, 2]
        }
        res = switcher.get(uso, "ERROR")    
        return res            
    def get_new_tp_mongo(self):
        '''
        Method para obtener la base de datos de Mongo con los target prices más recientes, sirve como input para comparar con lo actual
        OUT:
            df.- pandas DataFrame con IssuerId.compass_issuer_alias, IssuerId.id, date_tp, tp, Type_recommendation.id
        '''
        
        if getpass.getuser().lower() == 'pgarza':
            cluster= MongoClient("mongodb://ntividor:testuser@assetmanagement-shard-00-00.08zxk.mongodb.net:27017,assetmanagement-shard-00-01.08zxk.mongodb.net:27017,assetmanagement-shard-00-02.08zxk.mongodb.net:27017/test?replicaSet=atlas-p8cmv2-shard-0&ssl=true&authSource=admin",  ssl_cert_reqs=False)
        else:
            cluster= MongoClient("mongodb+srv://ntividor:testuser@assetmanagement.08zxk.mongodb.net/asman?retryWrites=true&w=majority", ssl_cert_reqs=False)
        
        db=cluster['asman']
        estimates=db['estimates']
        c=[]
        result = estimates.aggregate([
            {
                '$lookup': {
                    'from': 'issuers',
                    'localField': 'Est_Issuer_Compass_Id',
                    'foreignField': 'Issuer_Compass_Id',
                    'as': 'Alias'
                }
            },
        
            {"$match":{'Current':1}},
        
            {"$match":{"Est_Account_Id":{"$in":[39,43]}}},
        
            {'$unwind':'$Alias'},
            {'$unwind':'$Estimates'},
            {'$project':{"_id":0,
                         "Est_Account_Id":1,
                         'Est_Issuer_Compass_Id':1,
                         "Est_Period":"$Estimates.Est_Period",
                         "Est_Period":1,
                         "Est_Value":"$Estimates.Est_Value",
                         "Issuer_Compass_Alias":"$Alias.Issuer_Compass_Alias",
                         "Est_Forecast_Date":1}},
        
        ])
        for r in result:
            c.append(r)
        bd=pd.DataFrame(c).set_index('Issuer_Compass_Alias')
        bd.rename(columns={'Est_Account_Id':'Account_Id','Est_Value':'Value',
                            'Est_Issuer_Compass_Id':'issuerid_id',
                           'Est_Forecast_Date':'date_e'},inplace=True)
        bd1=bd.reset_index()
        bd1['Value'] = bd1['Value'].astype(str).str.strip()
        bd1['Value'] = bd1['Value'].astype(str).str.replace("HOLD", "0")
        bd1['Value'] = bd1['Value'].astype(str).str.replace("BUY", "1")
        bd1['Value'] = bd1['Value'].astype(str).str.replace("SELL", "-1")
        bd1['Value']=pd.to_numeric(bd1['Value'], errors = 'coerce').dropna()
        df = pd.pivot_table(data=bd1, index=['Issuer_Compass_Alias','issuerid_id','date_e'],columns=['Account_Id'], values='Value')
        df = df.reset_index()
        df = df.rename(columns={39:'target_price',43:'type_recommendation_id'})
        df = df[['issuerid_id', 'Issuer_Compass_Alias', 'target_price', 'type_recommendation_id', 'date_e']]
        df = df.sort_values(by = 'Issuer_Compass_Alias')
        return df
    def get_new_internal_ratings_mongo(self):
        '''
        Method para obtener la base de datos de Mongo con los target prices más recientes, sirve como input para comparar con lo actual
        OUT:
            df.- pandas DataFrame con IssuerId.compass_issuer_alias, IssuerId.id, date_tp, tp, Type_recommendation.id
        '''        
        if getpass.getuser().lower() == 'pgarza':
            cluster= MongoClient("mongodb://ntividor:testuser@assetmanagement-shard-00-00.08zxk.mongodb.net:27017,assetmanagement-shard-00-01.08zxk.mongodb.net:27017,assetmanagement-shard-00-02.08zxk.mongodb.net:27017/test?replicaSet=atlas-p8cmv2-shard-0&ssl=true&authSource=admin",  ssl_cert_reqs=False)
        else:
            cluster= MongoClient("mongodb+srv://ntividor:testuser@assetmanagement.08zxk.mongodb.net/asman?retryWrites=true&w=majority", ssl_cert_reqs=False)        
        db=cluster['asman']
        estimates=db['estimates']
        c=[]
        result = estimates.aggregate([
            {
                '$lookup': {
                    'from': 'issuers',
                    'localField': 'Est_Issuer_Compass_Id',
                    'foreignField': 'Issuer_Compass_Id',
                    'as': 'Alias'
                }
            },
            {"$match":{'Current':1}},
            {"$match":{"Est_Account_Id":{"$in":[44]}}},
            {'$unwind':'$Alias'},
            {'$unwind':'$Estimates'},
            {'$project':{"_id":0,
                         "Est_Account_Id":1,
                         'Est_Issuer_Compass_Id':1,
                         "Est_Period":"$Estimates.Est_Period",
                         "Est_Period":1,
                         "Est_Value":"$Estimates.Est_Value",
                         "Issuer_Compass_Alias":"$Alias.Issuer_Compass_Alias",
                         "Est_Forecast_Date":1}},
        
        ])
        for r in result:
            c.append(r)
        bd=pd.DataFrame(c).set_index('Issuer_Compass_Alias')
        bd.rename(columns={'Est_Account_Id':'Account_Id','Est_Value':'Value',
                            'Est_Issuer_Compass_Id':'issuerid_id',
                           'Est_Forecast_Date':'date_e'},inplace=True)
        bd1=bd.reset_index()
        bd1 = bd1[['Issuer_Compass_Alias', 'issuerid_id', 'date_e', 'Value']]
        bd1 = bd1.rename(columns = {'Issuer_Compass_Alias':'compass_issuer_alias', 'Value':'internal_rating_score'})
        df = bd1.sort_values(by = 'compass_issuer_alias')
        return df
    def get_new_gscore_mongo(self):
        '''
        Method que extrae los gscores disponibles en Mongo.
        OUT:
            df: pandas DataFrame con date_e, issuerid_id, g_score
        '''
        if getpass.getuser().lower() == 'pgarza':
            cluster= MongoClient("mongodb://ntividor:testuser@assetmanagement-shard-00-00.08zxk.mongodb.net:27017,assetmanagement-shard-00-01.08zxk.mongodb.net:27017,assetmanagement-shard-00-02.08zxk.mongodb.net:27017/test?replicaSet=atlas-p8cmv2-shard-0&ssl=true&authSource=admin",  ssl_cert_reqs=False)
        else:
            cluster= MongoClient("mongodb+srv://ntividor:testuser@assetmanagement.08zxk.mongodb.net/asman?retryWrites=true&w=majority", ssl_cert_reqs=False)
        db = cluster['asman']
        esg = pd.DataFrame(db['esg-forms'].find({}, {
            '_id': 0
        }))
        data = db['esg-forms'].aggregate([
            {"$unwind":"$data"},
            {
              "$group": {
                "_id": "$Issuer_Compass_Id",
                "doc": { "$max": { 
                    "date": "$date",
                    "Issuer_Compass_Id": "$Issuer_Compass_Id",
                    "Issuer_Compass_Alias":"$Issuer_Compass_Id",
                    "original_score":"$original_score"
                } }
              }
            },
            {
              "$replaceRoot": { "newRoot": "$doc" }
            }
          ])
        df = pd.DataFrame(data)
        df = df.rename(columns = {'date':'date_e', 'Issuer_Compass_Id': 'issuerid_id', 'original_score':'g_score'})
        df = df.drop_duplicates(subset = ['issuerid_id'])
        df['date_e'] = pd.to_datetime(df['date_e'])
        return df
    def send_email(self, email_sender , email_recipient, email_subject, 
                   email_message,attachment_location = '', html_format = False):
        '''
        Method that sends an email with attachments
        IN: 
            email_sender: email from which the email will be sent
            email_recipient: email list of people to recieve the email
            email_subject: subject of the email
            email_message: body of the message
            attachment_location: path to where the attachment is located
            html_format: to send a part that contains html code
        OUT:
            ret: boolean TRUE if email was sent, FALSE if not sent
        '''
        # Dictionary with the e-mails and passwords of each sender
        email_dict = {'Pablo.Garza@cgcompass.com': 'flqbdctgqkldxnqh', 
                      'Bernardo.Alcantara@cgcompass.com': 'cswnylmkscslvdsd',
                      'daniela.benavides@cgcompass.com': 'dqbcpjlnchvxnzph'}
        
        
        password = email_dict[email_sender]
        if html_format == True:
            msg = MIMEMultipart('mixed')
        else:
            msg = MIMEMultipart()
            
        msg['From'] = email_sender 
        if isinstance(email_recipient, list):
            email_recipient_string = ';'.join(email_recipient)
            msg['To'] = email_recipient_string
        else:
            msg['To'] = email_recipient
        msg['Subject'] = email_subject
        
        if html_format == True:
            msg.attach(MIMEText(email_message, 'html'))
        else:
            msg.attach(MIMEText(email_message, 'plain'))
        
        
        if attachment_location != '':       
            # if list add al files on list
            if isinstance(attachment_location, list): 
                for path_file in attachment_location:
                    filename = os.path.basename(path_file)
                    attachment = open(path_file, "rb")
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(attachment.read())
                    encoders.encode_base64(part)
                    part.add_header('Content-Disposition',
                                    "attachment; filename= %s" % filename)
                    msg.attach(part)       
            else:
                filename = os.path.basename(attachment_location)
                attachment = open(attachment_location, "rb")
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition',
                                "attachment; filename= %s" % filename)
                msg.attach(part)
        try:
            server = smtplib.SMTP('smtp.office365.com', 587)
            server.ehlo()
            server.starttls()
            server.login(email_sender, password)
            text = msg.as_string()
            if isinstance(email_recipient, list):
                for recipient in email_recipient:
                    server.sendmail(email_sender, recipient, text)
            else:
                server.sendmail(email_sender, email_recipient, text)
            print('email sent')
            server.quit()
            not_sent = False
        except:
            print("SMPT server connection error")
            not_sent = True
        if not_sent:
            ret = False
        else: 
            ret = True
        return ret          
    def pandas_to_html(self, path, html_name, df, styler = False):
        '''
        metodo que escribe un archivo de html en la ruta especificada
        IN:
            path: donde escribir el archivo de HTML. OJO debe de ser un directorio
            html_name: nombre que tendrá el archivo de html
            df: pandas a convertir a html
            styler: boolean si es un dataframe con formato, default es False
        OUT:
            ret: string con resultado
        '''
        
        if os.path.exists(path):
            if styler == False:
                html = df.to_html()
            else: 
                html = df.render()
            try:
                text_file = open(path+html_name+'.html', "w") 
                text_file.write(html) 
                text_file.close() 
                ret = True
            except:
                ret = 'Error when writing html'
        else:
            ret = 'Path does not exist'       
        return ret       
    def generic_get_from_sm(self, query_str):
        '''
        Equivalent to the catch-all method for Security_Master inserts.
        This method returns a pandas DataFrame with the resulting table after a "SELECT FROM" query.
        IN:
            query_string: string containing a "SELECT" statement SQL query.
        OUT:
            df_res: pandas DataFrame containing result from the query.
        '''
        #conn = self.conexion_sm()
        try:
            #df_res = pd.read_sql_query(query_str, conn)
            df_res = pd.read_sql_query(query_str, self.engine)
        except:
            print('Error reading query, try again.')
            df_res = pd.DataFrame()
        return df_res
class committee():
    def __init__(self, port, otros_ports, date_ini, date_fin, date_past, dpi = 1000, alpha = 0.7, title_fontsize = 10, axislabel_fontsize = 8, tick_fontsize = 6):
        '''
        Parameters
        ----------
        port : portafolio principal en cuestión pensado en temas de comité. Lista.
            Ejemplo: ['INVLASC']
            Dado que la cuenta de ROCA está partida en dos, se hace una cláusula para unir los portafolios.
        otros_ports : lista de otros portafolios para comparación con el port. Lista de strings
            Ejemplo: ['INVLAE', 'CRECE+']
        date_ini : string de la forma 'aaaa-mm-dd'
            Fecha desde la cual queremos la data
        date_fin : string de la forma 'aaaa-mm-dd'
            Fecha hasta la cual queremos la data
        date_past : lista de strings de la forma ['aaaa1-mm1-dd1', 'aaaa2-mm2-dd2', ..., 'aaaan-mmn-ddn']
            Lista de fechas para fotos anteriores. Esto pensado tipo en un Overlap o para un trend.
        orig : instancia de Security_Master con la fecha final para el portafolio de interés
        otros : instancia de Security_Master con la fecha final para los portafolios contra los que se compara
        past : instancia de Security_Master con la fecha previa del portafolio de interés
        engine : cadena necesaria de conexión para insertar al Security_Master
            Trae un default que se debe cambiar cuando el Security_Master migre de servidor
        dpi : entero para resolución de gráficas
            Default es 1000
        alpha : decimal, transparencia de las gráficas
            Default es 0.7
        title_fontsize : entero. Tamaño de fuente para títulos en gráficas
            Default trae 10
        axislabel_fontsize : entero. Tamaño de fuente para labels de los ejes.
            Default trae 8
        rojo_compass : el rojo para todas las gráficas. En realidad deberíamos definir la paleta de colores.
            Default es '#C00000'
        gris1 : uno de los grises cuando necesitamos otro color que no sea el rojo compass. De nuevo, deberíamos definir una paleta de colores
            Default es '#BFBFBF'
        '''
        if 'ROCKC' in port or 'ROCKC-BR' in port: 
            self.port = ['ROCKC','ROCKC-BR']
        else: 
            self.port = port
        self.otros_ports = otros_ports
        
        self.date_ini = date_ini
        self.date_fin = date_fin
        self.date_past = date_past
        
        self.orig = Security_Master(date_ini = self.date_ini, date_fin = self.date_fin)
        self.orig_mkt_value = self.orig.mkt_value(self.port)
        
        self.orig_foto = Security_Master(date_ini = self.date_fin, date_fin = self.date_fin)
        self.orig_foto_mkt_value =  self.orig_foto.mkt_value(self.port)
        
        self.otros = Security_Master(date_ini = self.date_ini, date_fin = self.date_fin)
        self.otros_mkt_value = self.otros.mkt_value(self.otros_ports)
        
        self.otros_foto = Security_Master(date_ini = self.date_fin, date_fin = self.date_fin)
        self.otros_foto_mkt_value =  self.orig_foto.mkt_value(self.otros_ports)       
        
        self.past = Security_Master(date_ini = self.date_past, date_fin = self.date_past)
        self.past_mkt_value = self.past.mkt_value(self.port)
        
        self.sec_iss_all = self.orig.get_security_issuer_all()
        self.engine = self.orig.engine
        self.dpi = dpi
        self.alpha = alpha
        self.title_fontsize = title_fontsize
        self.axislabel_fontsize = axislabel_fontsize
        self.rojo_compass = '#C00000'
        self.gris1 = '#BFBFBF'
        self.tick_fontsize = tick_fontsize        
    def overlap(self):
        '''
        Method para hacer overlap de un fondo con respecto a otros.
        Out:
            overlap: pandas DataFrame con la tabla que genera Overlap. Se le mete al formato.
        '''

        sec_iss_all = self.orig_foto.get_security_issuer_all()
        cash_rows_secissall = sec_iss_all[sec_iss_all['securityid_id'].str.contains('CA')].index#Líneas que sean cash
        sec_iss_all.loc[cash_rows_secissall, 'issuerid_id'] = 'CA999999'#Cambio el id localmente
        sec_iss_all.loc[cash_rows_secissall, 'compass_issuer_name'] = 'Cash'#Cambio nombre localmente
        sec_iss_all.loc[cash_rows_secissall, 'ticker_issuer'] = 'Cash'#Cambio ticker localmente
        sec_iss = sec_iss_all[['securityid_id', 'issuerid_id']]#Solo secid con issuerid
        sec_iss_all = sec_iss_all[['issuerid_id', 'compass_issuer_name']].drop_duplicates()#Solo issuerid con compass_issuer_name
        cols = ['funds_id', 'position', 'date_p', 'aim_account', 'issuerid_id', 'mkt_value_usd', 'weight']
                
                
        mkt_orig = self.orig_foto_mkt_value
        mkt_orig['weight']=mkt_orig['mkt_value_usd']/mkt_orig['mkt_value_usd'].sum() #Agrego esta linea en caso que el portfolio sea ROCA, y tenga que hacer un recálculo de ROCKC y ROCK_BR
        cash_rows_mkt_orig = mkt_orig[mkt_orig['securityid_id'].str.contains('CA')].index#Líneas que sean cash
        mkt_orig.loc[cash_rows_mkt_orig, 'issuerid_id'] = 'CA999999'#Cambio el id localmente
        mkt_orig = mkt_orig[cols]
                
        relevant_securities = self.orig_foto_mkt_value['securityid_id'].drop_duplicates()#Lista de securities actual
        funds_id_orig = mkt_orig['funds_id'].drop_duplicates().to_list() #Para ver las operaciones del o los fondos en cuestión (ROCA).Aquí no me interesa qué operaron otros fondos.
        # funds_id_orig = mkt_orig['funds_id'][0]#Para ver las operaciones del fondo en cuestión. Aquí no me interesa qué operaron otros fondos.
        transactions = self.orig.get_transactions(fecha_prev = self.date_past)
        transactions=transactions.loc[transactions['funds_id'].isin(funds_id_orig)] #Las del fondo o los fondos en cuestión (ROCA)
        # transactions = transactions[transactions['funds_id']==funds_id_orig]#Las del fondo en cuestión
        transactions = transactions[transactions['securityid_id'].isin(relevant_securities)]#Los securities que sí tengo actualmente. Para ver las compras y ventas TOTALES no necesito transactions, puedo verlo a través del merge
        transactions_grouped = transactions.groupby(['securityid_id', 'type']).sum().reset_index()#Suma por id
        buys = transactions_grouped[transactions_grouped['type']=='buy'].drop(['type'], axis = 1)#Ya sé que todos vienen del mismo fondo y ya filtré para ver de qué tipo son
        sells = transactions_grouped[transactions_grouped['type']=='sell'].drop(['type'], axis = 1)
        joined_transactions = buys.merge(sells, on = 'securityid_id', suffixes = ('_buy', '_sell'), how = 'outer').fillna(0)#el outer merge me deja ver lo que está fuera de la intersección para no contar doble. Al final fillna(0) para hacer el "net"
        joined_transactions['net_transactions'] = joined_transactions['quantity_buy']-joined_transactions['quantity_sell']
        joined_transactions = joined_transactions.merge(sec_iss, on = 'securityid_id', how = 'left')
        net_transactions = joined_transactions[['issuerid_id', 'net_transactions']].groupby('issuerid_id').sum().reset_index()
                       
        mkt_orig['funds_id'] = mkt_orig['funds_id'].astype(str)
                
                
        mkt_otros = self.otros_foto_mkt_value
        cash_rows_mkt_otros = mkt_otros[mkt_otros['securityid_id'].str.contains('CA')].index#Líneas que sean cash
        mkt_otros.loc[cash_rows_mkt_otros, 'issuerid_id'] = 'CA999999'#Cambio el id localmente
        mkt_otros = mkt_otros[cols]
        mkt_otros['funds_id'] = mkt_otros['funds_id'].astype(str)
        mkt_otros = mkt_otros.set_index(['funds_id', 'aim_account'])
                
        mkt_past = self.past_mkt_value
        mkt_past['weight']=mkt_past['mkt_value_usd']/mkt_past['mkt_value_usd'].sum()
        cash_rows_mkt_past = mkt_past[mkt_past['securityid_id'].str.contains('CA')].index#Líneas que sean cash
        mkt_past.loc[cash_rows_mkt_past, 'issuerid_id'] = 'CA999999'#Cambio el id localmente
        mkt_past = mkt_past[['position', 'date_p', 'issuerid_id', 'mkt_value_usd', 'weight']]
        mkt_orig = mkt_orig.groupby('issuerid_id').sum().reset_index()
        mkt_otros = mkt_otros.groupby(['issuerid_id', 'funds_id', 'aim_account']).sum().reset_index()
        mkt_past = mkt_past.groupby('issuerid_id').sum().reset_index()
        mkt_otros = mkt_otros.pivot(index = ['funds_id', 'issuerid_id'], columns = 'aim_account', values = 'weight')
                        
        df_overlap = mkt_orig.merge(mkt_past, on = 'issuerid_id', how = 'outer', suffixes = ('_today', '_old'), indicator = True)
        df_overlap = df_overlap.merge(net_transactions, on = 'issuerid_id', how = 'left')
        df_overlap['_merge'] = df_overlap['_merge'].replace({'left_only': 'New', 'right_only': 'Out', 'both': 'Kept'})
        df_overlap.loc[df_overlap['net_transactions']<0, '_merge_transaction'] = 'Sold'
        df_overlap.loc[df_overlap['net_transactions']>0, '_merge_transaction'] = 'Bought'
        df_overlap['_merge_transaction'] = df_overlap['_merge_transaction'].fillna(df_overlap['_merge'])
        df_overlap['_merge'] = df_overlap['_merge_transaction']
        df_overlap.drop('_merge_transaction', axis = 1, inplace=True)
        para_llenar = ['weight_today', 'weight_old', 'position_today', 'position_old', 'net_transactions']
        for i in para_llenar:
            df_overlap[i] = df_overlap[i].fillna(0)
        df_overlap['diff_weight'] = df_overlap['weight_today']-df_overlap['weight_old']
        df_overlap['diff_position'] = df_overlap['position_today']-df_overlap['position_old']
        df_overlap = df_overlap.merge(mkt_otros, on = 'issuerid_id', how = 'outer')
        for i in self.otros_ports:
            df_overlap[i] = df_overlap[i].fillna(0)
        df_overlap['fund'] = self.port[0]
        df_overlap = df_overlap.merge(sec_iss_all, how = 'left', on = 'issuerid_id')
        df_changes_port = df_overlap[['issuerid_id', 'fund', 'compass_issuer_name', 'mkt_value_usd_today', 'weight_today', '_merge', 'diff_weight', 'diff_position']].drop_duplicates()
        cols_o_otros_ports = ['issuerid_id']
        cols_o_otros_ports.extend(self.otros_ports)
        overlap_otros_ports = df_overlap[cols_o_otros_ports].groupby('issuerid_id').sum().reset_index()
        df_overlap = df_changes_port.merge(overlap_otros_ports, how = 'left', on = 'issuerid_id')
        df_overlap = df_overlap.sort_values(by = 'weight_today', ascending = False)

        return df_overlap
    def capacity(self, participation = 0.25, time_frame = '3M'):
        '''
        Method para obtener el Capacity de una lista de cuentas tomando cierto horizonte de tiempo.
        IN:
            participation:
                Mételo como número o string. Debe ser uno de [0.25, 0.2, 0.15, 0.1, 0.05]
            time_frame:
                String para determinar cuánto horizonte/historia de volumen utilizar.
                Elegir entre '1M', '3M', '6M', '1Y'
        OUT:
            df_chart_capacity: dataframe con columnas de qué día se liquida cuánto con y sin transacciones.
            df_cap_summary: dataframe con los días 1 y 126.
            
        '''
        participation = str(participation)
        dt_date_fin = datetime.strptime(self.date_fin, '%Y-%m-%d')
        horizon_start = dt_date_fin -dateutil.relativedelta.relativedelta(months=self._switch_time_horizon(time_frame))
        transactions = self.orig.get_transactions(str(horizon_start)[0:10])[['securityid_id', 'net_amount_usd', 'trade_date']]
        mkt = self.orig_foto_mkt_value
        lista_securities = mkt['securityid_id'].drop_duplicates()
        volume = self.orig.prices(lista_securities, price_source_id = 1, price_type_id = 7, fecha_ini = str(horizon_start)[0:10])[['securityid_id', 'price', 'date_p']].rename(columns = {'price': 'volume'})
        #Una pequeña parada para quitar días no hábiles
        volume['day_of_week'] = pd.to_datetime(volume['date_p']).dt.dayofweek
        volume = volume[volume['day_of_week']<5]#Va del 0 (lunes) al 6(domingo), entonces quito 5 y 6.
        volume.drop(['day_of_week'], inplace = True, axis = 1)#Tiro esta columna para no romper nada más.
        volume_transactions = volume.merge(transactions, how = 'left', left_on = ['securityid_id', 'date_p'], right_on = ['securityid_id', 'trade_date'])
        volume_transactions['volume_ex_compass'] = volume_transactions['volume'] - volume_transactions['net_amount_usd'].fillna(0)
        volume_transactions = volume_transactions[['securityid_id', 'volume', 'net_amount_usd', 'volume_ex_compass']].fillna(0)
        avg_volume = volume_transactions[['securityid_id', 'volume']].groupby(by = 'securityid_id').mean().reset_index()
        avg_volume_excompass = volume_transactions[['securityid_id', 'volume_ex_compass']].groupby(by = 'securityid_id').mean().reset_index()
        mktval_avgvol = mkt.merge(avg_volume, how = 'left', on = 'securityid_id')
        mktval_avgvol = mktval_avgvol.merge(avg_volume_excompass, how = 'left', on = 'securityid_id')
        mktval_avgvol = mktval_avgvol[['securityid_id', 'mkt_value_usd', 'weight', 'volume', 'volume_ex_compass']]
        cash = mktval_avgvol[mktval_avgvol.securityid_id.str.contains('CA')]['mkt_value_usd'].sum()
        aum = mktval_avgvol['mkt_value_usd'].sum()
        mktval_avgvol = mktval_avgvol[~mktval_avgvol.securityid_id.str.contains('CA')]
        cash = cash/aum
        vol_prop = [0.25,0.2, 0.15, 0.1, 0.05]        
        for i in vol_prop:
            mktval_avgvol[str(i)] = i*mktval_avgvol['volume']
            mktval_avgvol[str(i) + "_ex_compass"] = i*mktval_avgvol['volume_ex_compass'] 
        cols_capacity = ['securityid_id', 'day', 'mkt_value_usd', 'a_operar', 'a_operar_ex_compass', 'remaining', 'remaining_ex_compass', 'liquidated', 'liquidated_ex_compass']
        df_capacity = pd.DataFrame(columns = cols_capacity)
        df_temp = df_capacity
        
        for i in range(1,127):
            df_temp['securityid_id'] = mktval_avgvol['securityid_id']
            df_temp['day'] = i
            df_temp['mkt_value_usd'] = mktval_avgvol['mkt_value_usd']
            df_temp['a_operar'] = mktval_avgvol[participation]
            df_temp['a_operar_ex_compass'] = mktval_avgvol[participation+'_ex_compass']
            df_temp['remaining'] = df_temp['mkt_value_usd'] - i*df_temp['a_operar']
            df_temp['remaining_ex_compass'] = df_temp['mkt_value_usd'] - i*df_temp['a_operar_ex_compass']
            df_temp[['remaining', 'remaining_ex_compass']] = df_temp[['remaining', 'remaining_ex_compass']].clip(lower = 0)
            df_temp['liquidated'] = (df_temp['mkt_value_usd']-df_temp['remaining'])/aum
            df_temp['liquidated_ex_compass'] = (df_temp['mkt_value_usd']-df_temp['remaining_ex_compass'])/aum
            df_capacity = pd.concat([df_capacity, df_temp], axis = 0, ignore_index = True)
        
        df_capacity = df_capacity.drop_duplicates()
        df_capacity_tail = df_capacity[['securityid_id', 'day', 'mkt_value_usd', 'remaining', 'remaining_ex_compass']]
        df_capacity_tail = df_capacity_tail[df_capacity_tail['day']==126]
        df_capacity_tail = df_capacity_tail[(df_capacity_tail['remaining']>0) | (df_capacity_tail['remaining_ex_compass']>0)]
        df_capacity_tail = df_capacity_tail.merge(self.sec_iss_all[['securityid_id', 'compass_issuer_name']], on = 'securityid_id', how = 'left')
        
        df_cap_chart = (df_capacity.groupby(by = ['day']).sum()[['liquidated', 'liquidated_ex_compass']]+cash).reset_index()
        df_cap_chart = df_cap_chart.melt(id_vars=['day'], value_vars=['liquidated', 'liquidated_ex_compass']).rename(columns = {'variable': 'Type', 'value': 'fund_perc_liquidated'})
        df_cap_chart['Type'] = df_cap_chart['Type'].str.replace('liquidated_ex_compass','ADTV w/o Compass transactions')
        df_cap_chart['Type'] = df_cap_chart['Type'].str.replace('liquidated','Total ADTV')
        df_cap_summary = df_cap_chart[df_cap_chart['day'].isin([1,126])]
        
        fig, ax = plt.subplots()
        plt.rcParams['figure.dpi'] = self.dpi
        sns.lineplot(data = df_cap_chart, x = 'day', y = 'fund_perc_liquidated', hue = 'Type', palette = [self.rojo_compass, self.gris1])
        ax.set_title('Capacity ' + self.port[0], fontsize = self.title_fontsize)
        ax.set_xlabel('Days to liquidate', fontsize = self.axislabel_fontsize)
        ax.set_ylabel('Fund % liquidated', fontsize = self.axislabel_fontsize)
        ax.yaxis.set_major_formatter(PercentFormatter(1))
        plt.show()
        return df_cap_chart, df_cap_summary, df_capacity_tail, mktval_avgvol
    def return_matrix(self, time_frame = '12M', periodicity = '1d', price_source_id = 9):
        '''
        Method para sacar la matriz de retornos.
        IN:
            time_frame: Elegir entre '2d', '1M', '3M'='1Q', '6M'='2Q', '1Y'='12M', '14M', '3Q', '2Y', '18M', '3Y'.
                Responde a "cuánta historia necesito?"
            periodicity: Elegir entre '1d', '5d', '2w'='10d'
                String para determinar cuánto horizonte/historia de volumen utilizar.
            price_source_id:
        OUT:
            df_ret:
                pandas DataFrame con matriz de retornos con la periodicidad e historia especificadas.
        '''
        dt_date_fin = datetime.strptime(self.date_fin, '%Y-%m-%d')
        horizon_start = dt_date_fin - dateutil.relativedelta.relativedelta(months=self._switch_time_horizon(time_frame))
        periodicity = self._switch_periodicity(periodicity)
        mkt = self.orig_mkt_value
        lista_securities = mkt['securityid_id'].drop_duplicates()
        prices = self.orig.prices(lista_securities, price_source_id = price_source_id, fecha_ini = str(horizon_start)[0:10], in_usd = True)[['securityid_id', 'price', 'date_p']].drop_duplicates()
        prices_pivot = prices.pivot(index = 'date_p', columns = 'securityid_id', values = ['price'])
        df_ret = prices_pivot.copy()
        for c in prices_pivot.columns:
            df_ret[c ] = df_ret[c].pct_change(periodicity)
            #df_ret[c] = np.log(prices_pivot[c])-np.log(prices_pivot[c].shift(periodicity))
        return df_ret
    def cov_matrix(self, time_frame = '12M', periodicity = '1d', price_source_id = 9):
        '''
        Method para calcular la matriz de covarianzas entre retornos.
        IN:
            None
        OUT: 
            cov: pandas DataFrame con la matriz de covarianzas con todas las características especificadas. El cash se queda en su lugar para no tumbar nada.
        '''
        ret_matrix = self.return_matrix(time_frame = time_frame, periodicity = periodicity, price_source_id = price_source_id)
        cov = ret_matrix.cov()
        return cov
    def trend(self, by = ''):
        '''
        Method para sacar el trend de un portafolio entre dos fechas.
        IN:
            by: string que pide el agrupamiento. Puede ser por Issuer, sector, país... etc. You name it.
            De hecho creo que lo haré que escupa todo con el get_all_from_id para pivotear fácil con todo.
        OUT:
            trend: pandas DataFrame con fecha, mkt value, securityid_id (mínimo, luego le agrego los otros campos para agrupar por)
        '''
        return 0
    def turnover(self, chart = True):
        '''
        Method para calcular el turnover de un fondo en el plazo de interés. Toma los parámetros de date_ini y date_fin como horizonte de tiempo.
        IN:
            chart: boolean. Indica si es de interés o no arrojar gráfica. Por default la entrega.
        OUT:
            turnover: pandas DataFrame con ['fund_aum', 'funds_id', 'buy', 'sell', 'min_buys_sells', 'turnover', 'dates']
        '''
        transactions = self.orig.get_transactions()
        mkt_orig_trend = self.orig_mkt_value[['date_p', 'fund_aum', 'funds_id', 'aim_account']].drop_duplicates()
        mkt_orig_trend.set_index(pd.to_datetime(mkt_orig_trend['date_p']), inplace = True)
        mktval_mean = mkt_orig_trend.resample("M").mean()#calcula media por mes.
        funds_id_orig = mktval_mean['funds_id'].iloc[0]#para sacar solo el fondo que me interesa por ahora.
        transactions = transactions[transactions['funds_id']==funds_id_orig]
        transactions.set_index(pd.to_datetime(transactions['trade_date']), inplace = True)
        avg_transactions = transactions.groupby('type').resample("M").sum().reset_index()[['type', 'trade_date', 'net_amount_usd']]#Suma por tipo y mes de transactions
        avg_transactions_wide = pd.pivot_table(data = avg_transactions, values = 'net_amount_usd', index = 'trade_date', columns = 'type')#Regresa monto por fecha y mes.
        avg_transactions_wide['min_buys_sells'] = avg_transactions_wide.min(axis = 1)#mínimo de ambas as per definition of turnover
        turnover = mktval_mean.merge(avg_transactions_wide, left_index = True, right_index = True)#Relacionar transactions min por mes con mkt value promedio por mes.
        turnover['turnover'] = turnover['min_buys_sells']/turnover['fund_aum']
        turnover['dates'] = turnover.index.strftime("%b-%y")#Agregar fecha en formato MMM-YY
        if(chart):
            fig, ax = plt.subplots()
            plt.rcParams['figure.dpi'] = self.dpi
            ax.set_title('Monthly turnover ' + mkt_orig_trend['aim_account'].iloc[0], fontsize = self.title_fontsize)
            ax.set_xlabel('Date', fontsize = self.axislabel_fontsize)
            ax.set_ylabel('Turnover (%)', fontsize = self.axislabel_fontsize)
            ax.set_xticklabels(turnover['dates'], fontsize=self.tick_fontsize)
            plt.xticks(rotation=90)
            g = sns.barplot(data = turnover, x = 'dates', y = 'turnover', color = self.rojo_compass)
            g.set(xlabel = 'Date', ylabel = 'Turnover (%)')
            g.set_yticklabels(g.get_yticks(), size = self.tick_fontsize)
            ax.yaxis.set_major_formatter(PercentFormatter(1))
        return turnover
    def momentum(self, price_source_id = 9):
        '''
        Method que compara el retorno del último mes con respecto al del año previo al inicio del mes en cuestión.
            e.g marzo 2022 vs feb2021-feb2022
        IN:
            price_source_id
                El id del Security_Master para cambiar la fuente de ser necesario.
        OUT:
            momentum: pandas DataFrame con ['securityid_id', '1M_ret', 'last_year_ret', 'diff', 'default_rank',
       'min_rank', 'NA_bottom', 'pct_rank', 'compass_issuer_name']
            El orden del ranking en default_rank, min_rank, NA_bottom indica que a menor valor, más momentum trae.
            El orden del ranking pct_rank indica que a menor valor, peor momentum trae.
        '''
        ret_mat = self.return_matrix(time_frame = '14M', price_source_id = price_source_id)
        ret_mat_aux = ret_mat+1
        ret_mat_aux.index = pd.to_datetime(ret_mat_aux.index)
        ret_mat_monthly = ret_mat_aux.resample("M").prod()
        last_year = ret_mat_monthly.pct_change(12).iloc[-2].reset_index().iloc[:,1:3]
        last_year.columns = ['securityid_id', 'last_year_ret']
        last_month = ret_mat_monthly.tail(2).pct_change().dropna().iloc[-1].reset_index().iloc[:,1:3]
        last_month.columns = ['securityid_id', '1M_ret']
        one_month_vs_1y = last_month.merge(last_year, on = 'securityid_id')
        one_month_vs_1y['diff'] = one_month_vs_1y['1M_ret']-one_month_vs_1y['last_year_ret']
        one_month_vs_1y.sort_values(by = ['diff'], ascending = False, inplace = True)
        momentum = one_month_vs_1y.copy()
        momentum['default_rank'] = momentum['diff'].rank(ascending = False)
        momentum['min_rank'] = momentum['diff'].rank(method='min', ascending = False)
        momentum['NA_bottom'] = momentum['diff'].rank(na_option='bottom', ascending = False)
        momentum['pct_rank'] = momentum['diff'].rank(pct=True, method = 'min', ascending = True)
        secs = self.sec_iss_all[['securityid_id', 'compass_issuer_name']]
        momentum = momentum.merge(secs, on = 'securityid_id')
        return momentum
    def review_triggers(self, tol = 35, z_treshold = 1.645):
        '''
        Method que regresa un DataFrame con los detractores más allá del z_threshold y con impacto negativo mayor a tol básicos.
        IN:
            tol: debe reportarse en basis. Tipo 35 significa 0.0035.
            z_treshold: umbral para z_score para tomarlo como outlier. El default para 95% de datos toma 1.645
        OUT:
            rt_reporte: pandas DataFrame con Account, Issuer, End_weight, Return y Contribution
        '''
        tol = tol/10000 #Porque reporto basis
        mkt_cols_rt = ['funds_id', 'securityid_id', 'position', 'mkt_value_usd', 'weight']
        mkt_final = self.orig_mkt_value[mkt_cols_rt]
        mkt_inicial = self.past_mkt_value[mkt_cols_rt]
        mkt_final['px_usd'] = mkt_final['mkt_value_usd']/mkt_final['position'] #mkt value reporta precio original, pero quiero todo en la misma moneda
        mkt_inicial['px_usd'] = mkt_inicial['mkt_value_usd']/mkt_inicial['position']
        mkt_merge = mkt_inicial.merge(mkt_final, on = ['funds_id', 'securityid_id'], suffixes = ('_inicial', '_final'))#Juntar weights, precios etc final e iniciales
        mkt_merge['return'] = mkt_merge['px_usd_final']/mkt_merge['px_usd_inicial']-1
        mkt_merge['ctr'] = mkt_merge['return']*mkt_merge['weight_inicial']
        neg_tol = mkt_merge[mkt_merge['ctr']<=-1*tol][['funds_id', 'securityid_id', 'weight_final', 'return', 'ctr']]#No solo en términos relativos, pero Antonio quiere ver detractores de 35 básicos para abajo
        rt = mkt_merge.copy()[['funds_id', 'securityid_id', 'weight_final', 'return', 'ctr']]
        rt = rt.set_index(['funds_id', 'securityid_id'])#Me permite agrupar por portafolio
        rt = rt.groupby(level=rt.index.names.difference(['securityid_id'])).transform(lambda x: (x-x.mean()) / x.std())#Z_score por portafolio, instrumento
        rt = rt.reset_index().rename(columns = {'ctr':'z_score_ctr'})
        rt_detractors = rt[rt['z_score_ctr']<-1*z_treshold]
        rt_detractors = rt_detractors[['funds_id', 'securityid_id']].merge(mkt_merge[['funds_id', 'securityid_id', 'weight_final', 'return', 'ctr']], on = ['funds_id', 'securityid_id'])
        rt_tol_detractors = pd.concat([rt_detractors, neg_tol], ignore_index = True, axis = 0).drop_duplicates()
        secs = self.sec_iss_all[['securityid_id', 'compass_issuer_name']]
        ports_dict = self.orig._portfolio_dict()[['funds_id', 'fund_name']]
        rt_tol_detractors = rt_tol_detractors.merge(secs, how = 'left', on = 'securityid_id')
        rt_tol_detractors = rt_tol_detractors.merge(ports_dict, how = 'left', on = 'funds_id').rename(columns = {'compass_issuer_name':'Issuer', 'fund_name':'Account', 'weight_final':'End_weight', 'return': 'Return', 'ctr':'Contribution'})
        rt_report = rt_tol_detractors[['Account', 'Issuer', 'End_weight', 'Return', 'Contribution']]        
        return rt_report
    def _switch_time_horizon(self, time_frame):
        '''
        Method privado para sacar la cantidad de meses a echar para atrás cuando hacemos Capacity o en un futuro PA
        IN:
            time_frame:
                String para determinar cuánto horizonte/historia de volumen utilizar.
                Elegir entre '1M', '3M'='1Q', '6M'='2Q', '1Y'='12M', '3Q', '2Y', '18M', '3Y', '14M'
        OUT:
            Muy literalmente 1, 3, 6, o 12 (en enteros). Lo hago con un switcher porque tal vez en un futuro queramos sustraer días y no meses.
            Pensando en que 3M no siempre son 90 días, permitiría más control pero por ahora probemos esto.
            El default del switcher son 3 meses para evitar errores.
        '''
        if time_frame not in ['1M', '3M', '6M', '1Y', '12M', '1Q', '2Q', '3Q', '2Y', '18M', '3Y', '14M']:
            print("Se eligió: " + time_frame+ ", el cual no es ninguno de '1M', '3M', '6M', '1Y', '12M', '14M', '1Q', '2Q', '3Q','2Y', '18M', '3Y'. Ante este error se opta por correr 3M por default.")
        switcher = {
            '1M': 1,
            '3M': 3,
            '6M': 6,
            '1Y': 12,
            '12M': 12,
            '1Q': 3,
            '2Q': 6,
            '3Q':9,
            '2Y': 24,
            '18M':18, 
            '3Y':36,
            '14M': 14
        }
        res = switcher.get(time_frame, 3)
        return res
    def _switch_periodicity(self, periodicity='1d'):
        '''
        Method privado para sacar la cantidad de meses a echar para atrás cuando hacemos Capacity o en un futuro PA
        IN:
            time_frame:
                String para determinar cuánto horizonte/historia de volumen utilizar.
                Elegir entre '1d', '5d', '2w'='10d'
        OUT:
            Muy literalmente 1, 3, 6, o 12 (en enteros). Lo hago con un switcher porque tal vez en un futuro queramos sustraer días y no meses.
            Pensando en que 3M no siempre son 90 días, permitiría más control pero por ahora probemos esto.
            El default del switcher son 3 meses para evitar errores.
        '''
        if periodicity not in ['1d', '5d', '2w', '10d']:
            print("Se eligió: " + periodicity + ", el cual no es ninguno de '1d', '5d', '2w'='10d'. Ante este error se opta por correr 1d por default.")
        switcher = {
            '1d': 1,
            '5d': 5,
            '2w': 10,
            '10d': 10
        }
        res = switcher.get(periodicity, 1)
        return res 
class cargas():
    def __init__(self, date_ini, date_fin, tipo = ''):
        self.tipo = tipo
        self.date_ini = date_ini
        self.date_fin = date_fin
        self.orden_prices_columns = ['securityid_id', 'price', 'price_source_id', 'price_type_id', 'date_p']
    def carga_prices(self, price_source_id, missing = False):
        '''
        Method para cargar precios desde alguna fuente.
        IN:
            price_source_id: entero para determinar la fuente de precios que se está usando.
                6 es ValmerMX
                9 es Universo
                10 es DataLicense
                Hay otros pero estos son relevantes para el setup
            price_type_id: Entero para determinar el tipo de precio que se usa.
                1: clean
                2: dirty
                4: bid
                5: ask
                6: last
                7: volume
                Hay otros pero estos son los más usados.
            missing: Boolean que determina si se van a cargar precios de fechas cargadas,
                entonces solamente carga los que no están en la base para que no truene
                la restricción de únicos.
            
                False: Sube los precios normalmente
                True: Sube solamente los precios faltantes
                
        OUT:
            str_res: Cadena de texto indicando si subió algo, qué subió y entre qué fechas.
        '''
        if price_source_id == 6:
            global instrumentos_mx
            print('Estoy subiendo desde Valmer')
            obj_sm = Security_Master(date_ini = self.date_ini, date_fin = self.date_fin)
            instrumentos_mx_query = "SELECT id as securityid_id, local_id FROM SecurityId WHERE id LIKE '%MX%' AND local_id IS NOT NULL"
            exceptions_mx_query = "SELECT id as securityid_id, local_id FROM SecurityId WHERE (local_id like '91[_]%' or local_id LIKE '93[_]%' or local_id LIKE '94[_]%' or local_id LIKE '95[_]%' ) and id NOT LIKE 'MX%'"
            
            instrumentos_mx = obj_sm.generic_get_from_sm(instrumentos_mx_query)
            exceptions_mx = obj_sm.generic_get_from_sm(exceptions_mx_query)
            
            instrumentos_mx = instrumentos_mx.append(exceptions_mx)
            instrumentos_mx.drop_duplicates(keep = 'first', inplace = True)
            
            vector_source_path = r'Compass Group\Riesgo Financiero - Documentos\Riesgos GSU\Mexico GSU\vectores_isin_tv\vectores_md_all.csv'
            vectores = pd.read_csv(vector_source_path)
            vectores = vectores[vectores['Fecha'].between(self.date_ini, self.date_fin)] 
            secid_niveles = instrumentos_mx.merge(vectores, right_on = 'Instrumento', left_on = 'local_id')[['securityid_id', 'local_id', 'Fecha', 'TV', 'PrecioSucio', 'PrecioLimpio']]
            fi = secid_niveles[secid_niveles['TV'].isin(['S', 'M', 'LD', 'IQ', 'LD', 'IM', 'BI', 'IS', '91', '95', '93','94', 'LF', 'LG'])]
            eq = secid_niveles[secid_niveles['TV'].isin(['CF', '1'])]
            try:
                fi_clean = fi[['securityid_id', 'Fecha', 'PrecioLimpio']].rename(columns = {'Fecha':'date_p', 'PrecioLimpio':'price'})
                fi_clean['price_source_id'] = price_source_id
                fi_clean['price_type_id'] = 1
            except:
                fi_clean = pd.DataFrame(columns = ['securityid_id', 'date_p', 'price', 'price_source_id', 'price_type_id'])
                print('Precio limpio de Fixed Income viene vacío.\n')
            try:
                fi_dirty = fi[['securityid_id', 'Fecha', 'PrecioSucio']].rename(columns = {'Fecha':'date_p', 'PrecioSucio':'price'})
                fi_dirty['price_source_id'] = price_source_id
                fi_dirty['price_type_id'] = 2
            except:
                fi_dirty = pd.DataFrame(columns = ['securityid_id', 'date_p', 'price', 'price_source_id', 'price_type_id'])
                print('Precio sucio de Fixed Income viene vacío.\n')
            
            try:
                eq = eq[['securityid_id', 'Fecha', 'PrecioSucio']].rename(columns = {'Fecha':'date_p', 'PrecioSucio':'price'})
                eq['price_source_id'] = price_source_id
                eq['price_type_id'] = 6
            except:
                eq = pd.DataFrame(columns = ['securityid_id', 'date_p', 'price', 'price_source_id', 'price_type_id'])
                print('Precio de equity viene vacío.\n')
            
            df_carga = fi_clean.append(fi_dirty)
            df_carga = df_carga.append(eq)
            df_carga = df_carga[self.orden_prices_columns].drop_duplicates()
            
            
            # Si missing es True
            if missing:
                # Leer prices anteriores
                query_missing = 'SELECT * FROM Prices WHERE (price_source_id = ' + str(int(price_source_id)) +" ) and date_p BETWEEN '" + self.date_ini + "' and '" + self.date_fin + "'"
                prices_past = pd.read_sql(query_missing, obj_sm.conexion_sm())
                
                # quitar id
                prices_past.drop('id', axis =1, inplace = True)
                
                # Queremos obtener los que está en df_carga pero no en prices_past
                df_carga = df_carga.merge(prices_past, how = 'left', indicator = True)
                df_carga.drop(df_carga[df_carga._merge != 'left_only'].index, inplace = True)
                
                # Quitar la columna que no sirve
                df_carga.drop('_merge', axis = 1, inplace = True)

                
            
            ban = obj_sm.insert_into_security_master('Prices', df_carga)
            if(ban and len(df_carga)>0):
                print("Se insertaron bien los precios.")
            elif(len(df_carga)==0):
                    print("No tronó el insert, el dataframe de precios estaba vacío.")
            else:
                print("Error al cargar (te hablo desde el objeto de carga). Try again.")
        return ban
    def carga_volumes(self, price_source_id = 1, price_type_id = 7):
        data_equity = pd.read_csv(r'Compass Group\Riesgo Financiero - Documents\Data Bases\Volume\volume_eq.csv')
        data_equity = data_equity.rename(columns = {'date': 'date_p', 'volumen USD': 'price', 'ID': 'securityid_id'})
        data_fi = pd.read_csv(r'Compass Group\Riesgo Financiero - Documents\Data Bases\Volume\volume_fi.csv')
        data_fi = data_fi.rename(columns = {'Date': 'date_p', 'Volume USD': 'price', 'ID': 'securityid_id'})
        return 0
    def carga_ir_mongo(self):
        '''
        Method para sacar el dataframe "propuesta" de lo que hay nuevo en Mongo vs lo nuestro.
        OUT:
            df_carga:
                pandas DataFrame con columnas ['securityid_id', 'grades_min_mongo', 'grades_min_prev', 'date_r', 'score', 'compass_rating', 'compass_subrating']
        '''
        obj_sm = Security_Master(self.date_ini, self.date_fin)
        ir_sm = obj_sm.get_latest_internal_ratings()
        mongo_ir_clean = self.get_latest_ir_mongo_clean()
        columns_reporte = ['securityid_id', 'grades_min_mongo', 'grades_min_prev', 'date_r']
        columns_carga_reporte = ['securityid_id', 'grades_min_mongo', 'grades_min_prev', 'date_r', 'score', 'compass_rating', 'compass_subrating', 'compass_rating_mongo', 'compass_subrating_prev']
        
        ir_sm = ir_sm[['securityid_id', 'grades_min']]
        mongo_ir = mongo_ir_clean[['securityid_id', 'grades_min', 'date_r']]
        mongo_vs_prev = mongo_ir.merge(ir_sm, how = 'left', on = 'securityid_id', suffixes=('_mongo', '_prev'))
        #Si hay nuevos securities, al hacer merge left salen valores vacíos. Esos los guardo.
        new_ir = mongo_vs_prev[mongo_vs_prev['grades_min_prev'].isna()][columns_reporte]
        mongo_vs_prev.dropna(inplace = True)
        mongo_vs_prev['diff'] = mongo_vs_prev['grades_min_mongo'] - mongo_vs_prev['grades_min_prev']
        #Algunos ir son actualización de algo previo, los separo con diferencia distinta de cero.
        ir_actualizados = mongo_vs_prev[mongo_vs_prev['diff']!=0]
        ir_actualizados = ir_actualizados[columns_reporte]
        #Para que no truene, veo si hay o no algo para actualizar. Si no hay genero dataframes vacíos pero con las columnas correctas.
        hay_nuevos = False
        if(len(ir_actualizados) == 0):
            ir_actualizados = pd.DataFrame(columns = columns_reporte)
        else:
            hay_nuevos = True
        if(len(new_ir)== 0):
            new_ir = pd.DataFrame(columns =columns_reporte)
        else:
            hay_nuevos = True
            
        if (hay_nuevos):    
            conversion_score_letra = obj_sm.dict_rating_score_letter()
            df_carga = pd.concat([ir_actualizados, new_ir], ignore_index = True)
            df_carga = df_carga.merge(conversion_score_letra, how = 'left', left_on = 'grades_min_mongo', right_on = 'score')
            df_carga = df_carga.merge(conversion_score_letra, how = 'left', left_on = 'grades_min_prev', right_on = 'score', suffixes = ('_mongo', '_prev'))
        else:
            df_carga = pd.DataFrame(columns = columns_carga_reporte)
                
        # return df_carga[columns_carga_reporte]
        df_carga.drop_duplicates(inplace = True)
        return df_carga
    def carga_gscore_mongo(self):
        '''
            Method para PROPONER un dataframe con las diferencias entre lo que ya tenemos cargado y lo nuevo de mongo.
        '''
        obj_sm = Security_Master(self.date_ini, self.date_fin)
        g_mongo = obj_sm.get_new_gscore_mongo()#Jala query de mongo
        g_mongo['g_score'] = g_mongo['g_score']/100
        g_sm = obj_sm.get_latest_gscore()[['date_e', 'issuerid_id', 'g_score']]#Busco gscore de SM para comparar con lo que ya tengo        
        g_merge = g_mongo.merge(g_sm, on = 'issuerid_id', suffixes = ('_mongo', '_sm'))
        g_new = g_merge[g_merge['g_score_sm'].isna()]
        g_merge = g_merge.dropna()
        g_merge['diff'] = g_merge['g_score_mongo']-g_merge['g_score_sm']
        g_recientes = g_merge[g_merge['date_e_mongo']>g_merge['date_e_sm']]
        g_threshold_1 = g_recientes[np.abs(g_recientes['diff'])>=0.001]
        g_threshold_5 = g_recientes[np.abs(g_recientes['diff'])>=0.005]
        secs = obj_sm.get_security_issuer_all()[['issuerid_id', 'compass_issuer_name']].drop_duplicates()
        
        g_threshold_1 = g_threshold_1.merge(secs, on = 'issuerid_id')
        g_threshold_5 = g_threshold_5.merge(secs, on = 'issuerid_id')
        g_recientes = g_recientes.merge(secs, on = 'issuerid_id')
        g_new_carga = g_new[['issuerid_id', 'g_score_mongo', 'date_e_mongo']].rename(columns = {'g_score_mongo':'g_score', 'date_e_mongo':'date_e'})
        g_dif_carga = g_recientes[['issuerid_id', 'g_score_mongo', 'date_e_mongo']].rename(columns = {'g_score_mongo':'g_score', 'date_e_mongo':'date_e'})
        g_carga = pd.concat([g_new_carga, g_dif_carga], ignore_index = True, axis = 0)
        return g_carga
    def get_latest_ir_mongo_clean(self):
        '''
        Method para entregar el dataframe con de internal ratings de Mongo ya con nuestras conversiones.
        OUT:
            mongo_ir_sec:
                pandas DataFrame con ['compass_issuer_alias', 'issuerid_id', 'date_r', 'grades_min','securityid_id']
        '''
        obj_sm = Security_Master(self.date_ini, self.date_fin)
        rating_conversion = obj_sm.internal_external_conversion()[['score', 'score_ir', 'internal_rating']]
        mongo_ir = obj_sm.get_new_internal_ratings_mongo().sort_values(by = 'date_e', ascending = False).drop_duplicates(subset = ['issuerid_id'], keep = 'first')
        #Para separar los que traen "BBB" y los que traen número para el score
        mongo_ir['isnumeric'] = mongo_ir['internal_rating_score'].str.isnumeric()
        
        mongo_ir_letra = mongo_ir[mongo_ir['isnumeric']==False].drop('isnumeric', axis = 1)
        mongo_ir_letra = mongo_ir_letra.merge(rating_conversion[['internal_rating', 'score']], how = 'left', left_on = 'internal_rating_score', right_on = 'internal_rating').drop(['internal_rating', 'internal_rating_score'], axis = 1)
        
        mongo_ir_num = mongo_ir[mongo_ir['isnumeric'].isna()].drop('isnumeric', axis = 1)
        mongo_ir_num['internal_rating'] = mongo_ir_num['internal_rating_score'].astype('float64').apply(np.floor)
        mongo_ir_num.drop('internal_rating_score', axis = 1, inplace = True)
        mongo_ir_num = mongo_ir_num.merge(rating_conversion[['score_ir', 'score']], how = 'left', left_on = 'internal_rating', right_on = 'score_ir').drop(['internal_rating', 'score_ir'], axis = 1)
        mongo_ir = pd.concat([mongo_ir_num, mongo_ir_letra], ignore_index = True).rename(columns = {'score': 'grades_min', 'date_e':'date_r'})
        sec_iss_all = obj_sm.get_security_issuer_all()[['securityid_id', 'issuerid_id']]
        sec_iss_all = sec_iss_all[sec_iss_all['securityid_id'].str.contains('FI')]
        mongo_ir_sec = mongo_ir.merge(sec_iss_all, on = 'issuerid_id')
        mongo_ir_sec = mongo_ir_sec.dropna()
        mongo_ir_sec = mongo_ir_sec[mongo_ir_sec['grades_min']>=0]
        return mongo_ir_sec    
    def control_cargas(self):
        '''
        Method para realizar el control de cargas y enviarlo por correo

        Returns
        -------
        String con el resultado del envío del correo

        '''
        # =============================================================================
        # Currency Values
        # =============================================================================
        obj_sm = Security_Master(self.date_ini, self.date_fin)
        dt_date_fin = datetime.strptime(self.date_fin, "%Y-%m-%d")
        
        # Lista de currencies que se deben de mandar
        curr_list = [1,2,3,4,5,6,7,14,15,16,17,18,19,25,26,27]
        
        
        # Currency Dates
        querry_curr_dates = '''SELECT DISTINCT TOP(2) date_c 
        FROM Currency_values
        WHERE currency_id IN (''' + str(curr_list)[1:-1] + ''')
        ORDER by date_c DESC
        '''
        
        dates_curr = obj_sm.generic_get_from_sm(querry_curr_dates)
                    
        # Variable con el cuerpo del correo
        string_mail = "Control date ran: " + obj_sm.date_fin + "<br>"
        thresh_curr = 10
        
        
        if obj_sm.date_fin in dates_curr.date_c.to_list():
        
            query_curr = '''
            SELECT cv.currency_id, c.currency, cv.value, cv.date_c
            FROM Currency_values as cv
            LEFT JOIN currency as c ON c.id = cv.currency_id
            WHERE date_c IN ('''  +\
            str(dates_curr.date_c.to_list())[1:-1] + ") ORDER BY date_c DESC, currency_id"
            
            # Currency DataFrame
            curr_df = obj_sm.generic_get_from_sm(query_curr)
            curr_df_hoy = curr_df[curr_df.date_c == obj_sm.date_fin]
            # Resta entre la lista a checar y el dataframe de currencies
            rest = list(set(curr_list)-set(curr_df_hoy.currency_id.to_list()))
            
            
            curr_check = True
            # Si hay alguna que no se subió
            string_mail = string_mail + "Currency Values Check: <br>Threshold = " + str(thresh_curr) + "% <br>"
            if len(rest)>0:
                curr_check = False
                string_mail = string_mail + "<br>\tMissing Currencies by id: "
                for i in rest:
                    string_mail = string_mail + str(i) + " "
            
            # Si alguno de los currencies tiene valor cero o negativo o NA
            errors_curr = curr_df_hoy[(curr_df_hoy.value <= 0) | (curr_df_hoy.value.isna())]
                
            # Si alguna se subió con errores
            if not errors_curr.empty:
                curr_check = False
                string_mail  = string_mail + "<br>\tCurrencies that have problems: "
                for i in errors_curr.currency_id:
                    string_mail = string_mail + str(i) + " "
            
            # Duplicados    
            dups_curr = curr_df_hoy[curr_df_hoy.duplicated(subset = ['currency_id', 'date_c'], keep = False)]
            
            # Si hubo algun duplicado
            if not dups_curr.empty:
                curr_check = False
                string_mail  = string_mail + "<br>\tSome inputs are duplicated : <br>" + dups_curr.to_html()
                
            # Si no hubo errores
            if curr_check:
                
                aux_curr = curr_df.pivot(index =['currency'] , values = ['value'], columns = ['date_c'] )
                aux_curr.sort_index(axis = 1, ascending = False, inplace = True)
                aux_curr.columns = aux_curr.columns.droplevel(0)
                aux_curr['diff (%)'] = round((aux_curr.iloc[:,0].div(aux_curr.iloc[:,1]) -1)*100,2)
                aux_curr['threshold'] = ["Correct upload" if abs(x) < thresh_curr else 'Check upload' for x in aux_curr['diff (%)']]
                
                string_mail  = string_mail + "<br>\tCurrency values correctly uploaded <br>" + aux_curr.to_html()
                
        else:
            string_mail += "<br>\tThe date you are running has not been uploaded redo upload."
            
            
        # =============================================================================
        # Prices
        # =============================================================================
        # Threshold to check if the change is too big
        threshold = 10
        string_mail += f"<br><br>Prices Check<br>Threshold = {threshold}%"
        
        query_prices = '''
        select date_p, price_source_id,ps.source, price_type_id,pt.type, count(*) as 'count'
        from Prices
        left join Price_source as ps on ps.id = Prices.price_source_id
        left join Price_type as pt on pt.id = Prices.price_type_id
        --where price_source_id = 10
        group by date_p, price_source_id,ps.source, price_type_id,pt.type
        order by date_p desc, price_source_id,ps.source, price_type_id,pt.type
        '''
        # Obtener el dataframe
        prices_df = obj_sm.generic_get_from_sm(query_prices)
        # Datatime
        prices_df['date_p']= pd.to_datetime(prices_df['date_p'])
        # Eliminar los que sean menores a la fecha que estamos probando
        prices_df = prices_df[prices_df.date_p <= dt_date_fin]
        # Buscar solamente las dos fechas más altas que sean de la carga del universo
        unique_dates = prices_df[prices_df.price_source_id == 9].date_p.drop_duplicates().sort_values(ascending = False)
        top_dates = unique_dates[0:2]
        prices_df = prices_df[prices_df.date_p.isin(top_dates)]
 
        # Get only the prices for the date to check
        prices_date_fin = prices_df[prices_df.date_p == dt_date_fin]
        
        # For each pair of prices
        df_pair_prices = pd.DataFrame(columns = ['source', 'type', 'count_today','date_today','count_yesterday' ,'date_yesterday','diff (%)','threshold'])
        
        # Go Over all types of prices and all sources
        for pair in list(zip(prices_date_fin.source, prices_date_fin.type)):
            
            # Pair of prices
            pair_prices = prices_df[(prices_df.source == pair[0]) & (prices_df.type == pair[1])]
            
            # Should be two, one for each date to check
            if len(pair_prices) != 2:
                string_mail += "<br>\t Error one date is missing for the pair (source, price):" + str(pair)
                
            else:
                # Try to calculate the diference in prices
                try:
                    # Diference
                    diff = (pair_prices.iloc[0]['count']/pair_prices.iloc[1]['count'] -1)*100
                    # Check if it breaks the threashold
                    thresh = 'Correct upload' if abs(diff) <= threshold else 'Check upload'
   
                except:
                
                    # There was an error            
                    diff = "Error when calculating differences check values"
                    # Check if it breaks the threashold
                    threshold = "Error when calculating differences check values"
                 
                # Dataframe to attach
                df_aux = pd.DataFrame([pair[0] , pair[1] , pair_prices.iloc[0]['count'],pair_prices.iloc[0]['date_p'], pair_prices.iloc[1]['count'],pair_prices.iloc[1]['date_p'], diff, thresh ], index = df_pair_prices.columns)
                #Append to previous df
                df_pair_prices = pd.concat([df_pair_prices, df_aux.transpose()])
                    
                    
        string_mail += "<br>" + df_pair_prices.to_html()
        
                
        # =============================================================================
        # Target Prices
        # =============================================================================
        # Fecha
        date_today = datetime.strftime(datetime.today(), "%d-%b-%Y")
        
        string_mail += "<br>Target Prices Check for date: " + date_today
        # Revisar los dataframes de target prices
        tp_act,df_carga_nuevos_tp = self.carga_tp_a_sm(upload = False)   
        
        # Si están vacíos es por que la carga se hizo correctamente o por que no había nada que cargar
        if tp_act.empty and df_carga_nuevos_tp.empty:
            string_mail += "<br>Correct upload"
            # Revisar si hay algún target price para el día.
            query_tp = "select * from equity_recommendation where date_e = '" + date_today + "'"
            df_tp = obj_sm.generic_get_from_sm(query_tp)
            if df_tp.empty:
                string_mail += "<br>No target prices where uploaded today"
            else:
                string_mail += "<br>A total of " + str(len(df_tp)) + " target prices where uploaded today."
                
        else:
            tp_to_load = len(tp_act) + len(df_carga_nuevos_tp)
            string_mail += "<br>ERROR in upload. There are " + str(tp_to_load) + " target prices ready to be uploaded"
            
        
        
        # =============================================================================
        # Posiciones
        # =============================================================================
        string_mail += f"<br><br>Positions Check<br>Threshold = {threshold}%"
        query_pos = '''
        select date_p, position_source_id, psor.source, count(*) as 'count'
        from Position
        left join Position_source as psor on psor.id = Position.position_source_id
        group by date_p, position_source_id, psor.source
        order by date_p desc, position_source_id, psor.source
        '''
        # Obtener el dataframe
        pos_df = obj_sm.generic_get_from_sm(query_pos)
        # Datatime
        pos_df['date_p']= pd.to_datetime(pos_df['date_p'])
        # Eliminar los que sean menores a la fecha que estamos probando
        pos_df = pos_df[pos_df.date_p <= dt_date_fin]
        # Buscar solamente las dos fechas más altas que sean de la carga del universo
        unique_dates = pos_df[pos_df.position_source_id == 4].date_p.drop_duplicates().sort_values(ascending = False)
        top_dates = unique_dates[0:2]
        pos_df = pos_df[pos_df.date_p.isin(top_dates)]
        
        # Get only the positions for the date to check
        pos_date_fin = pos_df[pos_df.date_p == dt_date_fin]
        
        # For each position
        df_pair_pos = pd.DataFrame(columns = ['source', 'count_today','date_today','count_yesterday' ,'date_yesterday','diff (%)','threshold'])
        # Go Over all sources of positions
        for source in pos_date_fin.source.to_list():
        
            
            # Pair of prices
            pair_pos = pos_df[pos_df.source == source]
            
            # Should be two, one for each date to check
            if len(pair_pos) != 2:
                string_mail += "<br>\t Error one date is missing for the source:" + str(source)
                
            else:
                # Try to calculate the diference in prices
                try:
                    # Diference
                    diff = (pair_pos.iloc[0]['count']/pair_pos.iloc[1]['count'] -1)*100
                    # Check if it breaks the threashold
                    thresh = 'Correct upload' if abs(diff) <= threshold else 'Check upload'
   
                except:
                
                    # There was an error            
                    diff = "Error when calculating differences check values"
                    # Check if it breaks the threashold
                    threshold = "Error when calculating differences check values"
                 
                # Dataframe to attach
                df_aux = pd.DataFrame([source , pair_pos.iloc[0]['count'],pair_pos.iloc[0]['date_p'], pair_pos.iloc[1]['count'],pair_pos.iloc[1]['date_p'], diff, thresh ], index = df_pair_pos.columns)
                #Append to previous df
                df_pair_pos = pd.concat([df_pair_pos, df_aux.transpose()])
                    
                    
        string_mail += "<br>" + df_pair_pos.to_html()
        
        # =============================================================================
        # Market Value        
        # =============================================================================
        string_mail += "<br><br>Market Value Check"
        # Buscar todos los aim_accounts
        aim_accounts_query = "SELECT fund_name,aim_account FROM funds WHERE aim_account IS NOT NULL"
        aim_accounts = obj_sm.generic_get_from_sm(aim_accounts_query)
        
        mkt_values = obj_sm.mkt_value(aim_accounts.aim_account.tolist())
        
        # Dividir algunos fondos entre 100
        funds_to_div = [18,19,20,21]
        for id_fund in funds_to_div:
            mkt_values.loc[mkt_values.funds_id == id_fund,['mkt_value_usd','fund_aum']] = mkt_values.loc[mkt_values.funds_id == id_fund,['mkt_value_usd','fund_aum']]/100
        
        # NAs y Duplicados
        nas = mkt_values[mkt_values.isna().any(axis = 1)]
        dups = mkt_values[mkt_values.duplicated(keep = False, subset = ['funds_id','mkt_value_usd'])]
        
        # Resumen
        summary_mkt_values = mkt_values.drop_duplicates(subset = ['aim_account', 'fund_aum'])[['aim_account','date_c', 'fund_aum']]
        summary_mkt_values.sort_values(by = 'fund_aum', inplace = True, ascending = False)
        summary_mkt_values.fund_aum = [f'${round(x/1000000,2):,} M' if x >1000000 else f'${round(x/1000,2):,} k' for x in summary_mkt_values.fund_aum ]
        
        # Falsos caso default
        send_mkt_values_nas = False
        send_mkt_values_dups = False
        if nas.empty and dups.empty:
            string_mail += "<br>Correct upload market values"
            
        if not nas.empty:
            string_mail += "<br>Incorrect upload market values, check for NAs in file, currencies could be missing"
            nas.to_excel(r'C:\Users\ '[:-1] + getpass.getuser() + r'\Compass Group\Riesgo Financiero - Documentos\Data Bases\Security Master\Scripts\Reports\MarketValues\mkt_values_nas_'+ self.date_fin.replace('-','') +'.xlsx')
            send_mkt_values_nas = True
        if not dups.empty:
            string_mail += "<br>Incorrect upload market values, check for duplicates in file, check for repeated isins"
            dups.to_excel(r'C:\Users\ '[:-1] + getpass.getuser() + r'\Compass Group\Riesgo Financiero - Documentos\Data Bases\Security Master\Scripts\Reports\MarketValues\mkt_values_duplicates_'+ self.date_fin.replace('-','') +'.xlsx')
            send_mkt_values_dups = True    
            
        
        string_mail += "<br>Market Value in USD" + summary_mkt_values.reset_index(drop = True).to_html()
                
        
        
        # =============================================================================
        # Ratings
        # =============================================================================

        
        string_mail += '<br><br>Ratings Check for date: ' + str(date_today)
        # Abrir archivo de history_ratings, que tiene las fechas en la que se corrió
        history_filename =  r'C:\Users\ '[:-1] + getpass.getuser() + r"\Compass Group\Riesgo Financiero - Documentos\Data Bases\Security Master\Scripts\Reports\Nuevos Reportes\ratings_history.xlsx"
        ratings_hist_df = pd.read_excel(history_filename)
        # Datatime
        ratings_hist_df['Fecha']= pd.to_datetime(ratings_hist_df['Fecha'])
        #Obtener solamente los que coinciden en fecha
        ratings_df_date = ratings_hist_df[ratings_hist_df == datetime.today().replace(minute=0, hour=0, second=0, microsecond=0)].dropna()
        
        
        # Buscar ratings en Security Master
        query_ratings = "SELECT * FROM Rating as r LEFT JOIN Rating_source as rs ON rs.id = r.rating_source_id  WHERE date_r = '" + date_today + "'"
        query_ratings_sum = " SELECT rs.source, count(*) as 'count' FROM Rating as r LEFT JOIN Rating_source as rs ON rs.id = r.rating_source_id WHERE date_r = '" + date_today + "' GROUP BY source"
        ratings_df_sm = obj_sm.generic_get_from_sm(query_ratings)
        ratings_df_sm.drop(['id'], axis = 1, inplace = True)
        
        # Summary df
        ratings_df_sm_sum = obj_sm.generic_get_from_sm(query_ratings_sum)
        
        # Duplicados
        dups_ratings = ratings_df_sm[ratings_df_sm.duplicated(subset = ['securityid_id','rating_source_id' ], keep = False)]
        
        
        if len(ratings_df_date) != 1:
            if len(ratings_df_date) > 1:
                string_mail += "<br>Error in ratings, the date your are checking was ran more than once"
            else:
                string_mail += "<br>Error in ratings, the date your are checking was not ran."
        else:
            if not dups_ratings.empty:
                string_mail += "<br>Error in ratings, check for thess duplicates"
                string_mail += "<br>" + dups_ratings.to_html()
            else:
                string_mail += "<br>Ratings correctly uploaded"
                if not ratings_df_sm.empty:
                    string_mail += "<br>" + ratings_df_sm_sum.to_html()
                else:
                    string_mail +="<br>No ratings changes on this date"

        # =============================================================================
        # Data License         
        # =============================================================================
        
        string_mail += '<br><br>Datalicense Check'
        # Query for prices with source DATALICENSE
        query_dl = "select p.date_p,pt.type, p.price,p.securityid_id,sec.isin from prices as p LEFT JOIN SecurityId as sec ON sec.id = p.securityid_id left join Price_type as pt ON pt.id = p.price_type_id where price_source_id = 10 and date_p =  '" + self.date_fin + "'"
        df_dl = obj_sm.generic_get_from_sm(query_dl) 
        
        # Drop those with price not equal to zero or na
        df_dl = df_dl[(df_dl.price <=0) | (df_dl.price.isna())]
        
        # Group by source id
        df_dl_sum = df_dl.groupby('type')['price'].count().to_frame()
        df_dl_sum.rename(columns = {'price':'count'}, inplace = True)
        df_dl_sum.reset_index(inplace = True)
        
        # If datalicense has zeros
        send_dl = False
        if not df_dl.empty:
            string_mail += '<br>Datalicense has zeros check summary: <br>' + df_dl_sum.to_html()
            # Save a file with the isins with errors
            df_dl.to_excel(os.getcwd() + r'\Compass Group\Riesgo Financiero - Documentos\Data Bases\Security Master\Scripts\Reports\Datalicense\Datalicense_zeros_' + self.date_fin.replace('-','') +'.xlsx')
            # Send attachment by mail
            send_dl = True
        else:
             string_mail += '<br>Datalicense has no zeros'
        
        
        # =============================================================================
        # Mandar Correo            
        # =============================================================================
        
        
        # Body del correo
        email_subject = 'Cargas Control as of: ' + date_today
        email_message = string_mail
        
        # Sender, debe de tener password en marketrisk_setup        
        email_sender = 'Pablo.Garza@cgcompass.com'
        # email_sender = 'Bernardo.Alcantara@cgcompass.com'
        
        # Quien recibe el correo, si son varios hacer una lista con los correos
        email_recipient = ['Marketrisk@cgcompass.com', 'gonzalo.bardin@cgcompass.com']
        # email_recipient = ['Pablo.Garza@cgcompass.com']
        attachment_location = []
        if send_dl == True:
            attachment_location.append(os.getcwd() + r'\Compass Group\Riesgo Financiero - Documentos\Data Bases\Security Master\Scripts\Reports\Datalicense\Datalicense_zeros_' + self.date_fin.replace('-','') +'.xlsx')
        if send_mkt_values_nas:
            attachment_location.append(r'C:\Users\ '[:-1] + getpass.getuser() + r'\Compass Group\Riesgo Financiero - Documentos\Data Bases\Security Master\Scripts\Reports\MarketValues\mkt_values_nas_'+ self.date_fin.replace('-','') +'.xlsx')
        if send_mkt_values_dups:
            attachment_location.append(r'C:\Users\ '[:-1] + getpass.getuser() + r'\Compass Group\Riesgo Financiero - Documentos\Data Bases\Security Master\Scripts\Reports\MarketValues\mkt_values_duplicates_'+ self.date_fin.replace('-','') +'.xlsx')
        # Envía el correo
        obj_sm.send_email(email_sender,email_recipient, email_subject, email_message, html_format = True, attachment_location = attachment_location )
    def price_changes_color(self,df):
        '''
        Define color of New Target Price
        If larget than past target price green if lower then red
        '''
        green_col = 'background-color: mediumaquamarine'
        c2 = ''
        green = df['New Target Price']> df['Past Target Price']
        df1 = pd.DataFrame(c2, index=df.index, columns=df.columns)
        df1.loc[green, 'New Target Price'] = green_col
        
        red_col = 'background-color: lightcoral'
        red = df['New Target Price']< df['Past Target Price']
        df1.loc[red, 'New Target Price'] = red_col   
        return df1    
    def recom_changes_color(self,df):
        '''
        Define color of New Target Price
        If larget than past target price green if lower then red
        '''
        green_col = 'background-color: mediumaquamarine'
        c2 = ''
        green = df['type_recommendation_id']> df['type_recommendation_sm']
        df1 = pd.DataFrame(c2, index=df.index, columns=df.columns)
        df1.loc[green, 'New Recommendation'] = green_col
        
        red_col = 'background-color: lightcoral'
        red = df['type_recommendation_id']< df['type_recommendation_sm']
        df1.loc[red, 'New Recommendation'] = red_col   
        return df1
    def formato_reporte_tp(self,df):
        df_style = df.style.format(precision = 2, thousands = ',')
        # Borders
        df_style.set_properties(
            **{'color': 'black !important',
               'border': '1px black solid !important', 
               'text-align':'left', 
               'padding': '0px', 
               })
        # Headers
        headers = {
            'selector': 'th:not(.index_name)',
            'props': 'background-color: crimson; color: white; font-size :20px; border: 0.5px solid black;'
        }
        
        # Disminuir espacio entre lineas
        rows = {'selector': 'td', 'props': 'padding-top: 0x; padding-top: 0x'}
        
        # Da el formato lo anterior lo define
        df_style.set_table_styles([headers,rows])
        
        df_style.set_table_styles([  # create internal CSS classes
                                           {'selector': '.right_align', 'props': 'text-align : right', 
                                            'selector': '.center_align', 'props': 'text-align : center'},]
                                          , overwrite=False)
        
        # Alinear textos
        text_align = pd.DataFrame('',index = df_style.index, columns =['New Recommendation', 'New Target Price', 'Update Date','Last Recommendation','Past Target Price', 'Last Update Date'] )
        text_align[['New Recommendation','New Target Price', 'Update Date','Last Recommendation','Past Target Price', 'Last Update Date']] = 'center_align'
        text_align['recommendation'] = 'center_align'
        
        df_style.set_td_classes(text_align)
        
        # Color de Precios
        df_style.apply(self.price_changes_color, axis = None)
        
        
        # Cambiar nombres 
        
        
        # Color de Recommendación
        df_style.apply(self.recom_changes_color, axis = None)
        
        # Caption
        df_style.set_caption('<br>New Target Price: green if new target price is greater than past, red if lower <br> New Recommendation: green if greater than past red if lower ')\
            .set_table_styles([{
                'selector': 'caption',
                'props': 'caption-side: bottom; font-size:1em; color: black; text-align:left;'
                }],        overwrite=False)
            
        # Quitar indice
        df_style.hide_index()
        
        # Quitar columnas que no sirve
        df_style.hide_columns(subset = ['type_recommendation_id', 'type_recommendation_sm'])
        
        
        return df_style       
    def formato_reporte_sec(self,df):
        df_style = df.style.format(precision = 2, thousands = ',')
    
        # Borders
        df_style.set_properties(
            **{'color': 'black !important',
               'border': '1px black solid !important', 
               'text-align':'left', 
               'padding': '0px', 
               })
        # Headers
        headers = {
            'selector': 'th:not(.index_name)',
            'props': 'background-color: crimson; color: white; font-size :20px; border: 0.5px solid black;'
        }
        
        # Disminuir espacio entre lineas
        rows = {'selector': 'td', 'props': 'padding-top: 0x; padding-top: 0x'}
        
        # Da el formato lo anterior lo define
        df_style.set_table_styles([headers,rows])
        
        df_style.set_table_styles([  # create internal CSS classes
                                           {'selector': '.right_align', 'props': 'text-align : right', 
                                            'selector': '.center_align', 'props': 'text-align : center'},]
                                          , overwrite=False)
        
        # Alinear textos
        text_align = pd.DataFrame('',index = df_style.index, columns =['date_in'] )
        text_align['date_in'] = 'center_align'
        
        df_style.set_td_classes(text_align)
            
        # Quitar indice
        df_style.hide_index()
        
        return df_style    
    def ratings_changes_color(self,df):
        '''
        Define color of New ratings
        If larget than past target price green if lower then red
        '''
        green_col = 'background-color: mediumaquamarine'
        c2 = ''
        green = df['grade_hoy']< df['grade_ayer']
        df1 = pd.DataFrame(c2, index=df.index, columns=df.columns)
        df1.loc[green, 'subrating_hoy'] = green_col
        
        red_col = 'background-color: lightcoral'
        red = df['grade_hoy']> df['grade_ayer']
        df1.loc[red, 'subrating_hoy'] = red_col   
        return df1            
    def formato_reporte_ratings(self,df):
        df_style = df.style.format(precision = 2, thousands = ',')
        
        # Borders
        df_style.set_properties(
        **{'color': 'black !important',
           'border': '1px black solid !important', 
           'text-align':'left', 
           'padding': '0px', 
           })
        # Headers
        headers = {
        'selector': 'th:not(.index_name)',
        'props': 'background-color: crimson; color: white; font-size :16px; border: 0.5px solid black;'
        }
        
        # Disminuir espacio entre lineas
        rows = {'selector': 'td', 'props': 'padding-top: 0x; padding-top: 0x'}
        
        # Da el formato lo anterior lo define
        df_style.set_table_styles([headers,rows])
        
        df_style.set_table_styles([  # create internal CSS classes
                                       {'selector': '.right_align', 'props': 'text-align : right', 
                                        'selector': '.center_align', 'props': 'text-align : center'},]
                                      , overwrite=False)
        
        # Alinear textos
        text_align = pd.DataFrame('',index = df_style.index, columns =['grade_hoy','grade_ayer', 'rating_hoy', 'rating_ayer', 'subrating_hoy', 'subrating_ayer', 'credit_hoy', 'credit_ayer' ] )
        text_align[['grade_hoy','grade_ayer', 'rating_hoy', 'rating_ayer', 'subrating_hoy', 'subrating_ayer', 'credit_hoy', 'credit_ayer']] = 'center_align'
        
        df_style.set_td_classes(text_align)
        
        # Color de Precios
        df_style.apply(self.ratings_changes_color, axis = None)
        
        # Caption
        df_style.set_caption('<br>subrating_hoy: green if new rating is greater than past, red if lower')\
        .set_table_styles([{
            'selector': 'caption',
            'props': 'caption-side: bottom; font-size:1em; color: black; text-align:left;'
            }],        overwrite=False)
        
        # Quitar indice
        df_style.hide_index()
        
        
        return df_style        
    def send_reports(self, mail_sender):
        '''
        Method that sends reports of target prices, ratings, internal ratings, new securities.

        Parameters
        ----------
        mail_sender : string
            who sends the email must be in list of available senders in security_master object send_mail method

        Returns
        -------
        None.

        '''
        
        # =============================================================================
        # Generate new Target Prices
        # =============================================================================
        obj_sm = Security_Master(self.date_ini, self.date_fin)
        
        # Genera nuevos Target Prices
        tp_act, tp_new = obj_sm.carga_new_tp()
        
        # Para los que no tenían target price, solamente buscamos los de equity
        iss_id_nuevos = tp_new['issuerid_id'].tolist()
        secs_de_nuevos_tp = obj_sm.get_all_from_id(iss_id_nuevos, by = 'issuerid_id')
        secs_de_nuevos_tp = secs_de_nuevos_tp[secs_de_nuevos_tp['securityid_id'].str.contains('EQ')]
        df_nuevos_tp = tp_new.merge(secs_de_nuevos_tp, how = 'inner', on = 'issuerid_id')
        df_nuevos_tp.rename(columns = {'securityid_id_x': 'securityid_id', 
                                       'ticker_issuer_x': 'ticker_issuer', 
                                       'compass_issuer_name_x':'compass_issuer_name'}, 
                            inplace = True)
        df_nuevos_tp = df_nuevos_tp.drop_duplicates(subset = ['tp_mongo', 'date_e_mongo', 'type_recommendation_id', 'compass_issuer_name', 'securityid_id', 'ticker_issuer', 'isin']).reset_index()
        df_nuevos_tp = df_nuevos_tp[tp_act.columns]
        
        
        # Si no esta vacio entonces append al tp_act
        if not df_nuevos_tp.empty:
            tp_act = tp_act.append(df_nuevos_tp)
        
        
        # Genera una copia
        tp_reporte = tp_act.copy()
        # Datetime a fecha de mongo
        tp_reporte.date_e_mongo = tp_reporte.date_e_mongo.apply(datetime.strftime,
                                                                args = ['%Y-%m-%d'])
        # Cambiar nombres de columnas
        tp_reporte.rename(columns = {'tp_mongo': 'New Target Price', 
                                 'tp_sm': 'Past Target Price', 
                                 'date_e_sm':'Last Update Date', 
                                 'date_e_mongo': 'Update Date', 
                                 'recommendation_mongo': 'New Recommendation', 
                                 'recommendation': 'Last Recommendation', 
                                 }, inplace = True)
        
        #  Seleccionar solamente columnas relevantes
        tp_reporte = tp_reporte[['issuerid_id', 'Issuer_Compass_Alias', 'securityid_id', 
                                 'New Recommendation', 'New Target Price','Update Date',
                                 'Last Recommendation','Past Target Price' , 
                                 'Last Update Date', 'type_recommendation_id', 
                                 'type_recommendation_sm']]
        
        # Agregar la cobertura
        coverage = obj_sm.generic_get_from_sm("SELECT issuerid_id, analyst_id,date_in from Coverage")
        coverage = coverage.sort_values(by = 'date_in', ascending = False).drop_duplicates(subset = ['issuerid_id'], keep = 'first')
        coverage = coverage.drop(columns=['date_in'])
        
        #Como el coverage tiene id de analista necesitamos ponerle el nombre para identificarlos
        analyst1 = obj_sm.generic_get_from_sm("SELECT id, analyst_name,date_out, mail from Analyst")
        analyst1 = analyst1.rename(columns={'id':'analyst_id'})
        analyst1 = analyst1[analyst1.date_out.isnull()]
        analyst1 = analyst1.drop(columns=['date_out'])
        
        coverage = coverage.merge(analyst1, how = 'left', on = 'analyst_id')
        
        tp_reporte = tp_reporte.merge(coverage, how = 'left', on = 'issuerid_id')
        tp_reporte.drop(['analyst_id','mail'], axis =1, inplace = True)
        
        
        # Checar si está vacío
        if tp_reporte.empty:
            send_tp = False
        else:
            # Si no está vacio entonces hay nuevos cambios, guardar la variable y actualizar html
            send_tp = True
            path = r'\Compass Group\Riesgo Financiero - Documentos\Data Bases\Security Master\Scripts\Reports\Nuevos Reportes\ '[:-1]
            tp_reporte_style = self.formato_reporte_tp(tp_reporte)
            obj_sm.pandas_to_html(os.getcwd()+path, 'Nuevos Target Prices Formato', tp_reporte_style)
        
        # =============================================================================
        # Generate new securities
        # =============================================================================
       

        # Generar nuevos securities cargados
        try:
            df_nuevos_cargados = obj_sm.control_cargas_securities()
            df_nuevos_cargados.sort_values(['securityid_id', 'date_in'], ascending = [True, False], inplace = True)
            df_nuevos_cargados.drop_duplicates(subset = ['securityid_id'], keep = 'first', inplace = True)
            df_nuevos_cargados_style = self.formato_reporte_sec(df_nuevos_cargados)
        except:
            # df_nuevos_cargados = pd.DataFrame(data = [1], columns = ['Prueba'])
            df_nuevos_cargados = pd.DataFrame()
            
        
        # Checar si está vacío
        if df_nuevos_cargados.empty:
            send_new_sec = False
        else:
            # Si no está vacio entonces hay nuevos cambios, guardar la variable y actualizar html
            path = r'\Compass Group\Riesgo Financiero - Documentos\Data Bases\Security Master\Scripts\Reports\Nuevos Reportes\ '[:-1]
            obj_sm.pandas_to_html(os.getcwd()+path, 'Nuevos Securities Formato', df_nuevos_cargados_style)
            send_new_sec = True
        
        
        # =============================================================================
        # Reporte de ratings
        # =============================================================================
        
        # Path al reporte generado
        path = r"Compass Group\Riesgo Financiero - Documentos\Data Bases\Security Master\Scripts\Reports\Ratings\Credit migration.html"
        # Fecha de modificación
        mod_time = datetime.fromtimestamp( os.path.getmtime(path))
        mod_time = datetime.strftime(mod_time, '%Y-%m-%d')
        # Fecha de hoy
        todays_date = datetime.strftime(datetime.today(), '%Y-%m-%d')
        
        
        # Si las fechas coinciden
        if todays_date == mod_time:
            # Si se enviaría el reporte
            send_ratings = True
            ratings_df = pd.read_html(path)[0]
            # genera issuerid_id
            ratings_df['issuerid_id'] = [x[0:8] for x in ratings_df['securityid_id']]
            # Pegar cobertura
            ratings_df = ratings_df.merge(coverage, how = 'left', on = 'issuerid_id')
            
            ratings_df.drop(['issuerid_id', 'analyst_id','mail'], axis =1, inplace = True)
            
            ratings_df_style = self.formato_reporte_ratings(ratings_df)
            # Si esta actualizado convierto a HTML con formato
            path = r'\Compass Group\Riesgo Financiero - Documentos\Data Bases\Security Master\Scripts\Reports\Nuevos Reportes\ '[:-1]
            obj_sm.pandas_to_html(os.getcwd()+path, 'Nuevos Ratings Formato', ratings_df_style)
        else:
            # No se envía
            send_ratings = False
        
        # =============================================================================
        # Cargas Internal Ratings
        # =============================================================================
        
        # Obtener los que hayan cambiado
        dfCarga = self.carga_ir_mongo()
        
        # Hacer cambios para preparar para carga
        
        carga = dfCarga[['securityid_id', 'grades_min_mongo',  'date_r']].copy()
        carga['rating_source_id'] = 3
        carga['country_id'] = 30
        carga['grades_prom'] = float("NaN")
        carga['grades_max'] = float("NaN")
        carga['rating_outlook_id'] = float("NaN")
        # reordenar
        columns = ['securityid_id', 'rating_source_id','country_id',  'grades_min_mongo','grades_prom', 'grades_max', 'rating_outlook_id', 'date_r']
        carga = carga[columns]
        # CAmbiar nombre
        carga.rename(columns = {'grades_min_mongo':'grades_min'}, 
                     inplace = True)
        
        
        # Reporte
        if not carga.empty:

            send_internal = True
            # Hacer cambios a los nombres del reporte
            secs_query = '''
            SELECT s.id as securityid_id, s.ticker_issuer, i.compass_issuer_name
            FROM SecurityId as s
            LEFT JOIN IssuerId as i ON s.issuerid_id = i.id
            '''
            
            secs = obj_sm.generic_get_from_sm(secs_query)
            
            dfCarga = dfCarga.merge(secs, how = 'left', on = 'securityid_id')
            dfCarga.rename(columns = {'score_mongo': 'grade_hoy' , 
                                      'score_prev': 'grade_ayer', 
                                       'compass_rating_mongo': 'rating_hoy' ,
                                       'compass_subrating_mongo' : 'subrating_hoy' ,
                                       'compass_rating_prev' : 'rating_ayer',
                                       'compass_subrating_prev' : 'subrating_ayer',
                                      }, 
                           inplace = True)
            
            # Reordenar
            dfCarga = dfCarga[['securityid_id', 'ticker_issuer', 'compass_issuer_name',
                   'grade_hoy', 'grade_ayer', 'rating_hoy', 'rating_ayer', 'subrating_hoy',
                   'subrating_ayer']]
            
            # genera issuerid_id
            dfCarga['issuerid_id'] = [x[0:8] for x in dfCarga['securityid_id']]
            # Pegar cobertura
            dfCarga = dfCarga.merge(coverage, how = 'left', on = 'issuerid_id')
            
            dfCarga.drop(['issuerid_id', 'analyst_id','mail'], axis =1, inplace = True)
            
            ratings_int_df_style = self.formato_reporte_ratings(dfCarga)
            # Si esta actualizado convierto a HTML con formato
            path = r'\Compass Group\Riesgo Financiero - Documentos\Data Bases\Security Master\Scripts\Reports\Nuevos Reportes\ '[:-1]
            obj_sm.pandas_to_html(os.getcwd()+path, 'Nuevos Ratings Internal', ratings_int_df_style)
        
        else:
            send_internal = False
        
        # =============================================================================
        # Send if either report is generated
        # =============================================================================
     
        if send_tp:
            # Fecha de hoy con nuevo formato
            date = datetime.strftime(datetime.today(), "%d-%b-%Y")

            # Body del correo
            email_subject = 'Update reports as of: ' + date
            email_message = 'Greetings, attached you will find the updates for the date: ' + date + '.'+\
                '\nThe following files have been updated: \n'
            attachment_location = []
            count = 1
            analyst_tosend= list()
            
            # Cambiar attachments y body del correo dependiendo si se envía o no el correo.
            # Nuevos target prices
            if send_tp == True:
                attachment_location.append(os.getcwd()+r'\Compass Group\Riesgo Financiero - Documentos\Data Bases\Security Master\Scripts\Reports\Nuevos Reportes\Nuevos Target Prices Formato.html')
                email_message = email_message + '\n\t{:1.0f}. New Target Prices'.format(count)
                count +=1
                analyst_tosend = analyst_tosend + list(tp_reporte.analyst_name.unique())
        
            # Sender, debe de tener password en marketrisk_setup        
            email_sender = mail_sender
            # email_sender = 'Bernardo.Alcantara@cgcompass.com'
            
            # Quien recibe el correo, si son varios hacer una lista con los correos
            email_recipient = ['Marketrisk@cgcompass.com', 'antonio.miranda@cgcompass.com'] 
            
            
            mails = analyst1[analyst1.analyst_name.isin(analyst_tosend)].mail.to_list()
            email_recipient	= email_recipient + mails
            # Quitar duplicados
            set_recipient = set(email_recipient)
            email_recipient = list(set_recipient)
            
            # email_recipient = ['Pablo.Garza@cgcompass.com']
            # email_recipient = ['Bernardo.Alcantara@cgcompass.com', 'Pablo.Garza@cgcompass.com', 'juan.carrillo@cgcompass.com']
            # email_recipient = ['Pablo.Garza@cgcompass.com','pablo.garza96@gmail.com']
            
            # Envía el correo
            obj_sm.send_email(email_sender,email_recipient, email_subject, email_message, attachment_location )
        
    
        if send_ratings or send_internal:
            # Fecha de hoy con nuevo formato
            date = datetime.strftime(datetime.today(), "%d-%b-%Y")
            
            # Body del correo
            email_subject = 'Update reports as of: ' + date
            email_message = 'Greetings, attached you will find the updates for the date: ' + date + '.'+\
                '\nThe following files have been updated: \n'
            attachment_location = []
            count = 1
            analyst_tosend= list()
            
            # Cambiar attachments y body del correo dependiendo si se envía o no el correo.
            if send_ratings == True:
                attachment_location.append(os.getcwd()+r'\Compass Group\Riesgo Financiero - Documentos\Data Bases\Security Master\Scripts\Reports\Nuevos Reportes\Nuevos Ratings Formato.html')
                email_message = email_message + '\n\t{:1.0f}. Credit Migration'.format(count)
                count +=1
                analyst_tosend = analyst_tosend + list(ratings_df.analyst_name.unique())
            # Nuevos Internal ratings
            if send_internal == True:
                attachment_location.append(os.getcwd()+r'\Compass Group\Riesgo Financiero - Documentos\Data Bases\Security Master\Scripts\Reports\Nuevos Reportes\Nuevos Ratings Internal.html')
                email_message = email_message + '\n\t{:1.0f}. Ratings Internos'.format(count)
                count +=1
                analyst_tosend = analyst_tosend + list(dfCarga.analyst_name.unique())
  
            # Sender, debe de tener password en marketrisk_setup        
            email_sender = mail_sender
            # email_sender = 'Bernardo.Alcantara@cgcompass.com'
            
            # Quien recibe el correo, si son varios hacer una lista con los correos
            email_recipient = ['Marketrisk@cgcompass.com', 'antonio.miranda@cgcompass.com','luis.pardo@cgcompass.com','tomas.venezian@cgcompass.com','nicolas.garciahuidobro@cgcompass.com'] 
            
            
            mails = analyst1[analyst1.analyst_name.isin(analyst_tosend)].mail.to_list()
            email_recipient	= email_recipient + mails
            # Quitar duplicados
            set_recipient = set(email_recipient)
            email_recipient = list(set_recipient)
            
            # email_recipient = ['Pablo.Garza@cgcompass.com']
            # email_recipient = ['Bernardo.Alcantara@cgcompass.com', 'Pablo.Garza@cgcompass.com', 'juan.carrillo@cgcompass.com']
            # email_recipient = ['Pablo.Garza@cgcompass.com','pablo.garza96@gmail.com']
            
            # Envía el correo
            obj_sm.send_email(email_sender,email_recipient, email_subject, email_message, attachment_location )
        

        if send_new_sec:
            date = datetime.strftime(datetime.today(), "%d-%b-%Y")
            
            
            # Body del correo
            email_subject = 'Update new securites as of: ' + date
            email_message = 'Greetings, attached you will find the updates for the date: ' + date + '.'+\
                '\nThe following files have been updated: \n'
            attachment_location = []
            count = 1
            # Cambiar attachments y body del correo dependiendo si se envía o no el correo.
            # Nuevos securities
            attachment_location.append(os.getcwd()+r'\Compass Group\Riesgo Financiero - Documentos\Data Bases\Security Master\Scripts\Reports\Nuevos Reportes\Nuevos Securities Formato.html')
            email_message = email_message + '\n\t{:1.0f}. New Securities'.format(count)
            count +=1
            
            # Sender, debe de tener password en marketrisk_setup        
            email_sender = mail_sender
            # email_sender = 'Bernardo.Alcantara@cgcompass.com'
            
            # Quien recibe el correo, si son varios hacer una lista con los correos
            email_recipient = ['Marketrisk@cgcompass.com', 'antonio.miranda@cgcompass.com'] 
            # email_recipient = ['Pablo.Garza@cgcompass.com']
            
             # Envía el correo
            obj_sm.send_email(email_sender,email_recipient, email_subject, email_message, attachment_location )
    def carga_tp_a_sm(self, upload = True):
        '''
        Method that uploads the target prices to the security_master
        
        Parameters
        -------
            upload: Boolean
                if true then is uploads to sm, if false it creates the database and returns two dataframes
        Returns
        -------
        None.

        '''
        obj_sm = Security_Master(self.date_ini, self.date_fin)
        tp_act, tp_new = obj_sm.carga_new_tp()
        tp_act = tp_act.rename(columns = {'tp_mongo': 'target_price', 'date_e_mongo': 'date_e'})
        tp_act = tp_act[['securityid_id', 'type_recommendation_id', 'target_price', 'date_e']]
        tp_act = tp_act.dropna()

        #estas líneas solo para cargar algunos selectos de los que salgan de tp_new. Algunos son FI, otros son de EQ y hay que asociar un securityid_id apropiado.
        iss_id_nuevos = tp_new['issuerid_id'].tolist()
        secs_de_nuevos_tp = obj_sm.get_all_from_id(iss_id_nuevos, by = 'issuerid_id')
        secs_de_nuevos_tp = secs_de_nuevos_tp[secs_de_nuevos_tp['securityid_id'].str.contains('EQ')]
        df_nuevos_tp = tp_new[['issuerid_id', 'tp_mongo', 'date_e_mongo', 'type_recommendation_id']].merge(secs_de_nuevos_tp, how = 'inner', on = 'issuerid_id')
        df_nuevos_tp = df_nuevos_tp[['tp_mongo', 'date_e_mongo', 'type_recommendation_id', 'compass_issuer_name', 'securityid_id', 'ticker_issuer', 'isin']].drop_duplicates().reset_index()
        #df_carga_nuevos_tp = df_nuevos_tp.iloc[[0,1,2,3,4,5,7,8]].rename(columns = {'tp_mongo':'target_price', 'date_e_mongo':'date_e'})
        df_carga_nuevos_tp = df_nuevos_tp.rename(columns = {'tp_mongo':'target_price', 'date_e_mongo':'date_e'})
        df_carga_nuevos_tp = df_carga_nuevos_tp[['securityid_id', 'type_recommendation_id', 'target_price', 'date_e']]
        
        if upload:
            # Insertar a Security Master DB
            if not tp_act.empty:
                obj_sm.insert_into_security_master('Equity_recommendation', tp_act)
            
            if not df_carga_nuevos_tp.empty:
                obj_sm.insert_into_security_master('Equity_recommendation', df_carga_nuevos_tp)
        else:
            return tp_act,df_carga_nuevos_tp
    def carga_internal_ratings_a_sm(self):
        '''
        Method that inserts internal ratings to the security_master

        Returns
        -------
        None.

        '''
        obj_sm = Security_Master(self.date_ini, self.date_fin)
        dfCarga = self.carga_ir_mongo()
        
        # Hacer cambios para preparar para carga
        
        carga = dfCarga[['securityid_id', 'grades_min_mongo',  'date_r']].copy()
        carga['rating_source_id'] = 3
        carga['country_id'] = 30
        carga['grades_prom'] = float("NaN")
        carga['grades_max'] = float("NaN")
        carga['rating_outlook_id'] = float("NaN")
        # reordenar
        columns = ['securityid_id', 'rating_source_id','country_id',  'grades_min_mongo','grades_prom', 'grades_max', 'rating_outlook_id', 'date_r']
        carga = carga[columns]
        # CAmbiar nombre
        carga.rename(columns = {'grades_min_mongo':'grades_min'}, 
                     inplace = True)
        
        
        # Carga
        if not carga.empty:
            # Hacer la carga
            obj_sm.insert_into_security_master('Rating', carga)                                    
class t:
    def __init__(self):
        self.root = tk.Tk()
        self.s = ttk.Style(self.root)
        self.s.theme_use('clam')
        self.last_date = ''
        self.next_date = ''
        self.past_date = ''
        self.b1 = ttk.Button(self.root, text='Fecha inicial', command=self.set_first_date).pack(padx=10, pady=10)
        self.b2 = ttk.Button(self.root, text='Fecha final', command=self.set_last_date).pack(padx=10, pady=10)
        self.b4 = ttk.Button(self.root, text='Fecha pasada', command=self.set_past_date).pack(padx=10, pady=10)
        self.b3 = ttk.Button(self.root, text='show', command=self.my_print).pack(padx=10, pady=10)
        self.root.mainloop()
    def my_print(self):
        print ('{}\n{}'.format(self.last_date, self.next_date))
    def set_first_date(self):
        def print_sel():
            print('"{}"'.format(cal.selection_get()))
            self.last_date = str(cal.selection_get())
        def quit1():
            top.destroy()
        top = tk.Toplevel(self.root)
        today_calendar = date.today()
        cal = Calendar(top,
                       font="Arial 14", selectmode='day',
                       cursor="hand1", year=today_calendar.year, month=today_calendar.month, day=5)
        cal.pack(fill="both", expand=True)
        ttk.Button(top, text="OK", command=print_sel).pack()
        ttk.Button(top, text="Exit", command=quit1).pack()
    def set_last_date(self):
        def print_sel():
            print('"{}"'.format(cal.selection_get()))
            self.next_date = str(cal.selection_get())
        def quit1():
            top.destroy()
        top = tk.Toplevel(self.root)
        today_calendar = date.today()
        cal = Calendar(top,
                       font="Arial 14", selectmode='day',
                       cursor="hand1", year=today_calendar.year, month=today_calendar.month, day=5)
        cal.pack(fill="both", expand=True)
        ttk.Button(top, text="OK", command=print_sel).pack()
        ttk.Button(top, text="Exit", command=quit1).pack()
    def set_past_date(self):
        def print_sel():
            print('"{}"'.format(cal.selection_get()))
            self.past_date = str(cal.selection_get())
        def quit1():
            top.destroy()
        top = tk.Toplevel(self.root)
        today_calendar = date.today()
        cal = Calendar(top,
                       font="Arial 14", selectmode='day',
                       cursor="hand1", year=today_calendar.year, month=today_calendar.month, day=5)
        cal.pack(fill="both", expand=True)
        ttk.Button(top, text="OK", command=print_sel).pack()
        ttk.Button(top, text="Exit", command=quit1).pack()  