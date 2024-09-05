import pandas as pd
import streamlit as st
import functools
import numpy as np
import gc
import io
import xlsxwriter
import base64


st.set_page_config(layout='wide')
background_image ="Untitleddesign.jpg"
st.markdown(
     f"""
     <iframe src="data:image/jpg;base64,{base64.b64encode(open(background_image, 'rb').read()).decode(

    )}" style="width:3000px;height:9000px;position: absolute;top:-3vh;right:-350px;opacity: 0.5;background-size: cover;background-position: center;"></iframe>
     """,
     unsafe_allow_html=True )
class SpedProcessor:
   
    def __init__(self, file_paths):
        self.file_paths = file_paths
        self.listaL100 = []
        self.listaL300 = []
        self.listaM300 = []
        self.listaM350 = []
        self.listaN630 = []
        self.listaN670 = []

    def lendoELimpandoDadosSped(self, file_path):
        data = []
        with open(file_path, 'r', encoding='latin-1') as file:
            for linha in file:
                linha = linha.strip()
                if linha.startswith('|'):
                    valores = linha.split('|')[1:]
                    data.append(valores)

        df = pd.DataFrame(data).iloc[:, :13]
        df['Data Inicial'] = df.iloc[0, 9]
        df['Data Final'] = df.iloc[0, 10]
        df['Ano'] = df['Data Inicial'].astype(str).str[-4:]
        df['CNPJ'] = df.iloc[0, 3]
        df['Período Apuração'] = None
        df['Período Apuração Trimestral'] = None
        df['Data Inicial'] = pd.to_datetime(df['Data Inicial'], format='%d%m%Y').dt.strftime('%d/%m/%Y')
        df['Data Final'] = pd.to_datetime(df['Data Final'], format='%d%m%Y').dt.strftime('%d/%m/%Y')
        
        return df

    def classificaPeriodoDeApuracao(self, arquivo, referencia):
        bloco_iniciado = False
        data_index = 0

        perido_apuracao = [
                'A01 – Balanço de Suspensão e Redução até Janeiro',
                'A02 – Balanço de Suspensão e Redução até Fevereiro',
                'A03 – Balanço de Suspensão e Redução até Março',
                'A04 – Balanço de Suspensão e Redução até Abril',
                'A05 – Balanço de Suspensão e Redução até Maio',
                'A06 – Balanço de Suspensão e Redução até Junho',
                'A07 – Balanço de Suspensão e Redução até Julho',
                'A08 – Balanço de Suspensão e Redução até Agosto',
                'A09 – Balanço de Suspensão e Redução até Setembro',
                'A10 – Balanço de Suspensão e Redução até Outubro',
                'A11 – Balanço de Suspensão e Redução até Novembro',
                'A12 – Balanço de Suspensão e Redução até Dezembro',
                'A00 – Receita Bruta/Balanço de Suspensão e Redução Anual']
        
        trimestres = ['T01 – 1º Trimestre',
                    'T02 – 2º Trimestre',
                    'T03 – 3º Trimestre',
                    'T04 – 4º Trimestre']

        for i in range(len(arquivo)):
            if arquivo.loc[i, 2] == referencia:
                if bloco_iniciado:
                    data_index += 1
                else:
                    bloco_iniciado = True

                if data_index < len(perido_apuracao):
                    arquivo.loc[i:, 'Período Apuração'] = perido_apuracao[data_index]
                    try:
                        arquivo.loc[i:, 'Período Apuração Trimestral'] = trimestres[data_index] 
                    except:
                        pass
        return arquivo

    def gerandoArquivosECF(self, caminho):
        df_sped = self.lendoELimpandoDadosSped(caminho)

        df_sped_l100 = df_sped[(df_sped[0] == 'L100') | (df_sped[0] == 'N030')].reset_index(drop=True)
        df_sped_l100 = self.classificaPeriodoDeApuracao(df_sped_l100, 'ATIVO')


        df_sped_l300 = df_sped[df_sped[0] == 'L300'].reset_index(drop=True)
        df_sped_l300 = self.classificaPeriodoDeApuracao(df_sped_l300, 'RESULTADO LÍQUIDO DO PERÍODO')

        df_sped_m300 = df_sped[df_sped[0] == 'M300'].reset_index(drop=True)
        df_sped_m300 = self.classificaPeriodoDeApuracao(df_sped_m300, 'ATIVIDADE GERAL')

        df_sped_m350 = df_sped[df_sped[0] == 'M350'].reset_index(drop=True)
        df_sped_m350 = self.classificaPeriodoDeApuracao(df_sped_m350, 'ATIVIDADE GERAL')

        df_sped_n630 = df_sped[df_sped[0] == 'N630'].reset_index(drop=True)
        df_sped_n630['Período Apuração'] = 'A00 – Receita Bruta/Balanço de Suspensão e Redução Anual'

        df_sped_n670 = df_sped[df_sped[0] == 'N670'].reset_index(drop=True)
        df_sped_n670['Período Apuração'] = 'A00 – Receita Bruta/Balanço de Suspensão e Redução Anual'

        return df_sped_l100, df_sped_l300, df_sped_m300, df_sped_m350, df_sped_n630, df_sped_n670

    def processar_arquivos(self):
        for file_path in self.file_paths:
            df_sped_l100, df_sped_l300, df_sped_m300, df_sped_m350, df_sped_n630, df_sped_n670 = self.gerandoArquivosECF(file_path)
            self.listaL100.append(df_sped_l100)
            self.listaL300.append(df_sped_l300)
            self.listaM300.append(df_sped_m300)
            self.listaM350.append(df_sped_m350)
            self.listaN630.append(df_sped_n630)
            self.listaN670.append(df_sped_n670)

    def concatenar_dfs(self):
        L100_final = pd.concat(self.listaL100).reset_index(drop=True).rename(columns={
            1: 'Conta Referencial', 2: 'Descrição Conta Referencial', 3: "Tipo Conta", 4: 'Nível Conta',
            5: 'Natureza Conta', 6: 'Conta Superior', 8: 'D/C Saldo Final', 11: 'Vlr Saldo Final'}).drop(columns=[7, 9, 10, 0])
        L100_final = L100_final[['CNPJ', 'Data Inicial', 'Data Final', 'Ano', 'Período Apuração','Período Apuração Trimestral',
                                 'Conta Referencial', 'Conta Superior', 'Descrição Conta Referencial',
                                 'Natureza Conta', 'Tipo Conta', 'Nível Conta', 'Vlr Saldo Final', 'D/C Saldo Final']]

        L300_final = pd.concat(self.listaL300).reset_index(drop=True).rename(columns={
            1: "Conta Referencial", 2: 'Descrição Conta Referencial', 3: 'Tipo Conta', 4: "Nível Conta",
            5: 'Natureza Conta', 6: 'Conta Superior', 7: 'Vlr Saldo Final', 8: 'D/C Saldo Final'})
        L300_final = L300_final[['CNPJ', 'Data Inicial', 'Data Final', 'Ano', 'Período Apuração','Período Apuração Trimestral',
                                 'Conta Referencial', 'Conta Superior', 'Descrição Conta Referencial',
                                 'Natureza Conta', 'Tipo Conta', 'Nível Conta', 'Vlr Saldo Final', 'D/C Saldo Final']]

        M300_final = pd.concat(self.listaM300).reset_index(drop=True).rename(columns={
            1: 'Código Lançamento e-Lalur', 2: 'Descrição Lançamento e-Lalur', 3: 'Tipo Lançamento',
            4: 'Indicador Relação Parte A', 5: 'Vlr Lançamento e-Lalur', 6: 'Histórico e-Lalur'})
        M300_final = M300_final[['CNPJ', 'Data Inicial', 'Data Final', 'Ano', 'Período Apuração','Período Apuração Trimestral',
                                 'Código Lançamento e-Lalur', 'Descrição Lançamento e-Lalur', 'Tipo Lançamento',
                                 'Indicador Relação Parte A', 'Vlr Lançamento e-Lalur']]

        M350_final = pd.concat(self.listaM350).reset_index(drop=True).rename(columns={
            1: 'Código Lançamento e-Lacs', 2: 'Descrição Lançamento e-Lacs', 4: 'Indicador Relação Parte A',
            5: 'Vlr Lançamento e-Lacs', 6: 'Histórico e-Lacs'})
        M350_final = M350_final[['CNPJ', 'Data Inicial', 'Data Final', 'Ano', 'Período Apuração','Período Apuração Trimestral',
                                 'Código Lançamento e-Lacs', 'Descrição Lançamento e-Lacs',
                                 'Indicador Relação Parte A', 'Vlr Lançamento e-Lacs', 'Histórico e-Lacs']]

        N630_final = pd.concat(self.listaN630).reset_index(drop=True).rename(columns={
            1: 'Código Lançamento', 2: "Descrição Lançamento", 3: 'Vlr Lançamento'})
        N630_final = N630_final[['CNPJ', 'Data Inicial', 'Data Final', 'Ano', 'Período Apuração','Período Apuração Trimestral',
                                 'Código Lançamento', 'Descrição Lançamento', 'Vlr Lançamento']]

        N670_final = pd.concat(self.listaN670).reset_index(drop=True).rename(columns={
            1: 'Código Lançamento', 2: 'Descrição Lançamento', 3: "Vlr Lançamento"})
        N670_final = N670_final[['CNPJ', 'Data Inicial', 'Data Final', 'Ano', 'Período Apuração','Período Apuração Trimestral',
                                 'Código Lançamento', 'Descrição Lançamento', 'Vlr Lançamento']]

        return {
            "L100": L100_final,
            "L300": L300_final,
            "M300": M300_final,
            "M350": M350_final,
            "N630": N630_final,
            "N670": N670_final
        }


    def tratandoTiposDeDados(self,dfs_concatenados):
            
            L100_final = dfs_concatenados["L100"]
            L100_final['Vlr Saldo Final'] = L100_final['Vlr Saldo Final'].str.replace(',','.').astype(float)
            L300_final = dfs_concatenados["L300"]
            L300_final['Vlr Saldo Final'] = L300_final['Vlr Saldo Final'].str.replace(',','.').astype(float)

            M300_final = dfs_concatenados["M300"]
            M300_final['Vlr Lançamento e-Lalur'] = (M300_final['Vlr Lançamento e-Lalur'].str.replace(',', '.').replace('', np.nan))
            M300_final['Vlr Lançamento e-Lalur'] = pd.to_numeric(M300_final['Vlr Lançamento e-Lalur'])
            M300_final['Código Lançamento e-Lalur'] = pd.to_numeric(M300_final['Código Lançamento e-Lalur'])
            M300_final['Vlr Lançamento e-Lalur'].fillna(0, inplace=True)

            M350_final = dfs_concatenados["M350"]
            M350_final['Vlr Lançamento e-Lacs'] = (M350_final['Vlr Lançamento e-Lacs'].str.replace(',', '.').replace('', np.nan))
            M350_final['Vlr Lançamento e-Lacs'] = pd.to_numeric(M350_final['Vlr Lançamento e-Lacs'])
            M350_final['Código Lançamento e-Lacs'] = pd.to_numeric(M350_final['Código Lançamento e-Lacs'])
            M350_final['Vlr Lançamento e-Lacs'].fillna(0, inplace=True)

            N630_final = dfs_concatenados["N630"]
            N630_final['Vlr Lançamento'] = (N630_final['Vlr Lançamento'].str.replace(',', '.').replace('', np.nan))
            N630_final['Vlr Lançamento'] = pd.to_numeric(N630_final['Vlr Lançamento'])
            N630_final['Código Lançamento'] = pd.to_numeric(N630_final['Código Lançamento'])
            N630_final['Vlr Lançamento'].fillna(0, inplace=True)

            N670_final = dfs_concatenados["N670"]
            N670_final['Vlr Lançamento'] = (N670_final['Vlr Lançamento'].str.replace(',', '.').replace('', np.nan))
            N670_final['Vlr Lançamento'] = pd.to_numeric(N670_final['Vlr Lançamento'])
            N670_final['Código Lançamento'] = pd.to_numeric(N670_final['Código Lançamento'])
            N670_final['Vlr Lançamento'].fillna(0, inplace=True)   

            return L100_final,L300_final,M300_final,M350_final,N630_final,N670_final 



if __name__=='__main__':
    uploaded_files = st.sidebar.file_uploader("Escolha os arquivos SPED", type=['txt'], accept_multiple_files=True)
    col1,col2,col3 = st.columns(3)
    if uploaded_files:
            file_paths = []
            for uploaded_file in uploaded_files:
                file_path = uploaded_file.name
                with open(file_path, 'wb') as f:
                    f.write(uploaded_file.getbuffer())
                file_paths.append(file_path)
            
            sped_processor = SpedProcessor(file_paths)  
            sped_processor.processar_arquivos()
            dfs_concatenados = sped_processor.concatenar_dfs()
            L100_final, L300_final, M300_final, M350_final, N630_final, N670_final = sped_processor.tratandoTiposDeDados(dfs_concatenados)


    with col1:
        st.subheader('L100')
        st.dataframe(L100_final)
        st.subheader('L300')
        st.dataframe(L300_final)
    with col2:
        st.subheader('M300')
        st.dataframe(M300_final)
        st.subheader('M350')
        st.dataframe(M350_final)    

    with col3:
        st.subheader('N630')
        st.dataframe(N630_final)
        st.subheader('N670')
        st.dataframe(N670_final)

    
    
    
    output9 = io.BytesIO()
    with pd.ExcelWriter(output9, engine='xlsxwriter') as writer:L100_final.to_excel(writer,sheet_name=f'L100',index=False)
    st.download_button(type='primary',label="Exportar L100",data=output9,file_name=f'L100.xlsx',key='download_buttonL100')
    
    output10 = io.BytesIO()
    with pd.ExcelWriter(output10, engine='xlsxwriter') as writer:L300_final.to_excel(writer,sheet_name=f'L300',index=False)
    st.download_button(type='primary',label="Exportar L300",data=output10,file_name=f'L300.xlsx',key='download_buttonL300')
    
    output11 = io.BytesIO()
    with pd.ExcelWriter(output11, engine='xlsxwriter') as writer:M300_final.to_excel(writer,sheet_name=f'M300',index=False)
    st.download_button(type='primary',label="Exportar M300",data=output11,file_name=f'M300.xlsx',key='download_buttonM300')
    
    output7 = io.BytesIO()
    with pd.ExcelWriter(output7, engine='xlsxwriter') as writer:M350_final.to_excel(writer,sheet_name=f'M350',index=False)
    st.download_button(type='primary',label="Exportar M350",data=output7,file_name=f'M350.xlsx',key='download_buttonM350')
    
    output6 = io.BytesIO()
    with pd.ExcelWriter(output6, engine='xlsxwriter') as writer:N630_final.to_excel(writer,sheet_name=f'N630',index=False)
    st.download_button(type='primary',label="Exportar N630",data=output6,file_name=f'N630.xlsx',key='download_buttonN630')
    
    output5 = io.BytesIO()
    with pd.ExcelWriter(output5, engine='xlsxwriter') as writer:N670_final.to_excel(writer,sheet_name=f'N670',index=False)
    st.download_button(type='primary',label="Exportar N670",data=output5,file_name=f'N670.xlsx',key='download_button')

