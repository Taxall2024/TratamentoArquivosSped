import pandas as pd
import streamlit as st


st.set_page_config(layout='wide')
# Caminho do arquivo de texto
sped1 = r'C:\Users\lauro.loyola\Desktop\Data Lake\SPEDECF-79283065000141-20190101-20191231-20231208111202.txt'
sped2 = r'C:\Users\lauro.loyola\Desktop\Data Lake\SPEDECF-79283065000141-20200101-20201231-20231208111910.txt'
sped3 = r'C:\Users\lauro.loyola\Desktop\Data Lake\SPEDECF-79283065000141-20210101-20211231-20231016174350.txt'
sped4 = r'C:\Users\lauro.loyola\Desktop\Data Lake\SPEDECF-79283065000141-20220101-20221231-20231114173914.txt'
sped5 = r'C:\Users\lauro.loyola\Desktop\Data Lake\SPEDECF-79283065000141-20230101-20231231-20240417183332.txt'
listaDeArquivos= [sped1,sped2,sped3,sped4,sped5]

listaL100 = []
listaL300 = []
listaM300 = []
listaM350 = []
listaN630 = []
listaN670 = []

def lendoELimpandoDadosSped(filePath):
# Inicializa uma lista para armazenar todas as linhas formatadas
    data = []

    # Abre o arquivo e lê linha por linha
    with open(filePath, 'r', encoding='latin-1') as file:
        for linha in file:
            # Remove espaços em branco ao redor da linha
            linha = linha.strip()
            
            # Verifica se a linha começa com um padrão de 4 dígitos seguido por '|'
            if linha.startswith('|'):
                # Separa os valores utilizando '|' como delimitador
                valores = linha.split('|')[1:]  # Ignora o primeiro elemento vazio antes do primeiro '|'
                data.append(valores)

    df = pd.DataFrame(data).iloc[:,:13]
    df['Data Inicial'] = df.iloc[0,9]
    df['Data Final'] = df.iloc[0,10]
    df['Ano'] = df['Data Inicial'].astype(str).str[-4:]
    df['CNPJ'] = df.iloc[0,3]
    df['Período Apuração'] = None
    df['Data Inicial'] = pd.to_datetime(df['Data Inicial'], format='%d%m%Y').dt.strftime('%d/%m/%Y')
    df['Data Final'] = pd.to_datetime(df['Data Final'], format='%d%m%Y').dt.strftime('%d/%m/%Y')
    
    return df
peridoApuracao = [
    'A00 – Receita Bruta/Balanço de Suspensão e Redução Anual',
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
    'A12 – Balanço de Suspensão e Redução até Dezembro']


def classificaPeriodoDeApuracao(arquivo, referencia):
    bloco_iniciado = False
    data_index = 0
    for i in range(len(arquivo)):
        if arquivo.loc[i, 2] == referencia:
            if bloco_iniciado:
                data_index += 1
            else:
                # Inicia o bloco na primeira ocorrência
                bloco_iniciado = True
            
            # Atualizar a coluna 'Data Final' para o bloco atual
            if data_index < len(peridoApuracao):
                arquivo.loc[i:, 'Período Apuração'] = peridoApuracao[data_index]
    return arquivo

# dfSped1 = lendoELimpandoDadosSped(sped1)

# dfSped1L100 =  dfSped1[dfSped1[0]=='L100'].reset_index(drop='index')
# dfSped1L100 = classificaPeriodoDeApuracao(dfSped1L100,'ATIVO')

# dfSped1L300 =  dfSped1[dfSped1[0]=='L300'].reset_index(drop='index')
# dfSped1L300 = classificaPeriodoDeApuracao(dfSped1L300,'RESULTADO LÍQUIDO DO PERÍODO')

# dfSped1M300 =  dfSped1[dfSped1[0]=='M300'].reset_index(drop='index')
# dfSped1M300 = classificaPeriodoDeApuracao(dfSped1M300,'ATIVIDADE GERAL')

# dfSped1M350 =  dfSped1[dfSped1[0]=='M350'].reset_index(drop='index')
# dfSped1M350 = classificaPeriodoDeApuracao(dfSped1M350,'ATIVIDADE GERAL')

# dfSped1N630 =  dfSped1[dfSped1[0]=='N630'].reset_index(drop='index')
# dfSped1N630['Período Apuração'] = 'A00 – Receita Bruta/Balanço de Suspensão e Redução Anual'

# dfSped1N670 =  dfSped1[dfSped1[0]=='N670'].reset_index(drop='index')
# dfSped1N670['Período Apuração'] = 'A00 – Receita Bruta/Balanço de Suspensão e Redução Anual'



def gerandoArquivosECF(caminho):
    dfSped1 = lendoELimpandoDadosSped(caminho)

    dfSped1L100 =  dfSped1[dfSped1[0]=='L100'].reset_index(drop='index')
    dfSped1L100 = classificaPeriodoDeApuracao(dfSped1L100,'ATIVO')

    dfSped1L300 =  dfSped1[dfSped1[0]=='L300'].reset_index(drop='index')
    dfSped1L300 = classificaPeriodoDeApuracao(dfSped1L300,'RESULTADO LÍQUIDO DO PERÍODO')

    dfSped1M300 =  dfSped1[dfSped1[0]=='M300'].reset_index(drop='index')
    dfSped1M300 = classificaPeriodoDeApuracao(dfSped1M300,'ATIVIDADE GERAL')

    dfSped1M350 =  dfSped1[dfSped1[0]=='M350'].reset_index(drop='index')
    dfSped1M350 = classificaPeriodoDeApuracao(dfSped1M350,'ATIVIDADE GERAL')

    dfSped1N630 =  dfSped1[dfSped1[0]=='N630'].reset_index(drop='index')
    dfSped1N630['Período Apuração'] = 'A00 – Receita Bruta/Balanço de Suspensão e Redução Anual'

    dfSped1N670 =  dfSped1[dfSped1[0]=='N670'].reset_index(drop='index')
    dfSped1N670['Período Apuração'] = 'A00 – Receita Bruta/Balanço de Suspensão e Redução Anual'

    return dfSped1L100, dfSped1L300, dfSped1M300, dfSped1M350, dfSped1N630, dfSped1N670

for i in listaDeArquivos:
    dfSped1L100, dfSped1L300, dfSped1M300, dfSped1M350, dfSped1N630, dfSped1N670 = gerandoArquivosECF(i)
    listaL100.append(dfSped1L100)
    listaL300.append(dfSped1L300)
    listaM300.append(dfSped1M300)
    listaM350.append(dfSped1M350)
    listaN630.append(dfSped1N630)
    listaN670.append(dfSped1N670)

L100Final = pd.concat(listaL100).reset_index(drop='index')
L300Final = pd.concat(listaL300).reset_index(drop='index')
M300Final = pd.concat(listaM300).reset_index(drop='index')
M350Final = pd.concat(listaM350).reset_index(drop='index')
N630Final = pd.concat(listaN630).reset_index(drop='index')
N670Final = pd.concat(listaN670).reset_index(drop='index')

st.dataframe(L100Final)
st.dataframe(L300Final)
st.dataframe(M300Final)
st.dataframe(M350Final)
st.dataframe(N630Final)
st.dataframe(N670Final)


# arquivoCompilado = []
# listaL100 = []
# listaL300 = []
# listaM300 = []
# listaM350 = []
# listaN630 = []
# listaN670 = []

# # Contador para rastrear o índice das datas_finais
# data_index = 0

# # Flag para rastrear o início de um novo bloco
# bloco_iniciado = False

# listaDeArquivos= [sped1,sped2,sped3,sped4,sped5,]

# for i in listaDeArquivos:
#     arquivo1 = lendoELimpandoDadosSped(i).iloc[:,:13]
#     arquivo1['Data Inicial'] = arquivo1.iloc[0,9]
#     arquivo1['Data Final'] = arquivo1.iloc[0,10]
#     arquivo1['Ano'] = arquivo1['Data Inicial'].astype(str).str[-4:]
#     arquivo1['CNPJ'] = arquivo1.iloc[0,3]
#     arquivo1['Período Apuração'] = None

#     arquivo1['Data Inicial'] = pd.to_datetime(arquivo1['Data Inicial'], format='%d%m%Y').dt.strftime('%d/%m/%Y')
#     arquivo1['Data Final'] = pd.to_datetime(arquivo1['Data Final'], format='%d%m%Y').dt.strftime('%d/%m/%Y')
    
#     arquivoCompilado.append(arquivo1)

# arquivoCompiladoDF = pd.concat(arquivoCompilado)



# l100 = arquivoCompiladoDF[arquivoCompiladoDF[0]=='L100'].reset_index(drop='index').rename(columns = {1:'Conta Referencial',
#                                         2:'Descrição Conta Referencial',
#                                         3:'Tipo Conta',
#                                         4:'Nível Conta',
#                                         5:'Natureza Conta',
#                                         6:'Conta Superior',
#                                         8:'D/C Saldo Final',
#                                         11:'Vlr Saldo Final'
#                                         }).drop(columns=[7,9,10,12])


# #l100 = l100[['Data Inicial','Data Final','Ano','']]

# datas_finais = [
#     'A00 – Receita Bruta/Balanço de Suspensão e Redução Anual',
#     'A01 – Balanço de Suspensão e Redução até Janeiro',
#     'A02 – Balanço de Suspensão e Redução até Fevereiro',
#     'A03 – Balanço de Suspensão e Redução até Março',
#     'A04 – Balanço de Suspensão e Redução até Abril',
#     'A05 – Balanço de Suspensão e Redução até Maio',
#     'A06 – Balanço de Suspensão e Redução até Junho',
#     'A07 – Balanço de Suspensão e Redução até Julho',
#     'A08 – Balanço de Suspensão e Redução até Agosto',
#     'A09 – Balanço de Suspensão e Redução até Setembro',
#     'A10 – Balanço de Suspensão e Redução até Outubro',
#     'A11 – Balanço de Suspensão e Redução até Novembro',
#     'A12 – Balanço de Suspensão e Redução até Dezembro']

# l300 = arquivoCompiladoDF[arquivoCompiladoDF[0]=='L300'].reset_index(drop='index')
# m300 = arquivoCompiladoDF[arquivoCompiladoDF[0]=='M300'].reset_index(drop='index')
# m350 = arquivoCompiladoDF[arquivoCompiladoDF[0]=='M350'].reset_index(drop='index')
# n630 = arquivoCompiladoDF[arquivoCompiladoDF[0]=='N630'].reset_index(drop='index')
# n670 = arquivoCompiladoDF[arquivoCompiladoDF[0]=='N670'].reset_index(drop='index')


# # Iterar sobre o DataFrame para verificar a presença de 'ATIVO'
# for i in range(len(l100)):
#     if l100.loc[i, 'Descrição Conta Referencial'] == 'ATIVO':
#         if bloco_iniciado:
#             data_index += 1
#         else:
#             # Inicia o bloco na primeira ocorrência
#             bloco_iniciado = True
        
#         # Atualizar a coluna 'Data Final' para o bloco atual
#         if data_index < len(datas_finais):
#             l100.loc[i:, 'Período Apuração'] = datas_finais[data_index]

# col1,col2,col3,col4,col5,col6 = st.columns(6)

# with col1:
#     st.subheader('L100 Final')
#     st.dataframe(l100)
#     l100['Vlr Saldo Final'] = l100['Vlr Saldo Final'].str.replace(',','.').astype(float)
#     saldoFinal = l100['Vlr Saldo Final'].sum()
#     st.warning(f"{saldoFinal:,.2f}")

# with col2:

#     st.subheader('L300 Final')
#     st.dataframe(l300)

#     l300[7] = l300[7].str.replace(',','.').astype(float)
#     saldoFinal = l300[7].sum()
#     st.warning(f"{saldoFinal:,.2f}")

# with col3:


#     st.subheader('M300 Final')
#     st.dataframe(m300)

# with col4:

#     st.subheader('M350 Final')
#     st.dataframe(m350)

# with col5:

#     st.subheader('N630 Final')
#     st.dataframe(n630)

# with col6:


#     st.subheader('N670 Final')
#     st.dataframe(n670)

# # st.subheader('M300')
# # st.dataframe(m300)

# # st.subheader('M350')
# # st.dataframe(m350)

# # st.subheader('N630')
# # st.dataframe(n630)

# # st.subheader('N670')
# # st.dataframe(n670)

# st.dataframe(arquivo1)


