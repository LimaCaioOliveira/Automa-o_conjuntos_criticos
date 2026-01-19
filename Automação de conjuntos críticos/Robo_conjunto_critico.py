import pyodbc
import pandas as pd
import os
import logging
import requests
import sys
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv

#Configuração de Ambiente
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')

load_dotenv()

#Configuração de Logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    handlers=[
        logging.FileHandler("robo_conjunto_critico.log", encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class RoboOracle:
    def __init__(self):
        #Credenciais Oracle
        self.dsn = os.getenv('ORACLE_DSN')
        self.user = os.getenv('ORACLE_USER')
        self.password = os.getenv('ORACLE_PASS')
        self.conn_str = f"DSN={self.dsn};UID={self.user};PWD={self.password}"

    def conectar(self):
        try:
            return pyodbc.connect(self.conn_str)
        except Exception as e:
            logger.error(f"Erro de conexão (HMLGDS): {e}")
            return None

    def _padronizar_dataframe(self, df):
        """Padronizando nomes de colunas e tipos de dados."""
        if df is None or df.empty:
            return df

        #Primeira tratativa - Colunas para MAIÚSCULO e sem espaços
        df.columns = [str(col).upper().strip() for col in df.columns]
        
        #Segunda tratativa - Padronização de Chaves
        if 'DES_CONJUNTO' in df.columns:
            df['DES_CONJUNTO'] = df['DES_CONJUNTO'].astype(str).str.strip().str.upper()

        #Terceira tratativa - Limpeza ROBUSTA da ocorrência
        if 'OCORRENCIA' in df.columns:
            # Remove .0 do final se existir
            df['OCORRENCIA'] = df['OCORRENCIA'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)

        #Quarta tratativa - Conversão de Datas
        cols_data = ['Data_Reclamação', 'Data_carga']
        for col in cols_data:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        #Quinta tratativa - Conversão Numérica
        if 'CI' in df.columns:
            #Removendo os pontos/vírgulas
            df['CI'] = pd.to_numeric(df['CI'], errors='coerce').fillna(0).astype(int)

        return df

    def buscar_dados_conjunto(self):
        logger.info("Buscando dados de Ocorrências")
        conn = self.conectar()
        
        #MODO SIMULAÇÃO - para teste do envio
        if not conn:
            logger.warning(" Utilizando os dados SIMULADOS.")
            now = datetime.now()
            df = pd.DataFrame({
                'REGIONAL': ['01', '02', '03', '04', '05'],
                'OCORRENCIA': ['001', '002', '003', '004', '005'],
                'ABRANGENCIA': ['A', 'B', 'C', 'D', 'E'],
                'DES_CONJUNTO': ['CAMPO GRANDE', 'DUQUE DE CAXIAS', 'BANGU', 'CAMPO GRANDE', 'JACAREPAGUA'],
                'SITUACAO': ['P', 'A', 'P', 'D', 'P'],
                'DH_RECLA': [now, now, now, now, now],
                'CI': [150, 2000, 50, 300, 96] 
            })
            return self._padronizar_dataframe(df)

        try:
            # Query Completa dummy
            query = """
 SELECT * FROM (
    SELECT
        rt.load_date
        ,rt.region_name
        ,rt.service_id
        ,rt.scope_area
        ,rt.complaint_time
        -- Lógica de formatação de tempo (HH:MM) baseada no Sysdate
        ,TO_CHAR(TRUNC((SYSDATE - rt.complaint_time) * 24),'FM9900') || ':' || 
         TO_CHAR(TRUNC(((SYSDATE - rt.complaint_time) * 24 - TRUNC((SYSDATE - rt.complaint_time)*24)) * 60), 'FM00') AS time_elapsed
        -- Cálculo condicional de tempo pendente
        ,CASE 
            WHEN rt.status LIKE 'PENDING' THEN
                TO_CHAR(TRUNC((SYSDATE - rt.complaint_time) * 24),'FM9900') || ':' || 
                TO_CHAR(TRUNC(((SYSDATE - rt.complaint_time) * 24 - TRUNC((SYSDATE - rt.complaint_time)*24)) * 60), 'FM00')
            ELSE NULL
         END AS pending_time
        ,rt.vehicle_id
        ,rt.service_type
        -- Transformação de Status baseada em Regra de Negócio
        ,CASE 
            WHEN rt.service_type = 'PRIORITY_TYPE' THEN 'D'
            ELSE rt.status 
         END AS final_status
        ,rt.customer_count
        -- Cálculo de impacto (Ex: CHI - Customer Hours Interrupted)
        ,ROUND((((SYSDATE - rt.complaint_time)*24*60*60)/3600) * rt.customer_count, 0) AS impact_metric
        ,rt.operation_label
        ,kpi.cluster_name
        ,kpi.indicator_value
        ,kpi.indicator_target
        -- Percentual de consumo da meta
        ,(ROUND(kpi.indicator_value / kpi.indicator_target, 2)) * 100 AS consumption_pct
        ,ROUND(kpi.indicator_value / kpi.indicator_target, 2) AS consumption_ratio
        ,rt.notes
        ,rt.neighborhood
        -- Tag de criticidade (consumo >= 90%)
        ,CASE 
            WHEN ROUND(kpi.indicator_value / kpi.indicator_target, 2) >= 0.90 THEN 'Y' 
            ELSE 'N' 
         END AS is_critical_cluster
        -- Flag para conjuntos específicos monitorados
        ,CASE 
            WHEN rt.cluster_id IN ('ID_001', 'ID_002', 'ID_003', 'ID_004', 'ID_005') THEN 'Y' 
            ELSE 'N' 
         END AS is_monitored_cluster
    
    FROM 
        OPERATIONAL_DB.REALTIME_SERVICE_TICKETS rt
    
    -- Join para buscar indicadores de qualidade e metas (Subquery)
    LEFT JOIN (
        SELECT 
            ind.cluster_id_ref
            ,ind.cluster_name
            ,ROUND(ind.monthly_indicator, 2) AS indicator_value
            ,tgt.target_value AS indicator_target
        FROM 
            QUALITY_DB.MONTHLY_INDICATORS ind
        JOIN 
            QUALITY_DB.ANNUAL_TARGETS tgt 
            ON tgt.id_ref = ind.cluster_id_ref 
            AND tgt.year = 2024
            AND ind.month_year_id = TO_CHAR(SYSDATE, 'yyyymm')
    ) kpi ON kpi.cluster_id_ref = rt.cluster_id
    
    WHERE 
        rt.status IN ('PENDING', 'DISPATCHED', 'ASSIGNED', 'EXECUTING')
        AND rt.customer_count > 1
    
    ORDER BY 
        ROUND(kpi.indicator_value / kpi.indicator_target, 2) DESC
)
            """
            
            df = pd.read_sql(query, conn)
            return self._padronizar_dataframe(df)
            
        except Exception as e:
            logger.error(f"Erro na query principal: {e}")
            return None
        finally:
            if conn: conn.close()

    def carregar_planilha_filtro(self):
        logger.info("Carregando filtro de Conjuntos Críticos sazonais do excel") #para os conjuntos sazonais
        arquivo = "Conj critico.xlsx"
        
        # Simulação
        if not os.path.exists(arquivo):
            logger.warning(f"⚠️ Arquivo '{arquivo}' não encontrado. Usando simulação.")
            return pd.DataFrame({
                'CONJUNTO': ['CAMPO GRANDE', 'DUQUE DE CAXIAS', 'JACAREPAGUA'], 
                'CRITICO?': ['Conj Crítico', 'Conj Crítico', 'Conj Crítico']
            })

        try:
            df = pd.read_excel(arquivo)
            df.columns = [str(c).upper().strip() for c in df.columns]
            col_critico = next((c for c in df.columns if 'CRITICO' in c), None)
            
            if col_critico and 'CONJUNTO' in df.columns:
                df['CONJUNTO'] = df['CONJUNTO'].astype(str).str.strip().str.upper()
                df_filtrado = df[df[col_critico].astype(str).str.strip() == 'Conj Crítico'].copy()
                logger.info(f"Excel carregado. {len(df_filtrado)} conjuntos críticos identificados.")
                return df_filtrado[['CONJUNTO']]
            else:
                logger.error("Colunas 'Conjunto' ou 'critico?' não encontradas no Excel.")
                return None
        except Exception as e:
            logger.error(f"Erro ao ler Excel: {e}")
            return None

class GerenciadorEstado:
    ARQUIVO_ESTADO = "panorama_conjunto.json"

    @staticmethod
    def ler_anterior():
        if os.path.exists(GerenciadorEstado.ARQUIVO_ESTADO):
            try:
                with open(GerenciadorEstado.ARQUIVO_ESTADO, 'r') as f:
                    return json.load(f).get('total_ocorrencias', 0)
            except:
                return 0
        return 0

    @staticmethod
    def salvar_atual(total):
        try:
            with open(GerenciadorEstado.ARQUIVO_ESTADO, 'w') as f:
                json.dump({'total_ocorrencias': total, 'data': str(datetime.now())}, f)
        except Exception as e:
            logger.error(f"Erro ao salvar panorama: {e}")

class FormatadorMensagem:
    @staticmethod
    def _traduzir_situacao(sigla):
        sigla = str(sigla).upper().strip()
        mapa = {
            'P': 'PENDENTE', 'D': 'DESIGNADO', 'A': 'ACIONADO', 'E': 'EXECUTANDO', 'EN': 'ENCERRADO'
        }
        return mapa.get(sigla, sigla)

    @staticmethod
    def _get_emoji_situacao(situacao):
        s = str(situacao).upper()
        if 'DESIGNADO' in s or s == 'D': return '🟢'
        if 'ACIONADO' in s or s == 'A': return '🔵'
        if 'PENDENTE' in s or s == 'P': return '⚪'
        if 'EXECUTANDO' in s or s == 'E': return '🟠'
        return '⚫'

    @staticmethod
    def gerar_texto(df):
        if df is None or df.empty:
            return "⚠️ *Alerta*: Nenhum conjunto crítico com ocorrências no momento."

        agora = datetime.now().strftime('%d/%m %H:%M')
        
        #CÁLCULOS GERAIS
        qtd_total = len(df)
        conjuntos_afetados = df['DES_CONJUNTO'].nunique() if 'DES_CONJUNTO' in df.columns else 0

        #DISTRIBUIÇÃO
        bloco_regional = ""
        regionais_ordenadas = []

        if 'REGIONAL' in df.columns and 'CI' in df.columns:
            resumo_reg = df.groupby('REGIONAL').agg(
                Qtd_Ocorrencias=('Ocorrencia', 'count'),
                Soma_CI=('CI', 'sum')
            ).reset_index().sort_values(by='Soma_CI', ascending=False)
            
            regionais_ordenadas = resumo_reg['REGIONAL'].tolist()

            for _, row in resumo_reg.iterrows():
                bloco_regional += f"🔸 *{row['REGIONAL']}:* {row['Qtd_Ocorrencias']} Ocorr. | CI: {row['Soma_CI']}\n"

        #DISTRIBUIÇÃO POR ABRANGÊNCIA
        bloco_abrangencia = ""
        if 'ABRANGENCIA' in df.columns:
            resumo_abr = df['ABRANGENCIA'].value_counts().sort_values(ascending=False)
            for abr, qtd in resumo_abr.items():
                bloco_abrangencia += f"\t{abr}: {qtd}\n"

        #DETALHAMENTO POR OCORRÊNCIA
        bloco_detalhe = ""
        
        if 'REGIONAL' in df.columns:
            if not regionais_ordenadas:
                regionais_ordenadas = sorted(df['REGIONAL'].dropna().unique())
            
            for reg in regionais_ordenadas:
                bloco_detalhe += f"\n*{reg}*\n"
                
                df_reg = df[df['REGIONAL'] == reg].sort_values(by='CI', ascending=False)
                
                for _, row in df_reg.iterrows():
                    conjunto = row.get('DES_CONJUNTO', '-')
                    numserv = row.get('OCORRENCIA', '-')
                    abrangencia = row.get('ABRANGENCIA', '-')
                    ci = row.get('CI', 0)
                    
                    situacao_raw = row.get('SITUACAO', '-')
                    situacao_txt = FormatadorMensagem._traduzir_situacao(situacao_raw)
                    emoji = FormatadorMensagem._get_emoji_situacao(situacao_raw)
                    bloco_detalhe += f"{conjunto} - {numserv} - {abrangencia} - CI: {ci} - *{situacao_txt} {emoji}*\n"

        #PANORAMA COMPARATIVO
        anterior = GerenciadorEstado.ler_anterior()
        diferenca = qtd_total - anterior
        sinal = "+" if diferenca > 0 else ""
        GerenciadorEstado.salvar_atual(qtd_total)

        mensagem = f"""
📊 *Alerta de Conjunto Crítico - {agora}*

📋 *Resumo:*
\tTotal de Ocorrências: *{qtd_total}*
\tConjuntos Afetados: *{conjuntos_afetados}*

🗺️ *Distribuição Regional:*
{bloco_regional}

🏗️ *Distribuição Por Abrangência:*
{bloco_abrangencia}

📍 *Detalhamento das Ocorrências:*
{bloco_detalhe}
📉 *Panorama Comparativo*
\tPanorama anterior: {anterior}
\tPanorama atual: {qtd_total}
\tDiferença: {sinal}{diferenca}
        """
        return mensagem.strip()

class EnviadorTelegram:
    def __init__(self):
        self.token = os.getenv('TELEGRAM_TOKEN')
        self.chat_id = os.getenv('TELEGRAM_CHAT_ID')

    def enviar_mensagem(self, mensagem):
        if not self.token or not self.chat_id:
            logger.error("Credenciais do Telegram ausentes no .env.")
            return

        try:
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            payload = {
                "chat_id": self.chat_id,
                "text": mensagem,
                "parse_mode": "Markdown"
            }
            requests.post(url, data=payload, timeout=20, verify=False)
            logger.info("✅ Mensagem enviada via Telegram!")

        except Exception as e:
            logger.error(f"❌ Erro Telegram: {e}")

def job_envio():
    logger.info("--- Job Iniciado (Conjunto Crítico) ---")
    
    robo = RoboOracle()
    
    #Busca Dados do Banco
    df_oracle = robo.buscar_dados_conjunto()
    
    #Busca Filtro do Excel
    df_filtro = robo.carregar_planilha_filtro()
    
    #Cruzamento (INNER JOIN)
    df_final = pd.DataFrame()
    
    if df_oracle is not None and not df_oracle.empty and df_filtro is not None:
        if 'DES_CONJUNTO' in df_oracle.columns:
            df_oracle['DES_CONJUNTO'] = df_oracle['DES_CONJUNTO'].astype(str).str.strip().str.upper()
        if 'CONJUNTO' in df_filtro.columns:
            df_filtro['CONJUNTO'] = df_filtro['CONJUNTO'].astype(str).str.strip().str.upper()
            
        df_final = pd.merge(
            df_oracle, 
            df_filtro, 
            left_on='DES_CONJUNTO', 
            right_on='CONJUNTO', 
            how='inner'
        )
        logger.info(f"Cruzamento realizado. Ocorrências: {len(df_final)}")
    else:
        logger.warning("Não foi possível cruzar os dados.")
    
    #Envia
    if not df_final.empty:
        msg = FormatadorMensagem.gerar_texto(df_final)
        bot = EnviadorTelegram()
        bot.enviar_mensagem(msg)
    else:
        logger.info("Nenhuma ocorrência crítica. Mensagem não enviada.")
    
    logger.info("--- Job Finalizado ---")

if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    job_envio()