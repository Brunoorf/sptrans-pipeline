from datetime import datetime, timedelta

import requests
from airflow.sdk import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

BASE_URL = "http://api.olhovivo.sptrans.com.br/v2.1"


def autenticar() -> requests.Session:
    sessao = requests.Session()
    token = Variable.get("sptrans_api_token")
    auth = sessao.post(
        f"{BASE_URL}/Login/Autenticar",
        params={"token": token},
        data="",
        timeout=30,
    )
    auth.raise_for_status()
    if auth.text.strip().lower() != "true":
        raise ValueError("Autenticacao na API Olho Vivo falhou")
    return sessao


def registrar_execucao(hook, dag_id, run_id, status,
                       recebidos=None, dedup=None,
                       gravados=None, mensagem=None):
    sql = """
        INSERT INTO sptrans_raw.etl_execucao
            (dag_id, run_id, status, recebidos,
             deduplicados, gravados, mensagem)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (dag_id, run_id) DO UPDATE
        SET status       = EXCLUDED.status,
            recebidos    = EXCLUDED.recebidos,
            deduplicados = EXCLUDED.deduplicados,
            gravados     = EXCLUDED.gravados,
            mensagem     = EXCLUDED.mensagem,
            executado_em = now();
    """
    conn = hook.get_conn()
    with conn.cursor() as cur:
        cur.execute(sql, (dag_id, run_id, status, recebidos,
                          dedup, gravados, mensagem))
    conn.commit()
    conn.close()


@dag(
    dag_id="sptrans_fato_posicao",
    start_date=datetime(2026, 9, 1),
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["sptrans", "fato"],
)
def sptrans_fato_posicao():

    @task
    def coletar_posicoes(**context) -> dict:
        dag_id = context["dag"].dag_id
        run_id = context["run_id"]
        hook = PostgresHook(postgres_conn_id="postgres_sptrans")

        try:
            sessao = autenticar()
            resp = sessao.get(f"{BASE_URL}/Posicao", timeout=90)
            resp.raise_for_status()

            payload = resp.json()
            if not payload or not payload.get("l"):
                raise ValueError("API retornou payload vazio em /Posicao")

            hora_consulta = payload.get("hr")
            observacoes = {}
            recebidos = 0

            for grupo in payload["l"]:
                for v in grupo["vs"]:
                    recebidos += 1

                    if v.get("py") is None or v.get("px") is None:
                        continue

                    chave = (str(v["p"]), v["ta"])
                    observacoes[chave] = (
                        str(v["p"]),
                        v["ta"],
                        grupo["cl"],
                        grupo["c"],
                        grupo["sl"],
                        v["a"],
                        v["py"],
                        v["px"],
                        hora_consulta,
                    )

            registros = list(observacoes.values())

            sql = """
                INSERT INTO sptrans_raw.raw_posicao
                    (prefixo_veiculo, ta_utc, codigo_linha, letreiro,
                     sentido, acessivel, latitude, longitude, hora_consulta)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (prefixo_veiculo, ta_utc) DO NOTHING;
            """

            conn = hook.get_conn()
            with conn.cursor() as cur:
                cur.executemany(sql, registros)
                gravados = cur.rowcount
            conn.commit()
            conn.close()

            print(f"Recebidos da API:  {recebidos}")
            print(f"Apos deduplicacao: {len(registros)}")
            print(f"Gravados de fato:  {gravados}")

            registrar_execucao(hook, dag_id, run_id, "sucesso",
                               recebidos, len(registros), gravados)

            return {
                "recebidos": recebidos,
                "deduplicados": len(registros),
                "gravados": gravados,
            }

        except Exception as e:
            registrar_execucao(hook, dag_id, run_id, "erro",
                               mensagem=str(e)[:500])
            raise

    coletar_posicoes()


sptrans_fato_posicao()