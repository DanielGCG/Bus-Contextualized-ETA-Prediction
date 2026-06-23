import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import requests
import ijson

# ---------------- Configurações ----------------
BASE_URL = "https://dados.mobilidade.rio/gps/sppo"

CAMPO_FILTRO = "linha"
VALOR_FILTRO = "913"

DATA_INICIAL = datetime(2025, 6, 1, 0, 0, 1)
DATA_FINAL = datetime(2026, 6, 1, 1, 0, 1)
JANELA = timedelta(hours=1)

PREFIXO_ITEM = "item"

ARQUIVO_SAIDA = "linha_913.jsonl"
ARQUIVO_CHECKPOINT = "checkpoint_913.txt"

TIMEOUT = 180
MAX_TENTATIVAS = 5

NUM_THREADS = 15

lock_saida = threading.Lock()
lock_checkpoint = threading.Lock()


def formatar(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def gerar_janelas():
    janelas = []
    atual = DATA_INICIAL
    while atual < DATA_FINAL:
        proximo = min(atual + JANELA, DATA_FINAL)
        janelas.append((atual, proximo))
        atual = proximo
    return janelas


def carregar_checkpoint(todas):
    feitas = set()
    if not os.path.exists(ARQUIVO_CHECKPOINT):
        return feitas

    with open(ARQUIVO_CHECKPOINT) as f:
        linhas = [l.strip() for l in f if l.strip()]

    if len(linhas) == 1:
        try:
            ponto = datetime.fromisoformat(linhas[0])
            convertido = {i.isoformat() for (i, fim) in todas if fim <= ponto}
            if convertido:
                print(f"Checkpoint antigo detectado ({linhas[0]}). "
                      f"Convertendo: {len(convertido)} janelas marcadas como concluídas.")
                return convertido
        except ValueError:
            pass

    return set(linhas)


def marcar_concluida(inicio: datetime):
    with lock_checkpoint:
        with open(ARQUIVO_CHECKPOINT, "a") as f:
            f.write(inicio.isoformat() + "\n")


def gravar_registros(registros):
    if not registros:
        return
    with lock_saida:
        with open(ARQUIVO_SAIDA, "a", encoding="utf-8") as f:
            for r in registros:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")


def criar_sessao():
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0 (extrator-servico-616/1.0)"})
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=NUM_THREADS, pool_maxsize=NUM_THREADS
    )
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def baixar_janela(session, inicio, fim):
    params = {"dataInicial": formatar(inicio), "dataFinal": formatar(fim)}
    ultimo_erro = None
    for tentativa in range(1, MAX_TENTATIVAS + 1):
        try:
            with session.get(BASE_URL, params=params, stream=True, timeout=TIMEOUT) as resp:
                resp.raise_for_status()
                resp.raw.decode_content = True
                encontrados = []
                total = 0
                for registro in ijson.items(resp.raw, PREFIXO_ITEM):
                    total += 1
                    if str(registro.get(CAMPO_FILTRO, "")).strip() == VALOR_FILTRO:
                        encontrados.append(registro)
                return encontrados, total
        except Exception as e:
            ultimo_erro = e
            time.sleep(2 ** tentativa)

    raise RuntimeError(
        f"Falha {formatar(inicio)} -> {formatar(fim)} após {MAX_TENTATIVAS} tentativas: {ultimo_erro!r}"
    )


def processar_janela(session, inicio, fim):
    registros, total = baixar_janela(session, inicio, fim)
    gravar_registros(registros)
    marcar_concluida(inicio)
    return len(registros), total


def main():
    todas = gerar_janelas()
    feitas = carregar_checkpoint(todas)
    pendentes = [(i, fim) for (i, fim) in todas if i.isoformat() not in feitas]

    print(f"Total de janelas: {len(todas)} | já concluídas: {len(feitas)} | pendentes: {len(pendentes)}")
    if not pendentes:
        print("Nada a fazer.")
        return

    session = criar_sessao()
    total_novo = 0
    concluidas = 0
    erros = []

    executor = ThreadPoolExecutor(max_workers=NUM_THREADS)
    futuros = {
        executor.submit(processar_janela, session, i, fim): (i, fim)
        for (i, fim) in pendentes
    }

    try:
        for fut in as_completed(futuros):
            i, fim = futuros[fut]
            try:
                n, total = fut.result()
                total_novo += n
                concluidas += 1
                aviso = "" if total > 0 else "   <-- resposta da API veio vazia"
                print(f"[{concluidas}/{len(pendentes)}] {formatar(i)} -> {formatar(fim)} : "
                      f"{n} encontrados (de {total} registros){aviso}")
            except Exception as e:
                erros.append((i, fim, e))
                print(f"  ERRO em {formatar(i)} -> {formatar(fim)}: {e}")
    except KeyboardInterrupt:
        print("\nInterrompido. Cancelando o que ainda não começou "
              "(o que já terminou tá salvo no checkpoint)...")
        executor.shutdown(wait=False, cancel_futures=True)
        return

    executor.shutdown()

    print(f"\nConcluído. Novos registros: {total_novo} | janelas com erro: {len(erros)}")
    if erros:
        print("Janelas que falharam:")
        for i, fim, e in erros:
            print(f"  {formatar(i)} -> {formatar(fim)}: {e}")


if __name__ == "__main__":
    main()