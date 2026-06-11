"""
scanner.py — Escáner automático de señales sobre el S&P 500 completo.
Se ejecuta vía GitHub Actions cada 30 min durante la sesión US.

Flujo:
  1. Comprueba que el mercado US está abierto (sale si no).
  2. Obtiene los ~500 tickers del S&P 500 (Wikipedia).
  3. Descarga 2 años de histórico en bulk (chunks de 100).
  4. Aplica las estrategias del Screener: Pullback en tendencia y
     Breakout con volumen (cada run) + Momentum Sistemático (solo
     en el primer run del día).
  5. Escribe las señales en la pestaña 'senales' del Google Sheet del TFM.
  6. (Opcional) Notifica por Telegram las señales nuevas del día.

Secrets necesarios (variables de entorno):
  GCP_SERVICE_ACCOUNT_JSON  — JSON completo del service account (obligatorio)
  TELEGRAM_BOT_TOKEN        — token del bot (opcional)
  TELEGRAM_CHAT_ID          — chat id de destino (opcional)
"""

import json
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
import yfinance as yf

SHEET_ID = "1Yj2KkMypva14ZzpbnP9hDMexhzDGljWU6yhtnsVN980"
TAB_SENALES = "senales"
COLS = ["timestamp", "estrategia", "ticker", "precio", "entrada",
        "stop", "tp1", "tp2", "detalle"]
MAX_FILAS = 600  # límite de histórico en la pestaña


# ──────────────────────────────────────────────────────────────────
# 1. Mercado abierto
# ──────────────────────────────────────────────────────────────────
def mercado_abierto():
    """True si la bolsa US está en sesión regular (9:30-16:00 ET, L-V)."""
    ahora = datetime.now(ZoneInfo("America/New_York"))
    if ahora.weekday() >= 5:
        return False
    minutos = ahora.hour * 60 + ahora.minute
    return (9 * 60 + 30) <= minutos <= (16 * 60)


def es_primer_run_del_dia():
    """True en la primera media hora de sesión (para Momentum diario)."""
    ahora = datetime.now(ZoneInfo("America/New_York"))
    minutos = ahora.hour * 60 + ahora.minute
    return (9 * 60 + 30) <= minutos < (10 * 60 + 5)


# ──────────────────────────────────────────────────────────────────
# 2. Universo
# ──────────────────────────────────────────────────────────────────
def get_sp500_tickers():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    tablas = pd.read_html(url)
    tickers = tablas[0]["Symbol"].astype(str).str.replace(".", "-", regex=False)
    return [t for t in tickers.tolist() if t and t != "nan"]


# ──────────────────────────────────────────────────────────────────
# 3. Datos y métricas (misma lógica que el Screener de la app)
# ──────────────────────────────────────────────────────────────────
def descargar_bulk(tickers, chunk=100):
    """Descarga 2y en chunks. Devuelve dict ticker -> DataFrame OHLCV."""
    out = {}
    for i in range(0, len(tickers), chunk):
        grupo = tickers[i:i + chunk]
        try:
            raw = yf.download(grupo, period="2y", auto_adjust=True,
                              progress=False, group_by="column", threads=True)
        except Exception as e:
            print(f"  chunk {i//chunk}: error {e}", flush=True)
            continue
        if raw is None or raw.empty:
            continue
        if not isinstance(raw.columns, pd.MultiIndex):
            if len(grupo) == 1:
                out[grupo[0]] = raw.dropna(subset=["Close"])
            continue
        for t in grupo:
            try:
                cols = {}
                for campo in ["Close", "High", "Low", "Volume"]:
                    if campo in raw.columns.get_level_values(0) and \
                       t in raw[campo].columns:
                        cols[campo] = raw[campo][t]
                if "Close" in cols:
                    df_t = pd.DataFrame(cols).dropna(subset=["Close"])
                    if len(df_t) >= 60:
                        out[t] = df_t
            except Exception:
                continue
    return out


def calc_rsi(closes, period=14):
    delta = closes.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - 100 / (1 + rs)


def metricas(df_t):
    """Métricas técnicas de un ticker. None si datos insuficientes."""
    try:
        c = df_t["Close"].dropna()
        if len(c) < 60:
            return None
        precio = float(c.iloc[-1])
        if precio <= 0:
            return None
        h = df_t["High"].dropna() if "High" in df_t.columns else c
        l = df_t["Low"].dropna() if "Low" in df_t.columns else c
        v = df_t["Volume"].dropna() if "Volume" in df_t.columns else pd.Series(dtype=float)

        mom_12_1 = None
        if len(c) > 274:
            mom_12_1 = (float(c.iloc[-22]) / float(c.iloc[-274]) - 1) * 100

        ma50 = float(c.tail(50).mean())
        ma200 = float(c.tail(200).mean()) if len(c) >= 200 else None
        rsi_s = calc_rsi(c)
        rsi = float(rsi_s.iloc[-1]) if not pd.isna(rsi_s.iloc[-1]) else None

        tr = pd.concat([h - l, (h - c.shift(1)).abs(),
                        (l - c.shift(1)).abs()], axis=1).max(axis=1)
        atr = float(tr.rolling(14).mean().iloc[-1])

        vol_rel = None
        if len(v) >= 21:
            v20 = float(v.tail(21).iloc[:-1].mean())
            if v20 > 0:
                vol_rel = float(v.iloc[-1]) / v20

        high_60_prev = float(h.tail(61).iloc[:-1].max()) if len(h) > 61 else None

        c1y = c.tail(min(252, len(c)))
        ret_1y = float(c1y.iloc[-1]) / float(c1y.iloc[0]) - 1
        dd = float((c1y / c1y.cummax() - 1).min())
        calmar = (ret_1y / abs(dd)) if dd < 0 else None

        return {"precio": precio, "mom_12_1": mom_12_1, "ma50": ma50,
                "ma200": ma200, "rsi": rsi, "atr": atr, "vol_rel": vol_rel,
                "high_60_prev": high_60_prev, "calmar": calmar}
    except Exception:
        return None


def plan(precio, atr, entrada=None):
    """Stop 2xATR cap -15% anclado a la entrada; TP1 2R, TP2 4R."""
    entrada = entrada if entrada else precio
    stop = max(entrada - 2 * atr, entrada * 0.85)
    r = entrada - stop
    return round(entrada, 2), round(stop, 2), round(entrada + 2 * r, 2), \
        round(entrada + 4 * r, 2)


# ──────────────────────────────────────────────────────────────────
# 4. Estrategias
# ──────────────────────────────────────────────────────────────────
def escanear(datos, incluir_momentum):
    señales = []
    metricas_all = {}
    for t, df_t in datos.items():
        m = metricas(df_t)
        if m:
            metricas_all[t] = m

    print(f"  Métricas calculadas: {len(metricas_all)}", flush=True)

    for t, m in metricas_all.items():
        # PULLBACK en tendencia
        if (m["ma200"] and m["rsi"] is not None
                and m["precio"] > m["ma200"] and m["ma50"] > m["ma200"]
                and m["ma50"] * 0.97 <= m["precio"] <= m["ma50"] * 1.03
                and 35 <= m["rsi"] <= 55):
            e, s, t1, t2 = plan(m["precio"], m["atr"], entrada=m["ma50"])
            señales.append(("Pullback", t, m["precio"], e, s, t1, t2,
                            f"RSI {m['rsi']:.0f} · precio en MA50 con tendencia alcista"))

        # BREAKOUT con volumen
        if (m["high_60_prev"] and m["vol_rel"]
                and m["precio"] >= m["high_60_prev"]
                and m["vol_rel"] >= 1.5
                and m["atr"] / m["precio"] * 100 < 8):
            e, s, t1, t2 = plan(m["precio"], m["atr"])
            señales.append(("Breakout", t, m["precio"], e, s, t1, t2,
                            f"Vol x{m['vol_rel']:.1f} · ruptura máx 60d"))

    # MOMENTUM SISTEMÁTICO (solo primer run del día)
    if incluir_momentum:
        cand = [(t, m) for t, m in metricas_all.items()
                if m["mom_12_1"] and m["mom_12_1"] > 0 and m["calmar"]]
        cand.sort(key=lambda x: -x[1]["calmar"])
        for t, m in cand[:10]:
            e, s, t1, t2 = plan(m["precio"], m["atr"])
            señales.append(("Momentum", t, m["precio"], e, s, t1, t2,
                            f"12-1: +{m['mom_12_1']:.0f}% · Calmar {m['calmar']:.2f} · top 10 diario"))

    return señales


# ──────────────────────────────────────────────────────────────────
# 5. Google Sheets
# ──────────────────────────────────────────────────────────────────
def conectar_sheet():
    import gspread
    from google.oauth2.service_account import Credentials
    info = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])
    creds = Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    client = gspread.authorize(creds)
    sh = client.open_by_key(SHEET_ID)
    try:
        ws = sh.worksheet(TAB_SENALES)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=TAB_SENALES, rows=MAX_FILAS + 50, cols=12)
        ws.append_row(COLS, value_input_option="RAW")
    return ws


def guardar_señales(ws, señales, ts):
    filas = [[ts, estrategia, t, precio, e, s, t1, t2, detalle]
             for estrategia, t, precio, e, s, t1, t2, detalle in señales]
    if filas:
        # CRÍTICO: RAW para evitar interpretación regional de los decimales
        ws.append_rows(filas, value_input_option="RAW")

    # Mantener el tamaño bajo control: recortar filas antiguas
    try:
        n = len(ws.get_all_values())
        if n > MAX_FILAS:
            ws.delete_rows(2, n - MAX_FILAS + 1)
    except Exception:
        pass


def señales_ya_notificadas_hoy(ws, hoy):
    """Set de (estrategia, ticker) ya guardadas hoy — para no duplicar avisos."""
    vistos = set()
    try:
        for fila in ws.get_all_values()[1:]:
            if len(fila) >= 3 and fila[0].startswith(hoy):
                vistos.add((fila[1], fila[2]))
    except Exception:
        pass
    return vistos


# ──────────────────────────────────────────────────────────────────
# 6. Telegram (opcional)
# ──────────────────────────────────────────────────────────────────
def notificar_telegram(señales_nuevas):
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat or not señales_nuevas:
        return
    lineas = ["📡 Señales del escáner:"]
    for estrategia, t, precio, e, s, t1, _, detalle in señales_nuevas[:10]:
        lineas.append(f"• {estrategia} {t} @ {precio:.2f} — entrada {e}, "
                      f"stop {s}, TP1 {t1} ({detalle})")
    if len(señales_nuevas) > 10:
        lineas.append(f"... y {len(señales_nuevas) - 10} más en la app")
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat, "text": "\n".join(lineas)},
            timeout=15)
    except Exception as e:
        print(f"  Telegram falló: {e}", flush=True)


# ──────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────
def main():
    forzar = "--force" in sys.argv
    if not mercado_abierto() and not forzar:
        print("Mercado cerrado — nada que hacer. (usa --force para probar)")
        return

    print("1/4 Obteniendo universo S&P 500...", flush=True)
    tickers = get_sp500_tickers()
    print(f"  {len(tickers)} tickers", flush=True)

    print("2/4 Descargando históricos (bulk, ~2-4 min)...", flush=True)
    datos = descargar_bulk(tickers)
    print(f"  {len(datos)} tickers con datos", flush=True)
    if len(datos) < 100:
        print("⚠️ Muy pocos datos (¿rate limit?). Abortando sin escribir.")
        sys.exit(1)

    incluir_momentum = es_primer_run_del_dia() or forzar
    print(f"3/4 Escaneando (momentum diario: {incluir_momentum})...", flush=True)
    señales = escanear(datos, incluir_momentum)
    print(f"  {len(señales)} señales", flush=True)

    print("4/4 Guardando en Google Sheets...", flush=True)
    ws = conectar_sheet()
    ahora_ny = datetime.now(ZoneInfo("America/New_York"))
    ts = ahora_ny.strftime("%Y-%m-%d %H:%M")
    hoy = ahora_ny.strftime("%Y-%m-%d")

    vistos_hoy = señales_ya_notificadas_hoy(ws, hoy)
    nuevas = [s for s in señales if (s[0], s[1]) not in vistos_hoy]

    guardar_señales(ws, señales, ts)
    notificar_telegram(nuevas)
    print(f"✅ Guardadas {len(señales)} señales ({len(nuevas)} nuevas hoy)")


if __name__ == "__main__":
    main()
