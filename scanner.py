"""
scanner.py v3 — Escáner interpretativo del S&P 500.

Filosofía: notificar CAMBIOS, no estados. Tres escaneos diarios con rol propio:
  · APERTURA  (9:30-11:30 NY)  — briefing: contexto de mercado + señales nuevas
                                  vs ayer + top-10 momentum + macro del día
  · MEDIODÍA  (11:30-15:30 NY) — solo eventos: señales nuevas desde la apertura
                                  y movimientos anómalos del día
  · CIERRE    (15:45-17:30 NY) — resumen del día y estado para mañana

Solo se notifican señales con ENTRADA CLARA: R/R estructural >= 2 calculado
contra soportes/resistencias reales (pivots + clustering), no contra múltiplos
arbitrarios.

Pestañas en Google Sheets:
  · 'senales'         — log histórico de altas + filas de control '_run'
  · 'senales_activas' — estado vivo (se sobrescribe en cada run)

Secrets (variables de entorno):
  GCP_SERVICE_ACCOUNT_JSON  (obligatorio)
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID  (opcionales)
"""

import json
import os
import sys
from datetime import datetime
from io import StringIO
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
import yfinance as yf

SHEET_ID = "1Yj2KkMypva14ZzpbnP9hDMexhzDGljWU6yhtnsVN980"
TAB_LOG = "senales"
TAB_ACTIVAS = "senales_activas"
COLS_LOG = ["timestamp", "estrategia", "ticker", "precio", "entrada",
            "stop", "tp1", "rr", "detalle"]
COLS_ACTIVAS = ["estrategia", "ticker", "fecha_alta", "entrada", "stop",
                "tp1", "rr", "detalle"]
MAX_FILAS_LOG = 600
RR_MINIMO = 2.0          # filtro de "entrada clara"
RR_MINIMO_VIVA = 1.4     # una activa sobrevive mientras conserve estructura

INDICES_CTX = ["^GSPC", "^VIX", "^TNX"]
SECTOR_ETFS = {"XLK": "Tecnología", "XLF": "Financiero", "XLV": "Salud",
               "XLE": "Energía", "XLY": "Consumo Disc.", "XLP": "Consumo Bás.",
               "XLI": "Industrial", "XLU": "Utilities", "XLB": "Materiales",
               "XLRE": "Inmobiliario", "XLC": "Comunicación"}

FALLBACK_TICKERS = ['AAPL', 'ABBV', 'ABNB', 'ABT', 'ADBE', 'AEP', 'ALB', 'AMD', 'AMGN', 'AMT', 'AMZN', 'APD', 'ARE', 'AVB', 'AVGO', 'AWK', 'AXP', 'BA', 'BAC', 'BKNG', 'BLK', 'BMY', 'BX', 'C', 'CAT', 'CCI', 'CHTR', 'CL', 'CMCSA', 'CMG', 'COF', 'COP', 'COST', 'CRM', 'CSCO', 'CTVA', 'CVS', 'CVX', 'D', 'DASH', 'DD', 'DE', 'DHR', 'DIS', 'DLR', 'DOW', 'DUK', 'DVN', 'EA', 'ECL', 'ED', 'EIX', 'ELV', 'EMR', 'EOG', 'EQIX', 'EQR', 'ETN', 'ETR', 'EXC', 'EXR', 'F', 'FANG', 'FCX', 'GD', 'GE', 'GILD', 'GIS', 'GM', 'GOOGL', 'GS', 'HD', 'HES', 'HON', 'HSY', 'IBM', 'INTC', 'ITW', 'JNJ', 'JPM', 'KHC', 'KMB', 'KMI', 'KO', 'LIN', 'LLY', 'LMT', 'LOW', 'MA', 'MAR', 'MCD', 'MDLZ', 'MDT', 'META', 'MLM', 'MMM', 'MO', 'MPC', 'MRK', 'MS', 'MSFT', 'MUX', 'NEE', 'NEM', 'NFLX', 'NKE', 'NOC', 'NOW', 'NUE', 'NVDA', 'O', 'ORCL', 'ORLY', 'OXY', 'PANW', 'PCG', 'PEG', 'PEP', 'PFE', 'PG', 'PHM', 'PLD', 'PM', 'PPG', 'PSA', 'PSX', 'PXD', 'PYPL', 'QCOM', 'ROKU', 'RTX', 'SBAC', 'SBUX', 'SCHW', 'SHW', 'SLB', 'SO', 'SPG', 'SPGI', 'SPOT', 'SRE', 'STLD', 'STZ', 'SYY', 'T', 'TJX', 'TMO', 'TMUS', 'TSLA', 'TTWO', 'TXN', 'UNH', 'UNP', 'UPS', 'V', 'VLO', 'VMC', 'VTR', 'VZ', 'WBD', 'WEC', 'WELL', 'WFC', 'WMB', 'WMT', 'XEL', 'XOM']


# ──────────────────────────────────────────────────────────────────
# Roles y horario
# ──────────────────────────────────────────────────────────────────
def rol_actual(ahora_ny):
    """Determina el rol de este run según la hora de NY. None = fuera de franja."""
    if ahora_ny.weekday() >= 5:
        return None
    m = ahora_ny.hour * 60 + ahora_ny.minute
    if (9 * 60 + 30) <= m < (11 * 60 + 30):
        return "apertura"
    if (11 * 60 + 30) <= m < (15 * 60 + 30):
        return "mediodia"
    if (15 * 60 + 45) <= m < (17 * 60 + 30):
        return "cierre"
    return None


# ──────────────────────────────────────────────────────────────────
# Universo y descarga
# ──────────────────────────────────────────────────────────────────
def get_sp500_tickers():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/124.0 Safari/537.36")}
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        tablas = pd.read_html(StringIO(resp.text))
        tickers = tablas[0]["Symbol"].astype(str).str.replace(".", "-", regex=False)
        out = [t for t in tickers.tolist() if t and t != "nan"]
        if len(out) > 400:
            return out
        print(f"  ⚠️ Wikipedia devolvió solo {len(out)} — uso fallback")
    except Exception as e:
        print(f"  ⚠️ Wikipedia falló ({e}) — uso fallback de {len(FALLBACK_TICKERS)}")
    return FALLBACK_TICKERS


def descargar_bulk(tickers, chunk=100):
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


# ──────────────────────────────────────────────────────────────────
# Indicadores
# ──────────────────────────────────────────────────────────────────
def calc_rsi(closes, period=14):
    delta = closes.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - 100 / (1 + rs)


def detectar_sr(df, max_niveles=3, n_pivote=5):
    """Soportes/resistencias por pivots + clustering (misma lógica que la app)."""
    try:
        c = df["Close"].astype(float)
        h = df["High"].astype(float) if "High" in df.columns else c
        l = df["Low"].astype(float) if "Low" in df.columns else c
        n = len(c)
        if n < 40:
            return [], []
        precio = float(c.iloc[-1])
        tr = pd.concat([h - l, (h - c.shift(1)).abs(),
                        (l - c.shift(1)).abs()], axis=1).max(axis=1)
        atr_loc = float(tr.rolling(14).mean().iloc[-1])
        umbral = max(0.6 * atr_loc, precio * 0.008)

        pivots = []
        for i in range(n_pivote, n - n_pivote):
            if h.iloc[i] >= h.iloc[i - n_pivote:i + n_pivote + 1].max():
                pivots.append((float(h.iloc[i]), i))
            if l.iloc[i] <= l.iloc[i - n_pivote:i + n_pivote + 1].min():
                pivots.append((float(l.iloc[i]), i))
        if not pivots:
            return [], []

        pivots.sort(key=lambda x: x[0])
        clusters, actual = [], [pivots[0]]
        for p in pivots[1:]:
            if p[0] - actual[-1][0] <= umbral:
                actual.append(p)
            else:
                clusters.append(actual)
                actual = [p]
        clusters.append(actual)

        niveles = []
        for cl in clusters:
            nivel = float(np.mean([p[0] for p in cl]))
            niveles.append({"nivel": nivel, "toques": len(cl),
                            "score": len(cl) + (max(p[1] for p in cl) / n) * 0.5})
        soportes = sorted([x for x in niveles if x["nivel"] < precio * 0.998],
                          key=lambda x: -x["score"])[:max_niveles]
        resist = sorted([x for x in niveles if x["nivel"] > precio * 1.002],
                        key=lambda x: -x["score"])[:max_niveles]
        soportes.sort(key=lambda x: precio - x["nivel"])
        resist.sort(key=lambda x: x["nivel"] - precio)
        return soportes, resist
    except Exception:
        return [], []


def metricas(df_t):
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

        mom_12_1 = (float(c.iloc[-22]) / float(c.iloc[-274]) - 1) * 100 \
            if len(c) > 274 else None
        ma50 = float(c.tail(50).mean())
        ma200 = float(c.tail(200).mean()) if len(c) >= 200 else None
        ma50_prev = float(c.iloc[-53:-3].mean()) if len(c) >= 203 else None
        ma200_prev = float(c.iloc[-203:-3].mean()) if len(c) >= 203 else None
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
        ret_dia = (precio / float(c.iloc[-2]) - 1) * 100 if len(c) >= 2 else 0
        c1y = c.tail(min(252, len(c)))
        ret_1y = float(c1y.iloc[-1]) / float(c1y.iloc[0]) - 1
        dd = float((c1y / c1y.cummax() - 1).min())
        calmar = (ret_1y / abs(dd)) if dd < 0 else None
        return {"precio": precio, "mom_12_1": mom_12_1, "ma50": ma50,
                "ma200": ma200, "ma50_prev": ma50_prev, "ma200_prev": ma200_prev,
                "rsi": rsi, "atr": atr, "vol_rel": vol_rel,
                "high_60_prev": high_60_prev, "ret_dia": ret_dia,
                "calmar": calmar}
    except Exception:
        return None


def rr_estructural(df_t, m, entrada):
    """R/R real: stop bajo soporte (regla max(2ATR, sop-0.5ATR) cap 15%),
    objetivo en la primera resistencia útil (>= entrada + 1R)."""
    sops, res = detectar_sr(df_t.tail(252))
    atr = m["atr"]
    stop = entrada - 2 * atr
    sops_below = [s for s in sops if s["nivel"] < entrada * 0.995]
    if sops_below:
        s_rel = max(sops_below, key=lambda s: s["nivel"])
        stop = max(stop, s_rel["nivel"] - 0.5 * atr)
    stop = max(stop, entrada * 0.85)
    r = entrada - stop
    if r <= 0:
        return None, None, None
    res_above = sorted([x for x in res if x["nivel"] > entrada * 1.005],
                       key=lambda x: x["nivel"])
    tp1 = None
    for x in res_above:
        if x["nivel"] >= entrada + r:
            tp1 = x["nivel"]
            break
    if tp1 is None:
        tp1 = entrada + 2 * r   # sin resistencia útil: 2R (subida libre)
    return round(stop, 2), round(tp1, 2), round((tp1 - entrada) / r, 2)


# ──────────────────────────────────────────────────────────────────
# Detección de señales (con filtro de entrada clara)
# ──────────────────────────────────────────────────────────────────
def detectar_senales(datos):
    """Devuelve (señales_claras, metricas_all). Cada señal:
    dict(estrategia, ticker, precio, entrada, stop, tp1, rr, detalle)."""
    señales = []
    metricas_all = {}
    for t, df_t in datos.items():
        if t in INDICES_CTX or t in SECTOR_ETFS:
            m = metricas(df_t)
            if m:
                metricas_all[t] = m
            continue
        m = metricas(df_t)
        if m is None:
            continue
        metricas_all[t] = m

        candidatos = []
        # PULLBACK en tendencia
        if (m["ma200"] and m["rsi"] is not None
                and m["precio"] > m["ma200"] and m["ma50"] > m["ma200"]
                and m["ma50"] * 0.97 <= m["precio"] <= m["ma50"] * 1.03
                and 35 <= m["rsi"] <= 55):
            candidatos.append(("Pullback", m["ma50"],
                               f"RSI {m['rsi']:.0f} · precio en MA50, tendencia alcista"))
        # BREAKOUT con volumen
        if (m["high_60_prev"] and m["vol_rel"]
                and m["precio"] >= m["high_60_prev"] and m["vol_rel"] >= 1.5
                and m["atr"] / m["precio"] * 100 < 8):
            candidatos.append(("Breakout", m["precio"],
                               f"Vol x{m['vol_rel']:.1f} · ruptura máx 60d"))
        # GOLDEN CROSS (MA50 cruza sobre MA200 en los últimos ~3 días)
        if (m["ma200"] and m["ma50_prev"] and m["ma200_prev"]
                and m["ma50"] > m["ma200"] and m["ma50_prev"] <= m["ma200_prev"]):
            candidatos.append(("GoldenCross", m["precio"],
                               "MA50 cruza sobre MA200 — cambio de régimen"))

        for estrategia, entrada, detalle in candidatos:
            stop, tp1, rr = rr_estructural(df_t, m, entrada)
            if rr is None:
                continue
            señales.append({"estrategia": estrategia, "ticker": t,
                            "precio": round(m["precio"], 2),
                            "entrada": round(entrada, 2), "stop": stop,
                            "tp1": tp1, "rr": rr,
                            "detalle": detalle})
    claras = [s for s in señales if s["rr"] >= RR_MINIMO]
    return claras, metricas_all


def top_momentum(metricas_all, n=10):
    cand = [(t, m) for t, m in metricas_all.items()
            if t not in INDICES_CTX and t not in SECTOR_ETFS
            and m.get("mom_12_1") and m["mom_12_1"] > 0 and m.get("calmar")]
    cand.sort(key=lambda x: -x[1]["calmar"])
    return [(t, m["mom_12_1"], m["calmar"]) for t, m in cand[:n]]


def anomalias_dia(metricas_all, n=3):
    """Movimientos > 2.5 ATR hoy con volumen >= 2x (para el run de mediodía)."""
    out = []
    for t, m in metricas_all.items():
        if t in INDICES_CTX or t in SECTOR_ETFS:
            continue
        if m["atr"] and m["precio"] > 0 and m.get("vol_rel"):
            mov_atr = abs(m["ret_dia"] / 100 * m["precio"]) / m["atr"]
            if mov_atr >= 2.5 and m["vol_rel"] >= 2:
                out.append((t, m["ret_dia"], m["vol_rel"], mov_atr))
    out.sort(key=lambda x: -x[3])
    return out[:n]


# ──────────────────────────────────────────────────────────────────
# Contexto de mercado interpretado
# ──────────────────────────────────────────────────────────────────
def contexto_mercado(metricas_all):
    ctx = {}
    spx = metricas_all.get("^GSPC")
    vix = metricas_all.get("^VIX")
    tnx = metricas_all.get("^TNX")
    ctx["spx_dia"] = spx["ret_dia"] if spx else None
    ctx["vix"] = vix["precio"] if vix else None

    # Amplitud: % de valores sobre su MA50 / MA200
    valores = [(t, m) for t, m in metricas_all.items()
               if t not in INDICES_CTX and t not in SECTOR_ETFS]
    if valores:
        sobre_ma50 = sum(1 for _, m in valores if m["precio"] > m["ma50"])
        con_ma200 = [(t, m) for t, m in valores if m["ma200"]]
        sobre_ma200 = sum(1 for _, m in con_ma200 if m["precio"] > m["ma200"])
        ctx["breadth_ma50"] = sobre_ma50 / len(valores) * 100
        ctx["breadth_ma200"] = (sobre_ma200 / len(con_ma200) * 100) if con_ma200 else None
    else:
        ctx["breadth_ma50"] = ctx["breadth_ma200"] = None

    # Sector líder y cola del día
    secs = [(SECTOR_ETFS[t], m["ret_dia"]) for t, m in metricas_all.items()
            if t in SECTOR_ETFS]
    if secs:
        secs.sort(key=lambda x: -x[1])
        ctx["sector_lider"] = secs[0]
        ctx["sector_cola"] = secs[-1]
        ctx["sectores_verde"] = sum(1 for _, r in secs if r > 0)
        ctx["sectores_total"] = len(secs)

    # Velocidad del 10Y (^TNX ya viene en % x10 → da igual, usamos z-score)
    if tnx:
        ctx["tnx_dia"] = tnx["ret_dia"]
    return ctx


def semaforo(ctx):
    """🟢/🟡/🔴 según SPX, amplitud y VIX — interpretación de un vistazo."""
    puntos = 0
    if ctx.get("spx_dia") is not None and ctx["spx_dia"] > 0:
        puntos += 1
    if ctx.get("breadth_ma50") and ctx["breadth_ma50"] > 55:
        puntos += 1
    if ctx.get("vix") and ctx["vix"] < 18:
        puntos += 1
    if puntos >= 3:
        return "🟢", "Mercado constructivo"
    if puntos == 2:
        return "🟡", "Mercado mixto"
    return "🔴", "Mercado defensivo"


def evento_macro_hoy():
    """Mini-calendario heurístico: NFP, CPI, FOMC."""
    ahora = datetime.now(ZoneInfo("America/New_York"))
    d, wd, mes = ahora.day, ahora.weekday(), ahora.month
    if wd == 4 and d <= 7:
        return "📊 Hoy: Non-Farm Payrolls (14:30 CET) — volatilidad probable"
    if 10 <= d <= 15 and wd in (2, 3):
        return "💰 Posible CPI esta semana (días 10-15) — atento a sorpresas de inflación"
    if wd == 2 and 20 <= d <= 26 and mes in (1, 3, 5, 6, 7, 9, 11, 12):
        return "🏦 Semana de FOMC — decisión de tipos miércoles 20:00 CET"
    return None


# ──────────────────────────────────────────────────────────────────
# Google Sheets: log + estado activo
# ──────────────────────────────────────────────────────────────────
def conectar(nombre_tab, cols):
    import gspread
    from google.oauth2.service_account import Credentials
    info = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])
    creds = Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    sh = gspread.authorize(creds).open_by_key(SHEET_ID)
    try:
        ws = sh.worksheet(nombre_tab)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=nombre_tab, rows=MAX_FILAS_LOG + 50, cols=12)
        ws.append_row(cols, value_input_option="RAW")
    return ws


def leer_activas(ws_act):
    """Estado anterior: dict (estrategia,ticker) -> fila dict."""
    estado = {}
    try:
        for fila in ws_act.get_all_records():
            k = (str(fila.get("estrategia", "")), str(fila.get("ticker", "")))
            if all(k):
                estado[k] = fila
    except Exception:
        pass
    return estado


def escribir_activas(ws_act, activas):
    """Sobrescribe el estado con las señales vivas."""
    filas = [[s["estrategia"], s["ticker"], s["fecha_alta"], s["entrada"],
              s["stop"], s["tp1"], s["rr"], s["detalle"]] for s in activas]
    ws_act.clear()
    ws_act.append_row(COLS_ACTIVAS, value_input_option="RAW")
    if filas:
        ws_act.append_rows(filas, value_input_option="RAW")


def log_altas(ws_log, nuevas, ts):
    filas = [[ts, s["estrategia"], s["ticker"], s["precio"], s["entrada"],
              s["stop"], s["tp1"], s["rr"], s["detalle"]] for s in nuevas]
    if filas:
        ws_log.append_rows(filas, value_input_option="RAW")
    try:
        n = len(ws_log.get_all_values())
        if n > MAX_FILAS_LOG:
            ws_log.delete_rows(2, n - MAX_FILAS_LOG + 1)
    except Exception:
        pass


def log_run(ws_log, ts, rol, resumen):
    """Fila de control: registra que el rol se ejecutó + resumen del contexto."""
    ws_log.append_row([ts, "_run", rol, "", "", "", "", "", resumen],
                      value_input_option="RAW")


def roles_ejecutados_hoy(ws_log, hoy):
    roles = set()
    try:
        for fila in ws_log.get_all_values()[1:]:
            if len(fila) >= 3 and fila[0].startswith(hoy) and fila[1] == "_run":
                roles.add(fila[2])
    except Exception:
        pass
    return roles


# ──────────────────────────────────────────────────────────────────
# Telegram
# ──────────────────────────────────────────────────────────────────
def enviar_telegram(texto):
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat or not texto:
        return
    try:
        requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                      json={"chat_id": chat, "text": texto}, timeout=15)
    except Exception as e:
        print(f"  Telegram falló: {e}", flush=True)


def linea_contexto(ctx):
    sem, etiqueta = semaforo(ctx)
    partes = [f"{sem} {etiqueta}"]
    if ctx.get("spx_dia") is not None:
        partes.append(f"S&P {ctx['spx_dia']:+.1f}%")
    if ctx.get("sectores_verde") is not None:
        partes.append(f"{ctx['sectores_verde']}/{ctx['sectores_total']} sectores en verde")
    if ctx.get("sector_lider"):
        partes.append(f"lidera {ctx['sector_lider'][0]}")
    lineas = [" · ".join(partes)]
    if ctx.get("breadth_ma50") is not None:
        b = ctx["breadth_ma50"]
        salud = "amplitud sana" if b > 55 else ("amplitud débil — subida estrecha" if b < 45 else "amplitud neutra")
        lineas.append(f"📐 {b:.0f}% del S&P sobre su MA50 ({salud})")
    if ctx.get("vix"):
        v = ctx["vix"]
        lineas.append(f"⚡ VIX {v:.0f} ({'calma' if v < 16 else 'normal' if v < 22 else 'tensión'})")
    return "\n".join(lineas)


def formatear_senal(s):
    return (f"• {s['estrategia']} {s['ticker']} @ {s['precio']:.2f} — "
            f"entrada {s['entrada']}, stop {s['stop']}, TP {s['tp1']} "
            f"(R/R {s['rr']:.1f}) · {s['detalle']}")


# ──────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────
def main():
    forzar = "--force" in sys.argv
    ahora_ny = datetime.now(ZoneInfo("America/New_York"))
    rol = rol_actual(ahora_ny) or ("apertura" if forzar else None)
    if rol is None:
        print("Fuera de franja horaria — nada que hacer. (--force simula apertura)")
        return

    ts = ahora_ny.strftime("%Y-%m-%d %H:%M")
    hoy = ahora_ny.strftime("%Y-%m-%d")

    ws_log = conectar(TAB_LOG, COLS_LOG)
    ws_act = conectar(TAB_ACTIVAS, COLS_ACTIVAS)

    hechos = roles_ejecutados_hoy(ws_log, hoy)
    if rol in hechos and not forzar:
        print(f"Rol '{rol}' ya ejecutado hoy — salgo.")
        return

    print(f"Rol: {rol} | {ts} NY", flush=True)
    print("1/4 Universo...", flush=True)
    tickers = get_sp500_tickers() + INDICES_CTX + list(SECTOR_ETFS.keys())
    print(f"  {len(tickers)} tickers", flush=True)

    print("2/4 Descarga bulk...", flush=True)
    datos = descargar_bulk(tickers)
    print(f"  {len(datos)} con datos", flush=True)
    if len(datos) < 100:
        print("⚠️ Muy pocos datos (¿rate limit?). Abortando.")
        sys.exit(1)

    print("3/4 Señales + contexto...", flush=True)
    claras, metricas_all = detectar_senales(datos)
    ctx = contexto_mercado(metricas_all)
    estado_prev = leer_activas(ws_act)

    # Delta: nuevas / siguen / desaparecen
    claves_hoy = {(s["estrategia"], s["ticker"]) for s in claras}
    nuevas = [s for s in claras if (s["estrategia"], s["ticker"]) not in estado_prev]
    siguen = [s for s in claras if (s["estrategia"], s["ticker"]) in estado_prev]
    desaparecen = [v for k, v in estado_prev.items() if k not in claves_hoy]

    # Estado nuevo: las que siguen conservan su fecha de alta
    activas = []
    for s in claras:
        k = (s["estrategia"], s["ticker"])
        s_out = dict(s)
        s_out["fecha_alta"] = estado_prev[k].get("fecha_alta", hoy) \
            if k in estado_prev else hoy
        activas.append(s_out)

    print(f"  nuevas {len(nuevas)} · siguen {len(siguen)} · "
          f"desaparecen {len(desaparecen)}", flush=True)

    print("4/4 Guardado + notificación...", flush=True)
    escribir_activas(ws_act, activas)
    log_altas(ws_log, nuevas, ts)
    resumen_ctx = (f"{semaforo(ctx)[0]} SPX {ctx.get('spx_dia', 0):+.1f}% · "
                   f"breadth {ctx.get('breadth_ma50') or 0:.0f}% · "
                   f"nuevas {len(nuevas)} · activas {len(activas)} · "
                   f"caen {len(desaparecen)}")
    log_run(ws_log, ts, rol, resumen_ctx)

    # ── Mensaje según rol ─────────────────────────────────────────
    msg = None
    if rol == "apertura":
        bloques = [f"☀️ BRIEFING DE APERTURA — {hoy}", linea_contexto(ctx)]
        ev = evento_macro_hoy()
        if ev:
            bloques.append(ev)
        if nuevas:
            bloques.append(f"\n🆕 Entradas claras nuevas ({len(nuevas)}):")
            bloques += [formatear_senal(s) for s in nuevas[:6]]
        else:
            bloques.append("\n🆕 Sin entradas claras nuevas (R/R ≥ 2) hoy por ahora.")
        if siguen:
            bloques.append(f"⏳ Siguen activas {len(siguen)} señales de días previos.")
        if desaparecen:
            bloques.append(f"💀 Caen {len(desaparecen)}: " +
                           ", ".join(str(d.get("ticker", "?")) for d in desaparecen[:8]))
        mom = top_momentum(metricas_all)
        if mom:
            bloques.append("\n📐 Top momentum (cartera mensual): " +
                           ", ".join(t for t, _, _ in mom))
        msg = "\n".join(bloques)

    elif rol == "mediodia":
        anom = anomalias_dia(metricas_all)
        if nuevas or anom:
            bloques = [f"🕐 EVENTOS DE MEDIA SESIÓN"]
            if nuevas:
                bloques.append(f"🆕 Nuevas desde la apertura ({len(nuevas)}):")
                bloques += [formatear_senal(s) for s in nuevas[:5]]
            if anom:
                bloques.append("⚡ Movimientos anómalos (>2.5 ATR, vol ≥2x):")
                bloques += [f"• {t}: {r:+.1f}% (vol x{v:.1f})" for t, r, v, _ in anom]
            msg = "\n".join(bloques)
        else:
            print("  Mediodía sin eventos — silencio deliberado.")

    elif rol == "cierre":
        bloques = [f"🌙 RESUMEN DE CIERRE — {hoy}", linea_contexto(ctx)]
        bloques.append(f"\n📊 Día: {len(nuevas)} señales nuevas en este escaneo · "
                       f"{len(activas)} activas en total · {len(desaparecen)} caídas")
        if activas:
            mejores = sorted(activas, key=lambda s: -s["rr"])[:5]
            bloques.append("🎯 Mejores entradas vivas (por R/R):")
            bloques += [formatear_senal(s) for s in mejores]
        msg = "\n".join(bloques)

    if msg:
        enviar_telegram(msg)
        print("  Mensaje enviado.")
    print(f"✅ Run '{rol}' completado")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception:
        import traceback
        print("\n💥 ERROR NO CONTROLADO — traceback completo:")
        traceback.print_exc()
        sys.exit(1)
