"""
TFM — AGENTE DE DESCUBRIMIENTO DE MERCADO v2 (FMP)
Master IA Sector Financiero — VIU 2025/26

Fuente de datos: Financial Modeling Prep (FMP)
  - Funciona desde GitHub Actions (no bloquea IPs cloud)
  - Plan gratuito: 250 llamadas/día
  - Estrategia eficiente: batch quotes (50 tickers por llamada)
    → 1.400 tickers = ~28 llamadas para quotes
    → Solo descarga histórico de los 30 candidatos más interesantes
    → Total estimado: ~60 llamadas/día (bien dentro del límite)

Pipeline:
  1. Universo: S&P500 + S&P400 + S&P600 desde Wikipedia
  2. Batch quotes FMP (28 llamadas) → volumen, precio, cambio del día
  3. Pre-filtrado rápido por vol_ratio y cambio precio
  4. Histórico FMP solo para top 30 candidatos (30 llamadas)
  5. Detección de anomalías con score 0-100
  6. Top 10 → Google Sheets + Gmail
"""

import os
import json
import time
import smtplib
import requests
import numpy as np
import pandas as pd
import gspread
from io import StringIO
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from google.oauth2.service_account import Credentials

# ╔══════════════════════════════════════════════════════════════╗
# ║  CONFIGURACIÓN                                               ║
# ╚══════════════════════════════════════════════════════════════╝
SHEET_ID   = "1Yj2KkMypva14ZzpbnP9hDMexhzDGljWU6yhtnsVN980"
EMAIL_TO   = "josemartinezpardo1@gmail.com"
EMAIL_FROM = os.environ.get("GMAIL_USER", "")
EMAIL_PASS = os.environ.get("GMAIL_APP_PASSWORD", "")
GCP_CREDS  = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
FMP_KEY    = os.environ.get("FMP_KEY", "")

FMP_BASE   = "https://financialmodelingprep.com/api/v3"

# Tickers sobreconocidos a excluir
EXCLUDE = {
    "AAPL","MSFT","NVDA","GOOGL","GOOG","AMZN","META","TSLA","AVGO","BRK-B",
    "JPM","LLY","V","XOM","UNH","MA","HD","PG","JNJ","COST","NFLX","BAC",
    "ORCL","ABBV","CVX","KO","WMT","MRK","ADBE","CRM","PEP","TMO","LIN",
    "ACN","CSCO","MCD","DIS","AMD","ABT","IBM","INTC","PFE","NOW","TXN",
    "WFC","DHR","AXP","QCOM","NEE","AMGN","VZ","CMCSA","T","PM","GE","RTX",
    "CAT","SPGI","GS","MS","UBER","BLK","PYPL","SBUX","NKE","BKNG","PLD",
    "GILD","LOW","TJX","DE","SYK","ADP","MDT","LMT","MMC","ETN","PGR",
    "BSX","CB","ISRG","VRTX","CI","SO","FI","DUK","KLAC","INTU","SCHW",
    "BMY","AMAT","ANET","HON","AMT","CRWD","REGN","PANW","ELV","TMUS","FDX"
}

CFG = {
    "min_price":       5.0,
    "min_vol_avg":     300_000,
    "pre_filter_vol":  2.0,    # vol_ratio mínimo para pasar al análisis histórico
    "top_candidates":  30,     # máx tickers para descargar histórico
    "top_n_email":     10,     # top final al email
}


# ╔══════════════════════════════════════════════════════════════╗
# ║  UNIVERSO DE TICKERS (Wikipedia, sin API)                    ║
# ╚══════════════════════════════════════════════════════════════╝
def get_universe():
    universe = set()
    sources = [
        ("S&P 500",          "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"),
        ("S&P 400 MidCap",   "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies"),
        ("S&P 600 SmallCap", "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies"),
    ]
    for name, url in sources:
        try:
            html = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15).text
            tables = pd.read_html(StringIO(html))
            for t in tables:
                for col in t.columns:
                    if "symbol" in str(col).lower() or "ticker" in str(col).lower():
                        tickers = t[col].dropna().astype(str).tolist()
                        for tk in tickers:
                            tk = tk.strip().replace(".", "-").upper()
                            if tk and tk not in ("-", "NAN", "NONE"):
                                universe.add(tk)
                        break
            print(f"  ✅ {name}: universo acumulado {len(universe)}")
        except Exception as e:
            print(f"  ⚠️ {name} falló: {e}")

    universe = universe - EXCLUDE
    return sorted(universe)


# ╔══════════════════════════════════════════════════════════════╗
# ║  FMP — BATCH QUOTES (eficiente: 50 tickers por llamada)      ║
# ╚══════════════════════════════════════════════════════════════╝
def fmp_batch_quotes(tickers: list) -> dict:
    """
    Descarga quotes actuales para todos los tickers en lotes de 50.
    Cada quote incluye: price, changesPercentage, volume, avgVolume,
    yearHigh, yearLow, open, previousClose.
    Retorna dict {ticker: quote_data}
    """
    results = {}
    batch_size = 50
    n_batches  = (len(tickers) + batch_size - 1) // batch_size

    print(f"  Descargando quotes en {n_batches} lotes de {batch_size}...")

    for i in range(n_batches):
        batch = tickers[i * batch_size : (i + 1) * batch_size]
        symbols = ",".join(batch)
        try:
            url  = f"{FMP_BASE}/quote/{symbols}?apikey={FMP_KEY}"
            resp = requests.get(url, timeout=15)
            if resp.status_code != 200:
                print(f"    ⚠️ Lote {i+1}: HTTP {resp.status_code}")
                continue
            data = resp.json()
            if isinstance(data, list):
                for q in data:
                    if q.get("symbol"):
                        results[q["symbol"]] = q
        except Exception as e:
            print(f"    ⚠️ Lote {i+1} error: {e}")

        time.sleep(0.3)  # respetar rate limit FMP

    print(f"  ✅ Quotes obtenidos: {len(results)}/{len(tickers)}")
    return results


# ╔══════════════════════════════════════════════════════════════╗
# ║  FMP — HISTÓRICO PARA CANDIDATOS                             ║
# ╚══════════════════════════════════════════════════════════════╝
def fmp_historical(ticker: str, days: int = 90) -> pd.DataFrame | None:
    """Descarga histórico OHLCV de los últimos N días para un ticker."""
    try:
        url  = f"{FMP_BASE}/historical-price-full/{ticker}?timeseries={days}&apikey={FMP_KEY}"
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            return None
        data = resp.json()
        hist = data.get("historical", [])
        if not hist:
            return None
        df = pd.DataFrame(hist)
        df["date"]   = pd.to_datetime(df["date"])
        df = df.sort_values("date").reset_index(drop=True)
        df = df.rename(columns={"close": "Close", "volume": "Volume",
                                 "high": "High", "low": "Low", "open": "Open"})
        return df
    except Exception:
        return None


# ╔══════════════════════════════════════════════════════════════╗
# ║  INDICADORES                                                 ║
# ╚══════════════════════════════════════════════════════════════╝
def sma(series, n):
    if len(series) < n: return None
    return float(series.iloc[-n:].mean())

def rsi(series, period=14):
    if len(series) < period + 1: return None
    delta = series.diff().dropna()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rs    = gain / loss.replace(0, 1e-10)
    val   = (100 - 100 / (1 + rs)).iloc[-1]
    return float(val) if not pd.isna(val) else None


# ╔══════════════════════════════════════════════════════════════╗
# ║  DETECCIÓN DE ANOMALÍAS                                      ║
# ╚══════════════════════════════════════════════════════════════╝
def analizar_con_historico(ticker: str, quote: dict,
                            df: pd.DataFrame, spy_chg: float) -> dict | None:
    """
    Análisis completo con datos históricos.
    El quote ya tiene: price, changesPercentage, volume, avgVolume, yearHigh, yearLow
    El df histórico tiene 90 días de OHLCV
    """
    try:
        if df is None or len(df) < 30:
            return None

        closes  = df["Close"]
        volumes = df["Volume"]

        precio      = float(quote.get("price", 0))
        ret_d       = float(quote.get("changesPercentage", 0))
        vol_today   = float(quote.get("volume", 0))
        vol_avg     = float(quote.get("avgVolume", 1))
        year_high   = float(quote.get("yearHigh", precio))
        year_low    = float(quote.get("yearLow", precio))
        open_today  = float(quote.get("open", precio))
        prev_close  = float(quote.get("previousClose", precio))

        if precio < CFG["min_price"] or vol_avg < CFG["min_vol_avg"]:
            return None

        vol_ratio = vol_today / vol_avg if vol_avg > 0 else 0

        # Indicadores técnicos del histórico
        ma50     = sma(closes, min(50, len(closes)))
        ma200    = sma(closes, 200) if len(closes) >= 200 else None
        rsi_val  = rsi(closes)
        high_52w = year_high
        low_52w  = year_low

        rng = high_52w - low_52w
        pos_52w = ((precio - low_52w) / rng * 100) if rng > 0 else 50

        mom_5d  = (precio / float(closes.iloc[-6])  - 1) * 100 if len(closes) > 5 else 0
        mom_20d = (precio / float(closes.iloc[-21]) - 1) * 100 if len(closes) > 20 else 0

        rango_60d   = closes.tail(60)
        rng_pct_60d = (rango_60d.max() / rango_60d.min() - 1) * 100 if rango_60d.min() > 0 else 100
        dias_consol = 60 if rng_pct_60d < 15 else 0

        alpha = ret_d - spy_chg if spy_chg is not None else None

        # ─── SCORES ─────────────────────────────────────────────
        scores = {}

        # 1. Volumen extremo
        if   vol_ratio >= 10: scores["VOL_EXTREMO"] = 100
        elif vol_ratio >= 5:  scores["VOL_EXTREMO"] = 75
        elif vol_ratio >= 3:  scores["VOL_EXTREMO"] = 50

        # 2. Gap con continuación
        gap_pct = (open_today / prev_close - 1) * 100 if prev_close > 0 else 0
        if abs(gap_pct) > 2 and abs(ret_d) > 2 and np.sign(gap_pct) == np.sign(ret_d):
            scores["GAP_CONTINUATION"] = min(100, abs(gap_pct) * 15)

        # 3. Nuevo máximo 52w con volumen
        if precio >= high_52w * 0.995 and vol_ratio >= 1.5:
            scores["NEW_52W_HIGH"] = min(100, vol_ratio * 20)

        # 4. Breakout tras consolidación
        if dias_consol >= 40 and abs(ret_d) > 3 and vol_ratio >= 2:
            scores["CONSOLIDATION_BREAK"] = min(100, abs(ret_d) * 10 + vol_ratio * 5)

        # 5. Divergencia vs mercado
        if spy_chg is not None and alpha is not None:
            if abs(alpha) > 3 and np.sign(ret_d) != np.sign(spy_chg):
                scores["DIVERGENCIA"] = min(100, abs(alpha) * 8)

        if not scores:
            return None

        anomaly_score = max(scores.values())
        n_signals     = len(scores)
        if n_signals >= 2:
            anomaly_score = min(100, anomaly_score + 10 * (n_signals - 1))

        return {
            "ticker":         ticker,
            "precio":         round(precio, 2),
            "ret_dia_pct":    round(ret_d, 2),
            "alpha":          round(alpha, 2) if alpha is not None else None,
            "vol_ratio":      round(vol_ratio, 2),
            "rsi":            round(rsi_val, 1) if rsi_val else None,
            "pos_52w_pct":    round(pos_52w, 1),
            "mom_5d":         round(mom_5d, 1),
            "mom_20d":        round(mom_20d, 1),
            "vs_ma50":        round((precio/ma50-1)*100, 1) if ma50 else None,
            "high_52w":       round(high_52w, 2),
            "scores":         scores,
            "anomaly_score":  round(anomaly_score, 1),
            "n_signals":      n_signals,
            "primary_signal": max(scores.items(), key=lambda x: x[1])[0],
        }
    except Exception:
        return None


# ╔══════════════════════════════════════════════════════════════╗
# ║  EMAIL                                                       ║
# ╚══════════════════════════════════════════════════════════════╝
def enviar_email(top, today, spy_chg, n_candidatos, n_universe):
    if not EMAIL_FROM or not EMAIL_PASS:
        print("  ⚠️ Gmail no configurado")
        return

    signal_emojis = {
        "VOL_EXTREMO": "🔊", "GAP_CONTINUATION": "⚡",
        "NEW_52W_HIGH": "🏆", "CONSOLIDATION_BREAK": "🚀", "DIVERGENCIA": "🌊"
    }
    signal_desc = {
        "VOL_EXTREMO":         "Volumen extremo — institucional posiblemente entrando",
        "GAP_CONTINUATION":    "Brecha + continuación — reacción fuerte a noticia",
        "NEW_52W_HIGH":        "Nuevo máximo 52 semanas con volumen",
        "CONSOLIDATION_BREAK": "Ruptura tras consolidación larga — energía acumulada",
        "DIVERGENCIA":         "Diverge del mercado — algo específico del ticker",
    }

    finviz_url   = lambda t: f"https://finviz.com/quote.ashx?t={t}"
    yahoo_url    = lambda t: f"https://finance.yahoo.com/quote/{t}/news"
    stockanal_url = lambda t: f"https://stockanalysis.com/stocks/{t.lower()}/"

    if not top:
        tickers_html = "<p style='color:#888;text-align:center;padding:30px'>No se detectaron anomalías hoy.</p>"
    else:
        tickers_html = ""
        for i, r in enumerate(top, 1):
            primary  = r["primary_signal"]
            emoji    = signal_emojis.get(primary, "📊")
            all_sigs = " + ".join([f"{signal_emojis.get(s,'•')} {s}" for s in r["scores"].keys()])
            chg_color = "#2ecc71" if r["ret_dia_pct"] > 0 else "#e74c3c"
            ticker   = r["ticker"]

            tickers_html += f"""
<div style="background:#1e1e2e;border-left:3px solid #8be9fd;padding:14px;margin-bottom:12px;border-radius:4px">
  <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px">
    <div>
      <span style="font-size:18px;font-weight:bold;color:#f8f8f2">#{i} {emoji} {ticker}</span>
      <span style="color:{chg_color};font-weight:bold;margin-left:10px">{r['ret_dia_pct']:+.2f}%</span>
      <span style="color:#888;font-size:13px">  ${r['precio']}</span>
    </div>
    <div style="background:#44475a;padding:4px 10px;border-radius:12px;font-size:13px;color:#8be9fd">
      Score: {r['anomaly_score']}/100
    </div>
  </div>
  <div style="margin-top:8px;font-size:13px;color:#bbb">
    <b>Señales:</b> {all_sigs}<br>
    <b>Volumen:</b> ×{r['vol_ratio']} &nbsp;|&nbsp;
    <b>RSI:</b> {r['rsi'] or 'N/A'} &nbsp;|&nbsp;
    <b>Pos 52w:</b> {r['pos_52w_pct']}% &nbsp;|&nbsp;
    <b>Mom 5d:</b> {r['mom_5d']}% &nbsp;|&nbsp;
    <b>Mom 20d:</b> {r['mom_20d']}%
  </div>
  <div style="margin-top:6px;font-size:12px;color:#888;font-style:italic">
    {signal_desc.get(primary, '')}
  </div>
  <div style="margin-top:10px;display:flex;gap:8px;flex-wrap:wrap">
    <a href="{finviz_url(ticker)}" style="background:#2E75B6;color:white;padding:4px 10px;border-radius:4px;text-decoration:none;font-size:12px">📊 Finviz</a>
    <a href="{yahoo_url(ticker)}" style="background:#6001D2;color:white;padding:4px 10px;border-radius:4px;text-decoration:none;font-size:12px">📰 Noticias</a>
    <a href="{stockanal_url(ticker)}" style="background:#27ae60;color:white;padding:4px 10px;border-radius:4px;text-decoration:none;font-size:12px">📈 Fundamentales</a>
  </div>
</div>"""

    spy_str   = f"{spy_chg:+.2f}%" if spy_chg is not None else "N/A"
    spy_color = "#2ecc71" if (spy_chg or 0) >= 0 else "#e74c3c"

    html = f"""<!DOCTYPE html>
<html><head><style>
body{{font-family:-apple-system,Arial,sans-serif;max-width:680px;margin:0 auto;background:#0d0d1a;color:#f8f8f2}}
.hdr{{background:#1a1a2e;padding:22px;border-radius:8px 8px 0 0}}
.hdr h1{{margin:0;font-size:22px}}
.hdr p{{margin:6px 0 0;color:#888;font-size:13px}}
.bar{{display:flex;gap:8px;padding:15px;background:#12121f;flex-wrap:wrap}}
.m{{flex:1;min-width:100px;text-align:center;padding:12px;background:#1e1e2e;border-radius:6px}}
.mv{{font-size:22px;font-weight:bold}}
.ml{{font-size:11px;color:#888;margin-top:4px}}
.cnt{{padding:18px 22px;background:#12121f}}
.cnt h2{{color:#8be9fd;font-size:17px;margin-top:0}}
.ftr{{background:#1a1a2e;padding:12px;text-align:center;font-size:11px;color:#888;border-radius:0 0 8px 8px}}
</style></head><body>
<div class="hdr">
  <h1>🔎 Agente de Descubrimiento</h1>
  <p>Movimientos inusuales fuera del ruido mediático · {today}</p>
</div>
<div class="bar">
  <div class="m"><div class="mv" style="color:{spy_color}">{spy_str}</div><div class="ml">S&P 500</div></div>
  <div class="m"><div class="mv">{n_universe}</div><div class="ml">Analizados</div></div>
  <div class="m"><div class="mv">{n_candidatos}</div><div class="ml">Candidatos</div></div>
  <div class="m"><div class="mv">{len(top)}</div><div class="ml">Top del día</div></div>
</div>
<div class="cnt">
  <h2>🏆 Top {len(top)} — ordenados por anomaly score</h2>
  {tickers_html}
  <hr style="border-color:#44475a;margin:20px 0"/>
  <p style="font-size:11px;color:#888;line-height:1.6">
    <b>Cómo usar:</b> click en 📊 Finviz para gráfico técnico, 📰 Noticias para contexto reciente,
    📈 Fundamentales para ratios. El universo excluye los 100 tickers más conocidos para forzar el descubrimiento.<br><br>
    Señal estadística — investiga siempre antes de tomar decisiones.
  </p>
</div>
<div class="ftr">TFM — Plataforma de Inversión Inteligente · Master IA en Finanzas · VIU</div>
</body></html>"""

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"🔎 Agente Descubrimiento · {today} · {len(top)} oportunidades · SPY {spy_str}"
        msg["From"]    = EMAIL_FROM
        msg["To"]      = EMAIL_TO
        msg.attach(MIMEText(html, "html"))
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(EMAIL_FROM, EMAIL_PASS)
            server.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())
        print("  ✅ Email enviado")
    except Exception as e:
        print(f"  ❌ Error email: {e}")


# ╔══════════════════════════════════════════════════════════════╗
# ║  MAIN                                                        ║
# ╚══════════════════════════════════════════════════════════════╝
def main():
    today = datetime.now().strftime("%Y-%m-%d")
    print(f"\n{'='*70}")
    print(f"TFM AGENTE DE DESCUBRIMIENTO — {today}")
    print(f"{'='*70}\n")

    if not FMP_KEY:
        print("❌ FMP_KEY no configurada — añádela como secret en GitHub")
        return

    # 1. Universo desde Wikipedia
    print("📋 Obteniendo universo...")
    universe = get_universe()
    print(f"  Universo final: {len(universe)} tickers\n")

    # 2. SPY referencia (1 llamada FMP)
    print("📡 Descargando SPY como referencia...")
    spy_chg = None
    try:
        resp = requests.get(f"{FMP_BASE}/quote/SPY?apikey={FMP_KEY}", timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data:
                spy_chg = round(float(data[0].get("changesPercentage", 0)), 2)
    except Exception as e:
        print(f"  ⚠️ SPY error: {e}")
    print(f"  SPY hoy: {spy_chg}%\n")

    # 3. Batch quotes para todo el universo (~28 llamadas FMP)
    print(f"📊 Batch quotes para {len(universe)} tickers...")
    quotes = fmp_batch_quotes(universe)
    print()

    # 4. Pre-filtrado rápido: vol_ratio y precio (sin llamadas extra)
    print("🔍 Pre-filtrando candidatos con volumen inusual...")
    candidatos = []
    for ticker, q in quotes.items():
        try:
            precio   = float(q.get("price", 0))
            vol      = float(q.get("volume", 0))
            vol_avg  = float(q.get("avgVolume", 1))
            ret_d    = float(q.get("changesPercentage", 0))
            year_high = float(q.get("yearHigh", precio))

            if precio < CFG["min_price"]: continue
            if vol_avg < CFG["min_vol_avg"]: continue

            vol_ratio = vol / vol_avg if vol_avg > 0 else 0

            # Pre-filtro: al menos vol_ratio >= 2 O cerca máx 52w O gap
            open_t = float(q.get("open", precio))
            prev_c = float(q.get("previousClose", precio))
            gap    = abs((open_t / prev_c - 1) * 100) if prev_c > 0 else 0

            if (vol_ratio >= CFG["pre_filter_vol"] or
                (precio >= year_high * 0.99 and vol_ratio >= 1.5) or
                (gap > 3 and abs(ret_d) > 3)):
                candidatos.append((ticker, q, vol_ratio))
        except Exception:
            continue

    candidatos.sort(key=lambda x: x[2], reverse=True)
    candidatos = candidatos[:CFG["top_candidates"]]
    print(f"  {len(candidatos)} candidatos seleccionados para análisis histórico\n")

    # 5. Histórico solo para candidatos (~30 llamadas FMP)
    print(f"📈 Descargando histórico para {len(candidatos)} candidatos...")
    resultados = []
    for ticker, quote, _ in candidatos:
        df = fmp_historical(ticker, days=90)
        resultado = analizar_con_historico(ticker, quote, df, spy_chg)
        if resultado:
            resultados.append(resultado)
        time.sleep(0.2)

    resultados.sort(key=lambda x: x["anomaly_score"], reverse=True)
    top = resultados[:CFG["top_n_email"]]

    print(f"\n✅ Análisis completado — {len(resultados)} anomalías detectadas")
    print(f"\n🏆 TOP {len(top)} — MÁS INUSUALES DEL DÍA")
    print("─" * 70)
    for i, r in enumerate(top, 1):
        sigs = " + ".join(r["scores"].keys())
        alpha_str = f" α={r['alpha']:+.1f}%" if r['alpha'] is not None else ""
        print(f"  {i:2d}. {r['ticker']:<6} {r['ret_dia_pct']:+6.2f}%{alpha_str}  "
              f"vol×{r['vol_ratio']:<5.1f} score={r['anomaly_score']:5.1f}")
        print(f"      [{sigs}]")

    # 6. Google Sheets
    if GCP_CREDS and top:
        print("\n📊 Guardando en Google Sheets...")
        try:
            creds = Credentials.from_service_account_info(
                json.loads(GCP_CREDS),
                scopes=["https://www.googleapis.com/auth/spreadsheets"]
            )
            client = gspread.authorize(creds)
            sh = client.open_by_key(SHEET_ID)
            tab_name = "agente_anomalias"
            try:
                ws = sh.worksheet(tab_name)
            except gspread.exceptions.WorksheetNotFound:
                ws = sh.add_worksheet(title=tab_name, rows=10000, cols=20)
                ws.append_row(["fecha","ticker","precio","ret_dia_pct","alpha",
                               "vol_ratio","rsi","pos_52w_pct","mom_5d","mom_20d",
                               "vs_ma50","anomaly_score","primary_signal",
                               "all_signals","n_signals"])
            rows = []
            for r in top:
                rows.append([
                    today, r["ticker"], r["precio"], r["ret_dia_pct"],
                    r["alpha"] if r["alpha"] is not None else "",
                    r["vol_ratio"], r["rsi"] if r["rsi"] else "",
                    r["pos_52w_pct"], r["mom_5d"], r["mom_20d"],
                    r["vs_ma50"] if r["vs_ma50"] else "",
                    r["anomaly_score"], r["primary_signal"],
                    " + ".join(r["scores"].keys()), r["n_signals"],
                ])
            ws.append_rows(rows, value_input_option="RAW")
            print(f"  ✅ {len(rows)} filas añadidas a '{tab_name}'")
        except Exception as e:
            print(f"  ❌ Error Sheets: {e}")

    # 7. Email
    print("\n📧 Enviando email...")
    enviar_email(top, today, spy_chg, len(candidatos), len(universe))

    print(f"\n{'='*70}")
    print("✅ COMPLETADO")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
