"""
TFM — AGENTE DE DESCUBRIMIENTO DE MERCADO
Master IA Sector Financiero — VIU 2025/26

Objetivo: detectar movimientos inusuales en el mercado US que NO están
en el foco habitual de medios financieros. Cada día identifica los
tickers con mayor "grado de anomalía" y los presenta para análisis.

Arquitectura:
  - Universo: Russell 1000 + S&P 400 MidCap + S&P 600 SmallCap (~1500 tickers)
  - Fuente: Stooq (gratis, funciona desde GitHub Actions)
  - Excluye: top 100 más mencionados (S&P 100) para forzar descubrimiento
  - 5 detectores de anomalía, cada uno con su score
  - Filtros anti-basura: vol avg >300k, precio >$5
  - Output: Top 10 tickers del día ordenados por anomalía

Ejecución automática: GitHub Actions cada día hábil a las 21:35 UTC
"""

import os
import json
import time
import smtplib
import numpy as np
import pandas as pd
import requests
import gspread
from io import StringIO
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from google.oauth2.service_account import Credentials

# ╔══════════════════════════════════════════════════════════════╗
# ║  CONFIGURACIÓN                                               ║
# ╚══════════════════════════════════════════════════════════════╝
SHEET_ID    = "1Yj2KkMypva14ZzpbnP9hDMexhzDGljWU6yhtnsVN980"
EMAIL_TO    = "josemartinezpardo1@gmail.com"
EMAIL_FROM  = os.environ.get("GMAIL_USER", "")
EMAIL_PASS  = os.environ.get("GMAIL_APP_PASSWORD", "")
GCP_CREDS   = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")

# ── Exclusión: S&P 100 (los más mencionados — no queremos descubrir esto) ──
EXCLUDE_OVERKNOWN = {
    "AAPL","MSFT","NVDA","GOOGL","GOOG","AMZN","META","TSLA","AVGO","BRK-B",
    "JPM","LLY","V","XOM","UNH","MA","HD","PG","JNJ","COST",
    "NFLX","BAC","ORCL","ABBV","CVX","KO","WMT","MRK","ADBE","CRM",
    "PEP","TMO","LIN","ACN","CSCO","MCD","DIS","AMD","ABT","IBM",
    "INTC","PFE","NOW","TXN","WFC","DHR","AXP","QCOM","NEE","AMGN",
    "VZ","CMCSA","T","PM","GE","RTX","CAT","SPGI","GS","MS",
    "UBER","BLK","PYPL","SBUX","NKE","BKNG","PLD","GILD","LOW","TJX",
    "DE","SYK","ADP","MDT","LMT","MMC","ETN","PGR","BSX","CB",
    "ISRG","VRTX","CI","SO","FI","DUK","KLAC","INTU","SCHW","BMY",
    "AMAT","ANET","HON","AMT","CRWD","REGN","PANW","ELV","TMUS","FDX"
}

# ── Filtros anti-basura ──────────────────────────────────────────
CFG = {
    "min_price":           5.0,
    "min_vol_avg":         300_000,
    "min_days_data":       60,
    "top_n_email":         10,
}


# ╔══════════════════════════════════════════════════════════════╗
# ║  UNIVERSO DE TICKERS                                         ║
# ╚══════════════════════════════════════════════════════════════╝
def get_universe():
    """Obtiene universo amplio combinando Russell 1000 + S&P 400 + S&P 600."""
    universe = set()

    # S&P 400 MidCap + S&P 600 SmallCap (desde Wikipedia, funciona siempre)
    sources = [
        ("S&P 400 MidCap",  "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies"),
        ("S&P 600 SmallCap","https://en.wikipedia.org/wiki/List_of_S%26P_600_companies"),
        ("S&P 500",         "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"),
    ]

    for name, url in sources:
        try:
            html = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15).text
            tables = pd.read_html(html)
            found = 0
            for t in tables:
                symbol_col = None
                for col in t.columns:
                    if "symbol" in str(col).lower() or "ticker" in str(col).lower():
                        symbol_col = col
                        break
                if symbol_col:
                    tickers = t[symbol_col].dropna().astype(str).tolist()
                    for tk in tickers:
                        tk = tk.strip().replace(".", "-").upper()
                        if tk and tk not in ("-", "NAN", "NONE"):
                            universe.add(tk)
                            found += 1
                    break
            print(f"  ✅ {name}: +{found} tickers  (total acumulado: {len(universe)})")
        except Exception as e:
            print(f"  ⚠️ {name} falló: {e}")

    # Excluir los sobre-conocidos
    universe = universe - EXCLUDE_OVERKNOWN

    # Lista de respaldo si todo falla
    if len(universe) < 100:
        print("  ⚠️ Usando lista de respaldo")
        universe = {
            "AMKR","ANF","AOS","APA","AR","ARW","ASH","ATI","AVT","AWI","AZZ",
            "BCO","BDC","BJ","BLD","BMI","BOX","BTU","BWA","BYD","CACI","CAR",
            "CBT","CCK","CDP","CELH","CENX","CFR","CHE","CHX","CIEN","CMC",
            "CNX","COHR","COLB","CR","CRC","CROX","CRUS","CSGS","CTLT","CW",
            "CWH","CXT","CYH","DCI","DINO","DIOD","DKS","DNB","DOCS","DORM",
            "DSGX","DY","EGP","EHC","ELF","ELS","EME","ENR","ENS","ENV",
            "EPAM","EPR","ESAB","ESE","EVR","EWBC","EXLS","EXPI","FAF","FBP",
            "FCN","FFIN","FIX","FIZZ","FL","FLO","FLR","FMC","FNB","FOUR",
            "FRPT","FSS","FTDR","FUL","FULT","GEF","GGG","GKOS","GLPI","GMS",
            "GNTX","GO","GPI","GPK","GT","GTES","GVA","HAE","HELE","HLI",
            "HLIT","HOG","HOMB","HR","HRB","HTH","HUBG","HXL","IAC","ICUI",
            "IDA","IDCC","IDYA","IIPR","INDB","INT","IOSP","IPAR","IRDM","ITRI"
        }

    return sorted(universe)


# ╔══════════════════════════════════════════════════════════════╗
# ║  DESCARGA DE DATOS — STOOQ                                   ║
# ╚══════════════════════════════════════════════════════════════╝
def stooq_download(ticker, days=120):
    """Descarga OHLCV de Stooq para un ticker US."""
    try:
        symbol = ticker.lower() + ".us"
        url    = f"https://stooq.com/q/d/l/?s={symbol}&i=d"
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)

        if r.status_code != 200 or "Date,Open" not in r.text:
            return None

        df = pd.read_csv(StringIO(r.text))
        if df.empty or "Close" not in df.columns:
            return None

        df["Date"] = pd.to_datetime(df["Date"])
        df = df.sort_values("Date").tail(days).reset_index(drop=True)
        return df
    except Exception:
        return None


# ╔══════════════════════════════════════════════════════════════╗
# ║  INDICADORES                                                 ║
# ╚══════════════════════════════════════════════════════════════╝
def sma(series, n):
    if len(series) < n:
        return None
    return float(series.iloc[-n:].mean())

def rsi(series, period=14):
    if len(series) < period + 1:
        return None
    delta = series.diff().dropna()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rs    = gain / loss.replace(0, 1e-10)
    val   = (100 - 100 / (1 + rs)).iloc[-1]
    return float(val) if not pd.isna(val) else None


# ╔══════════════════════════════════════════════════════════════╗
# ║  DETECTORES DE ANOMALÍA                                      ║
# ╚══════════════════════════════════════════════════════════════╝
def analizar_ticker(ticker, df, spy_ret_today):
    if df is None or len(df) < CFG["min_days_data"]:
        return None

    try:
        closes  = df["Close"]
        volumes = df["Volume"]

        precio      = float(closes.iloc[-1])
        precio_prev = float(closes.iloc[-2])

        if precio < CFG["min_price"]:
            return None
        vol_avg_60 = float(volumes.tail(60).mean())
        if vol_avg_60 < CFG["min_vol_avg"]:
            return None

        ret_d = (precio / precio_prev - 1) * 100

        vol_today  = float(volumes.iloc[-1])
        vol_avg_20 = float(volumes.tail(21).iloc[:-1].mean())
        vol_ratio  = vol_today / vol_avg_20 if vol_avg_20 > 0 else 0

        ma50     = sma(closes, min(50, len(closes)))
        ma200    = sma(closes, 200) if len(closes) >= 200 else None
        high_52w = float(closes.tail(min(252, len(closes))).max())
        low_52w  = float(closes.tail(min(252, len(closes))).min())

        rng = high_52w - low_52w
        pos_52w = ((precio - low_52w) / rng * 100) if rng > 0 else 50

        mom_5d  = (precio / float(closes.iloc[-6])  - 1) * 100 if len(closes) > 5 else 0
        mom_20d = (precio / float(closes.iloc[-21]) - 1) * 100 if len(closes) > 20 else 0

        rango_60d   = closes.tail(60)
        rng_pct_60d = (rango_60d.max() / rango_60d.min() - 1) * 100 if rango_60d.min() > 0 else 100
        dias_consolidacion = 60 if rng_pct_60d < 15 else 0

        rsi_val = rsi(closes)
        alpha   = ret_d - spy_ret_today if spy_ret_today is not None else None

        # ─── SCORES POR DETECTOR ────────────────────────────────
        scores = {}

        # 1. Volumen extremo
        if   vol_ratio >= 10: scores["VOL_EXTREMO"] = 100
        elif vol_ratio >= 5:  scores["VOL_EXTREMO"] = 75
        elif vol_ratio >= 3:  scores["VOL_EXTREMO"] = 50

        # 2. Gap con continuación
        open_today = float(df["Open"].iloc[-1])
        gap_pct    = (open_today / precio_prev - 1) * 100
        if abs(gap_pct) > 2 and abs(ret_d) > 2 and np.sign(gap_pct) == np.sign(ret_d):
            scores["GAP_CONTINUATION"] = min(100, abs(gap_pct) * 15)

        # 3. Nuevo máximo 52w con volumen
        if precio >= high_52w * 0.995 and vol_ratio >= 1.5:
            scores["NEW_52W_HIGH"] = min(100, vol_ratio * 20)

        # 4. Breakout tras consolidación
        if dias_consolidacion >= 40 and abs(ret_d) > 3 and vol_ratio >= 2:
            scores["CONSOLIDATION_BREAK"] = min(100, abs(ret_d) * 10 + vol_ratio * 5)

        # 5. Divergencia vs mercado
        if spy_ret_today is not None and alpha is not None:
            if abs(alpha) > 3 and np.sign(ret_d) != np.sign(spy_ret_today):
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
            "ma50":           round(ma50, 2) if ma50 else None,
            "ma200":          round(ma200, 2) if ma200 else None,
            "vs_ma50":        round((precio/ma50-1)*100, 1) if ma50 else None,
            "high_52w":       round(high_52w, 2),
            "vol_avg_60":     int(vol_avg_60),
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
def enviar_email(top, today, spy_chg, n_total, n_universe):
    if not EMAIL_FROM or not EMAIL_PASS:
        print("  ⚠️ Gmail no configurado")
        return

    signal_emojis = {
        "VOL_EXTREMO":         "🔊",
        "GAP_CONTINUATION":    "⚡",
        "NEW_52W_HIGH":        "🏆",
        "CONSOLIDATION_BREAK": "🚀",
        "DIVERGENCIA":         "🌊",
    }
    signal_desc = {
        "VOL_EXTREMO":         "Volumen extremo — institucional posiblemente entrando",
        "GAP_CONTINUATION":    "Brecha + continuación — reacción a noticia",
        "NEW_52W_HIGH":        "Nuevo máximo 52 semanas con volumen",
        "CONSOLIDATION_BREAK": "Ruptura tras consolidación larga — energía acumulada",
        "DIVERGENCIA":         "Diverge del mercado — algo específico del ticker",
    }

    if not top:
        tickers_html = "<p style='color:#888;text-align:center;padding:30px'>No se detectaron anomalías hoy.</p>"
    else:
        tickers_html = ""
        for i, r in enumerate(top, 1):
            primary = r["primary_signal"]
            emoji = signal_emojis.get(primary, "📊")
            all_sigs = " + ".join([f"{signal_emojis.get(s,'•')} {s}" for s in r["scores"].keys()])
            chg_color = "#2ecc71" if r["ret_dia_pct"] > 0 else "#e74c3c"

            tickers_html += f"""
<div style="background:#1e1e2e;border-left:3px solid #8be9fd;padding:14px;margin-bottom:12px;border-radius:4px">
  <div style="display:flex;justify-content:space-between;align-items:center">
    <div>
      <span style="font-size:18px;font-weight:bold;color:#f8f8f2">#{i} {emoji} {r['ticker']}</span>
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
</div>"""

    spy_str   = f"{spy_chg:+.2f}%" if spy_chg is not None else "N/A"
    spy_color = "#2ecc71" if (spy_chg or 0) >= 0 else "#e74c3c"

    html = f"""<!DOCTYPE html>
<html><head><style>
body{{font-family:-apple-system,Arial,sans-serif;max-width:680px;margin:0 auto;background:#0d0d1a;color:#f8f8f2}}
.hdr{{background:#1a1a2e;padding:22px;border-radius:8px 8px 0 0}}
.hdr h1{{margin:0;font-size:22px}}
.hdr p{{margin:6px 0 0;color:#888;font-size:13px}}
.bar{{display:flex;gap:10px;padding:15px;background:#12121f}}
.m{{flex:1;text-align:center;padding:12px;background:#1e1e2e;border-radius:6px}}
.mv{{font-size:24px;font-weight:bold}}
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
  <div class="m"><div class="mv">{n_total}</div><div class="ml">Anomalías</div></div>
  <div class="m"><div class="mv">{len(top)}</div><div class="ml">Top del día</div></div>
</div>
<div class="cnt">
  <h2>🏆 Top {len(top)} — ordenados por anomaly score</h2>
  {tickers_html}
  <hr style="border-color:#44475a;margin:20px 0"/>
  <p style="font-size:11px;color:#888;line-height:1.6">
    <b>Cómo interpretar:</b> el universo excluye los 100 tickers más mencionados (S&P 100)
    para forzar el descubrimiento. Los tickers aquí se están moviendo de forma inusual
    pero probablemente <b>no están en titulares</b>. Siempre investiga la razón detrás
    de cada señal antes de tomar decisiones.<br><br>
    <b>Universo:</b> S&P 500 + S&P 400 + S&P 600, excluyendo megacaps populares.
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

    # 1. SPY referencia
    print("📡 Descargando SPY como referencia...")
    spy_df = stooq_download("SPY", days=10)
    spy_chg = None
    if spy_df is not None and len(spy_df) >= 2:
        spy_chg = round((float(spy_df["Close"].iloc[-1]) /
                          float(spy_df["Close"].iloc[-2]) - 1) * 100, 2)
    print(f"  SPY hoy: {spy_chg}%\n")

    # 2. Universo
    print("📋 Obteniendo universo de tickers...")
    universe = get_universe()
    print(f"\n  Universo final (excl. sobre-conocidos): {len(universe)} tickers\n")

    # 3. Análisis ticker a ticker
    print(f"📊 Analizando {len(universe)} tickers vía Stooq...")
    print("  (Esto tarda ~8-12 minutos)\n")

    resultados = []
    n_ok = n_skip = n_error = 0

    for i, ticker in enumerate(universe):
        if (i + 1) % 100 == 0:
            print(f"  {i+1}/{len(universe)} — {len(resultados)} anomalías")

        df = stooq_download(ticker, days=120)
        if df is None:
            n_error += 1
            continue

        resultado = analizar_ticker(ticker, df, spy_chg)
        if resultado:
            resultados.append(resultado)
            n_ok += 1
        else:
            n_skip += 1

        time.sleep(0.15)

    print(f"\n✅ Análisis completado")
    print(f"   Anomalías: {n_ok} | Descartados: {n_skip} | Errores: {n_error}")

    # 4. Top N
    resultados.sort(key=lambda x: x["anomaly_score"], reverse=True)
    top = resultados[:CFG["top_n_email"]]

    print(f"\n🏆 TOP {len(top)} — MÁS INUSUALES DEL DÍA")
    print("─" * 70)
    for i, r in enumerate(top, 1):
        signals_str = " + ".join(r["scores"].keys())
        alpha_str = f" α={r['alpha']:+.1f}%" if r['alpha'] is not None else ""
        print(f"  {i:2d}. {r['ticker']:<6} {r['ret_dia_pct']:+6.2f}%{alpha_str}  "
              f"vol×{r['vol_ratio']:<4.1f}  score={r['anomaly_score']:5.1f}")
        print(f"      [{signals_str}]")

    # 5. Google Sheets
    if GCP_CREDS and resultados:
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
                ws = sh.add_worksheet(title=tab_name, rows=10000, cols=30)
                headers = ["fecha","ticker","precio","ret_dia_pct","alpha","vol_ratio",
                           "rsi","pos_52w_pct","mom_5d","mom_20d","vs_ma50",
                           "anomaly_score","primary_signal","all_signals","n_signals"]
                ws.append_row(headers)

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

    # 6. Email
    print("\n📧 Enviando email...")
    enviar_email(top, today, spy_chg, len(resultados), len(universe))

    print(f"\n{'='*70}")
    print("✅ COMPLETADO")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
