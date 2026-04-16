"""
TFM — Agente Diario de Mercado
Ejecutado automáticamente por GitHub Actions cada día hábil a las 17:30 ET

Hace tres cosas:
  1. Descarga datos del día y detecta setups técnicos (S&P 500)
  2. Rellena retornos forward (3d, 5d, 10d) de setups pasados en Google Sheets
  3. Añade los setups de hoy al Google Sheet y envía email resumen
"""

import os
import json
import time
import smtplib
import numpy as np
import pandas as pd
import yfinance as yf
import gspread
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from google.oauth2.service_account import Credentials

# ╔══════════════════════════════════════════════════════════════╗
# ║  CONFIGURACIÓN                                               ║
# ╚══════════════════════════════════════════════════════════════╝
SHEET_ID    = "1Yj2KkMypva14ZzpbnP9hDMexhzDGljWU6yhtnsVN980"
SHEET_TAB   = "agent_setups_2024-04-16_2026-03-31"
EMAIL_TO    = "josemartinezpardo1@gmail.com"
EMAIL_FROM  = os.environ.get("GMAIL_USER", "")
EMAIL_PASS  = os.environ.get("GMAIL_APP_PASSWORD", "")
GCP_CREDS   = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")

UNIVERSE = [
    "AAPL","MSFT","NVDA","GOOGL","META","AMZN","TSLA","AVGO","BRK-B","JPM",
    "V","UNH","LLY","XOM","MA","JNJ","PG","HD","COST","MRK","ABBV","CVX",
    "CRM","NFLX","BAC","AMD","WFC","TMO","ORCL","KO","ACN","PEP","CSCO",
    "LIN","MCD","ABT","TXN","WMT","QCOM","GE","CAT","GS","MS","DHR","RTX",
    "HON","AMGN","INTU","SPGI","ISRG","BKNG","NOW","AXP","PLD","MDLZ","SYK",
    "ADI","GILD","MMC","ZTS","BLK","CB","CME","ETN","TJX","MO","REGN","PGR",
    "BSX","C","ITW","ADP","BMY","NKE","SO","DUK","NEE","SHW","CL","WM","EMR",
    "SBUX","GD","HUM","APD","F","GM","DAL","BA","LMT","NOC","DE","MMM","FDX",
    "UPS","CSX",
]

SECTOR_MAP = {
    "AAPL":"Technology","MSFT":"Technology","NVDA":"Technology","GOOGL":"Technology",
    "META":"Technology","AVGO":"Technology","AMD":"Technology","INTC":"Technology",
    "ORCL":"Technology","CRM":"Technology","CSCO":"Technology","QCOM":"Technology",
    "TXN":"Technology","ADBE":"Technology","NOW":"Technology","INTU":"Technology",
    "AMAT":"Technology","ACN":"Technology","ADI":"Technology","SPGI":"Financials",
    "JPM":"Financials","BAC":"Financials","WFC":"Financials","GS":"Financials",
    "MS":"Financials","BLK":"Financials","AXP":"Financials","SCHW":"Financials",
    "C":"Financials","PNC":"Financials","MMC":"Financials","CME":"Financials",
    "CB":"Financials","V":"Financials","MA":"Financials","BRK-B":"Financials",
    "UNH":"Health Care","JNJ":"Health Care","LLY":"Health Care","ABBV":"Health Care",
    "MRK":"Health Care","PFE":"Health Care","TMO":"Health Care","ABT":"Health Care",
    "DHR":"Health Care","AMGN":"Health Care","ISRG":"Health Care","VRTX":"Health Care",
    "GILD":"Health Care","SYK":"Health Care","ZTS":"Health Care","BSX":"Health Care",
    "REGN":"Health Care","HUM":"Health Care","BMY":"Health Care","MDT":"Health Care",
    "AMZN":"Consumer Discretionary","TSLA":"Consumer Discretionary",
    "HD":"Consumer Discretionary","MCD":"Consumer Discretionary",
    "NKE":"Consumer Discretionary","SBUX":"Consumer Discretionary",
    "TJX":"Consumer Discretionary","BKNG":"Consumer Discretionary",
    "F":"Consumer Discretionary","GM":"Consumer Discretionary",
    "PG":"Consumer Staples","KO":"Consumer Staples","PEP":"Consumer Staples",
    "WMT":"Consumer Staples","COST":"Consumer Staples","MDLZ":"Consumer Staples",
    "CL":"Consumer Staples","MO":"Consumer Staples",
    "CAT":"Industrials","BA":"Industrials","GE":"Industrials","UPS":"Industrials",
    "HON":"Industrials","LMT":"Industrials","RTX":"Industrials","DE":"Industrials",
    "MMM":"Industrials","FDX":"Industrials","NOC":"Industrials","GD":"Industrials",
    "ETN":"Industrials","ITW":"Industrials","EMR":"Industrials","CSX":"Industrials",
    "XOM":"Energy","CVX":"Energy","COP":"Energy","SLB":"Energy",
    "LIN":"Materials","APD":"Materials","SHW":"Materials","NEM":"Materials",
    "NEE":"Utilities","DUK":"Utilities","SO":"Utilities","WM":"Utilities",
    "AMT":"Real Estate","PLD":"Real Estate","EQIX":"Real Estate",
    "NFLX":"Communication Services","DIS":"Communication Services",
    "CMCSA":"Communication Services","T":"Communication Services",
    "ADP":"Industrials","PGR":"Financials","WFC":"Financials",
}

CFG = {
    "ma50_vol_min":   1.5,
    "rsi_oversold":   32,
    "vol_surge":      2.5,
    "high52_prox":    0.98,
    "golden_prox":    0.02,
    "min_price":      5.0,
    "min_vol_avg":    500_000,
}


# ╔══════════════════════════════════════════════════════════════╗
# ║  UTILIDADES                                                  ║
# ╚══════════════════════════════════════════════════════════════╝
def add_trading_days(date: datetime, days: int) -> datetime:
    d, added = date, 0
    while added < days:
        d -= timedelta(days=1)
        if d.weekday() < 5:
            added += 1
    return d

def sma(series: pd.Series, n: int):
    if len(series) < n:
        return None
    return series.iloc[-n:].mean()

def calc_rsi(series: pd.Series, period: int = 14):
    if len(series) < period + 1:
        return None
    delta = series.diff().dropna()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rs    = gain / loss.replace(0, 1e-10)
    rsi   = 100 - (100 / (1 + rs))
    return float(rsi.iloc[-1]) if not pd.isna(rsi.iloc[-1]) else None

def vol_ratio(volumes: pd.Series):
    if len(volumes) < 21:
        return None
    last = volumes.iloc[-1]
    avg  = volumes.iloc[-21:-1].mean()
    return float(last / avg) if avg > 0 else None


# ╔══════════════════════════════════════════════════════════════╗
# ║  GOOGLE SHEETS                                               ║
# ╚══════════════════════════════════════════════════════════════╝
def conectar_sheets():
    """Conecta a Google Sheets via Service Account."""
    creds_dict = json.loads(GCP_CREDS)
    creds = Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    client = gspread.authorize(creds)
    sh     = client.open_by_key(SHEET_ID)
    return sh.worksheet(SHEET_TAB)

def leer_sheet(ws) -> pd.DataFrame:
    """Lee el Sheet completo como DataFrame."""
    data = ws.get_all_records()
    if not data:
        return pd.DataFrame()
    return pd.DataFrame(data)

def rellenar_forward_returns(ws, df: pd.DataFrame, precios_hoy: dict, today: str):
    """
    Para las filas con fwd_ret vacío y fecha = hace 3/5/10 días hábiles,
    calcula el retorno y actualiza la celda en Sheets.
    """
    now = datetime.strptime(today, "%Y-%m-%d")
    date3d  = add_trading_days(now, 3).strftime("%Y-%m-%d")
    date5d  = add_trading_days(now, 5).strftime("%Y-%m-%d")
    date10d = add_trading_days(now, 10).strftime("%Y-%m-%d")

    if df.empty:
        return 0

    # Cabecera para saber número de columna
    headers  = ws.row_values(1)
    col_map  = {h: i+1 for i, h in enumerate(headers)}

    updates  = []
    n_updated = 0

    for idx, row in df.iterrows():
        fecha  = str(row.get("fecha", ""))
        ticker = str(row.get("ticker", ""))
        precio_base = float(row.get("precio", 0) or 0)
        precio_hoy  = precios_hoy.get(ticker)

        if not precio_hoy or precio_base <= 0:
            continue

        row_num = idx + 2  # +2 por 1-indexed y header

        def ret(p_base, p_hoy):
            return round((p_hoy / p_base - 1) * 100, 2)

        if fecha == date3d and not str(row.get("fwd_ret_3d_pct", "")).strip():
            col = col_map.get("fwd_ret_3d_pct")
            if col:
                updates.append({"range": gspread.utils.rowcol_to_a1(row_num, col),
                                 "values": [[ret(precio_base, precio_hoy)]]})
                n_updated += 1

        if fecha == date5d and not str(row.get("fwd_ret_5d_pct", "")).strip():
            col = col_map.get("fwd_ret_5d_pct")
            if col:
                updates.append({"range": gspread.utils.rowcol_to_a1(row_num, col),
                                 "values": [[ret(precio_base, precio_hoy)]]})
                n_updated += 1

        if fecha == date10d and not str(row.get("fwd_ret_10d_pct", "")).strip():
            col = col_map.get("fwd_ret_10d_pct")
            if col:
                updates.append({"range": gspread.utils.rowcol_to_a1(row_num, col),
                                 "values": [[ret(precio_base, precio_hoy)]]})
                n_updated += 1

    if updates:
        ws.batch_update(updates)
        print(f"  ✅ {n_updated} retornos forward actualizados")
    else:
        print("  ℹ️  No hay retornos forward que actualizar hoy")

    return n_updated

def append_setups(ws, setups: list):
    """Añade los setups de hoy al final del Sheet."""
    if not setups:
        print("  ℹ️  No hay setups nuevos que añadir")
        return

    headers = ws.row_values(1)
    rows    = []
    for s in setups:
        rows.append([str(s.get(h, "")) for h in headers])

    ws.append_rows(rows, value_input_option="RAW")
    print(f"  ✅ {len(setups)} setups añadidos al Sheet")


# ╔══════════════════════════════════════════════════════════════╗
# ║  DETECCIÓN DE SETUPS                                         ║
# ╚══════════════════════════════════════════════════════════════╝
def detectar_setups(today: str) -> tuple[list, dict, float, float, str]:
    """
    Descarga datos y detecta setups técnicos.
    Retorna (setups, precios_hoy, spy_chg, vix_val, regimen)
    """
    print(f"\n📡 Descargando datos para {today}...")

    # ── Benchmark y VIX ────────────────────────────────────────
    spy_chg   = None
    spy_price = None
    vix_val   = None
    regimen   = "N/A"

    try:
        spy = yf.download("SPY", period="5d", progress=False, auto_adjust=True)
        if not spy.empty and len(spy) >= 2:
            spy_price = float(spy["Close"].iloc[-1])
            spy_prev  = float(spy["Close"].iloc[-2])
            spy_chg   = round((spy_price / spy_prev - 1) * 100, 2)
            if   spy_chg >  1.0: regimen = "Risk-ON fuerte"
            elif spy_chg >  0.2: regimen = "Risk-ON"
            elif spy_chg > -0.2: regimen = "Neutro"
            elif spy_chg > -1.0: regimen = "Risk-OFF"
            else:                regimen = "Risk-OFF fuerte"
    except Exception as e:
        print(f"  ⚠️ SPY error: {e}")

    try:
        vix = yf.download("^VIX", period="5d", progress=False, auto_adjust=True)
        if not vix.empty:
            vix_val = round(float(vix["Close"].iloc[-1]), 1)
    except Exception as e:
        print(f"  ⚠️ VIX error: {e}")

    print(f"  SPY: {spy_chg}% | VIX: {vix_val} | {regimen}")

    # ── Descarga bulk S&P 500 ───────────────────────────────────
    print(f"\n📊 Descargando {len(UNIVERSE)} tickers (bulk)...")
    raw = yf.download(
        UNIVERSE, period="6mo",
        auto_adjust=True, progress=False
    )

    if raw.empty:
        print("  ❌ Sin datos de yfinance")
        return [], {}, spy_chg, vix_val, regimen

    closes  = raw["Close"]
    volumes = raw["Volume"]

    print(f"  ✅ {closes.shape[1]} tickers con datos | {len(closes)} sesiones")

    # ── Precio de hoy por ticker (para forward returns) ─────────
    precios_hoy = {}
    for t in closes.columns:
        v = closes[t].dropna()
        if not v.empty:
            precios_hoy[t] = round(float(v.iloc[-1]), 2)

    # ── Detección de setups ─────────────────────────────────────
    setups    = []
    n_total   = 0
    n_skipped = 0

    for ticker in closes.columns:
        try:
            c = closes[ticker].dropna()
            v = volumes[ticker].dropna()

            if len(c) < 30: n_skipped += 1; continue

            precio      = float(c.iloc[-1])
            precio_prev = float(c.iloc[-2])

            if precio < CFG["min_price"]:         n_skipped += 1; continue
            if v.tail(20).mean() < CFG["min_vol_avg"]: n_skipped += 1; continue

            ret_d    = (precio / precio_prev - 1) * 100
            ma50_val = sma(c, min(50, len(c)))
            ma50_p   = sma(c.iloc[:-1], min(50, len(c)-1))
            ma200_v  = sma(c, 200) if len(c) >= 200 else None
            ma200_p  = sma(c.iloc[:-1], 200) if len(c) >= 201 else None
            rsi_val  = calc_rsi(c)
            rsi_prev = calc_rsi(c.iloc[:-1])
            vol_r    = vol_ratio(v)
            high52   = float(c.iloc[-min(252,len(c)):].max())
            mom_1m   = ((precio / float(c.iloc[-22]) - 1)*100) if len(c) > 22 else None
            alpha    = round(ret_d - spy_chg, 2) if spy_chg is not None else None

            setup = None

            # MA50 BREAKOUT
            if (ma50_val and ma50_p and
                precio_prev < ma50_p and precio > ma50_val and
                vol_r and vol_r >= CFG["ma50_vol_min"]):
                setup = "MA50_BREAKOUT"

            # RSI RECOVERY
            elif (rsi_val and rsi_prev and
                  rsi_prev <= CFG["rsi_oversold"] and
                  rsi_val > CFG["rsi_oversold"] and ret_d > 0):
                setup = "RSI_RECOVERY"

            # 52W HIGH
            elif (precio >= high52 * CFG["high52_prox"] and
                  mom_1m is not None and mom_1m > 5 and
                  vol_r and vol_r >= 1.0):
                setup = "52W_HIGH"

            # VOLUME SURGE
            elif vol_r and vol_r >= CFG["vol_surge"] and ret_d > 1.0:
                setup = "VOLUME_SURGE"

            # GOLDEN CROSS NEAR
            elif (ma50_val and ma200_v and ma50_p and ma200_p and
                  ma50_p < ma200_p and
                  abs(ma50_val / ma200_v - 1) < CFG["golden_prox"] and
                  ma50_val > ma50_p):
                setup = "GOLDEN_CROSS_NEAR"

            if setup:
                setups.append({
                    "fecha":           today,
                    "dia_semana":      datetime.strptime(today, "%Y-%m-%d").strftime("%A"),
                    "ticker":          ticker,
                    "sector":          SECTOR_MAP.get(ticker, "Unknown"),
                    "setup":           setup,
                    "precio":          round(precio, 2),
                    "ret_dia_pct":     round(ret_d, 2),
                    "alpha_vs_spy":    alpha,
                    "vol_ratio":       round(vol_r, 2) if vol_r else None,
                    "rsi":             round(rsi_val, 1) if rsi_val else None,
                    "ma50":            round(ma50_val, 2) if ma50_val else None,
                    "ma200":           round(ma200_v, 2) if ma200_v else None,
                    "vs_ma50_pct":     round((precio/ma50_val-1)*100, 2) if ma50_val else None,
                    "vs_ma200_pct":    round((precio/ma200_v-1)*100, 2) if ma200_v else None,
                    "mom_1m_pct":      round(mom_1m, 1) if mom_1m else None,
                    "high_52w":        round(high52, 2),
                    "spy_chg_pct":     spy_chg,
                    "vix":             vix_val,
                    "regimen":         regimen,
                    "fwd_ret_3d_pct":  "",
                    "fwd_ret_5d_pct":  "",
                    "fwd_ret_10d_pct": "",
                    "precio_3d":       "",
                    "precio_5d":       "",
                    "precio_10d":      "",
                })
                n_total += 1

        except Exception:
            n_skipped += 1
            continue

    print(f"  ✅ {n_total} setups detectados | {n_skipped} tickers sin datos suficientes")
    return setups, precios_hoy, spy_chg, vix_val, regimen


# ╔══════════════════════════════════════════════════════════════╗
# ║  EMAIL                                                       ║
# ╚══════════════════════════════════════════════════════════════╝
def enviar_email(setups: list, today: str, spy_chg, vix_val, regimen: str):
    """Envía el resumen diario por Gmail SMTP."""
    if not EMAIL_FROM or not EMAIL_PASS:
        print("  ⚠️ Gmail no configurado — saltando email")
        return

    by_setup = {}
    for s in setups:
        by_setup.setdefault(s["setup"], []).append(s)

    emojis = {
        "MA50_BREAKOUT": "📈", "RSI_RECOVERY": "🔄",
        "52W_HIGH": "🏆", "VOLUME_SURGE": "🔊",
        "GOLDEN_CROSS_NEAR": "✨"
    }

    setups_html = ""
    if not setups:
        setups_html = "<p style='color:#888'>No se detectaron setups técnicos hoy.</p>"
    else:
        for setup, items in by_setup.items():
            e = emojis.get(setup, "📊")
            setups_html += f"<h3 style='color:#8be9fd'>{e} {setup} — {len(items)} señal(es)</h3><ul>"
            for s in items[:5]:
                alpha_str = f" | Alpha: {s['alpha_vs_spy']:+.2f}%" if s.get("alpha_vs_spy") is not None else ""
                ret_str   = f"{s['ret_dia_pct']:+.2f}%"
                setups_html += (f"<li><b>{s['ticker']}</b> ({s['sector']}) — "
                                f"${s['precio']} | {ret_str}{alpha_str} | "
                                f"RSI: {s['rsi'] or 'N/A'} | Vol: {s['vol_ratio'] or 'N/A'}x</li>")
            setups_html += "</ul>"

    regimen_color = {
        "Risk-ON fuerte": "#27ae60", "Risk-ON": "#2ecc71",
        "Neutro": "#f39c12", "Risk-OFF": "#e74c3c",
        "Risk-OFF fuerte": "#c0392b"
    }.get(regimen, "#7f8c8d")

    spy_str = f"{spy_chg:+.2f}%" if spy_chg is not None else "N/A"
    spy_color = "#2ecc71" if (spy_chg or 0) >= 0 else "#e74c3c"

    html = f"""<!DOCTYPE html>
<html><head><style>
body{{font-family:Arial,sans-serif;max-width:600px;margin:0 auto;background:#0d0d1a;color:#f8f8f2}}
.hdr{{background:#1a1a2e;padding:20px;border-radius:8px 8px 0 0}}
.hdr h1{{margin:0;font-size:20px}}
.hdr p{{margin:5px 0 0;color:#888;font-size:13px}}
.bar{{display:flex;gap:10px;padding:15px;background:#12121f}}
.m{{flex:1;text-align:center;padding:10px;background:#1e1e2e;border-radius:6px}}
.mv{{font-size:22px;font-weight:bold}}
.ml{{font-size:11px;color:#888;margin-top:3px}}
.cnt{{padding:20px;background:#12121f}}
.ftr{{background:#1a1a2e;padding:12px;text-align:center;font-size:11px;color:#888;border-radius:0 0 8px 8px}}
</style></head><body>
<div class="hdr"><h1>📊 TFM — Agente de Mercado</h1><p>Resumen diario · {today}</p></div>
<div class="bar">
  <div class="m"><div class="mv" style="color:{spy_color}">{spy_str}</div><div class="ml">S&P 500</div></div>
  <div class="m"><div class="mv">{vix_val or 'N/A'}</div><div class="ml">VIX</div></div>
  <div class="m"><div class="mv">{len(setups)}</div><div class="ml">Setups</div></div>
  <div class="m"><div class="mv" style="color:{regimen_color};font-size:13px">{regimen}</div><div class="ml">Régimen</div></div>
</div>
<div class="cnt">
  <h2>🔍 Setups técnicos detectados hoy</h2>
  {setups_html}
  <hr style="border-color:#44475a"/>
  <p style="font-size:11px;color:#888">
    Señal estadística basada en patrones históricos — no es recomendación de inversión.<br>
    Los retornos forward (+3/+5/+10 días) se actualizarán automáticamente.
  </p>
</div>
<div class="ftr">TFM — Plataforma de Inversión Inteligente · Master IA en Finanzas · VIU</div>
</body></html>"""

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"📊 TFM Agente · {today} · SPY {spy_str} · {len(setups)} setups · {regimen}"
        msg["From"]    = EMAIL_FROM
        msg["To"]      = EMAIL_TO
        msg.attach(MIMEText(html, "html"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(EMAIL_FROM, EMAIL_PASS)
            server.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())

        print(f"  ✅ Email enviado a {EMAIL_TO}")
    except Exception as e:
        print(f"  ❌ Error enviando email: {e}")


# ╔══════════════════════════════════════════════════════════════╗
# ║  MAIN                                                        ║
# ╚══════════════════════════════════════════════════════════════╝
def main():
    today = datetime.now().strftime("%Y-%m-%d")
    print(f"\n{'='*60}")
    print(f"TFM AGENTE DIARIO — {today}")
    print(f"{'='*60}")

    # 1. Detectar setups
    setups, precios_hoy, spy_chg, vix_val, regimen = detectar_setups(today)

    # 2. Google Sheets
    if not GCP_CREDS:
        print("\n⚠️  GOOGLE_SERVICE_ACCOUNT_JSON no configurado — saltando Sheets")
    else:
        print("\n📊 Conectando a Google Sheets...")
        try:
            ws = conectar_sheets()
            df = leer_sheet(ws)
            print(f"  ✅ Sheet conectado — {len(df)} filas existentes")

            print("\n🔄 Rellenando retornos forward pendientes...")
            rellenar_forward_returns(ws, df, precios_hoy, today)

            print("\n➕ Añadiendo setups de hoy...")
            append_setups(ws, setups)

        except Exception as e:
            print(f"  ❌ Error en Sheets: {e}")

    # 3. Email
    print("\n📧 Enviando email resumen...")
    enviar_email(setups, today, spy_chg, vix_val, regimen)

    print(f"\n{'='*60}")
    print(f"✅ COMPLETADO — {len(setups)} setups detectados")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
