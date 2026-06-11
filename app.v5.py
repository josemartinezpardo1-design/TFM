"""
TFM — PLATAFORMA DE INVERSIÓN INTELIGENTE v4
Master IA Sector Financiero — VIU 2025/26

Novedades v4:
  · FMP (Financial Modeling Prep) como 3ª fuente de datos — mejora cobertura no-US
  · Renta Fija vía ETF proxy — curva tipos, spreads, duración
  · Diversificación sectorial en Cartera con 3 perfiles: Agresivo / Neutro / Balanceado
"""

import os
import json
import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import requests, time
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")

st.set_page_config(page_title="TFM — Investment Intelligence", page_icon="📊", layout="wide")


# ╔═══════════════════════════════════════════════════════════════╗
# ║  CLIENTES API                                                 ║
# ╚═══════════════════════════════════════════════════════════════╝
fh_client = None
try:
    import finnhub
    _k = st.secrets.get("FINNHUB_KEY", "")
    if _k:
        fh_client = finnhub.Client(api_key=_k)
except Exception:
    pass

fred_client = None
try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False

try:
    from fredapi import Fred
    _k = st.secrets.get("FRED_KEY", "")
    if _k:
        fred_client = Fred(api_key=_k)
except Exception:
    pass

FMP_KEY = ""
try:
    FMP_KEY = st.secrets.get("FMP_KEY", "")
except Exception:
    pass

FMP_BASE = "https://financialmodelingprep.com/api/v3"


# ╔═══════════════════════════════════════════════════════════════╗
# ║  HELPER FMP                                                   ║
# ╚═══════════════════════════════════════════════════════════════╝

# ╔═══════════════════════════════════════════════════════════════╗
# ║  HELPERS GLOBALES — colores, formato                          ║
# ╚═══════════════════════════════════════════════════════════════╝
def _color_pct(v):
    """Colorea valores de porcentaje: verde si positivo, rojo si negativo."""
    if pd.isna(v):
        return ""
    return "color: #50fa7b" if v > 0 else "color: #ff5555"


def _color_score(v):
    """Colorea scores 0-100: verde fuerte si >=80, amarillo >=60."""
    if pd.isna(v):
        return ""
    if v >= 80:
        return "color: #50fa7b; font-weight: bold"
    if v >= 60:
        return "color: #f1fa8c"
    return ""


@st.cache_data(ttl=3600)
def fmp_get(endpoint: str):
    """GET a FMP endpoint; returns parsed JSON or None."""
    if not FMP_KEY:
        return None
    try:
        r = requests.get(f"{FMP_BASE}/{endpoint}", params={"apikey": FMP_KEY}, timeout=8)
        if r.status_code == 200:
            data = r.json()
            return data if data else None
    except Exception:
        pass
    return None


# ╔═══════════════════════════════════════════════════════════════╗
# ║  DESCARGA DE DATOS — Cascade: Finnhub → yfinance → FMP       ║
# ╚═══════════════════════════════════════════════════════════════╝
def descargar(ticker: str, period: str = "1y"):
    """
    Retorna (hist: DataFrame, info: dict).
    Estrategia de fuentes:
      Precios  → Finnhub (primario) → yfinance (fallback)
      Info     → Finnhub → yfinance → FMP (cubre gaps, especialmente no-US)
    info["_source_prices"] / "_source_profile" / "_source_metrics" registran la fuente usada.
    """
    days = {"6mo": 180, "1y": 365, "2y": 730, "5y": 1825}.get(period, 365)
    hist = pd.DataFrame()
    info: dict = {}

    # ── 1. Precios: Finnhub ──────────────────────────────────────
    if fh_client:
        try:
            now   = int(datetime.now().timestamp())
            start = int((datetime.now() - timedelta(days=days)).timestamp())
            r = fh_client.stock_candles(ticker, "D", start, now)
            if r and r.get("s") == "ok" and r.get("c") and len(r["c"]) > 10:
                hist = pd.DataFrame({
                    "Open": r["o"], "High": r["h"], "Low": r["l"],
                    "Close": r["c"], "Volume": r["v"]
                }, index=pd.to_datetime(r["t"], unit="s"))
                hist.index.name = "Date"
                info["_source_prices"] = "Finnhub"
        except Exception:
            pass

    # ── 2. Precios: yfinance (fallback) ─────────────────────────
    if hist.empty:
        for _ in range(2):
            try:
                t = yf.Ticker(ticker)
                h = t.history(period=period)
                if not h.empty and len(h) > 10:
                    hist = h
                    info["_source_prices"] = "Yahoo Finance"
                    break
            except Exception:
                pass
            time.sleep(1)

    # ── 2b. Precios: FMP (segundo fallback) ──────────────────────
    if hist.empty and FMP_KEY:
        try:
            url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}?timeseries={days}&apikey={FMP_KEY}"
            r = requests.get(url, timeout=15)
            if r.status_code == 200:
                data = r.json()
                hist_data = data.get("historical", []) if isinstance(data, dict) else []
                if hist_data:
                    df = pd.DataFrame(hist_data)
                    df["date"] = pd.to_datetime(df["date"])
                    df = df.sort_values("date").set_index("date")
                    hist = df.rename(columns={
                        "open": "Open", "high": "High", "low": "Low",
                        "close": "Close", "volume": "Volume"
                    })[["Open","High","Low","Close","Volume"]]
                    hist.index.name = "Date"
                    info["_source_prices"] = "FMP"
        except Exception:
            pass

    # ── 3. Info: Finnhub ─────────────────────────────────────────
    if fh_client:
        try:
            p = fh_client.company_profile2(symbol=ticker)
            if p:
                info["longName"]   = p.get("name", ticker)
                info["sector"]     = p.get("finnhubIndustry", "N/A")
                info["industry"]   = p.get("finnhubIndustry", "N/A")
                info["marketCap"]  = (p.get("marketCapitalization") or 0) * 1e6
                info["currency"]   = p.get("currency", "USD")
                info["_source_profile"] = "Finnhub"
        except Exception:
            pass
        try:
            m = fh_client.company_basic_financials(ticker, "all").get("metric", {})
            if m:
                info["trailingPE"]                    = m.get("peBasicExclExtraTTM")
                info["forwardPE"]                     = m.get("peTTM")
                info["priceToBook"]                   = m.get("pbAnnual")
                info["priceToSalesTrailing12Months"]  = m.get("psTTM")
                def _pct(v): return v / 100 if v else None
                info["returnOnEquity"]  = _pct(m.get("roeTTM"))
                info["returnOnAssets"]  = _pct(m.get("roaTTM"))
                info["profitMargins"]   = _pct(m.get("netProfitMarginTTM"))
                info["grossMargins"]    = _pct(m.get("grossMarginTTM"))
                info["dividendYield"]   = _pct(m.get("dividendYieldIndicatedAnnual"))
                info["beta"]            = m.get("beta")
                info["debtToEquity"]    = m.get("totalDebt/totalEquityAnnual")
                info["currentRatio"]    = m.get("currentRatioAnnual")
                info["_source_metrics"] = "Finnhub"
        except Exception:
            pass
        try:
            pt = fh_client.price_target(ticker)
            if pt:
                info["targetMeanPrice"] = pt.get("targetMean")
                info["targetHighPrice"] = pt.get("targetHigh")
                info["targetLowPrice"]  = pt.get("targetLow")
                info["_source_target"]  = "Finnhub"
        except Exception:
            pass
        try:
            recs = fh_client.recommendation_trends(ticker)
            if recs and len(recs) > 0:
                r0 = recs[0]
                b = r0.get("strongBuy", 0) + r0.get("buy", 0)
                s = r0.get("strongSell", 0) + r0.get("sell", 0)
                info["recommendationKey"] = "BUY" if b > s else ("SELL" if s > b else "HOLD")
        except Exception:
            pass

    # ── 4. Info: yfinance (base + complementa Finnhub) ──────────
    try:
        yf_info = yf.Ticker(ticker).info or {}
        # yfinance es la base; Finnhub sobreescribe campos que tenga
        merged = {**yf_info, **{k: v for k, v in info.items()
                                if v is not None and v != 0 and v != "N/A"}}
        info = merged
    except Exception:
        pass

    # ── 5. Info: FMP (cubre gaps, especialmente acciones no-US) ──
    if FMP_KEY:
        # Perfil empresa
        fmp_profile = fmp_get(f"profile/{ticker}")
        if fmp_profile and isinstance(fmp_profile, list) and len(fmp_profile) > 0:
            p = fmp_profile[0]
            if not info.get("sector") or str(info.get("sector")) in ("N/A", "", "None"):
                info["sector"]   = p.get("sector") or "N/A"
            if not info.get("industry") or str(info.get("industry")) in ("N/A", "", "None"):
                info["industry"] = p.get("industry") or "N/A"
            if not info.get("longName"):
                info["longName"] = p.get("companyName", ticker)
            if not info.get("marketCap") or info.get("marketCap") == 0:
                info["marketCap"] = p.get("mktCap", 0)
            if not info.get("currency"):
                info["currency"] = p.get("currency", "USD")
            info["_fmp_country"]   = p.get("country", "")
            info["_fmp_exchange"]  = p.get("exchangeShortName", "")
            if not info.get("_source_profile"):
                info["_source_profile"] = "FMP"

        # Ratios TTM
        fmp_ratios = fmp_get(f"ratios-ttm/{ticker}")
        if fmp_ratios and isinstance(fmp_ratios, list) and len(fmp_ratios) > 0:
            r = fmp_ratios[0]
            def _fill(key, fmp_key):
                if not info.get(key) or info.get(key) == 0:
                    v = r.get(fmp_key)
                    if v is not None and v != 0:
                        info[key] = v
            _fill("trailingPE",    "peRatioTTM")
            _fill("priceToBook",   "priceToBookRatioTTM")
            _fill("returnOnEquity","returnOnEquityTTM")
            _fill("profitMargins", "netProfitMarginTTM")
            _fill("debtToEquity",  "debtEquityRatioTTM")
            _fill("currentRatio",  "currentRatioTTM")
            if not info.get("dividendYield") or info.get("dividendYield") == 0:
                dy = r.get("dividendYieldTTM") or r.get("dividendYieldPercentageTTM", 0) or 0
                if dy > 0:
                    # FMP puede retornar 0.02 (decimal) o 2.0 (porcentaje)
                    info["dividendYield"] = dy if dy < 1 else dy / 100
            if not info.get("_source_metrics"):
                info["_source_metrics"] = "FMP"

        # Price target si no existe
        if not info.get("targetMeanPrice"):
            fmp_pt = fmp_get(f"price-target-consensus/{ticker}")
            if fmp_pt and isinstance(fmp_pt, dict):
                info["targetMeanPrice"] = fmp_pt.get("targetConsensus")
                if not info.get("_source_target"):
                    info["_source_target"] = "FMP"

    return hist, info


def descargar_financials(ticker: str):
    """Estados financieros para análisis fundamental (yfinance)."""
    try:
        t = yf.Ticker(ticker)
        return t.financials, t.balance_sheet, t.cashflow
    except Exception:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


# ╔═══════════════════════════════════════════════════════════════╗
# ║  INDICADORES TÉCNICOS                                        ║
# ╚═══════════════════════════════════════════════════════════════╝
def calc_rsi(s, p=14):
    d = s.diff()
    g = d.where(d > 0, 0).rolling(p).mean()
    l = (-d.where(d < 0, 0)).rolling(p).mean()
    return 100 - (100 / (1 + g / l))

def calc_macd(s, f=12, sl=26, sg=9):
    ef = s.ewm(span=f, adjust=False).mean()
    es = s.ewm(span=sl, adjust=False).mean()
    m = ef - es
    si = m.ewm(span=sg, adjust=False).mean()
    return m, si, m - si

def calc_adx(df, p=14):
    h, l, c = df["High"], df["Low"], df["Close"]
    tr  = pd.concat([h - l, (h - c.shift(1)).abs(), (l - c.shift(1)).abs()], axis=1).max(axis=1)
    up  = h - h.shift(1)
    dn  = l.shift(1) - l
    pdm = pd.Series(np.where((up > dn) & (up > 0), up, 0), index=df.index)
    mdm = pd.Series(np.where((dn > up) & (dn > 0), dn, 0), index=df.index)
    atr = tr.rolling(p).mean()
    pdi = 100 * (pdm.rolling(p).mean() / atr)
    mdi = 100 * (mdm.rolling(p).mean() / atr)
    dx  = 100 * (pdi - mdi).abs() / (pdi + mdi).replace(0, np.nan)
    return dx.rolling(p).mean(), pdi, mdi

def calc_bb(s, p=20, std=2):
    m  = s.rolling(p).mean()
    sg = s.rolling(p).std()
    u  = m + std * sg
    l  = m - std * sg
    pb = (s - l) / (u - l).replace(0, np.nan)
    return u, m, l, pb

def calc_atr(df, p=14):
    tr = pd.concat([df["High"] - df["Low"],
                    (df["High"] - df["Close"].shift(1)).abs(),
                    (df["Low"]  - df["Close"].shift(1)).abs()], axis=1).max(axis=1)
    a  = tr.rolling(p).mean()
    return a, a / df["Close"] * 100


# ╔═══════════════════════════════════════════════════════════════╗
# ║  SCORING TÉCNICO 0–10                                        ║
# ╚═══════════════════════════════════════════════════════════════╝
def score_tecnico(row, obv_t, obv_d):
    sc = 0.0
    det = {}

    rsi = row["RSI"]
    if   rsi < 30: p, m = 2.5, f"Sobrevendido ({rsi:.0f})"
    elif rsi < 45: p, m = 2.2, f"Zona acumulación ({rsi:.0f})"
    elif rsi < 55: p, m = 2.0, f"Neutral ({rsi:.0f})"
    elif rsi < 65: p, m = 1.5, f"Momentum+ ({rsi:.0f})"
    elif rsi < 75: p, m = 0.5, f"Zona caliente ({rsi:.0f})"
    else:          p, m = 0.0, f"Sobrecomprado ({rsi:.0f})"
    sc += p; det["RSI"] = {"pts": p, "max": 2.5, "val": f"{rsi:.1f}", "msg": m}

    mv, sv, hv = row["MACD"], row["Signal"], row["MACD_Hist"]
    ph  = row.get("MACD_Hist_prev", 0) or 0
    al  = mv > sv
    ac  = hv > ph
    if   al and ac:      p, m = 2.0, "Cruce alcista acelerando"
    elif al:             p, m = 1.5, "Por encima de señal"
    elif not al and ac:  p, m = 0.7, "Bajista perdiendo fuerza"
    else:                p, m = 0.0, "Bajista acelerando"
    sc += p; det["MACD"] = {"pts": p, "max": 2.0, "val": f"{mv:.4f}", "msg": m}

    av, dp, dm = row["ADX"], row["DI_Plus"], row["DI_Minus"]
    if   av > 30 and dp > dm: p, m = 2.0, f"Alcista FUERTE (ADX={av:.0f})"
    elif av > 20 and dp > dm: p, m = 1.5, f"Alcista moderada (ADX={av:.0f})"
    elif av > 20 and dp < dm: p, m = 0.3, f"Bajista activa (ADX={av:.0f})"
    elif av < 20:             p, m = 1.0, f"Lateralización (ADX={av:.0f})"
    else:                     p, m = 0.7, f"Débil (ADX={av:.0f})"
    sc += p; det["ADX"] = {"pts": p, "max": 2.0, "val": f"{av:.1f}", "msg": m}

    if   obv_t == 1  and obv_d == "Alcista": p, m = 1.5, "Compradora + div alcista"
    elif obv_t == 1:                         p, m = 1.2, "Compradora (OBV>SMA20)"
    elif obv_t == -1 and obv_d == "Bajista": p, m = 0.0, "Vendedora + div bajista"
    else:                                    p, m = 0.3, "Vendedora"
    sc += p; det["OBV"] = {"pts": p, "max": 1.5, "val": "Alcista" if obv_t == 1 else "Bajista", "msg": m}

    kv = row["Stoch_K"]
    if   kv < 20 and kv > row["Stoch_D"]: p, m = 1.0, f"Sobrevendido+cruce (%K={kv:.0f})"
    elif kv < 25:                          p, m = 0.8, f"Sobrevendida (%K={kv:.0f})"
    elif kv > 80:                          p, m = 0.0, f"Sobrecomprada (%K={kv:.0f})"
    elif kv > row["Stoch_D"]:             p, m = 0.6, f"Momentum+ (%K={kv:.0f}>%D)"
    else:                                  p, m = 0.2, f"Momentum- (%K={kv:.0f}<%D)"
    sc += p; det["Stochastic"] = {"pts": p, "max": 1.0, "val": f"{kv:.1f}", "msg": m}

    bb = row["BB_PctB"]
    if   bb < 0:    p, m = 1.0, f"Debajo banda inf (%B={bb:.2f})"
    elif bb < 0.35: p, m = 1.0, f"Zona inferior (%B={bb:.2f})"
    elif bb < 0.65: p, m = 0.5, f"Zona media (%B={bb:.2f})"
    elif bb < 1.0:  p, m = 0.1, f"Zona superior (%B={bb:.2f})"
    else:           p, m = 0.0, f"Encima banda sup (%B={bb:.2f})"
    sc += p; det["Bollinger%B"] = {"pts": p, "max": 1.0, "val": f"{bb:.2f}", "msg": m}

    return round(min(sc, 10.0), 1), det

def interpretar(sc):
    if sc >= 8.0: return "🟢 COMPRAR / ACUMULAR", "#50fa7b"
    if sc >= 6.0: return "🔵 MANTENER / VIGILAR", "#8be9fd"
    if sc >= 4.0: return "🟡 NEUTRO / ESPERAR",   "#f1fa8c"
    return          "🔴 REDUCIR / VENDER",         "#ff5555"


# ╔═══════════════════════════════════════════════════════════════╗
# ║  NIVELES OPERATIVOS                                          ║
# ╚═══════════════════════════════════════════════════════════════╝
def niveles_op(hist, info):
    last  = hist.iloc[-1]
    precio = last["Close"]
    tr     = pd.concat([hist["High"] - hist["Low"],
                        (hist["High"] - hist["Close"].shift(1)).abs(),
                        (hist["Low"]  - hist["Close"].shift(1)).abs()], axis=1).max(axis=1)
    atr_val  = tr.rolling(14).mean().iloc[-1]
    bb_low   = last.get("BB_Low", np.nan)
    sma50    = last.get("SMA_50", np.nan)
    soportes = [v for v in [bb_low, sma50] if pd.notna(v) and v < precio]
    entrada_opt = round(max(soportes), 2) if soportes else round(precio * 0.97, 2)
    sl_atr  = precio - 1.5 * atr_val
    sop_20d = hist["Low"].iloc[-20:].min()
    if sop_20d > sl_atr and sop_20d < precio:
        sl = round(sop_20d, 2); sl_nota = f"Soporte 20d ({sop_20d:.2f})"
    else:
        sl = round(sl_atr, 2); sl_nota = f"ATR×1.5 ({sl_atr:.2f})"
    riesgo = precio - sl
    riesgo_pct = round(riesgo / precio * 100, 2)
    tp1 = round(precio + 2 * riesgo, 2)
    tp2 = round(precio + 3 * riesgo, 2)
    tm  = info.get("targetMeanPrice")
    tp3 = round(tm, 2) if tm and tm > precio else None
    return {"precio": round(precio, 2), "entrada_agresiva": round(precio, 2),
            "entrada_optima": entrada_opt, "stop_loss": sl, "sl_nota": sl_nota,
            "riesgo": round(riesgo, 2), "riesgo_pct": riesgo_pct,
            "atr": round(atr_val, 2), "tp1": tp1, "tp2": tp2, "tp3": tp3,
            "soporte_20d": round(sop_20d, 2)}


# ╔═══════════════════════════════════════════════════════════════╗
# ║  FUNDAMENTALES                                                ║
# ╚═══════════════════════════════════════════════════════════════╝
def _sf(df, key, col=0, default=0):
    try:
        v = df.loc[key].iloc[col] if key in df.index else default
        return v if not pd.isna(v) else default
    except Exception:
        return default

def calc_piotroski(fin, bs, cf):
    """
    Piotroski F-Score (0-9). Devuelve (score, detalles).
    Devuelve (None, {...}) si no hay 2 años de datos para comparaciones YoY.
    """
    sc = 0; det = {}

    # VALIDACIÓN CRÍTICA: necesitamos al menos 2 columnas (años) en los estados
    n_cols_fin = fin.shape[1] if (fin is not None and not fin.empty) else 0
    n_cols_bs  = bs.shape[1]  if (bs  is not None and not bs.empty)  else 0
    if n_cols_fin < 2 or n_cols_bs < 2:
        det["_error"] = {"ok": False,
                         "val": "Datos insuficientes: se necesitan 2 años de estados financieros"}
        return None, det

    try:
        # Helper interno: devuelve None (no 0) si el dato falta de verdad
        def _val(df, key, col):
            try:
                if key in df.index:
                    v = df.loc[key].iloc[col]
                    return float(v) if not pd.isna(v) else None
            except Exception:
                pass
            return None

        ta0  = _val(bs, "Total Assets", 0)
        ta1  = _val(bs, "Total Assets", 1)
        ni0  = _val(fin, "Net Income", 0)
        ni1  = _val(fin, "Net Income", 1)
        cfo  = _val(cf, "Operating Cash Flow", 0) if (cf is not None and not cf.empty) else None
        ca0  = _val(bs, "Current Assets", 0)
        cl0  = _val(bs, "Current Liabilities", 0)
        ca1  = _val(bs, "Current Assets", 1)
        cl1  = _val(bs, "Current Liabilities", 1)
        ltd0 = _val(bs, "Long Term Debt", 0)
        ltd1 = _val(bs, "Long Term Debt", 1)
        rev0 = _val(fin, "Total Revenue", 0)
        rev1 = _val(fin, "Total Revenue", 1)
        gp0  = _val(fin, "Gross Profit", 0)
        gp1  = _val(fin, "Gross Profit", 1)
        # Acciones en circulación (para dilución real)
        sh0  = _val(bs, "Share Issued", 0) or _val(bs, "Common Stock", 0) or \
               _val(bs, "Ordinary Shares Number", 0)
        sh1  = _val(bs, "Share Issued", 1) or _val(bs, "Common Stock", 1) or \
               _val(bs, "Ordinary Shares Number", 1)

        # Cada test devuelve (nombre, resultado_o_None, valor_str)
        # Si algún dato necesario falta, el test devuelve None y NO suma punto
        def safe_div(a, b):
            if a is None or b is None or b == 0:
                return None
            return a / b

        roa0 = safe_div(ni0, ta0)
        roa1 = safe_div(ni1, ta1)
        liq0 = safe_div(ca0, cl0)
        liq1 = safe_div(ca1, cl1)
        lev0 = safe_div(ltd0, ta0)
        lev1 = safe_div(ltd1, ta1)
        gm0  = safe_div(gp0, rev0)
        gm1  = safe_div(gp1, rev1)
        rot0 = safe_div(rev0, ta0)
        rot1 = safe_div(rev1, ta1)

        tests = [
            ("F1 ROA positivo",  (roa0 > 0) if roa0 is not None else None,
                f"{roa0*100:.2f}%" if roa0 is not None else "sin datos"),
            ("F2 CFO positivo",  (cfo > 0) if cfo is not None else None,
                f"${cfo/1e6:.0f}M" if cfo is not None else "sin datos"),
            ("F3 ROA mejora",    (roa0 > roa1) if (roa0 is not None and roa1 is not None) else None,
                f"{roa0*100:.2f}% vs {roa1*100:.2f}%" if (roa0 is not None and roa1 is not None) else "sin datos"),
            ("F4 CFO > NI",      (cfo > ni0) if (cfo is not None and ni0 is not None) else None,
                f"CFO {cfo/1e6:.0f}M > NI {ni0/1e6:.0f}M" if (cfo is not None and ni0 is not None) else "sin datos"),
            ("F5 Menor deuda",   (lev0 < lev1) if (lev0 is not None and lev1 is not None) else None,
                f"{lev0:.3f} vs {lev1:.3f}" if (lev0 is not None and lev1 is not None) else "sin datos"),
            ("F6 Mejor liquidez",(liq0 > liq1) if (liq0 is not None and liq1 is not None) else None,
                f"{liq0:.2f} vs {liq1:.2f}" if (liq0 is not None and liq1 is not None) else "sin datos"),
            ("F7 Sin dilución",  (sh0 <= sh1 * 1.02) if (sh0 is not None and sh1 is not None) else None,
                f"{sh0/1e6:.0f}M vs {sh1/1e6:.0f}M acc." if (sh0 is not None and sh1 is not None) else "sin datos"),
            ("F8 Margen bruto+", (gm0 > gm1) if (gm0 is not None and gm1 is not None) else None,
                f"{gm0*100:.1f}% vs {gm1*100:.1f}%" if (gm0 is not None and gm1 is not None) else "sin datos"),
            ("F9 Rot activos+",  (rot0 > rot1) if (rot0 is not None and rot1 is not None) else None,
                f"{rot0:.3f} vs {rot1:.3f}" if (rot0 is not None and rot1 is not None) else "sin datos"),
        ]

        n_evaluables = 0
        for n, c, v in tests:
            if c is None:
                det[n] = {"ok": None, "val": v}  # no evaluable
            else:
                pt = 1 if c else 0
                sc += pt
                n_evaluables += 1
                det[n] = {"ok": bool(c), "val": v}

        det["_meta"] = {"ok": True, "val": f"{n_evaluables}/9 criterios evaluables"}
    except Exception as e:
        det["_error"] = {"ok": False, "val": str(e)}
        return None, det
    return sc, det

def calc_altman(info, fin, bs):
    """
    Altman Z-Score. Devuelve (z, zona).
    Devuelve (None, motivo) si faltan componentes clave o si es banco/financiera
    (el modelo original de 1968 no aplica a entidades financieras).
    """
    try:
        # El Altman Z original NO aplica a bancos/seguros/financieras
        sector = info.get("sector", "")
        if sector in ("Financial Services", "Financials"):
            return None, "No aplica a financieras"

        def _val(df, key, col=0):
            try:
                if key in df.index:
                    v = df.loc[key].iloc[col]
                    return float(v) if not pd.isna(v) else None
            except Exception:
                pass
            return None

        ta   = _val(bs, "Total Assets")
        ca   = _val(bs, "Current Assets")
        cl   = _val(bs, "Current Liabilities")
        re   = _val(bs, "Retained Earnings")
        ebit = _val(fin, "EBIT") or _val(fin, "Operating Income")
        tl   = _val(bs, "Total Liabilities Net Minority Interest") or _val(bs, "Total Debt")
        rev  = _val(fin, "Total Revenue")
        mc   = info.get("marketCap")

        # Validar que tenemos los componentes ESENCIALES (no rellenar con 0)
        faltantes = []
        if ta is None or ta == 0: faltantes.append("Total Assets")
        if re is None:            faltantes.append("Retained Earnings")
        if ebit is None:          faltantes.append("EBIT")
        if tl is None or tl == 0: faltantes.append("Total Liabilities")
        if mc is None or mc == 0: faltantes.append("Market Cap")
        if rev is None:           faltantes.append("Revenue")

        if faltantes:
            return None, f"Faltan: {', '.join(faltantes[:2])}"

        # Working capital puede ser None si falta ca o cl → usar 0 solo en ese término
        wc = (ca - cl) if (ca is not None and cl is not None) else 0

        z = (1.2 * (wc / ta) + 1.4 * (re / ta) + 3.3 * (ebit / ta)
             + 0.6 * (mc / tl) + 1.0 * (rev / ta))
        zona = "🟢 SEGURA" if z > 2.99 else ("🟡 GRIS" if z > 1.81 else "🔴 PELIGRO")
        return round(z, 2), zona
    except Exception:
        return None, "Error de cálculo"

def calc_graham(info):
    """
    Número de Graham = sqrt(22.5 * EPS * BookValue).
    Prioriza EPS trailing (real) sobre forward (estimado).
    Devuelve None si falta EPS o BookValue, o si son negativos.
    """
    try:
        eps = info.get("trailingEps")  # preferir trailing (dato real)
        if eps is None:
            eps = info.get("forwardEps")  # fallback a estimado
        bv  = info.get("bookValue")
        if eps and bv and eps > 0 and bv > 0:
            return round((22.5 * eps * bv) ** 0.5, 2)
    except Exception:
        pass
    return None

def calc_fcf_yield(info, cf):
    """
    FCF Yield = Free Cash Flow / Market Cap * 100.
    Usa freeCashflow de info si existe; si no, OCF - CapEx.
    En yfinance, Capital Expenditure ya viene NEGATIVO, así que se SUMA
    (OCF + CapEx_negativo = OCF - gasto). Usamos abs() para robustez ante
    fuentes que lo devuelvan positivo.
    """
    try:
        fcf = info.get("freeCashflow")
        if not fcf and cf is not None and not cf.empty:
            ocf = _sf(cf, "Operating Cash Flow", 0)
            capex = _sf(cf, "Capital Expenditure", 0)
            # CapEx es un gasto: restamos su valor absoluto independientemente del signo
            fcf = ocf - abs(capex)
        mc = info.get("marketCap")
        if fcf and mc and mc > 0:
            return round(fcf / mc * 100, 2)
    except Exception:
        pass
    return None

def mf(nombre, val, fmt, bueno, malo):
    if val is None: return f"**{nombre}:** N/A"
    ic = "✅" if bueno(val) else ("🔴" if malo(val) else "🟡")
    if fmt.endswith("%"):
        return f"{ic} **{nombre}:** {val:{fmt[:-1]}}%"
    return f"{ic} **{nombre}:** {val:{fmt}}"


# ╔═══════════════════════════════════════════════════════════════╗
# ║  SCREENER — TICKERS & SCORING                                ║
# ╚═══════════════════════════════════════════════════════════════╝
SP500_TICKERS = [
    "MMM","AOS","ABT","ABBV","ACN","ADBE","AMD","AES","AFL","A","APD","ABNB","AKAM","ALB",
    "ARE","ALGN","ALLE","LNT","ALL","GOOGL","GOOG","MO","AMZN","AMCR","AEE","AAL","AEP",
    "AXP","AIG","AMT","AWK","AMP","AME","AMGN","APH","ADI","ANSS","AON","APA","APO","AAPL",
    "AMAT","APTV","ACGL","ADM","ANET","AJG","AIZ","T","ATO","ADSK","ADP","AZO","AVB","AVY",
    "AXON","BKR","BALL","BAC","BAX","BDX","BRK-B","BBY","TECH","BIIB","BLK","BX","BK",
    "BA","BKNG","BSX","BMY","AVGO","BR","BRO","BF-B","BLDR","BG","BXP","CHRW","CDNS","CZR",
    "CPT","CPB","COF","CAH","KMX","CCL","CARR","CAT","CBOE","CBRE","CDW","CE","COR","CNC",
    "CNP","CF","CRL","SCHW","CHTR","CVX","CMG","CB","CHD","CI","CINF","CTAS","CSCO","C",
    "CFG","CLX","CME","CMS","KO","CTSH","COIN","CL","CMCSA","CAG","COP","ED","STZ","CEG",
    "COO","CPRT","GLW","CPAY","CTVA","CSGP","COST","CTRA","CRWD","CCI","CSX","CMI","CVS",
    "DHR","DRI","DVA","DAY","DECK","DE","DELL","DAL","DVN","DXCM","FANG","DLR","DG","DLTR",
    "D","DPZ","DASH","DOV","DOW","DHI","DTE","DUK","DD","EMN","ETN","EBAY","ECL","EIX",
    "EW","EA","ELV","EMR","ENPH","ETR","EOG","EPAM","EQT","EFX","EQIX","EQR","ERIE","ESS",
    "EL","EG","EVRG","ES","EXC","EXE","EXPE","EXPD","EXR","XOM","FFIV","FDS","FICO","FAST",
    "FRT","FDX","FIS","FITB","FSLR","FE","FI","F","FTNT","FTV","FOXA","FOX","BEN","FCX",
    "GRMN","IT","GE","GEHC","GEV","GEN","GNRC","GD","GIS","GM","GPC","GILD","GPN","GL",
    "GDDY","GS","HAL","HIG","HAS","HCA","DOC","HSIC","HSY","HES","HPE","HLT","HOLX","HD",
    "HON","HRL","HST","HWM","HPQ","HUBB","HUM","HBAN","HII","IBM","IEX","IDXX","ITW","INCY",
    "IR","PODD","INTC","ICE","IFF","IP","IPG","INTU","ISRG","IVZ","INVH","IQV","IRM","JBHT",
    "JBL","JKHY","J","JNJ","JCI","JPM","K","KVUE","KDP","KEY","KEYS","KMB","KIM","KMI",
    "KKR","KLAC","KHC","KR","LHX","LH","LRCX","LW","LVS","LDOS","LEN","LLY","LIN","LYV",
    "LKQ","LMT","L","LOW","LULU","LYB","MTB","MPC","MKTX","MAR","MMC","MLM","MAS","MA",
    "MCD","MCK","MDT","MRK","META","MET","MTD","MGM","MCHP","MU","MSFT","MAA","MRNA","MHK",
    "MOH","TAP","MDLZ","MPWR","MNST","MCO","MS","MOS","MSI","MSCI","NDAQ","NTAP","NFLX",
    "NEM","NWSA","NWS","NEE","NKE","NI","NDSN","NSC","NTRS","NOC","NCLH","NRG","NUE","NVDA",
    "NVR","NXPI","ORLY","OXY","ODFL","OMC","ON","OKE","ORCL","OTIS","PCAR","PKG","PLTR",
    "PANW","PARA","PH","PAYX","PAYC","PYPL","PNR","PEP","PFE","PCG","PM","PSX","PNW","PNC",
    "POOL","PPG","PPL","PFG","PG","PGR","PLD","PRU","PEG","PTC","PSA","PHM","PWR","QCOM",
    "DGX","RL","RJF","RTX","O","REG","REGN","RF","RSG","RMD","RVTY","ROK","ROL","ROP","ROST",
    "RCL","SPGI","CRM","SBAC","SLB","STX","SRE","NOW","SHW","SPG","SWKS","SJM","SW","SNA",
    "SOLV","SO","LUV","SWK","SBUX","STT","STLD","STE","SYK","SMCI","SYF","SNPS","SYY","TMUS",
    "TROW","TTWO","TPR","TRGP","TGT","TEL","TDY","TFX","TER","TSLA","TXN","TPL","TXT","TMO",
    "TJX","TKO","TSCO","TT","TDG","TRV","TRMB","TFC","TYL","TSN","USB","UBER","UDR","ULTA",
    "UNP","UAL","UPS","URI","UNH","UHS","VLO","VTR","VLTO","VRSN","VRSK","VZ","VRTX","VTRS",
    "VICI","V","VST","VMC","WRB","GWW","WAB","WBA","WMT","DIS","WBD","WM","WAT","WEC","WFC",
    "WELL","WST","WDC","WY","WSM","WMB","WTW","WDAY","WYNN","XEL","XYL","YUM","ZBRA","ZBH","ZTS"
]

@st.cache_data(ttl=86400)
def get_sp500():
    """S&P 500 — lista hardcoded (~503 tickers, sin dependencia de Wikipedia)."""
    return SP500_TICKERS

@st.cache_data(ttl=86400)
def get_ibex():
    return ["SAN.MC","BBVA.MC","ITX.MC","IBE.MC","TEF.MC","FER.MC","AMS.MC",
            "REP.MC","CABK.MC","ACS.MC","GRF.MC","MAP.MC","ENG.MC","RED.MC",
            "IAG.MC","FDR.MC","MEL.MC","COL.MC","CLNX.MC","SAB.MC"]

@st.cache_data(ttl=86400)
def get_dax():
    """DAX 40 — los 40 valores del índice principal alemán."""
    return [
        "SAP.DE","SIE.DE","ALV.DE","DTE.DE","AIR.DE","MBG.DE","DHL.DE",
        "BAS.DE","BMW.DE","IFX.DE","BEI.DE","BAYN.DE","ADS.DE","VOW3.DE",
        "DB1.DE","RWE.DE","CON.DE","DBK.DE","MRK.DE","SHL.DE",
        "MTX.DE","HEN3.DE","HEI.DE","FRE.DE","SY1.DE","ENR.DE","P911.DE",
        "ZAL.DE","BNR.DE","CBK.DE","RHM.DE","QIA.DE","SRT3.DE","EOAN.DE",
        "VNA.DE","HNR1.DE","PAH3.DE","FME.DE","BOSS.DE","GXI.DE"
    ]

SP400_TICKERS = [
    "AAL","ACA","ACIW","ACM","ADC","AEIS","AFG","AGCO","ALE","ALGM","ALK","ALKS","ALV",
    "AM","AMG","AMH","AMKR","AN","ANF","AOS","APAM","APG","APLE","APPF","ARMK","ARW",
    "ARWR","ASB","ASGN","ASH","ATI","ATR","AVNT","AVT","AWI","AYI","AZTA","BC","BCO",
    "BCPC","BDC","BERY","BIO","BJ","BKH","BLD","BLKB","BMI","BPOP","BRBR","BRKR","BRX",
    "BWXT","BXMT","CAR","CBSH","CBT","CBU","CC","CCK","CDP","CELH","CFR","CGNX","CHDN",
    "CHE","CHH","CHX","CIEN","CIVI","CLF","CMA","CMC","CNH","CNO","CNX","COHR","COKE",
    "COLB","COLM","COOP","CPK","CR","CRC","CROX","CRS","CRUS","CSL","CW","CWAN","CWH",
    "CWST","CXT","CYH","DAR","DBRG","DCI","DEI","DINO","DKS","DLB","DNB","DNLI","DOCS",
    "DPZ","DRVN","DT","DTM","DV","DY","EAT","EEFT","EHC","ELS","ELY","EME","ENR","ENS",
    "EPC","EQH","ESAB","ESI","ESNT","ETRN","EVR","EVRG","EWBC","EXEL","EXLS","EXP","EXPO",
    "FAF","FBP","FCF","FCN","FFIN","FHB","FHI","FIVE","FIVN","FIX","FIZZ","FL","FLO",
    "FLR","FN","FNB","FND","FOUR","FR","FRPT","FSS","FTDR","FUL","FULT","FYBR","G","GATX",
    "GBCI","GEF","GFF","GGG","GHC","GLPI","GME","GMED","GMS","GNL","GNTX","GNW","GO",
    "GPI","GPK","GT","GTES","GTLS","GVA","GXO","HAE","HALO","HASI","HBI","HBNC","HCC",
    "HCSG","HE","HELE","HGV","HIW","HL","HLI","HLNE","HLT","HOG","HOMB","HP","HPP","HQY",
    "HR","HRB","HTH","HUBG","HUN","HXL","IAC","IBKR","IBOC","ICUI","IDA","IDCC","IDYA",
    "IIPR","ILPT","IMG","INCY","INDB","INGM","INGR","INSW","INT","IONS","IOSP","IPAR",
    "IRDM","IRT","ITRI","IVR","IVZ","JBL","JBT","JEF","JLL","JOE","JWN","KAI","KBH","KBR",
    "KD","KEX","KMT","KN","KNF","KNX","KRG","KRYS","KTB","KW","LAD","LAMR","LANC","LBRT",
    "LCII","LFUS","LITE","LIVN","LKQ","LNTH","LNW","LOPE","LPX","LSCC","LSTR","M","MAN",
    "MASI","MAT","MATX","MC","MCY","MDU","MEDP","MGY","MIDD","MMS","MOG.A","MOH","MORN",
    "MP","MSA","MSM","MTH","MTN","MTSI","MTX","MUR","MUSA","NAVI","NBIX","NBR","NCNO",
    "NEU","NFE","NJR","NNN","NOG","NOV","NOVT","NSA","NSP","NWE","NYT","ODFL","OFC","OGE",
    "OGN","OGS","OHI","OII","OLED","OLLI","OLN","ONB","ONTO","ORA","ORI","OSK","OUT",
    "OVV","OWL","OXM","PACW","PAG","PAYC","PB","PBF","PBH","PCG","PCH","PCTY","PEB","PEN",
    "PENN","PFGC","PII","PINC","PIPR","PLNT","PNFP","PNM","PNW","POR","POST","POWI","PPC",
    "PR","PRDO","PRGO","PRI","PRMW","PRSP","PRVA","PSN","PVH","QLYS","R","RBA","RBC","RDN",
    "RDNT","REVG","REZI","RGA","RGEN","RGLD","RH","RIG","RIVN","RKT","RLI","RMBS","RNR",
    "ROAD","ROIV","ROL","RPM","RRC","RRX","RXO","RYAN","RYN","SAIA","SAIC","SAM","SBH",
    "SBNY","SCI","SCSC","SEE","SEIC","SF","SFM","SFNC","SGRY","SIGI","SITC","SITE","SJM",
    "SKT","SKX","SKY","SKYW","SLAB","SLG","SLGN","SM","SMG","SMP","SNA","SNDR","SNV",
    "SNX","SON","SPB","SPSC","SR","SRCL","SSB","SSD","ST","STAG","STC","STE","STER","STL",
    "STRA","STWD","SUI","SWX","SXT","TCBI","TCN","TDC","TDS","TEX","TFII","TGNA","THC",
    "THG","THO","THS","TKR","TMHC","TNDM","TNL","TPL","TPR","TPX","TR","TRMB","TRNO",
    "TRTX","TWO","TXNM","TXRH","UAA","UCBI","UE","UFPI","UGI","UHS","UNF","UNFI","UNM",
    "UNVR","URBN","USFD","USNA","UTL","UTZ","UVV","VAC","VC","VFC","VLY","VNT","VOYA",
    "VSCO","VSH","VSTS","VVI","VVV","WAB","WAFD","WBS","WCC","WD","WEN","WERN","WEX",
    "WFRD","WH","WHR","WLY","WMS","WOLF","WOR","WPC","WSC","WSM","WSO","WST","WTRG",
    "WTS","WTTR","WWD","WWE","X","XHR","XPO","XRAY","Y","YELP","ZD","ZIP"
]

@st.cache_data(ttl=86400)
def get_sp400():
    """S&P 400 MidCap — lista hardcoded ~300 tickers (sin dependencia de Wikipedia)."""
    return SP400_TICKERS

SP600_TICKERS = [
    "AAOI","AAP","ABCB","ABG","ABM","ACA","ACEL","ACIW","ACLS","ACMR","ACVA","ADUS","AEIS",
    "AESI","AGM","AGO","AGYS","AHCO","AHH","AIN","AIR","AKR","AL","ALEX","ALG","ALGT","ALKS",
    "ALRM","ALX","AMBA","AMC","AMN","AMR","AMRC","AMSF","AMWD","AMWL","ANDE","ANET","ANIK",
    "ANIP","AORT","AOS","AOSL","APAM","APLE","APOG","APPN","ARCB","ARCH","ARCT","ARI","ARIS",
    "ARLO","ARLP","AROC","ARQT","ARR","ARTNA","ARVN","ASIX","ASO","ASTE","ASTH","ATEN","ATGE",
    "ATI","ATKR","ATNI","ATSG","AUB","AVA","AVAV","AVNS","AVNW","AWR","AX","AXL","AZZ","B",
    "BANC","BANF","BANR","BBSI","BCC","BCRX","BDC","BFH","BFS","BGS","BHE","BHF","BHLB","BIG",
    "BIPC","BJRI","BKE","BKU","BL","BLBD","BLMN","BLX","BMRC","BNL","BOH","BOOT","BOX","BPMC",
    "BRC","BRY","BTU","BV","BVH","BXC","BXMT","BY","BYD","CABO","CAKE","CAL","CALX","CARG",
    "CARS","CASH","CATO","CATY","CBL","CBRL","CBT","CBU","CBZ","CCO","CCOI","CCRN","CCS",
    "CDE","CDP","CECO","CENT","CENTA","CENX","CERS","CEVA","CFFN","CHCO","CHCT","CHEF","CHGG",
    "CIVB","CKH","CLB","CLDX","CLW","CNDT","CNK","CNMD","CNO","CNS","CNX","COCO","COHU",
    "COKE","COLB","COLL","COLM","COMM","COOP","CORT","CPF","CPG","CPK","CPRX","CPS","CRC",
    "CRGY","CRI","CRK","CRMT","CRSR","CRUS","CSGS","CSV","CTBI","CTRE","CTS","CUBI","CURO",
    "CVBF","CVCO","CVI","CWAN","CWEN","CWH","CWK","CWST","CWT","CXT","CXW","CYH","DAN",
    "DCO","DDD","DDS","DEA","DEI","DENN","DFH","DFIN","DGII","DHC","DIN","DIOD","DJCO","DK",
    "DLX","DNUT","DOCN","DOLE","DORM","DRH","DRQ","DSGX","DV","DXC","DXPE","DY","DZSI",
    "EAT","EBC","EBS","ECPG","EE","EEX","EFC","EGBN","EGY","EIG","ELME","ENR","ENS","ENV",
    "ENVA","EOLS","EPAC","EPC","EPM","EPRT","EQC","ERII","ESE","ETD","EVH","EVTC","EXLS",
    "EXPI","EXPO","EXTR","FBK","FBNC","FBP","FBRT","FCF","FCFS","FCN","FCPT","FELE","FF",
    "FFBC","FFIN","FFIV","FG","FHB","FHI","FIBK","FISI","FIVN","FIZZ","FL","FLGT","FLNG",
    "FLO","FLR","FLWS","FMBH","FN","FNB","FNKO","FOLD","FORM","FORR","FOXF","FRBA","FRD",
    "FRG","FRME","FRO","FSP","FSS","FTDR","FUL","FUN","FWRD","FWRG","G","GATX","GBL","GBX",
    "GCI","GCMG","GDEN","GDOT","GEF","GEO","GES","GHC","GIII","GLNG","GMS","GMRE","GNTY",
    "GNW","GO","GOGO","GOLF","GOOS","GPI","GPK","GPRE","GRBK","GSL","GTLS","GTN","GTY",
    "GVA","HAE","HAFC","HAIN","HALO","HASI","HBCP","HBI","HBT","HCC","HCKT","HCSG","HCTI",
    "HEES","HELE","HFWA","HGV","HI","HIBB","HL","HLF","HLIT","HLX","HMN","HOG","HOLX",
    "HOMB","HONE","HOPE","HOV","HP","HPK","HPP","HQY","HR","HRB","HRMY","HRTG","HSC",
    "HTBI","HTBK","HTH","HTLD","HTZ","HUBG","HUN","HURN","HVT","HWC","HWKN","HY","I",
    "ICHR","ICUI","IDCC","IDT","IDYA","IIPR","INDB","INGM","INGR","INMD","INSW","INT","INVA"
]

@st.cache_data(ttl=86400)
def get_sp600():
    """S&P 600 SmallCap — lista hardcoded ~360 tickers (sin dependencia de Wikipedia)."""
    return SP600_TICKERS

@st.cache_data(ttl=86400)
def get_nasdaq100():
    """Nasdaq 100 — 100 mayores empresas no-financieras del Nasdaq."""
    return [
        "AAPL","MSFT","NVDA","GOOGL","GOOG","AMZN","META","AVGO","TSLA","COST",
        "NFLX","ADBE","PEP","CSCO","TMUS","INTC","CMCSA","TXN","QCOM","AMD",
        "AMAT","INTU","HON","BKNG","ISRG","SBUX","ADP","LRCX","GILD","MDLZ",
        "REGN","VRTX","ADI","KLAC","PANW","SNPS","CDNS","MRVL","CRWD","ORLY",
        "CSX","ASML","ABNB","FTNT","CTAS","WDAY","ROP","CHTR","NXPI","ADSK",
        "AEP","PCAR","FANG","PYPL","ROST","MNST","KDP","CPRT","XEL","FAST",
        "ODFL","BKR","DDOG","TEAM","EA","KHC","CTSH","DXCM","EXC","VRSK",
        "CCEP","IDXX","BIIB","CSGP","ON","ZS","CDW","ANSS","MDB","ILMN",
        "TTD","MCHP","GFS","WBD","TTWO","ENPH","DLTR","WBA","GEHC","SIRI",
        "PDD","LULU","MAR","ALGN","SMCI","ARM","LIN","SBNY","DASH","MELI"
    ]

@st.cache_data(ttl=86400)
def get_cac40():
    return ["MC.PA","TTE.PA","SAN.PA","AI.PA","OR.PA","BNP.PA","STLA.PA","SU.PA",
            "AIR.PA","RI.PA","DG.PA","ORA.PA","KER.PA","CS.PA","AXA.PA","VIE.PA",
            "SGO.PA","DSY.PA","EL.PA","BN.PA","CAP.PA","LR.PA","STM.PA","ATO.PA",
            "RMS.PA","ML.PA","TEP.PA","VIV.PA","GLE.PA","CA.PA","ENGI.PA","PUB.PA",
            "SAF.PA","FR.PA","URW.PA","AF.PA","SG.PA","HO.PA","ALO.PA","TFI.PA"]

@st.cache_data(ttl=86400)
def get_eurostoxx50():
    return ["ASML.AS","LVMH.PA","TTE.PA","SAP.DE","SAN.PA","SIE.DE","AIR.PA",
            "IDEXY","ALV.DE","BNP.PA","AI.PA","OR.PA","ABI.BR","MBG.DE","ING.AS",
            "DTE.DE","STLA.PA","SU.PA","BMW.DE","AXA.PA","DG.PA","BAS.DE","AIR.DE",
            "ENEL.MI","ENI.MI","ISP.MI","UCG.MI","INGA.AS","PHG.AS","CS.PA",
            "KER.PA","RI.PA","ORA.PA","VIE.PA","SGO.PA","DSY.PA","STM.PA","VOW3.DE",
            "RWE.DE","PHIA.AS","AD.AS","MT.AS","CRH","EL.PA","ATO.PA","ADS.DE",
            "DB1.DE","IFX.DE","ALO.PA","SAF.PA"]

@st.cache_data(ttl=86400)
def get_ftse100():
    """FTSE 100 — los 100 valores principales de la Bolsa de Londres."""
    return [
        "AZN.L","SHEL.L","HSBA.L","ULVR.L","BP.L","RIO.L","GSK.L","REL.L",
        "NG.L","LSEG.L","BATS.L","DGE.L","CPG.L","RKT.L","VOD.L","LLOY.L",
        "BARC.L","NWG.L","STAN.L","ABF.L","IHG.L","WTB.L","LAND.L","SBRY.L",
        "TSCO.L","MKS.L","JD.L","EZJ.L","IAG.L","RR.L","BA.L","WEIR.L",
        "HLMA.L","SDR.L","CRDA.L","MNDI.L","SMDS.L","EXPN.L","SAGE.L","AUTO.L",
        "SPX.L","SMIN.L","IMB.L","GLEN.L","BHP.L","AAL.L","ANTO.L","CNA.L",
        "SVT.L","UU.L","SSE.L","PNN.L","NXT.L","OCDO.L","HWDN.L","MRO.L",
        "ADM.L","AHT.L","ANG.L","ARCM.L","AVST.L","AVV.L","BEZ.L","BNZL.L",
        "BRBY.L","BT-A.L","CCH.L","CTEC.L","DCC.L","DPLM.L","ENT.L","FCIT.L",
        "FRAS.L","FRES.L","HIK.L","HL.L","HSX.L","ICP.L","III.L","IMI.L",
        "INF.L","ITRK.L","KGF.L","LGEN.L","LMP.L","MNG.L","MRON.L","NMC.L",
        "PHNX.L","PRU.L","PSON.L","RTO.L","SGE.L","SGRO.L","SHB.L","SKG.L",
        "SLA.L","SN.L","STJ.L","TW.L","UTG.L","VTY.L","WG.L","WIZZ.L"
    ]

@st.cache_data(ttl=86400)
def get_spi():
    """Swiss Performance Index — ~120 valores principales del SIX Swiss Exchange."""
    return [
        "NESN.SW","ROG.SW","NOVN.SW","ALC.SW","UHR.SW","CFR.SW","ZURN.SW",
        "ABBN.SW","GIVN.SW","LOGN.SW","SIKA.SW","GEBN.SW","LONN.SW","LHN.SW",
        "UBSG.SW","BRKN.SW","PGHN.SW","BARN.SW","SLHN.SW","SREN.SW","BAER.SW",
        "COTN.SW","TEMN.SW","DKSH.SW","SOFN.SW","ARBN.SW","LISN.SW","SCMN.SW",
        "MBTN.SW","EMMN.SW","HELN.SW","KARN.SW","BCGE.SW","BCVN.SW","BKW.SW",
        "BOBN.SW","CAG.SW","CLAN.SW","COHN.SW","DLKN.SW","EMSN.SW","FHZN.SW",
        "HIAG.SW","HUBN.SW","INRN.SW","JOEL.SW","KNIN.SW","LAHN.SW","MCHN.SW",
        "MOBN.SW","NBEN.SW","OBDC.SW","PEAN.SW","BUCN.SW","VACN.SW","ADEN.SW",
        "ALSN.SW","BANB.SW","BBN.SW","BCJ.SW","BELL.SW","BION.SW","BLKB.SW",
        "BNR.SW","BSKP.SW","BURY.SW","BVZN.SW","CALN.SW","CFT.SW","COPN.SW",
        "CPHN.SW","DAE.SW","DESN.SW","EFGN.SW","FORN.SW","FREN.SW","FTON.SW",
        "GALD.SW","GAM.SW","GLKBN.SW","GMI.SW","GURN.SW","HBLN.SW","HOCN.SW",
        "IFCN.SW","IMPN.SW","KOMN.SW","KUD.SW","LEHN.SW","LEMN.SW","LIND.SW",
        "LLBN.SW","MEDX.SW","METN.SW","MTG.SW","NWRN.SW","ORON.SW","OERL.SW",
        "PEHN.SW","PMN.SW","PNRG.SW","PRE.SW","PSPN.SW","RIEN.SW","ROBN.SW",
        "RSGN.SW","SAHN.SW","SCHP.SW","SENS.SW","SFPN.SW","SFZN.SW","SGSN.SW",
        "SIGN.SW","SLOG.SW","SOON.SW","SPSN.SW","STMN.SW","SUN.SW","SUNE.SW",
        "TIBN.SW","TIT.SW","UBXN.SW","VAHN.SW","VATN.SW","VBSN.SW","VPBN.SW",
        "VZN.SW","WAR.SW","WIHN.SW","ZEHN.SW","ZUGER.SW","ZWM.SW"
    ]

@st.cache_data(ttl=86400)
def get_nikkei225():
    """Nikkei 225 — los 225 valores principales de la Bolsa de Tokio."""
    return [
        "7203.T","9984.T","6861.T","8306.T","6758.T","6501.T","7267.T","9432.T","8316.T",
        "6702.T","4063.T","9433.T","7751.T","8035.T","6954.T","4661.T","2914.T","9022.T",
        "7832.T","4519.T","6367.T","8031.T","6098.T","7011.T","5401.T","4503.T","9021.T",
        "8411.T","3382.T","2802.T","4452.T","6471.T","9531.T","5108.T","8801.T","1925.T",
        "9201.T","9101.T","7733.T","6146.T","4568.T","4901.T","8802.T","3407.T","5713.T",
        "7741.T","6645.T","4523.T","8309.T","6302.T","8053.T","7269.T","6326.T","8001.T",
        "6273.T","9020.T","6981.T","6594.T","6920.T","8766.T","7270.T","6724.T","6752.T",
        "8830.T","6586.T","9613.T","9434.T","4502.T","4543.T","6301.T","8267.T","9009.T",
        "9301.T","9303.T","9532.T","9602.T","9735.T","9766.T","9983.T","2502.T","2503.T",
        "2531.T","2768.T","2801.T","2871.T","2897.T","3086.T","3092.T","3099.T","3101.T",
        "3401.T","3402.T","3405.T","3436.T","3863.T","3865.T","4004.T","4005.T","4021.T",
        "4042.T","4043.T","4061.T","4151.T","4183.T","4188.T","4208.T","4324.T","4385.T",
        "4507.T","4516.T","4528.T","4536.T","4540.T","4544.T","4549.T","4555.T","4631.T",
        "4689.T","4704.T","4751.T","4755.T","4768.T","4901.T","4902.T","4911.T","5019.T",
        "5020.T","5101.T","5201.T","5214.T","5232.T","5233.T","5301.T","5332.T","5333.T",
        "5406.T","5411.T","5541.T","5631.T","5703.T","5706.T","5707.T","5711.T","5714.T",
        "5801.T","5802.T","5803.T","5901.T","5938.T","5942.T","5947.T","6098.T","6103.T",
        "6113.T","6178.T","6305.T","6315.T","6361.T","6366.T","6471.T","6472.T","6473.T",
        "6479.T","6504.T","6506.T","6645.T","6701.T","6724.T","6753.T","6770.T","6841.T",
        "6857.T","6902.T","6952.T","6963.T","6971.T","6976.T","7003.T","7004.T","7012.T",
        "7186.T","7202.T","7211.T","7261.T","7272.T","7731.T","7735.T","7762.T","7911.T",
        "7912.T","7951.T","7974.T","8001.T","8002.T","8015.T","8028.T","8233.T","8252.T",
        "8253.T","8270.T","8303.T","8331.T","8354.T","8355.T","8410.T","8628.T","8630.T",
        "8697.T","8725.T","8729.T","8750.T","8795.T","8804.T","9007.T","9008.T","9602.T",
        "9613.T","9706.T","9831.T","9989.T","9986.T"
    ]

@st.cache_data(ttl=86400)
def get_russell1000():
    """Russell 1000 — aproximación con S&P 500 + S&P 400 (las 900 mayores US)."""
    universe = list(dict.fromkeys(SP500_TICKERS + SP400_TICKERS))
    return universe



# ╔═══════════════════════════════════════════════════════════════╗
# ║  LEI PROXY — Reconstrucción del Leading Economic Index        ║
# ║  Conference Board LEI replicado con componentes de FRED       ║
# ╚═══════════════════════════════════════════════════════════════╝

# Mapeo sector → multiplicadores por fase del ciclo económico
SECTOR_CICLO = {
    "Technology":             {"expansion": 1.25, "desaceleracion": 0.85, "contraccion": 0.65, "recuperacion": 1.30},
    "Consumer Cyclical":      {"expansion": 1.30, "desaceleracion": 0.75, "contraccion": 0.55, "recuperacion": 1.35},
    "Financial Services":     {"expansion": 1.20, "desaceleracion": 0.85, "contraccion": 0.70, "recuperacion": 1.25},
    "Communication Services": {"expansion": 1.10, "desaceleracion": 0.90, "contraccion": 0.80, "recuperacion": 1.10},
    "Industrials":            {"expansion": 1.25, "desaceleracion": 0.80, "contraccion": 0.60, "recuperacion": 1.30},
    "Energy":                 {"expansion": 1.15, "desaceleracion": 0.90, "contraccion": 0.80, "recuperacion": 1.05},
    "Basic Materials":        {"expansion": 1.20, "desaceleracion": 0.80, "contraccion": 0.65, "recuperacion": 1.25},
    "Real Estate":            {"expansion": 1.05, "desaceleracion": 0.90, "contraccion": 0.80, "recuperacion": 1.10},
    "Consumer Defensive":     {"expansion": 0.80, "desaceleracion": 1.15, "contraccion": 1.35, "recuperacion": 0.80},
    "Healthcare":             {"expansion": 0.90, "desaceleracion": 1.10, "contraccion": 1.25, "recuperacion": 0.90},
    "Utilities":              {"expansion": 0.70, "desaceleracion": 1.20, "contraccion": 1.40, "recuperacion": 0.70},
}

FASE_NOMBRES = {
    "expansion":      ("🟢 Expansión",      "Economía creciendo · favorable a cíclicos"),
    "desaceleracion": ("🟡 Desaceleración", "Crecimiento moderándose · rotación a defensivos"),
    "contraccion":    ("🔴 Contracción",    "Economía debilitándose · defensivos preferidos"),
    "recuperacion":   ("🔵 Recuperación",   "Repunte tras mínimos · cíclicos atractivos"),
}


@st.cache_data(ttl=43200)  # 12 horas
def calcular_lei_proxy():
    """
    Reconstruye un LEI proxy usando 6 componentes de FRED disponibles gratis:
      - AWHMAN     : Horas semanales en manufactura
      - ICSA       : Initial unemployment claims (invertido: menos claims = mejor)
      - PERMIT     : Permisos de construcción de vivienda
      - SP500      : Índice S&P 500
      - T10YFF     : Spread tipos 10Y - Fed Funds (curva)
      - UMCSENT    : Confianza del consumidor Michigan

    Retorna: (df con LEI normalizado, fase actual, dict componentes)
    """
    if not fred_client:
        return None, None, None

    componentes_fred = {
        "Horas manufactura":    ("AWHMAN",  False),  # más es mejor
        "Initial Claims":        ("ICSA",    True),   # menos es mejor (invertir)
        "Permisos vivienda":     ("PERMIT",  False),
        "S&P 500":               ("SP500",   False),
        "Spread tipos 10Y-FFR":  ("T10YFF",  False),  # spread positivo = curva normal
        "Confianza consumidor":  ("UMCSENT", False),
    }

    series_norm = {}
    raw_data = {}

    try:
        for nombre, (fred_id, invertir) in componentes_fred.items():
            try:
                s = fred_client.get_series(fred_id, observation_start="2010-01-01").dropna()
                if s.empty or len(s) < 24:
                    continue

                # Resampleo a fin de mes
                s_monthly = s.resample("ME").last().dropna()

                # Normalizar como % cambio vs media 12m
                s_smooth = s_monthly.rolling(3).mean().dropna()
                if invertir:
                    s_smooth = -s_smooth  # invertir si menor es mejor

                # Z-score sobre los últimos 60 meses (5 años)
                window = min(60, len(s_smooth))
                base   = s_smooth.tail(window)
                z      = (s_smooth - base.mean()) / base.std()

                series_norm[nombre] = z
                raw_data[nombre]    = s_monthly
            except Exception:
                continue

        if len(series_norm) < 3:
            return None, None, None

        # LEI proxy = promedio de Z-scores
        df_components = pd.DataFrame(series_norm).dropna(how="all")
        lei = df_components.mean(axis=1)

        # Convertir a base 100 (índice tipo Conference Board)
        lei_base = 100 * (1 + lei * 0.1)  # ajuste de escala

        # Calcular fase actual basada en tendencia
        if len(lei) < 7:
            return None, None, None

        lei_now  = lei.iloc[-1]
        lei_3m   = lei.iloc[-4] if len(lei) >= 4 else lei.iloc[0]
        lei_6m   = lei.iloc[-7] if len(lei) >= 7 else lei.iloc[0]

        cambio_3m = lei_now - lei_3m
        cambio_6m = lei_now - lei_6m

        # Determinar fase
        if cambio_6m > 0.3 and cambio_3m > 0:
            fase = "expansion"
        elif cambio_6m > 0 and cambio_3m < cambio_6m / 2:
            fase = "desaceleracion"
        elif cambio_6m < -0.3 and cambio_3m < 0:
            fase = "contraccion"
        elif cambio_6m < 0 and cambio_3m > cambio_6m / 2:
            fase = "recuperacion"
        elif cambio_6m >= 0:
            fase = "expansion"
        else:
            fase = "desaceleracion"

        info_fase = {
            "fase":      fase,
            "lei_now":   float(lei_now),
            "lei_base":  float(lei_base.iloc[-1]),
            "cambio_3m": float(cambio_3m),
            "cambio_6m": float(cambio_6m),
            "serie":     lei_base,
            "componentes": series_norm,
        }

        return df_components, info_fase, raw_data
    except Exception:
        return None, None, None


def macro_score_for_sector(sector, info_fase):
    """
    Calcula el score macro (0-100) para un sector dado en la fase actual.
    Score 50 = neutro. >70 favorable, <30 desfavorable.
    """
    if not info_fase or sector not in SECTOR_CICLO:
        return 50, "Sin datos de sector/macro"

    fase = info_fase["fase"]
    mult = SECTOR_CICLO[sector].get(fase, 1.0)

    # Fórmula: score 50 base, ajustado por multiplicador
    # mult 1.0 = 50, mult 1.4 = 80, mult 0.7 = 30
    score = 50 + (mult - 1) * 75
    score = max(0, min(100, score))

    fase_nombre, _ = FASE_NOMBRES.get(fase, ("?", ""))
    if mult > 1.15:
        nota = f"{fase_nombre} favorable a {sector}"
    elif mult < 0.85:
        nota = f"{fase_nombre} desfavorable a {sector}"
    else:
        nota = f"{fase_nombre} — sector neutral"

    return round(score, 0), nota


# ╔═══════════════════════════════════════════════════════════════╗
# ║  WATCHLIST — Persistencia en Google Sheets                    ║
# ╚═══════════════════════════════════════════════════════════════╝
WATCHLIST_SHEET_ID = "1Yj2KkMypva14ZzpbnP9hDMexhzDGljWU6yhtnsVN980"
WATCHLIST_TAB      = "watchlist"
WATCHLIST_COLS     = ["fecha_anadido", "ticker", "precio_inicial", "nota"]


@st.cache_data(ttl=300)
def _fetch_historico(ticker, period="5d"):
    """Helper robusto: devuelve DataFrame histórico probando múltiples fuentes.
    Retorna (df, error_msg). df vacío si todo falla.
    Cacheado 5 min para evitar peticiones redundantes en el mismo turn."""
    ticker = ticker.strip().upper()
    errors = []

    # 1. yf.Ticker.history
    try:
        t = yf.Ticker(ticker)
        h = t.history(period=period, auto_adjust=True)
        if not h.empty and "Close" in h.columns and len(h) >= 1:
            return h, None
        errors.append("yf.Ticker:vacío")
    except Exception as e:
        errors.append(f"yf.Ticker:{str(e)[:40]}")

    # 2. yf.download
    try:
        h = yf.download(ticker, period=period, progress=False, auto_adjust=True)
        if not h.empty and "Close" in h.columns and len(h) >= 1:
            return h, None
        errors.append("yf.download:vacío")
    except Exception as e:
        errors.append(f"yf.download:{str(e)[:40]}")

    return pd.DataFrame(), " | ".join(errors)


def _get_precio_actual(ticker):
    """Devuelve (precio_actual, error_msg). Usa _fetch_historico internamente."""
    h, err = _fetch_historico(ticker, "5d")
    if h.empty:
        # Último fallback: Finnhub
        try:
            if fh_client is not None:
                quote = fh_client.quote(ticker)
                if quote and quote.get("c", 0) > 0:
                    return float(quote["c"]), None
        except Exception:
            pass
        return None, err
    try:
        precio = float(h["Close"].iloc[-1])
        return precio if precio > 0 else None, None
    except Exception as e:
        return None, str(e)


def _get_cambio_dia(ticker):
    """Devuelve (precio_actual, cambio_pct, error). cambio_pct = % vs ayer."""
    h, err = _fetch_historico(ticker, "5d")
    if h.empty or len(h) < 2:
        return None, None, err or "datos insuficientes"
    try:
        precio = float(h["Close"].iloc[-1])
        ayer   = float(h["Close"].iloc[-2])
        if precio > 0 and ayer > 0:
            cambio = (precio / ayer - 1) * 100
            return precio, cambio, None
        return None, None, "precio inválido"
    except Exception as e:
        return None, None, str(e)


def _get_watchlist_error():
    """Devuelve string descriptivo del error de Sheets, o None si todo OK."""
    if not GSPREAD_AVAILABLE:
        return "Librería gspread no instalada. Añade `gspread` y `google-auth` a requirements.txt"
    try:
        if "gcp_service_account" not in st.secrets:
            return "Falta secret `[gcp_service_account]` en Streamlit. Settings → Secrets."
        creds_dict = dict(st.secrets["gcp_service_account"])
        if not creds_dict.get("client_email"):
            return "Secret `gcp_service_account` mal configurado (falta client_email)."
        return None
    except Exception as e:
        return f"Error leyendo secrets: {e}"

@st.cache_resource
def get_watchlist_ws():
    """Conecta a Google Sheets y retorna el worksheet de watchlist (lo crea si no existe)."""
    err = _get_watchlist_error()
    if err:
        return None
    try:
        creds_dict = dict(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(
            creds_dict,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        client = gspread.authorize(creds)
        sh = client.open_by_key(WATCHLIST_SHEET_ID)
        try:
            ws = sh.worksheet(WATCHLIST_TAB)
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title=WATCHLIST_TAB, rows=1000, cols=10)
            ws.append_row(WATCHLIST_COLS)
        return ws
    except Exception as e:
        # Guardar error en session_state para diagnóstico
        st.session_state["_watchlist_last_error"] = str(e)
        return None

def watchlist_load():
    """Carga la watchlist desde Google Sheets como DataFrame."""
    ws = get_watchlist_ws()
    if ws is None:
        return pd.DataFrame(columns=WATCHLIST_COLS)
    try:
        data = ws.get_all_records()
        if not data:
            return pd.DataFrame(columns=WATCHLIST_COLS)
        df = pd.DataFrame(data)
        for c in WATCHLIST_COLS:
            if c not in df.columns:
                df[c] = ""
        return df[WATCHLIST_COLS]
    except Exception:
        return pd.DataFrame(columns=WATCHLIST_COLS)

def watchlist_add(ticker, precio_inicial, nota=""):
    """
    Añade un ticker a la watchlist.
    Si precio_inicial es 0 o None, intenta obtenerlo del histórico antes de guardar.
    El precio se envía como número FLOAT con value_input_option='RAW' para que Sheets
    no aplique ninguna interpretación regional.
    Retorna True si tuvo éxito, False si ya existía o falló.
    """
    ws = get_watchlist_ws()
    if ws is None:
        return False
    try:
        df = watchlist_load()
        if ticker.upper() in df["ticker"].astype(str).str.upper().values:
            return False  # ya existe

        # Validar precio
        try:
            p_check = float(precio_inicial) if precio_inicial else 0
        except Exception:
            p_check = 0

        if p_check <= 0:
            try:
                t_yf = yf.Ticker(ticker.upper())
                h_yf = t_yf.history(period="5d", auto_adjust=True)
                if not h_yf.empty:
                    p_check = float(h_yf["Close"].iloc[-1])
            except Exception:
                pass

        if p_check <= 0:
            st.session_state["_watchlist_last_error"] = (
                f"No se pudo obtener precio inicial para {ticker.upper()}. "
                f"No se añadió a watchlist."
            )
            return False

        fecha = datetime.now().strftime("%Y-%m-%d")
        # CRÍTICO: enviar como NÚMERO FLOAT con value_input_option='RAW'
        # RAW = Sheets guarda el valor TAL CUAL sin interpretar nada
        precio_redondeado = round(float(p_check), 2)

        ws.append_row(
            [fecha, ticker.upper(), precio_redondeado, str(nota)],
            value_input_option="RAW"
        )
        return True
    except Exception as e:
        st.error(f"Error guardando en Sheets: {e}")
        return False

def watchlist_remove(ticker):
    """Elimina un ticker de la watchlist."""
    ws = get_watchlist_ws()
    if ws is None:
        return False
    try:
        all_vals = ws.get_all_values()
        if len(all_vals) <= 1:
            return False
        # Buscar fila del ticker (1-indexed, +1 por header)
        for i, row in enumerate(all_vals[1:], start=2):
            if len(row) >= 2 and row[1].upper() == ticker.upper():
                ws.delete_rows(i)
                return True
        return False
    except Exception:
        return False

def watchlist_clear():
    """Vacía toda la watchlist (deja header)."""
    ws = get_watchlist_ws()
    if ws is None:
        return False
    try:
        ws.clear()
        ws.append_row(WATCHLIST_COLS)
        return True
    except Exception:
        return False

def watchlist_reparar_precios():
    """
    Recorre la watchlist y repara precios inválidos. Casos detectados:
    - precio_inicial <= 0 o vacío
    - precio_inicial > 10000 (bug de conversión Sheets)
    - precio_inicial difiere del precio actual en factor >5x (claramente erróneo)
    Descarga el precio histórico real de la fecha_anadido y lo guarda como FLOAT RAW.
    Retorna (n_reparados, n_total, errores).
    """
    ws = get_watchlist_ws()
    if ws is None:
        return 0, 0, ["No conecta con Google Sheets"]

    try:
        all_vals = ws.get_all_values()
        if len(all_vals) <= 1:
            return 0, 0, []

        n_reparados = 0
        errores = []
        for i, row in enumerate(all_vals[1:], start=2):
            if len(row) < 4:
                continue
            fecha_alta, ticker, precio_str, nota = row[0], row[1], row[2], row[3]

            # Detectar precio inválido
            try:
                precio_val = float(precio_str) if precio_str else 0
            except Exception:
                precio_val = 0

            # Reparar si: vacío, 0, o absurdamente grande
            necesita_reparar = (
                precio_val <= 0
                or precio_val > 10000
            )

            # Excepción: Berkshire Hathaway Class A legítimamente >100k
            if ticker.upper() in ("BRK.A", "BRK-A") and 100000 < precio_val < 1000000:
                necesita_reparar = False

            # Comparar con precio actual: si difiere >5x, también es erróneo
            precio_actual = None
            if precio_val > 0 and not necesita_reparar:
                try:
                    t_check = yf.Ticker(ticker)
                    h_now = t_check.history(period="5d", auto_adjust=True)
                    if not h_now.empty:
                        precio_actual = float(h_now["Close"].iloc[-1])
                        # Si la ratio es >5 o <0.2 → claramente mal
                        if precio_actual > 0:
                            ratio = precio_val / precio_actual
                            if ratio > 5 or ratio < 0.2:
                                necesita_reparar = True
                except Exception:
                    pass

            if not necesita_reparar:
                continue

            # Descargar histórico real
            try:
                t_yf = yf.Ticker(ticker)
                h = t_yf.history(period="1y", auto_adjust=True)
                if h.empty:
                    errores.append(f"{ticker}: sin datos históricos")
                    continue

                f_alta = pd.to_datetime(fecha_alta)
                h_filtrado = h[h.index.date <= f_alta.date()]
                if h_filtrado.empty:
                    precio_inicial = float(h["Close"].iloc[0])
                else:
                    precio_inicial = float(h_filtrado["Close"].iloc[-1])

                # CRÍTICO: enviar como NÚMERO FLOAT con value_input_option='RAW'
                # RAW evita cualquier interpretación regional de Sheets
                precio_redondeado = round(float(precio_inicial), 2)

                ws.update(
                    range_name=f"C{i}",
                    values=[[precio_redondeado]],
                    value_input_option="RAW"
                )
                n_reparados += 1
            except Exception as e:
                errores.append(f"{ticker}: {str(e)[:80]}")

        return n_reparados, len(all_vals) - 1, errores
    except Exception as e:
        return 0, 0, [f"Error: {e}"]




INDICES = {
    "SP500":      ("🇺🇸 S&P 500",         get_sp500),
    "SP400":      ("🇺🇸 S&P 400 MidCap",  get_sp400),
    "SP600":      ("🇺🇸 S&P 600 SmallCap",get_sp600),
    "NASDAQ100":  ("🇺🇸 Nasdaq 100",       get_nasdaq100),
    "RUSSELL1000":("🇺🇸 Russell 1000",     get_russell1000),
    "IBEX35":     ("🇪🇸 IBEX 35",          get_ibex),
    "DAX40":      ("🇩🇪 DAX 40",           get_dax),
    "FTSE100":    ("🇬🇧 FTSE 100",         get_ftse100),
    "CAC40":      ("🇫🇷 CAC 40",           get_cac40),
    "EUROSTOXX50":("🇪🇺 EuroStoxx 50",     get_eurostoxx50),
    "SPI":        ("🇨🇭 SPI (Suiza)",      get_spi),
    "NIKKEI225":  ("🇯🇵 Nikkei 225",       get_nikkei225),
}

CL = {"COMPRAR": "#50fa7b", "VIGILAR": "#8be9fd", "NEUTRO": "#f1fa8c", "EVITAR": "#ff5555"}

def score_screener(r):
    s = 0.0
    mom = r.get("Mom 3M %", np.nan)
    if pd.notna(mom) and mom > 20:  s += 1.0
    elif pd.notna(mom) and mom > 10: s += 0.7
    elif pd.notna(mom) and mom > 0:  s += 0.4
    if pd.notna(r.get("vs SMA50 %")) and r["vs SMA50 %"] > 0: s += 0.5
    vr = r.get("Vol/Avg 20d", np.nan)
    if pd.notna(vr) and vr > 2.0:  s += 1.0
    elif pd.notna(vr) and vr > 1.3: s += 0.5
    per = r.get("PER", np.nan)
    if pd.notna(per) and 0 < per < 12: s += 1.5
    elif pd.notna(per) and 0 < per < 20: s += 1.0
    roe = r.get("ROE %", np.nan)
    if pd.notna(roe) and roe > 25: s += 1.0
    elif pd.notna(roe) and roe > 15: s += 0.7
    mg  = r.get("Margen Net %", np.nan)
    if pd.notna(mg) and mg > 20: s += 1.0
    elif pd.notna(mg) and mg > 10: s += 0.7
    de  = r.get("D/E", np.nan)
    if pd.notna(de) and de < 50: s += 0.5
    pot = r.get("Potencial %", np.nan)
    if pd.notna(pot) and pot > 20: s += 0.5
    return round(min(max(s, 0), 10), 1)

def label_sc(sc):
    if sc >= 7.5: return "COMPRAR"
    if sc >= 6.0: return "VIGILAR"
    if sc >= 4.0: return "NEUTRO"
    return "EVITAR"

def get_last_fred(series_id: str):
    """Retorna último valor de una serie FRED o None."""
    if not fred_client:
        return None
    try:
        s = fred_client.get_series(series_id, observation_start="2024-01-01").dropna()
        return float(s.iloc[-1]) if not s.empty else None
    except Exception:
        return None

def get_rf_etf_data(period="1y"):
    """Descarga datos de los ETFs de renta fija."""
    rows = []
    for etf, (nombre, categoria, dur) in RF_ETFS.items():
        h, _ = descargar(etf, period)
        if h.empty or len(h) < 5:
            continue
        precio = h["Close"].iloc[-1]
        chg_1d = (h["Close"].iloc[-1] / h["Close"].iloc[-2] - 1) * 100 if len(h) > 1 else np.nan
        chg_1m = (h["Close"].iloc[-1] / h["Close"].iloc[-22] - 1) * 100 if len(h) > 22 else np.nan
        chg_ytd = (h["Close"].iloc[-1] / h["Close"].iloc[0] - 1) * 100
        vol_a  = h["Close"].pct_change().std() * np.sqrt(252) * 100
        rows.append({
            "ETF": etf, "Nombre": nombre, "Categoría": categoria,
            "Duración (A)": dur,
            "Precio": round(precio, 2),
            "1D %": round(chg_1d, 2) if pd.notna(chg_1d) else np.nan,
            "1M %": round(chg_1m, 2) if pd.notna(chg_1m) else np.nan,
            "YTD %": round(chg_ytd, 2),
            "Vol Anual %": round(vol_a, 2),
        })
        time.sleep(0.2)
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ╔═══════════════════════════════════════════════════════════════╗
# ║  DIVERSIFICACIÓN SECTORIAL                                    ║
# ╚═══════════════════════════════════════════════════════════════╝

# Mapeo sector yfinance/Finnhub → ETF SPDR
SECTOR_TO_SPDR = {
    # yfinance sector names
    "Technology":              "XLK",
    "Financial Services":      "XLF",
    "Energy":                  "XLE",
    "Healthcare":              "XLV",
    "Consumer Cyclical":       "XLY",
    "Consumer Defensive":      "XLP",
    "Industrials":             "XLI",
    "Utilities":               "XLU",
    "Basic Materials":         "XLB",
    "Real Estate":             "XLRE",
    "Communication Services":  "XLC",
    # Finnhub industry names (frecuentes)
    "Banks":                   "XLF",
    "Insurance":               "XLF",
    "Investment Services":     "XLF",
    "Software":                "XLK",
    "Semiconductors":          "XLK",
    "Hardware & Equipment":    "XLK",
    "Pharmaceuticals":         "XLV",
    "Medical Devices":         "XLV",
    "Biotechnology":           "XLV",
    "Retail":                  "XLY",
    "Automobiles":             "XLY",
    "Hotels & Tourism":        "XLY",
    "Oil & Gas":               "XLE",
    "Telecommunications":      "XLC",
    "Media":                   "XLC",
    "Entertainment":           "XLC",
    "Food & Beverage":         "XLP",
    "Tobacco":                 "XLP",
    "Aerospace & Defense":     "XLI",
    "Transportation":          "XLI",
    "Machinery":               "XLI",
    "Construction":            "XLI",
    "Chemicals":               "XLB",
    "Mining":                  "XLB",
    "Electric Utilities":      "XLU",
    "Real Estate":             "XLRE",
    "REITs":                   "XLRE",
}

SPDR_INFO = {
    "XLK":  ("Tecnología",         "📱"),
    "XLF":  ("Financiero",         "🏦"),
    "XLE":  ("Energía",            "⛽"),
    "XLV":  ("Salud",              "💊"),
    "XLY":  ("Cons. Discrecional", "🛍️"),
    "XLP":  ("Cons. Básico",       "🛒"),
    "XLI":  ("Industrial",         "🏭"),
    "XLU":  ("Utilities",          "💡"),
    "XLB":  ("Materiales",         "⚒️"),
    "XLRE": ("Inmobiliario",       "🏠"),
    "XLC":  ("Comunicación",       "📡"),
}

# Sectores preferidos por perfil (orden = prioridad)
PROFILE_PREFS = {
    "agresivo":   ["XLK", "XLY", "XLC", "XLE", "XLF"],   # growth + cíclicos
    "neutro":     ["XLV", "XLI", "XLF", "XLK", "XLY"],   # equilibrado
    "balanceado": ["XLV", "XLP", "XLU", "XLRE", "XLB"],   # defensivos
}

def mapear_sector(sector_raw: str) -> str:
    """Devuelve el ticker SPDR correspondiente al sector, o 'OTRO'."""
    if not sector_raw or sector_raw in ("N/A", "", "None", None):
        return "OTRO"
    for key, spdr in SECTOR_TO_SPDR.items():
        if key.lower() in sector_raw.lower() or sector_raw.lower() in key.lower():
            return spdr
    return "OTRO"

def sector_weights_from_portfolio(tv, w, info_cache):
    """
    Calcula pesos sectoriales de la cartera.
    tv: lista tickers, w: array de pesos (0-1), info_cache: dict {ticker: info}
    Retorna dict {spdr_etf: pct_peso}
    """
    sw = {}
    for ticker, weight in zip(tv, w):
        sector_raw = info_cache.get(ticker, {}).get("sector", "")
        spdr = mapear_sector(sector_raw)
        sw[spdr] = sw.get(spdr, 0) + weight * 100
    return sw

def get_market_regime():
    """
    Determina régimen de mercado: 'risk_on', 'risk_off', 'neutral'.
    Usa VIX (yfinance) y spread 10Y-2Y (FRED).
    """
    vix_val    = None
    spread_val = None
    try:
        h, _ = descargar("^VIX", "6mo")
        if not h.empty:
            vix_val = round(float(h["Close"].iloc[-1]), 1)
    except Exception:
        pass
    try:
        y10 = get_last_fred("DGS10")
        y2  = get_last_fred("DGS2")
        if y10 and y2:
            spread_val = round(y10 - y2, 2)
    except Exception:
        pass

    # Régimen: VIX pesa más
    regime = "neutral"
    if vix_val:
        if   vix_val > 25: regime = "risk_off"
        elif vix_val < 15: regime = "risk_on"

    return regime, vix_val, spread_val

def recomendar_sectores(sector_weights_pct: dict, profile: str,
                         regime: str, n: int = 3) -> list:
    """
    Genera N recomendaciones sectoriales para diversificar la cartera.
    Retorna lista de dicts con info del sector recomendado.
    """
    prefs = PROFILE_PREFS.get(profile, PROFILE_PREFS["neutro"])
    candidates = []
    for etf, (nombre, emoji) in SPDR_INFO.items():
        current_w = sector_weights_pct.get(etf, 0)
        if current_w >= 15:   # ya bien representado → skip
            continue
        # Puntuación base según preferencia del perfil
        pref_score = (len(prefs) - prefs.index(etf)) if etf in prefs else 0
        # Ajuste por régimen de mercado
        if regime == "risk_off" and etf in ("XLV", "XLP", "XLU", "XLRE"):
            pref_score += 4
        elif regime == "risk_on" and etf in ("XLK", "XLY", "XLC", "XLE"):
            pref_score += 4
        # Bonus por ausencia total
        if current_w == 0:
            pref_score += 2
        candidates.append({
            "etf":        etf,
            "nombre":     nombre,
            "emoji":      emoji,
            "peso_actual": round(current_w, 1),
            "score":      pref_score,
        })
    candidates.sort(key=lambda x: x["score"], reverse=True)
    return candidates[:n]


# ╔═══════════════════════════════════════════════════════════════╗
# ║  SIDEBAR                                                      ║
# ╚═══════════════════════════════════════════════════════════════╝
with st.sidebar:
    st.title("📊 TFM Investment App")
    st.caption("Master IA Sector Financiero — VIU")
    st.divider()
    pagina = st.radio("Módulo", [
        "🌅 Outlook",
        "🔍 Screener",
        "🔎 Descubrimiento",
        "⭐ Watchlist",
        "📈 Análisis Individual",
        "💼 Cartera",
        "📊 Macro",
    ])
    st.divider()
    api_ok = f"{'✅' if fh_client else '❌'} Finnhub | {'✅' if fred_client else '❌'} FRED | ✅ yfinance | {'✅' if FMP_KEY else '❌'} FMP"
    st.caption(api_ok)
    if st.button("🗑️ Limpiar caché", use_container_width=True):
        st.cache_data.clear()
        st.success("Caché limpiado.")


# ╔═══════════════════════════════════════════════════════════════╗
# ║  MORNING OUTLOOK                                              ║
# ╚═══════════════════════════════════════════════════════════════╝
@st.cache_data(ttl=300, show_spinner=False)
def _outlook_mercado_bulk(tickers_tuple):
    """Descarga bulk del estado del mercado. Cacheada 5 min.
    Lanza excepción si obtiene <3 tickers para NO cachear fallos."""
    out = {}
    bulk_m = yf.download(list(tickers_tuple), period="5d",
                         auto_adjust=True, progress=False)
    if not bulk_m.empty and isinstance(bulk_m.columns, pd.MultiIndex):
        for ticker in tickers_tuple:
            try:
                if ticker in bulk_m["Close"].columns:
                    c = bulk_m["Close"][ticker].dropna()
                    if len(c) >= 2:
                        p = float(c.iloc[-1])
                        out[ticker] = (p, (p / float(c.iloc[-2]) - 1) * 100)
            except Exception:
                continue
    if len(out) < 3:
        raise RuntimeError("Bulk de mercado incompleto")
    return out


@st.cache_data(ttl=300, show_spinner=False)
def _outlook_sectores_bulk(etfs_tuple, ytd_start):
    """Descarga bulk de ETFs sectoriales y calcula retornos multi-periodo.
    Cacheada 5 min. Lanza excepción si no obtiene datos para NO cachear fallos."""
    bulk_s = yf.download(list(etfs_tuple), period="1y",
                         auto_adjust=True, progress=False)
    if bulk_s.empty or not isinstance(bulk_s.columns, pd.MultiIndex):
        raise RuntimeError("Bulk de sectores vacío")
    closes_all = bulk_s["Close"]
    out = {}
    for ticker in etfs_tuple:
        try:
            if ticker not in closes_all.columns:
                continue
            c_s = closes_all[ticker].dropna()
            if len(c_s) < 22:
                continue
            p = float(c_s.iloc[-1])
            d = {
                "hoy": (p / float(c_s.iloc[-2]) - 1) * 100,
                "1m":  (p / float(c_s.iloc[-22])  - 1) * 100 if len(c_s) > 22  else None,
                "3m":  (p / float(c_s.iloc[-64])  - 1) * 100 if len(c_s) > 64  else None,
                "6m":  (p / float(c_s.iloc[-127]) - 1) * 100 if len(c_s) > 127 else None,
            }
            c_ytd = c_s[c_s.index >= ytd_start]
            d["ytd"] = (p / float(c_ytd.iloc[0]) - 1) * 100 if not c_ytd.empty else None
            out[ticker] = d
        except Exception:
            continue
    if not out:
        raise RuntimeError("Sin datos sectoriales")
    return out


@st.cache_data(ttl=300, show_spinner=False)
def _outlook_watchlist_bulk(tickers_tuple):
    """Descarga bulk 6mo para la watchlist de Outlook. Cacheada 5 min."""
    out = {}
    if len(tickers_tuple) == 1:
        h_t, _ = descargar(tickers_tuple[0], "6mo")
        if not h_t.empty:
            out[tickers_tuple[0]] = h_t["Close"].dropna()
        return out
    bulk = yf.download(list(tickers_tuple), period="6mo",
                       auto_adjust=True, progress=False)
    if isinstance(bulk.columns, pd.MultiIndex):
        for t in tickers_tuple:
            if t in bulk["Close"].columns:
                c = bulk["Close"][t].dropna()
                if not c.empty:
                    out[t] = c
    return out


@st.cache_data(ttl=300, show_spinner=False)
def _leer_senales_escaner():
    """
    Lee la pestaña 'senales' del Sheet (escrita por scanner.py vía GitHub Actions).
    Devuelve DataFrame, DataFrame vacío si no hay señales, o None si la pestaña
    no existe / Sheets no está configurado.
    """
    if not GSPREAD_AVAILABLE:
        return None
    try:
        creds = Credentials.from_service_account_info(
            dict(st.secrets["gcp_service_account"]),
            scopes=["https://www.googleapis.com/auth/spreadsheets"])
        client = gspread.authorize(creds)
        ws = client.open_by_key(WATCHLIST_SHEET_ID).worksheet("senales")
        vals = ws.get_all_records()
        return pd.DataFrame(vals) if vals else pd.DataFrame()
    except Exception:
        return None



@st.cache_data(ttl=43200)
def calcular_velocidad_10y():
    """
    Señal de velocidad del bono a 10 años (Goldman Sachs / TKer):
    el NIVEL de tipos no predice retornos, pero un movimiento de >2σ
    en 1 mes históricamente genera retornos negativos del S&P a 1 mes.
    Calcula: cambio en 21 días hábiles vs sigma de cambios mensuales (3 años).
    Devuelve dict o None.
    """
    if not fred_client:
        return None
    try:
        s = fred_client.get_series("DGS10", observation_start="2018-01-01").dropna()
        if len(s) < 300:
            return None
        cambios_1m = s.diff(21).dropna()
        cambio_actual = float(cambios_1m.iloc[-1])
        sigma = float(cambios_1m.tail(756).std())  # ~3 años hábiles
        if sigma <= 0:
            return None
        z = cambio_actual / sigma
        return {
            "nivel":     float(s.iloc[-1]),
            "cambio_bp": cambio_actual * 100,
            "sigma_bp":  sigma * 100,
            "z":         z,
        }
    except Exception:
        return None



def detectar_soportes_resistencias(df, max_niveles=3, n_pivote=5):
    """
    Detecta soportes y resistencias por pivots + clustering.
    1. Pivots: máximos/mínimos locales en ventana de ±n_pivote velas.
    2. Clustering: niveles a menos de un umbral (max(0.6×ATR, 0.8% precio)) se fusionan.
    3. Score: nº de toques (más toques = nivel más respetado) + recencia.
    Devuelve (soportes, resistencias): listas de dicts {nivel, toques, ultimo_toque}
    ordenadas por cercanía al precio actual.
    Al recibir el df YA filtrado por periodo, los niveles dependen del timeframe.
    """
    try:
        c = df["Close"].astype(float)
        h = df["High"].astype(float) if "High" in df.columns else c
        l = df["Low"].astype(float)  if "Low"  in df.columns else c
        n = len(c)
        if n < 40:
            return [], []
        precio = float(c.iloc[-1])

        # ATR para el umbral de clustering
        tr = pd.concat([h - l, (h - c.shift(1)).abs(),
                        (l - c.shift(1)).abs()], axis=1).max(axis=1)
        atr_loc = float(tr.rolling(14).mean().iloc[-1])
        umbral = max(0.6 * atr_loc, precio * 0.008)

        # Pivots (excluimos las últimas n_pivote velas, aún sin confirmar)
        pivots = []  # (precio_nivel, idx)
        for i in range(n_pivote, n - n_pivote):
            ventana_h = h.iloc[i - n_pivote:i + n_pivote + 1]
            ventana_l = l.iloc[i - n_pivote:i + n_pivote + 1]
            if h.iloc[i] >= ventana_h.max():
                pivots.append((float(h.iloc[i]), i))
            if l.iloc[i] <= ventana_l.min():
                pivots.append((float(l.iloc[i]), i))
        if not pivots:
            return [], []

        # Clustering por proximidad de precio
        pivots.sort(key=lambda x: x[0])
        clusters = []
        actual = [pivots[0]]
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
            toques = len(cl)
            ultimo = max(p[1] for p in cl)
            # Score: toques pesan, recencia desempata
            score = toques + (ultimo / n) * 0.5
            niveles.append({"nivel": nivel, "toques": toques,
                            "recencia": ultimo / n, "score": score})

        # Solo niveles con al menos 1 toque real, separar por lado
        soportes     = [x for x in niveles if x["nivel"] < precio * 0.998]
        resistencias = [x for x in niveles if x["nivel"] > precio * 1.002]
        # Ordenar por score y quedarse con los mejores; luego por cercanía
        soportes     = sorted(soportes, key=lambda x: -x["score"])[:max_niveles]
        resistencias = sorted(resistencias, key=lambda x: -x["score"])[:max_niveles]
        soportes     = sorted(soportes, key=lambda x: precio - x["nivel"])
        resistencias = sorted(resistencias, key=lambda x: x["nivel"] - precio)
        return soportes, resistencias
    except Exception:
        return [], []


def analizar_volumen(df, ventana=60):
    """
    Lectura de volumen sobre el periodo visible:
    - vol_rel: volumen de hoy vs media 20 sesiones
    - ratio_ud: volumen medio en días alcistas / días bajistas (>1.15 acumulación,
      <0.85 distribución)
    - corr_mov_vol: correlación |retorno| vs volumen (¿los movimientos grandes
      van acompañados de volumen? sano si > 0.2)
    - obv_confirma: tendencia del OBV (20 sesiones) vs tendencia del precio —
      True confirma, False diverge, None indeterminado
    """
    try:
        if "Volume" not in df.columns:
            return None
        c = df["Close"].astype(float)
        v = df["Volume"].astype(float)
        if len(c) < 30 or v.sum() <= 0:
            return None

        out = {}
        v20 = float(v.tail(21).iloc[:-1].mean())
        out["vol_rel"] = float(v.iloc[-1]) / v20 if v20 > 0 else None

        sub = df.tail(min(ventana, len(df)))
        rets = sub["Close"].pct_change().dropna()
        vols = sub["Volume"].astype(float).reindex(rets.index)
        up_vol   = vols[rets > 0].mean()
        down_vol = vols[rets < 0].mean()
        out["ratio_ud"] = float(up_vol / down_vol) if down_vol and down_vol > 0 else None

        out["corr_mov_vol"] = float(rets.abs().corr(vols)) if len(rets) > 10 else None

        # OBV vectorizado vs precio (últimas 20 sesiones)
        obv = (np.sign(c.diff()).fillna(0) * v).cumsum()
        if len(obv) > 21:
            obv_up    = float(obv.iloc[-1]) > float(obv.iloc[-21])
            precio_up = float(c.iloc[-1])  > float(c.iloc[-21])
            out["obv_confirma"] = (obv_up == precio_up)
            out["obv_dir"]    = "↑" if obv_up else "↓"
            out["precio_dir"] = "↑" if precio_up else "↓"
        else:
            out["obv_confirma"] = None
        return out
    except Exception:
        return None



def _plan_estructural(entrada, stop, tp1, tp2, importe):
    """
    Plan con sizing por IMPORTE directo: shares = importe // entrada.
    Devuelve P&L exacto en $ y % para stop, TP1 y TP2 — lo que el usuario
    gana o pierde si el precio toca cada nivel. None si niveles inválidos.
    """
    try:
        if stop >= entrada or entrada <= 0 or importe <= 0:
            return None
        shares = int(importe // entrada)
        inversion = shares * entrada
        riesgo_acc = entrada - stop
        return {
            "entrada": entrada, "stop": stop,
            "tp1": tp1, "tp2": tp2,
            "rr1": (tp1 - entrada) / riesgo_acc if riesgo_acc > 0 else None,
            "shares": shares, "inversion": inversion,
            "pnl_stop": shares * (stop - entrada),
            "pnl_tp1":  shares * (tp1 - entrada),
            "pnl_tp2":  shares * (tp2 - entrada),
            "pct_stop": (stop / entrada - 1) * 100,
            "pct_tp1":  (tp1 / entrada - 1) * 100,
            "pct_tp2":  (tp2 / entrada - 1) * 100,
        }
    except Exception:
        return None


def _backtest_entradas(hist, modo="pullback", horizonte=21, max_stop_pct=15.0):
    """
    Mini-backtest de una regla de entrada sobre el histórico del PROPIO ticker.
    Simula: señal → entrada al cierre → stop 2xATR (cap -15%) gestionado con
    el Low diario → salida al stop o al cierre tras `horizonte` días.
    Devuelve dict con n señales, win rate y retorno medio, o None.
    ORIENTATIVO: sin comisiones ni slippage; pocas señales = poca significancia.
    """
    try:
        c = hist["Close"].astype(float)
        h = hist["High"].astype(float)  if "High"   in hist.columns else c
        l = hist["Low"].astype(float)   if "Low"    in hist.columns else c
        v = hist["Volume"].astype(float) if "Volume" in hist.columns else None
        if len(c) < 260:
            return None

        tr   = pd.concat([h - l, (h - c.shift(1)).abs(),
                          (l - c.shift(1)).abs()], axis=1).max(axis=1)
        atr  = tr.rolling(14).mean()
        ma50  = c.rolling(50).mean()
        ma200 = c.rolling(200).mean()
        rsi   = calc_rsi(c)
        vol20 = v.rolling(20).mean() if v is not None else None

        trades = []
        i, n = 210, len(c)
        while i < n - 1:
            senal = False
            if modo == "pullback":
                if (pd.notna(ma200.iloc[i]) and pd.notna(rsi.iloc[i])
                        and c.iloc[i] > ma200.iloc[i]
                        and ma50.iloc[i] > ma200.iloc[i]
                        and abs(c.iloc[i] / ma50.iloc[i] - 1) <= 0.03
                        and 35 <= rsi.iloc[i] <= 55):
                    senal = True
            else:  # breakout
                high60 = float(h.iloc[i-60:i].max())
                volrel_ok = True
                if vol20 is not None and pd.notna(vol20.iloc[i]) and vol20.iloc[i] > 0:
                    volrel_ok = float(v.iloc[i]) >= 1.5 * float(vol20.iloc[i])
                if float(c.iloc[i]) >= high60 and volrel_ok:
                    senal = True

            if senal and pd.notna(atr.iloc[i]) and atr.iloc[i] > 0:
                entrada = float(c.iloc[i])
                stop = max(entrada - 2 * float(atr.iloc[i]),
                           entrada * (1 - max_stop_pct / 100))
                fin = min(i + horizonte, n - 1)
                salida = None
                for j in range(i + 1, fin + 1):
                    if float(l.iloc[j]) <= stop:
                        salida = (stop / entrada - 1) * 100
                        break
                if salida is None:
                    salida = (float(c.iloc[fin]) / entrada - 1) * 100
                trades.append(salida)
                i = fin  # evitar señales solapadas
            else:
                i += 1

        if not trades:
            return {"n": 0}
        tr_s = pd.Series(trades)
        return {"n": len(trades),
                "win_rate": float((tr_s > 0).mean() * 100),
                "ret_medio": float(tr_s.mean()),
                "mejor": float(tr_s.max()),
                "peor": float(tr_s.min())}
    except Exception:
        return None




@st.cache_data(ttl=600, show_spinner=False)
def _news_general_cached():
    """Noticias generales Finnhub, cacheadas 10 min."""
    if fh_client is None:
        return []
    return fh_client.general_news("general", min_id=0) or []


@st.cache_data(ttl=600, show_spinner=False)
def _news_company_cached(ticker, desde, hasta):
    """Noticias de empresa Finnhub, cacheadas 10 min."""
    if fh_client is None:
        return []
    return fh_client.company_news(ticker, _from=desde, to=hasta) or []



def _screener_metricas(df_t):
    """
    Calcula métricas técnicas de un DataFrame OHLCV de ~2 años.
    Devuelve dict o None si datos insuficientes.
    Todo basado en precio, volumen y ATR — sin llamadas a APIs extra.
    """
    try:
        c = df_t["Close"].dropna()
        if len(c) < 60:
            return None
        precio = float(c.iloc[-1])
        if precio <= 0:
            return None

        h = df_t["High"].dropna()   if "High"   in df_t.columns else c
        l = df_t["Low"].dropna()    if "Low"    in df_t.columns else c
        v = df_t["Volume"].dropna() if "Volume" in df_t.columns else pd.Series(dtype=float)

        # Momentum
        mom_1m  = (precio / float(c.iloc[-22])  - 1) * 100 if len(c) > 22  else None
        mom_3m  = (precio / float(c.iloc[-64])  - 1) * 100 if len(c) > 64  else None
        mom_6m  = (precio / float(c.iloc[-127]) - 1) * 100 if len(c) > 127 else None
        # Momentum 12-1 (252d lookback, skip 21d) — el del paper
        mom_12_1 = None
        if len(c) > 274:
            mom_12_1 = (float(c.iloc[-22]) / float(c.iloc[-274]) - 1) * 100

        # Medias
        ma50  = float(c.tail(50).mean())
        ma200 = float(c.tail(200).mean()) if len(c) >= 200 else None

        # RSI
        rsi_s = calc_rsi(c)
        rsi   = float(rsi_s.iloc[-1]) if not rsi_s.empty else None

        # ATR (14) — sobre High/Low/Close reales
        tr = pd.concat([h - l,
                        (h - c.shift(1)).abs(),
                        (l - c.shift(1)).abs()], axis=1).max(axis=1)
        atr = float(tr.rolling(14).mean().iloc[-1])
        atr_pct = atr / precio * 100 if precio > 0 else None

        # Volumen relativo (hoy vs media 20d)
        vol_rel = None
        if len(v) >= 21:
            v20 = float(v.tail(21).iloc[:-1].mean())
            if v20 > 0:
                vol_rel = float(v.iloc[-1]) / v20

        # Máx/mín 52 semanas y distancia
        n52 = min(252, len(c))
        high_52 = float(h.tail(n52).max())
        low_52  = float(l.tail(n52).min())
        pos_52  = ((precio - low_52) / (high_52 - low_52) * 100) if high_52 > low_52 else 50

        # Máximo de 60 días ANTERIOR a hoy (para breakout)
        high_60_prev = float(h.tail(61).iloc[:-1].max()) if len(h) > 61 else None

        # Max drawdown y Calmar sobre último año
        c1y = c.tail(min(252, len(c)))
        ret_1y = (float(c1y.iloc[-1]) / float(c1y.iloc[0]) - 1)
        dd = (c1y / c1y.cummax() - 1).min()
        calmar = (ret_1y / abs(float(dd))) if dd < 0 else None

        return {
            "precio": precio, "mom_1m": mom_1m, "mom_3m": mom_3m,
            "mom_6m": mom_6m, "mom_12_1": mom_12_1,
            "ma50": ma50, "ma200": ma200, "rsi": rsi,
            "atr": atr, "atr_pct": atr_pct, "vol_rel": vol_rel,
            "pos_52": pos_52, "high_60_prev": high_60_prev,
            "ret_1y": ret_1y * 100, "max_dd": float(dd) * 100,
            "calmar": calmar,
        }
    except Exception:
        return None


def _detectar_sr(hist, precio, n_pivots=10, tol=0.02):
    """
    Detecta soportes y resistencias por pivots (mín/máx locales de 10 días)
    agrupando niveles que disten <2% entre sí. Más toques = nivel más fuerte.
    Devuelve (soportes, resistencias): listas de dicts {"nivel", "toques"}
    ordenadas por cercanía al precio actual.
    """
    try:
        h = hist["High"] if "High" in hist.columns else hist["Close"]
        l = hist["Low"]  if "Low"  in hist.columns else hist["Close"]
        h = h.dropna(); l = l.dropna()
        if len(h) < 40:
            return [], []

        piv_max, piv_min = [], []
        w = 5  # ventana del pivot: máximo/mínimo de 11 días centrado
        for i in range(w, len(h) - w):
            seg_h = h.iloc[i-w:i+w+1]
            seg_l = l.iloc[i-w:i+w+1]
            if h.iloc[i] == seg_h.max():
                piv_max.append(float(h.iloc[i]))
            if l.iloc[i] == seg_l.min():
                piv_min.append(float(l.iloc[i]))

        def agrupar(niveles):
            niveles = sorted(niveles)
            grupos = []
            for n in niveles:
                if grupos and abs(n / grupos[-1]["nivel"] - 1) < tol:
                    g = grupos[-1]
                    g["nivel"] = (g["nivel"] * g["toques"] + n) / (g["toques"] + 1)
                    g["toques"] += 1
                else:
                    grupos.append({"nivel": n, "toques": 1})
            return grupos

        soportes = [g for g in agrupar(piv_min) if g["nivel"] < precio]
        resist   = [g for g in agrupar(piv_max) if g["nivel"] > precio]
        # Ordenar por cercanía al precio
        soportes.sort(key=lambda g: precio - g["nivel"])
        resist.sort(key=lambda g: g["nivel"] - precio)
        return soportes[:3], resist[:3]
    except Exception:
        return [], []


def _plan_operativo(precio, atr, capital, riesgo_pct, entrada=None):
    """
    Genera plan operativo: entrada, stop (2xATR cap -15%), TP1/TP2,
    nº acciones según riesgo por operación, e inversión total.
    Stop anclado al precio de ENTRADA, no al de mercado.
    """
    entrada = entrada if entrada else precio
    stop = entrada - 2 * atr
    # Cap del stop al -15% (regla dura)
    stop = max(stop, entrada * 0.85)
    riesgo_accion = entrada - stop
    if riesgo_accion <= 0:
        return None
    riesgo_eur = capital * riesgo_pct / 100
    shares = int(riesgo_eur / riesgo_accion)
    inversion = shares * entrada
    # Cap de concentración: máx 25% del capital en una posición
    if inversion > capital * 0.25 and entrada > 0:
        shares = int(capital * 0.25 / entrada)
        inversion = shares * entrada
    tp1 = entrada + 2 * (entrada - stop)   # R/R 2:1
    tp2 = entrada + 4 * (entrada - stop)   # R/R 4:1
    return {
        "entrada": entrada, "stop": stop,
        "stop_pct": (stop / entrada - 1) * 100,
        "tp1": tp1, "tp2": tp2,
        "shares": shares, "inversion": inversion,
        "riesgo_eur": shares * riesgo_accion,
    }


if pagina == "🌅 Outlook":
    st.header("🌅 Resumen Ejecutivo del Mercado")
    st.markdown("Vista de 30 segundos: índices clave, sectores, watchlist, eventos macro y noticias.")

    refresh_outlook = st.button("🔄 Actualizar", type="primary")

    # ═══════════════════════════════════════════════════════════════
    # 1. ESTADO DEL MERCADO — métricas + gráficas expandibles
    # ═══════════════════════════════════════════════════════════════
    st.markdown("### 📊 Estado del mercado")
    st.caption("👉 Pulsa cualquier card para ver el gráfico de los últimos 6 meses")

    market_metrics = {
        "S&P 500":      ("^GSPC",   "📈", "Índice de las 500 mayores empresas de EEUU"),
        "VIX":          ("^VIX",    "⚡", "Índice de volatilidad — el 'medidor de miedo'"),
        "Dólar (DXY)":  ("DX-Y.NYB","💵", "Fuerza del dólar vs cesta de divisas"),
        "10Y Treasury": ("^TNX",    "🏦", "Rendimiento del bono americano a 10 años"),
        "Oro":          ("GC=F",    "🥇", "Activo refugio por excelencia"),
        "Bitcoin":      ("BTC-USD", "₿",  "Criptomoneda principal"),
    }

    with st.spinner("Cargando estado del mercado..."):
        market_data = {}
        tickers_m = tuple(v[0] for v in market_metrics.values())

        # Bulk cacheado (5 min): tras la primera carga es instantáneo
        try:
            datos_bulk = _outlook_mercado_bulk(tickers_m)
        except Exception:
            datos_bulk = {}

        for nombre, (ticker, emoji, desc) in market_metrics.items():
            if ticker in datos_bulk:
                precio, cambio = datos_bulk[ticker]
            else:
                # Fallback individual (también cacheado)
                precio, cambio, _err = _get_cambio_dia(ticker)
            if precio is not None and cambio is not None:
                market_data[nombre] = {
                    "precio": precio, "cambio": cambio, "emoji": emoji,
                    "ticker": ticker, "desc": desc
                }

    if not market_data:
        st.warning("⚠️ No se pudieron obtener datos de mercado. Reintenta en 1-2 minutos.")
    else:
        cols_market = st.columns(len(market_data))
        for i, (nombre, datos) in enumerate(market_data.items()):
            with cols_market[i]:
                color = "#50fa7b" if datos["cambio"] >= 0 else "#ff5555"
                st.markdown(f"""
                <div style="background:#1e1e2e;padding:10px 8px;border-radius:8px;text-align:center;border-left:3px solid {color};min-height:95px">
                  <div style="font-size:20px">{datos['emoji']}</div>
                  <div style="font-size:10px;color:#bbb;margin-top:3px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">{nombre}</div>
                  <div style="font-size:15px;font-weight:bold;color:#ffffff;margin-top:3px">{datos['precio']:.2f}</div>
                  <div style="font-size:12px;color:{color};margin-top:2px;font-weight:bold">{datos['cambio']:+.2f}%</div>
                </div>
                """, unsafe_allow_html=True)

        # ── Señal de velocidad del 10Y (regla 2σ Goldman Sachs) ──────
        v10 = calcular_velocidad_10y()
        if v10:
            z_abs = abs(v10["z"])
            if z_abs >= 2:
                v_color, v_icon = "#ff5555", "🔴"
                v_msg = (f"**Movimiento extremo de tipos**: el 10Y se ha movido "
                         f"{v10['cambio_bp']:+.0f}pb en 1 mes ({v10['z']:+.1f}σ). "
                         f"Históricamente, movimientos >2σ generan retornos negativos "
                         f"del S&P 500 a 1 mes — prudencia con nuevas entradas.")
            elif z_abs >= 1.5:
                v_color, v_icon = "#ffb86c", "🟡"
                v_msg = (f"**Tipos moviéndose rápido**: {v10['cambio_bp']:+.0f}pb en 1 mes "
                         f"({v10['z']:+.1f}σ). Aún no extremo, pero vigilar.")
            else:
                v_color, v_icon = "#50fa7b", "🟢"
                v_msg = (f"**Tipos estables**: el 10Y se ha movido {v10['cambio_bp']:+.0f}pb "
                         f"en el último mes ({v10['z']:+.1f}σ) — sin señal de estrés. "
                         f"Recuerda: lo que daña a la bolsa no es el nivel de tipos, "
                         f"sino su velocidad de cambio.")
            st.markdown(
                f"<div style='background:#1a1a2e;border-left:4px solid {v_color};"
                f"padding:12px 14px;border-radius:6px;margin:10px 0'>"
                f"<span style='font-size:14px;color:#e8e8e8'>{v_icon} {v_msg}</span><br>"
                f"<span style='font-size:11px;color:#888'>10Y actual: {v10['nivel']:.2f}% · "
                f"σ mensual (3a): {v10['sigma_bp']:.0f}pb · Fuente: FRED DGS10 + metodología Goldman Sachs</span>"
                f"</div>", unsafe_allow_html=True)

        # Selector de índice para ver gráfico
        sel_idx = st.selectbox(
            "📊 Ver gráfico detallado:",
            options=["— Selecciona un índice —"] + list(market_data.keys()),
            key="outlook_idx_chart"
        )

        if sel_idx != "— Selecciona un índice —":
            datos_sel = market_data[sel_idx]
            with st.spinner(f"Cargando gráfico de {sel_idx}..."):
                hist_idx, _ = descargar(datos_sel["ticker"], "6mo")

            if not hist_idx.empty:
                st.caption(f"💡 {datos_sel['desc']}")

                fig_idx = go.Figure()
                # Línea de precio con fill
                fig_idx.add_trace(go.Scatter(
                    x=hist_idx.index, y=hist_idx["Close"],
                    mode="lines", name=sel_idx,
                    line=dict(color="#8be9fd", width=2.5),
                    fill="tozeroy",
                    fillcolor="rgba(139,233,253,0.08)",
                    hovertemplate=f"{sel_idx}: %{{y:.2f}}<extra></extra>"
                ))
                # MA50 si hay suficientes datos
                if len(hist_idx) >= 50:
                    ma50_idx = hist_idx["Close"].rolling(50).mean()
                    fig_idx.add_trace(go.Scatter(
                        x=hist_idx.index, y=ma50_idx,
                        mode="lines", name="MA50",
                        line=dict(color="#ff79c6", width=1.5, dash="dot"),
                        hovertemplate="MA50: %{y:.2f}<extra></extra>"
                    ))

                fig_idx.update_layout(
                    template="plotly_dark",
                    paper_bgcolor="#12121f", plot_bgcolor="#12121f",
                    height=380,
                    margin=dict(l=10, r=10, t=30, b=10),
                    hovermode="x unified",
                    yaxis=dict(side="right", gridcolor="rgba(255,255,255,0.05)",
                               tickfont=dict(color="#bbb")),
                    xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02,
                                xanchor="center", x=0.5,
                                bgcolor="rgba(0,0,0,0)",
                                font=dict(size=11, color="#bbb"))
                )
                st.plotly_chart(fig_idx, use_container_width=True,
                                config={"displayModeBar": False})

                # Métricas adicionales del índice
                colm1, colm2, colm3, colm4 = st.columns(4)
                precio_6m_ago = float(hist_idx["Close"].iloc[0])
                precio_act    = float(hist_idx["Close"].iloc[-1])
                ret_6m = (precio_act/precio_6m_ago - 1) * 100
                high_6m = float(hist_idx["High"].max() if "High" in hist_idx.columns else hist_idx["Close"].max())
                low_6m  = float(hist_idx["Low"].min()  if "Low"  in hist_idx.columns else hist_idx["Close"].min())
                pos_6m  = ((precio_act - low_6m) / (high_6m - low_6m) * 100) if high_6m > low_6m else 50
                colm1.metric("Retorno 6M", f"{ret_6m:+.2f}%")
                colm2.metric("Máx 6M", f"{high_6m:.2f}")
                colm3.metric("Mín 6M", f"{low_6m:.2f}")
                colm4.metric("Pos. en rango", f"{pos_6m:.0f}%")

    st.divider()

    # ═══════════════════════════════════════════════════════════════
    # 2. SECTORES — Hoy y YTD + drill-down
    # ═══════════════════════════════════════════════════════════════
    # ═══════════════════════════════════════════════════════════════
    # SEÑALES DEL ESCÁNER AUTOMÁTICO (GitHub Actions → Sheets)
    # ═══════════════════════════════════════════════════════════════
    st.markdown("### 📡 Señales del escáner automático")
    df_sen = _leer_senales_escaner()
    if df_sen is None:
        st.caption("⚙️ Escáner aún no configurado — cuando actives el workflow de "
                   "GitHub Actions, las señales aparecerán aquí cada 30 min en sesión US.")
    elif df_sen.empty:
        st.caption("📭 Sin señales registradas todavía. El escáner escribe aquí "
                   "cada 30 minutos durante la sesión americana.")
    else:
        try:
            ultimo_ts = df_sen["timestamp"].astype(str).max()
            dia_ult   = ultimo_ts[:10]
            df_ult    = df_sen[df_sen["timestamp"].astype(str) == ultimo_ts]
            df_hoy    = df_sen[df_sen["timestamp"].astype(str).str.startswith(dia_ult)]

            col_se1, col_se2, col_se3 = st.columns(3)
            col_se1.metric("Último escaneo (hora NY)", ultimo_ts[11:] or ultimo_ts)
            col_se2.metric("Señales en el último escaneo", len(df_ult))
            col_se3.metric(f"Señales del día {dia_ult}", len(df_hoy))

            if not df_ult.empty:
                cols_show = [c for c in ["estrategia", "ticker", "precio", "entrada",
                                          "stop", "tp1", "tp2", "detalle"]
                             if c in df_ult.columns]
                st.dataframe(df_ult[cols_show], use_container_width=True,
                             hide_index=True)
            if len(df_hoy) > len(df_ult):
                with st.expander(f"📜 Todas las señales de {dia_ult} ({len(df_hoy)})"):
                    st.dataframe(df_hoy, use_container_width=True, hide_index=True)
        except Exception as e:
            st.warning(f"Señales ilegibles: {e}")

    st.divider()

    st.markdown("### 🏭 Sectores")

    sector_etfs = {
        "Tecnología":     "XLK",
        "Financiero":     "XLF",
        "Salud":          "XLV",
        "Energía":        "XLE",
        "Consumo Disc.":  "XLY",
        "Consumo Bás.":   "XLP",
        "Industrial":     "XLI",
        "Utilities":      "XLU",
        "Materiales":     "XLB",
        "Inmobiliario":   "XLRE",
        "Comunicación":   "XLC",
    }

    # Top tickers por sector (S&P 500) para el drill-down
    SECTOR_TICKERS_MAP = {
        "Tecnología":    ["AAPL","MSFT","NVDA","AVGO","ORCL","CRM","ADBE","AMD","INTC","CSCO","QCOM","TXN","IBM","NOW","PANW"],
        "Financiero":    ["JPM","BAC","WFC","GS","MS","C","BLK","SCHW","SPGI","AXP","V","MA","PYPL","COF","BX"],
        "Salud":         ["UNH","JNJ","LLY","PFE","ABBV","MRK","TMO","ABT","DHR","ELV","CVS","BMY","AMGN","GILD","MDT"],
        "Energía":       ["XOM","CVX","COP","EOG","SLB","MPC","PSX","VLO","OXY","PXD","KMI","WMB","HES","FANG","DVN"],
        "Consumo Disc.": ["AMZN","TSLA","HD","MCD","NKE","SBUX","BKNG","TJX","LOW","CMG","ABNB","ORLY","MAR","GM","F"],
        "Consumo Bás.":  ["WMT","PG","KO","PEP","COST","PM","MDLZ","MO","CL","KMB","GIS","KHC","SYY","STZ","HSY"],
        "Industrial":    ["GE","HON","UNP","UPS","RTX","BA","CAT","DE","LMT","NOC","GD","ETN","ITW","MMM","EMR"],
        "Utilities":     ["NEE","DUK","SO","D","AEP","SRE","XEL","PCG","ED","EXC","EIX","PEG","WEC","ETR","AWK"],
        "Materiales":    ["LIN","SHW","ECL","APD","FCX","NEM","DOW","DD","PPG","NUE","CTVA","MLM","ALB","VMC","STLD"],
        "Inmobiliario":  ["PLD","AMT","EQIX","PSA","O","WELL","CCI","SPG","DLR","SBAC","AVB","EQR","EXR","ARE","VTR"],
        "Comunicación":  ["GOOGL","META","NFLX","DIS","TMUS","T","VZ","CMCSA","CHTR","WBD","EA","TTWO","DASH","SPOT","ROKU"],
    }

    # Calcular rendimientos HOY, 1M, 3M, 6M y YTD — descarga BULK (1 llamada)
    sector_data = []
    today_year = datetime.now().year
    ytd_start  = datetime(today_year, 1, 1).strftime("%Y-%m-%d")

    with st.spinner("Analizando sectores..."):
        try:
            datos_sec = _outlook_sectores_bulk(tuple(sector_etfs.values()), ytd_start)
        except Exception:
            datos_sec = {}

        for nombre, ticker in sector_etfs.items():
            d = datos_sec.get(ticker)
            if d is None:
                continue
            sector_data.append({
                "Sector":   nombre,
                "Ticker":   ticker,
                "Hoy %":    round(d["hoy"], 2),
                "1M %":     round(d["1m"], 1) if d["1m"] is not None else None,
                "3M %":     round(d["3m"], 1) if d["3m"] is not None else None,
                "6M %":     round(d["6m"], 1) if d["6m"] is not None else None,
                "YTD %":    round(d["ytd"], 2) if d["ytd"] is not None else None,
            })

    if sector_data:
        df_sect = pd.DataFrame(sector_data)

        # Pestañas: HOY vs YTD vs Fuerza Relativa
        tab_hoy, tab_ytd, tab_fr = st.tabs(
            ["📅 Hoy", "📆 YTD (Año a la fecha)", "💪 Fuerza relativa"])

        with tab_hoy:
            df_h = df_sect.sort_values("Hoy %", ascending=False)
            col_h1, col_h2 = st.columns(2)
            with col_h1:
                st.markdown("**🟢 Líderes hoy**")
                for _, row in df_h.head(3).iterrows():
                    st.markdown(
                        f"<div style='padding:8px 12px;background:#1e3a1e;border-radius:6px;"
                        f"margin-bottom:5px;border-left:3px solid #50fa7b'>"
                        f"<b style='color:#ffffff'>{row['Sector']}</b>"
                        f"<span style='color:#50fa7b;float:right;font-weight:bold'>+{row['Hoy %']:.2f}%</span>"
                        f"</div>", unsafe_allow_html=True)
            with col_h2:
                st.markdown("**🔴 Más débiles hoy**")
                for _, row in df_h.tail(3).iloc[::-1].iterrows():
                    st.markdown(
                        f"<div style='padding:8px 12px;background:#3a1e1e;border-radius:6px;"
                        f"margin-bottom:5px;border-left:3px solid #ff5555'>"
                        f"<b style='color:#ffffff'>{row['Sector']}</b>"
                        f"<span style='color:#ff5555;float:right;font-weight:bold'>{row['Hoy %']:.2f}%</span>"
                        f"</div>", unsafe_allow_html=True)

        with tab_ytd:
            df_y = df_sect.dropna(subset=["YTD %"]).sort_values("YTD %", ascending=False)
            if df_y.empty:
                st.info("Sin datos YTD disponibles aún")
            else:
                # Tabla completa con barras
                fig_ytd = go.Figure()
                fig_ytd.add_trace(go.Bar(
                    y=df_y["Sector"],
                    x=df_y["YTD %"],
                    orientation="h",
                    marker=dict(
                        color=df_y["YTD %"],
                        colorscale=[[0, "#ff5555"], [0.5, "#f1fa8c"], [1, "#50fa7b"]],
                        cmid=0,
                    ),
                    text=df_y["YTD %"].apply(lambda x: f"{x:+.1f}%"),
                    textposition="outside",
                    hovertemplate="<b>%{y}</b><br>YTD: %{x:+.2f}%<extra></extra>"
                ))
                fig_ytd.update_layout(
                    template="plotly_dark",
                    paper_bgcolor="#12121f", plot_bgcolor="#12121f",
                    height=400,
                    margin=dict(l=10, r=50, t=20, b=10),
                    xaxis=dict(title="Rendimiento YTD %", gridcolor="rgba(255,255,255,0.05)",
                               tickfont=dict(color="#bbb")),
                    yaxis=dict(tickfont=dict(color="#ffffff")),
                    showlegend=False
                )
                st.plotly_chart(fig_ytd, use_container_width=True,
                                config={"displayModeBar": False})

        with tab_fr:
            st.caption(
                "Tendencias en curso: un sector líder en TODOS los plazos tiene "
                "fuerza persistente — donde el momentum funciona mejor. "
                "Verde en 1M pero rojo en 6M = posible giro incipiente."
            )
            cols_fr = ["Sector", "1M %", "3M %", "6M %", "YTD %"]
            df_fr = df_sect[[c for c in cols_fr if c in df_sect.columns]].copy()
            # Ranking medio entre periodos (fuerza relativa compuesta)
            rank_cols = [c for c in ["1M %", "3M %", "6M %"] if c in df_fr.columns]
            if rank_cols:
                df_fr["Rank FR"] = df_fr[rank_cols].rank(ascending=False).mean(axis=1).round(1)
                df_fr = df_fr.sort_values("Rank FR")
            st.dataframe(
                df_fr.style.map(_color_pct,
                    subset=[c for c in ["1M %","3M %","6M %","YTD %"] if c in df_fr.columns]),
                use_container_width=True, hide_index=True,
                height=60 + 36 * len(df_fr)
            )
            if rank_cols and len(df_fr) >= 3:
                lider = df_fr.iloc[0]["Sector"]
                cola  = df_fr.iloc[-1]["Sector"]
                st.markdown(f"💪 **Liderazgo persistente:** {lider} · "
                            f"📉 **Más débil:** {cola}")

        # ── Drill-down: ver acciones del sector ──────────────────────
        st.markdown("#### 🔍 Ver acciones de un sector")
        sel_sector = st.selectbox(
            "Selecciona un sector para ver sus principales acciones:",
            options=["— Selecciona —"] + list(sector_etfs.keys()),
            key="outlook_sec_drill"
        )

        if sel_sector != "— Selecciona —" and sel_sector in SECTOR_TICKERS_MAP:
            sec_tickers = SECTOR_TICKERS_MAP[sel_sector]
            st.caption(f"Top {len(sec_tickers)} acciones del sector **{sel_sector}** (S&P 500)")

            with st.spinner(f"Cargando {len(sec_tickers)} acciones..."):
                try:
                    bulk = yf.download(sec_tickers, period="5d",
                                        auto_adjust=True, progress=False)
                    rows_sec = []
                    if isinstance(bulk.columns, pd.MultiIndex):
                        for t in sec_tickers:
                            try:
                                if t in bulk.columns.get_level_values(1):
                                    c = bulk["Close"][t].dropna()
                                    if len(c) >= 2:
                                        precio_t = float(c.iloc[-1])
                                        cambio_t = (precio_t / float(c.iloc[-2]) - 1) * 100
                                        rows_sec.append({
                                            "Ticker": t,
                                            "Precio": round(precio_t, 2),
                                            "Hoy %":  round(cambio_t, 2),
                                        })
                            except Exception:
                                continue

                    if rows_sec:
                        df_sec_drill = pd.DataFrame(rows_sec).sort_values("Hoy %", ascending=False)

                        # Métricas resumen
                        n_up = int((df_sec_drill["Hoy %"] > 0).sum())
                        avg_chg = df_sec_drill["Hoy %"].mean()
                        col_s1, col_s2, col_s3 = st.columns(3)
                        col_s1.metric("Acciones subiendo", f"{n_up}/{len(df_sec_drill)}")
                        col_s2.metric("Cambio medio", f"{avg_chg:+.2f}%")
                        col_s3.metric("Mejor", f"{df_sec_drill['Ticker'].iloc[0]} ({df_sec_drill['Hoy %'].iloc[0]:+.1f}%)")

                        st.dataframe(
                            df_sec_drill.style.map(_color_pct, subset=["Hoy %"]),
                            use_container_width=True, hide_index=True, height=400
                        )
                    else:
                        st.warning("No se pudieron cargar las acciones del sector")
                except Exception as e:
                    st.error(f"Error: {e}")

    st.divider()

    # ═══════════════════════════════════════════════════════════════
    # 3. WATCHLIST — Performance real desde la fecha de alta
    # ═══════════════════════════════════════════════════════════════
    st.markdown("### ⭐ Tu watchlist")

    df_wl_o = watchlist_load() if GSPREAD_AVAILABLE else pd.DataFrame()
    if df_wl_o.empty:
        st.info("📋 No tienes tickers en watchlist. Ve a **⭐ Watchlist** para añadir alguno.")
    else:
        with st.spinner(f"Cargando {len(df_wl_o)} tickers..."):
            try:
                tickers_wl = df_wl_o["ticker"].tolist()
                # Bulk cacheado 5 min — instantáneo tras la primera carga
                try:
                    closes_wl = _outlook_watchlist_bulk(tuple(tickers_wl))
                except Exception:
                    closes_wl = {}
                hist_dict = {t: pd.DataFrame({"Close": c})
                             for t, c in closes_wl.items()}

                wl_rows = []
                for _, row_db in df_wl_o.iterrows():
                    t = row_db["ticker"]
                    if t not in hist_dict: continue
                    h_t = hist_dict[t]
                    if h_t.empty or len(h_t) < 2: continue

                    precio_hoy = float(h_t["Close"].iloc[-1])
                    cambio_dia = (precio_hoy / float(h_t["Close"].iloc[-2]) - 1) * 100

                    # PRECIO Y FECHA DE ALTA (datos reales guardados en Sheets)
                    try:
                        precio_alta = float(row_db["precio_inicial"])
                    except Exception:
                        precio_alta = None

                    fecha_alta_str = row_db["fecha_anadido"]
                    dias_seguido  = None
                    ret_desde_alta = None

                    if precio_alta and precio_alta > 0:
                        ret_desde_alta = (precio_hoy / precio_alta - 1) * 100

                    try:
                        f_alta = datetime.strptime(fecha_alta_str, "%Y-%m-%d")
                        dias_seguido = (datetime.now() - f_alta).days
                    except Exception:
                        pass

                    wl_rows.append({
                        "Ticker":        t,
                        "Precio actual": round(precio_hoy, 2),
                        "Precio alta":   round(precio_alta, 2) if precio_alta else None,
                        "Hoy %":         round(cambio_dia, 2),
                        "Desde alta %":  round(ret_desde_alta, 2) if ret_desde_alta is not None else None,
                        "Días":          dias_seguido,
                        "Fecha alta":    fecha_alta_str,
                    })

                if wl_rows:
                    df_wlo = pd.DataFrame(wl_rows).sort_values("Desde alta %", ascending=False)

                    col_w1, col_w2, col_w3, col_w4 = st.columns(4)
                    n_up_hoy = int((df_wlo["Hoy %"] > 0).sum())
                    col_w1.metric("Subiendo hoy", f"{n_up_hoy}/{len(df_wlo)}")

                    winners = df_wlo["Desde alta %"].dropna()
                    if len(winners) > 0:
                        n_win = int((winners > 0).sum())
                        col_w2.metric("En verde desde alta", f"{n_win}/{len(winners)}")
                        col_w3.metric("Mejor performer",
                                      f"{df_wlo['Ticker'].iloc[0]}",
                                      delta=f"{df_wlo['Desde alta %'].iloc[0]:+.1f}%")
                        avg_ret = winners.mean()
                        col_w4.metric("Retorno medio", f"{avg_ret:+.2f}%")

                    st.dataframe(
                        df_wlo.style.map(_color_pct,
                            subset=["Hoy %","Desde alta %"]),
                        use_container_width=True, hide_index=True
                    )
                    st.caption("💡 *'Desde alta %' = retorno desde el día que añadiste el ticker, "
                               "calculado con precio_inicial guardado en Google Sheets*")
            except Exception as e:
                st.error(f"Error cargando watchlist: {e}")

    st.divider()

    # ═══════════════════════════════════════════════════════════════
    # 4. CALENDARIO MACRO — Mejorado con próximos eventos
    # ═══════════════════════════════════════════════════════════════
    st.markdown("### 📅 Calendario macro")

    today        = datetime.now()
    weekday      = today.weekday()
    monday       = today - timedelta(days=weekday)
    nombre_dias  = ["Lunes","Martes","Miércoles","Jueves","Viernes"]

    # Calculamos eventos para esta semana Y la próxima (2 semanas en total)
    week_dates = [monday + timedelta(days=d) for d in range(14)]  # 2 semanas

    def es_primer_viernes(fecha):
        return fecha.weekday() == 4 and fecha.day <= 7

    eventos_macro = []
    for fecha in week_dates:
        dia_mes  = fecha.day
        dia_sem  = fecha.weekday()

        if es_primer_viernes(fecha):
            eventos_macro.append({"fecha":fecha,"evento":"📊 Non-Farm Payrolls (US)",
                "hora":"14:30 UTC","impacto":"🔴 Alto",
                "desc":"Empleo no agrícola — clave para la Fed"})

        if 10 <= dia_mes <= 15 and dia_sem in [2, 3]:
            eventos_macro.append({"fecha":fecha,"evento":"💰 CPI — Inflación (US)",
                "hora":"14:30 UTC","impacto":"🔴 Alto",
                "desc":"Índice de Precios al Consumo"})

        if 15 <= dia_mes <= 20 and dia_sem == 3:
            eventos_macro.append({"fecha":fecha,"evento":"🏭 PPI — Inflación productor (US)",
                "hora":"14:30 UTC","impacto":"🟡 Medio",
                "desc":"Precios al productor — anticipa CPI"})

        if dia_sem == 2 and 20 <= dia_mes <= 25:
            eventos_macro.append({"fecha":fecha,"evento":"🏦 Reunión FOMC (Fed)",
                "hora":"20:00 UTC","impacto":"🔴 Alto",
                "desc":"Decisión de tipos + press conference Powell"})

        if dia_sem == 3:
            eventos_macro.append({"fecha":fecha,"evento":"👥 Jobless Claims (US)",
                "hora":"14:30 UTC","impacto":"🟢 Bajo",
                "desc":"Solicitudes semanales de subsidio"})

        if dia_sem == 3 and 12 <= dia_mes <= 18:
            eventos_macro.append({"fecha":fecha,"evento":"🇪🇺 BCE — Decisión de tipos",
                "hora":"14:15 UTC","impacto":"🟡 Medio",
                "desc":"Banco Central Europeo — política EUR"})

        if today.month in [3, 6, 9, 12] and 25 <= dia_mes <= 30 and dia_sem == 3:
            eventos_macro.append({"fecha":fecha,"evento":"📈 GDP trimestral (US)",
                "hora":"14:30 UTC","impacto":"🔴 Alto",
                "desc":"PIB — crecimiento de la economía"})

        # Retail Sales típicamente día 15-17
        if 15 <= dia_mes <= 17 and dia_sem == 3:
            eventos_macro.append({"fecha":fecha,"evento":"🛍️ Retail Sales (US)",
                "hora":"14:30 UTC","impacto":"🟡 Medio",
                "desc":"Ventas minoristas — consumo doméstico"})

        # PMI primer día hábil de cada mes
        if dia_mes <= 3 and dia_sem < 5:
            eventos_macro.append({"fecha":fecha,"evento":"🏭 ISM Manufacturing PMI",
                "hora":"16:00 UTC","impacto":"🟡 Medio",
                "desc":"Sentimiento empresarial manufactura"})

    if eventos_macro:
        eventos_macro.sort(key=lambda x: x["fecha"])

        # Filtros visuales
        col_filt1, col_filt2 = st.columns([2, 1])
        with col_filt1:
            filtro_impacto = st.multiselect(
                "Filtrar por impacto:",
                options=["🔴 Alto", "🟡 Medio", "🟢 Bajo"],
                default=["🔴 Alto", "🟡 Medio"],
                key="cal_filter"
            )
        with col_filt2:
            vista_cal = st.radio("Vista:", ["Esta semana", "2 semanas"],
                                  horizontal=True, key="cal_view")

        eventos_filt = [e for e in eventos_macro if e["impacto"] in filtro_impacto]
        if vista_cal == "Esta semana":
            limite = monday + timedelta(days=7)
            eventos_filt = [e for e in eventos_filt if e["fecha"] < limite]

        if not eventos_filt:
            st.info("📭 Sin eventos para los filtros seleccionados")
        else:
            # Agrupar por día
            for fecha_dia in week_dates:
                eventos_dia = [e for e in eventos_filt if e["fecha"].date() == fecha_dia.date()]
                if eventos_dia:
                    dia_label = nombre_dias[fecha_dia.weekday()] if fecha_dia.weekday() < 5 else fecha_dia.strftime("%A")
                    fecha_str = fecha_dia.strftime("%d-%m")
                    es_hoy    = fecha_dia.date() == today.date()
                    es_pasado = fecha_dia.date() < today.date()
                    marcador  = " 👈 HOY" if es_hoy else ("  *(pasado)*" if es_pasado else "")
                    color_bg  = "#1a3a1a" if es_hoy else ("#1a1a1a" if es_pasado else "#1e1e2e")
                    opacity   = "opacity:0.6" if es_pasado else ""

                    st.markdown(f"**{dia_label} {fecha_str}**{marcador}")
                    for ev in eventos_dia:
                        st.markdown(f"""
                        <div style="background:{color_bg};padding:10px;margin-bottom:6px;border-radius:6px;border-left:3px solid #8be9fd;{opacity}">
                          <div style="display:flex;justify-content:space-between;flex-wrap:wrap">
                            <div>
                              <b style="color:#ffffff">{ev['evento']}</b>
                              <span style="color:#bbb;font-size:12px;margin-left:6px">{ev['hora']}</span>
                            </div>
                            <span style="font-size:12px">{ev['impacto']}</span>
                          </div>
                          <div style="font-size:11px;color:#aaa;margin-top:4px">{ev['desc']}</div>
                        </div>
                        """, unsafe_allow_html=True)
    else:
        st.info("📭 No hay eventos macro destacables próximamente")

    st.caption(
        "ℹ️ Eventos calculados según patrones típicos. Verifica fechas exactas en "
        "[Trading Economics](https://tradingeconomics.com/calendar) o "
        "[Investing.com](https://www.investing.com/economic-calendar/)"
    )

    st.divider()

    # ═══════════════════════════════════════════════════════════════
    # 5. NOTICIAS — Mercado general + Watchlist (separadas)
    # ═══════════════════════════════════════════════════════════════
    st.markdown("### 📰 Noticias")

    if fh_client is None:
        st.info("📭 Finnhub no configurado — sin noticias disponibles")
    else:
        tab_news_global, tab_news_wl = st.tabs(["🌍 Mercado global", "⭐ De tu watchlist"])

        # ── Noticias del mercado global ─────────────────────────────
        with tab_news_global:
            with st.spinner("Cargando noticias generales..."):
                try:
                    news_general = _news_general_cached()
                    if news_general:
                        for n in news_general[:8]:
                            fecha_n = datetime.fromtimestamp(n.get("datetime", 0)).strftime("%d-%m %H:%M") \
                                      if n.get("datetime") else ""
                            categoria = n.get("category", "general").capitalize()
                            st.markdown(f"""
                            <div style="background:#1e1e2e;padding:12px;margin-bottom:8px;border-radius:6px;border-left:3px solid #8be9fd">
                              <div style="display:flex;justify-content:space-between;flex-wrap:wrap;gap:6px">
                                <div>
                                  <span style="background:#44475a;padding:2px 8px;border-radius:10px;font-size:11px;color:#8be9fd">
                                    {categoria}
                                  </span>
                                  <span style="color:#bbb;font-size:11px;margin-left:6px">
                                    {n.get('source','')} · {fecha_n}
                                  </span>
                                </div>
                                <a href="{n.get('url','')}" target="_blank" style="color:#8be9fd;font-size:12px;text-decoration:none">
                                  Leer →
                                </a>
                              </div>
                              <div style="margin-top:6px;font-size:14px;color:#ffffff;font-weight:bold">
                                {n.get('headline','')}
                              </div>
                              <div style="margin-top:4px;font-size:12px;color:#aaa">
                                {(n.get('summary','') or '')[:200]}{'...' if len(n.get('summary','') or '') >= 200 else ''}
                              </div>
                            </div>
                            """, unsafe_allow_html=True)
                    else:
                        st.info("📭 Sin noticias generales disponibles")
                except Exception as e:
                    st.error(f"Error: {e}")

        # ── Noticias de la watchlist ────────────────────────────────
        with tab_news_wl:
            df_wl_news = watchlist_load() if GSPREAD_AVAILABLE else pd.DataFrame()
            if df_wl_news.empty:
                st.info("📋 Añade tickers a tu watchlist para ver noticias específicas")
            else:
                tickers_news = df_wl_news["ticker"].tolist()[:5]
                tickers_us = [t for t in tickers_news if "." not in t]

                if not tickers_us:
                    st.info("ℹ️ Finnhub solo soporta tickers US. Tus tickers actuales son extranjeros.")
                else:
                    st.caption(f"Noticias de tus {len(tickers_us)} tickers US en watchlist")
                    with st.spinner("Cargando noticias..."):
                        today_str    = datetime.now().strftime("%Y-%m-%d")
                        week_ago_str = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
                        todas = []
                        for t in tickers_us:
                            try:
                                items = _news_company_cached(t, week_ago_str, today_str)
                                for n in items[:3]:
                                    todas.append({
                                        "ticker":  t,
                                        "fecha":   n.get("datetime", 0),
                                        "fuente":  n.get("source", ""),
                                        "titular": n.get("headline", ""),
                                        "url":     n.get("url", ""),
                                        "resumen": (n.get("summary", "") or "")[:200],
                                    })
                            except Exception:
                                continue

                        todas.sort(key=lambda x: x["fecha"], reverse=True)
                        todas = todas[:10]

                    if todas:
                        for n in todas:
                            fecha_n = datetime.fromtimestamp(n["fecha"]).strftime("%d-%m %H:%M") if n["fecha"] else ""
                            st.markdown(f"""
                            <div style="background:#1e1e2e;padding:12px;margin-bottom:8px;border-radius:6px;border-left:3px solid #ff79c6">
                              <div style="display:flex;justify-content:space-between;flex-wrap:wrap;gap:6px">
                                <div>
                                  <span style="background:#44475a;padding:2px 8px;border-radius:10px;font-size:11px;color:#8be9fd">
                                    {n['ticker']}
                                  </span>
                                  <span style="color:#bbb;font-size:11px;margin-left:6px">
                                    {n['fuente']} · {fecha_n}
                                  </span>
                                </div>
                                <a href="{n['url']}" target="_blank" style="color:#8be9fd;font-size:12px;text-decoration:none">
                                  Leer →
                                </a>
                              </div>
                              <div style="margin-top:6px;font-size:14px;color:#ffffff;font-weight:bold">
                                {n['titular']}
                              </div>
                              <div style="margin-top:4px;font-size:12px;color:#aaa">
                                {n['resumen']}{'...' if len(n['resumen']) >= 200 else ''}
                              </div>
                            </div>
                            """, unsafe_allow_html=True)
                    else:
                        st.info("📭 Sin noticias recientes para tus tickers")

    st.divider()
    st.caption(f"🕐 Última actualización: {datetime.now().strftime('%H:%M %d-%m-%Y')}")


elif pagina == "🔍 Screener":
    # ═══════════════════════════════════════════════════════════════
    # SCREENER v2 — Estrategias de precio, volumen y ATR
    # Bulk download (rápido) + plan operativo con position sizing
    # ═══════════════════════════════════════════════════════════════
    ESTRATEGIAS = {
        "📐 Momentum Sistemático": (
            "Compra fuerza persistente: retorno de 12 meses excluyendo el último "
            "(el 'momentum 12-1' académico), ranqueado por Calmar ratio. "
            "Parámetros 252/21/top-10 validados con Deflated Sharpe Ratio y walk-forward "
            "(CAGR histórico 22.6% vs SPY 13.9%)."
        ),
        "🔄 Pullback en tendencia": (
            "Compra retrocesos sanos dentro de tendencias alcistas: precio sobre MA200, "
            "MA50 sobre MA200, precio tocando la MA50 (±3%) y RSI entre 35-55 "
            "(retroceso, no desplome). El clásico 'comprar el dip' con reglas."
        ),
        "🚀 Breakout con volumen": (
            "Compra rupturas confirmadas: precio supera el máximo de 60 días "
            "con volumen al menos 1.5× su media. El volumen valida que la ruptura "
            "tiene demanda real detrás."
        ),
        "🔧 Filtros manuales": (
            "Construye tu propio filtro combinando momentum, RSI, tendencia, "
            "volumen relativo y volatilidad (ATR%)."
        ),
    }

    with st.sidebar:
        indice = st.selectbox("Índice", list(INDICES.keys()),
                              format_func=lambda x: INDICES[x][0])
        estrategia = st.selectbox("Estrategia", list(ESTRATEGIAS.keys()))

        st.divider()
        st.markdown("**💰 Gestión del riesgo**")
        capital    = st.number_input("Capital disponible ($)", 1000, 10000000, 10000, step=1000)
        riesgo_pct = st.slider("Riesgo por operación (%)", 0.5, 3.0, 1.0, 0.25,
                               help="% del capital que pierdes si salta el stop. 1% es el estándar profesional.")

        # Filtros manuales solo si aplica
        if estrategia == "🔧 Filtros manuales":
            st.divider()
            st.markdown("**🔧 Filtros**")
            f_mom3_min  = st.slider("Momentum 3M mínimo (%)", -50, 50, 0)
            f_rsi       = st.slider("RSI entre", 0, 100, (35, 70))
            f_vol_rel   = st.slider("Volumen relativo mínimo", 0.0, 5.0, 0.0, 0.25)
            f_atr_max   = st.slider("ATR% máximo (volatilidad)", 1.0, 20.0, 10.0, 0.5,
                                    help="Excluye chicharros: ATR% alto = movimientos diarios salvajes")
            f_tendencia = st.checkbox("Solo tendencia alcista (precio>MA50>MA200)", value=True)

        limite = st.slider("Tickers a analizar", 50, 600, 200, step=50)
        st.divider()
        ejecutar = st.button("🚀 Ejecutar screener", type="primary", use_container_width=True)

    st.header(f"🔍 Screener — {estrategia}")
    st.markdown(f"*{ESTRATEGIAS[estrategia]}*")

    if not ejecutar:
        st.info("👈 Elige estrategia, define tu capital y riesgo, y pulsa **Ejecutar**.")
        st.markdown("""
        **Cómo funciona el plan operativo de cada resultado:**
        - 🛑 **Stop loss** = entrada − 2×ATR (máximo -15%). El ATR mide la volatilidad
          real del valor: un stop a 2×ATR aguanta el ruido normal sin salirte por nada.
        - 📏 **Tamaño de posición** = tu riesgo por operación (€) ÷ distancia al stop.
          Así, si salta el stop, pierdes exactamente lo que decidiste arriesgar — ni más ni menos.
        - 🎯 **TP1 / TP2** = objetivos a ratio 2:1 y 4:1 sobre el riesgo asumido.
        """)
        st.stop()

    # ── Descarga bulk ────────────────────────────────────────────
    with st.spinner("Obteniendo tickers del índice..."):
        tickers = INDICES[indice][1]()
    if not tickers:
        st.error("No se pudieron obtener tickers.")
        st.stop()

    ta = tickers[:limite]
    with st.spinner(f"Descargando 2 años de {len(ta)} tickers (bulk)..."):
        try:
            raw = yf.download(ta, period="2y", auto_adjust=True,
                              progress=False, group_by="column")
        except Exception as e:
            st.error(f"Error en descarga bulk: {e}")
            st.stop()

    if raw.empty:
        st.error("Descarga vacía. Reintenta en 1-2 minutos (posible rate limit).")
        st.stop()

    # ── Calcular métricas por ticker ─────────────────────────────
    resultados = []
    multi = isinstance(raw.columns, pd.MultiIndex)
    pb = st.progress(0, text="Calculando métricas...")
    for i, t in enumerate(ta):
        if i % 25 == 0:
            pb.progress(min((i + 1) / len(ta), 1.0), text=f"Analizando {t}...")
        try:
            if multi:
                cols_t = {}
                for campo in ["Close", "High", "Low", "Volume"]:
                    if campo in raw.columns.get_level_values(0) and \
                       t in raw[campo].columns:
                        cols_t[campo] = raw[campo][t]
                if "Close" not in cols_t:
                    continue
                df_t = pd.DataFrame(cols_t).dropna(subset=["Close"])
            else:
                df_t = raw.dropna(subset=["Close"])

            m = _screener_metricas(df_t)
            if m is None:
                continue
            m["ticker"] = t
            resultados.append(m)
        except Exception:
            continue
    pb.empty()

    if not resultados:
        st.error("Sin métricas calculables. Reintenta con otro índice o en unos minutos.")
        st.stop()

    df_m = pd.DataFrame(resultados)
    st.caption(f"📊 {len(df_m)} de {len(ta)} tickers con datos suficientes")

    # ── Aplicar estrategia ───────────────────────────────────────
    seleccion = pd.DataFrame()

    if estrategia == "📐 Momentum Sistemático":
        # Momentum 12-1 positivo, ranking por Calmar, top 10
        cand = df_m.dropna(subset=["mom_12_1", "calmar"])
        cand = cand[cand["mom_12_1"] > 0]
        if cand.empty:
            st.warning("⚠️ Ningún valor con momentum 12-1 positivo. "
                       "Históricamente esto sugiere mercado bajista: la estrategia "
                       "indica quedarse en renta fija este mes.")
            st.stop()
        seleccion = cand.sort_values("calmar", ascending=False).head(10).copy()
        seleccion["Señal"] = "Momentum 12-1 +" + seleccion["mom_12_1"].round(1).astype(str) + "%"
        if len(seleccion) < 10:
            st.info(f"ℹ️ Solo {len(seleccion)} valores con momentum positivo — "
                    f"la estrategia invierte solo en esos (peso igual).")

    elif estrategia == "🔄 Pullback en tendencia":
        cand = df_m.dropna(subset=["ma200", "rsi"])
        cand = cand[
            (cand["precio"] > cand["ma200"]) &
            (cand["ma50"] > cand["ma200"]) &
            (cand["precio"] >= cand["ma50"] * 0.97) &
            (cand["precio"] <= cand["ma50"] * 1.03) &
            (cand["rsi"] >= 35) & (cand["rsi"] <= 55)
        ].copy()
        if cand.empty:
            st.warning("Sin pullbacks limpios ahora mismo. Es normal: esta señal "
                       "aparece pocas veces — cuando aparece suele ser de calidad.")
            st.stop()
        # Mejor cuanto más cerca de la MA50 y mejor tendencia de fondo
        cand["dist_ma50"] = (cand["precio"] / cand["ma50"] - 1).abs()
        seleccion = cand.sort_values(["dist_ma50"]).head(15).copy()
        seleccion["Señal"] = "Pullback a MA50 · RSI " + seleccion["rsi"].round(0).astype(int).astype(str)
        # Para pullback la entrada óptima ES la MA50
        seleccion["entrada_custom"] = seleccion["ma50"]

    elif estrategia == "🚀 Breakout con volumen":
        cand = df_m.dropna(subset=["high_60_prev", "vol_rel"])
        cand = cand[
            (cand["precio"] >= cand["high_60_prev"]) &
            (cand["vol_rel"] >= 1.5) &
            (cand["atr_pct"] < 8)
        ].copy()
        if cand.empty:
            st.warning("Sin breakouts con volumen hoy. Las rupturas válidas no ocurren "
                       "todos los días — mejor esperar que forzar.")
            st.stop()
        seleccion = cand.sort_values("vol_rel", ascending=False).head(15).copy()
        seleccion["Señal"] = "Breakout 60d · Vol ×" + seleccion["vol_rel"].round(1).astype(str)

    else:  # Filtros manuales
        cand = df_m.copy()
        if f_tendencia:
            cand = cand.dropna(subset=["ma200"])
            cand = cand[(cand["precio"] > cand["ma50"]) & (cand["ma50"] > cand["ma200"])]
        cand = cand.dropna(subset=["mom_3m", "rsi", "atr_pct"])
        cand = cand[
            (cand["mom_3m"] >= f_mom3_min) &
            (cand["rsi"] >= f_rsi[0]) & (cand["rsi"] <= f_rsi[1]) &
            (cand["atr_pct"] <= f_atr_max)
        ]
        if f_vol_rel > 0:
            cand = cand.dropna(subset=["vol_rel"])
            cand = cand[cand["vol_rel"] >= f_vol_rel]
        if cand.empty:
            st.warning("Ningún valor cumple los filtros. Relájalos un poco.")
            st.stop()
        seleccion = cand.sort_values("mom_3m", ascending=False).head(25).copy()
        seleccion["Señal"] = "Filtro manual · Mom3M " + seleccion["mom_3m"].round(1).astype(str) + "%"

    # ── Construir tabla con plan operativo ───────────────────────
    filas = []
    for _, r in seleccion.iterrows():
        entrada = r.get("entrada_custom", r["precio"])
        plan = _plan_operativo(r["precio"], r["atr"], capital, riesgo_pct,
                                entrada=entrada)
        if plan is None:
            continue
        filas.append({
            "Ticker":       r["ticker"],
            "Señal":        r["Señal"],
            "Precio":       round(r["precio"], 2),
            "Entrada":      round(plan["entrada"], 2),
            "Stop":         round(plan["stop"], 2),
            "Stop %":       round(plan["stop_pct"], 1),
            "TP1":          round(plan["tp1"], 2),
            "TP2":          round(plan["tp2"], 2),
            "Acciones":     plan["shares"],
            "Inversión $":  round(plan["inversion"], 0),
            "Riesgo $":     round(plan["riesgo_eur"], 0),
            "Mom 3M %":     round(r["mom_3m"], 1) if pd.notna(r.get("mom_3m")) else None,
            "RSI":          round(r["rsi"], 0) if pd.notna(r.get("rsi")) else None,
            "ATR %":        round(r["atr_pct"], 1) if pd.notna(r.get("atr_pct")) else None,
            "Calmar":       round(r["calmar"], 2) if pd.notna(r.get("calmar")) else None,
        })

    if not filas:
        st.warning("No se pudo generar plan operativo para los candidatos.")
        st.stop()

    df_out = pd.DataFrame(filas)

    # Resumen de cartera propuesta
    inv_total = df_out["Inversión $"].sum()
    riesgo_total = df_out["Riesgo $"].sum()
    col_r1, col_r2, col_r3, col_r4 = st.columns(4)
    col_r1.metric("Candidatos", len(df_out))
    col_r2.metric("Inversión total", f"${inv_total:,.0f}")
    col_r3.metric("% del capital", f"{inv_total/capital*100:.0f}%")
    col_r4.metric("Riesgo total si saltan stops", f"${riesgo_total:,.0f}")

    if inv_total > capital:
        st.warning(f"⚠️ La inversión propuesta (${inv_total:,.0f}) supera tu capital. "
                   f"Elige menos posiciones o reduce el riesgo por operación.")

    st.dataframe(
        df_out.style.map(_color_pct, subset=["Mom 3M %","Stop %"]),
        use_container_width=True, hide_index=True,
        height=min(550, 60 + 36 * len(df_out))
    )

    st.caption(
        "💡 **Cómo leer la tabla:** compra 'Acciones' a precio 'Entrada', "
        "pon el stop en 'Stop' y vende la mitad en TP1 y el resto en TP2. "
        "Si salta el stop pierdes 'Riesgo $' — exactamente lo que configuraste."
    )

    # Botones de seguimiento por ticker
    st.markdown("#### ⭐ Añadir a watchlist")
    cols_btns = st.columns(min(5, len(df_out)))
    for i, (_, row_btn) in enumerate(df_out.head(10).iterrows()):
        with cols_btns[i % len(cols_btns)]:
            if st.button(f"⭐ {row_btn['Ticker']}", key=f"scr_wl_{row_btn['Ticker']}",
                          use_container_width=True):
                nota_wl = f"Screener {estrategia} | {row_btn['Señal']} | Stop: {row_btn['Stop']}"
                if watchlist_add(row_btn["Ticker"], row_btn["Precio"], nota_wl):
                    st.success(f"✅ {row_btn['Ticker']} añadido")
                else:
                    st.info("Ya estaba")

    st.download_button("📥 Descargar plan completo (CSV)",
                       df_out.to_csv(index=False).encode("utf-8"),
                       f"screener_{estrategia.split()[1]}_{datetime.now().strftime('%Y%m%d')}.csv",
                       "text/csv")

    st.session_state["screener_results"] = df_out


# ╔═══════════════════════════════════════════════════════════════╗
# ║  DESCUBRIMIENTO DE MERCADO                                    ║
# ╚═══════════════════════════════════════════════════════════════╝
elif pagina == "🔎 Descubrimiento":
    st.header("🔎 Agente de Descubrimiento de Mercado")
    st.markdown("Detecta movimientos inusuales en **tickers que no están en el foco mediático**. "
                "Elige el universo, los filtros y el tipo de anomalía que buscas.")

    # ── Controles en sidebar ─────────────────────────────────────
    with st.sidebar:
        st.markdown("### ⚙️ Configuración")

        indices_sel = st.multiselect(
            "Índices a analizar",
            options=list(INDICES.keys()),
            default=["SP500"],
            format_func=lambda x: INDICES[x][0]
        )

        n_activos = st.slider(
            "Nº máx. activos a analizar",
            min_value=50, max_value=500, value=150, step=50,
            help="Más activos = análisis más completo pero más lento"
        )

        señales_sel = st.multiselect(
            "Tipos de anomalía",
            options=["VOL_EXTREMO", "GAP_CONTINUATION", "NEW_52W_HIGH",
                     "CONSOLIDATION_BREAK", "DIVERGENCIA"],
            default=["VOL_EXTREMO", "GAP_CONTINUATION", "NEW_52W_HIGH",
                     "CONSOLIDATION_BREAK", "DIVERGENCIA"],
            help="Selecciona qué tipo de movimientos quieres detectar"
        )

        st.divider()
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            min_precio = st.number_input("Precio mín. ($)", value=5.0, min_value=0.5, step=0.5)
        with col_f2:
            min_vol = st.number_input("Vol. mín. (k)", value=300, min_value=50, step=50,
                                       help="Volumen medio 20d mínimo en miles")

        excluir_conocidos = st.checkbox(
            "Excluir megacaps (S&P 100)",
            value=True,
            help="Excluye AAPL, NVDA, TSLA, etc. para forzar el descubrimiento"
        )

        top_n = st.slider("Top resultados", min_value=5, max_value=30, value=10)

        st.divider()

        st.divider()
        st.markdown("**💰 Para el plan operativo**")
        st.session_state["disc_capital"] = st.number_input(
            "Capital ($)", 1000, 10000000,
            st.session_state.get("disc_capital", 10000), step=1000, key="disc_cap_in")
        st.session_state["disc_riesgo"] = st.slider(
            "Riesgo por operación (%)", 0.5, 3.0,
            st.session_state.get("disc_riesgo", 1.0), 0.25, key="disc_rsk_in")
        st.divider()
        analizar_btn = st.button("🚀 Analizar ahora", type="primary",
                                  use_container_width=True)

        # Descargar lista de tickers de los índices seleccionados
        if indices_sel:
            st.divider()
            st.caption("📋 Lista de tickers del universo")
            todos_tickers = []
            for idx_k in indices_sel:
                todos_tickers.extend(INDICES[idx_k][1]())
            todos_tickers = list(dict.fromkeys(todos_tickers))
            df_universo = pd.DataFrame({
                "Ticker": todos_tickers,
                "Índice": [next((INDICES[k][0] for k in indices_sel
                                 if t in INDICES[k][1]()), "?")
                            for t in todos_tickers]
            })
            st.download_button(
                f"📥 Descargar {len(todos_tickers)} tickers",
                df_universo.to_csv(index=False).encode("utf-8"),
                f"universo_descubrimiento.csv",
                "text/csv",
                use_container_width=True
            )

    # ── Tickers a excluir (S&P 100 sobreconocidos) ──────────────
    EXCLUIR_MEGACAPS = {
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

    # ── Función de detección de anomalías ────────────────────────
    def detectar_anomalia(hist, info, spy_chg, señales_activas):
        """Analiza un ticker y retorna su score de anomalía."""
        try:
            if hist.empty or len(hist) < 30:
                return None

            closes  = hist["Close"]
            volumes = hist["Volume"]
            precio  = float(closes.iloc[-1])
            prev_c  = float(closes.iloc[-2])

            ret_d     = (precio / prev_c - 1) * 100
            vol_hoy   = float(volumes.iloc[-1])
            vol_avg20 = float(volumes.tail(21).iloc[:-1].mean())
            vol_ratio = vol_hoy / vol_avg20 if vol_avg20 > 0 else 0

            # MAs
            n50      = min(50, len(closes))
            ma50_val = float(closes.tail(n50).mean())
            ma200_v  = float(closes.tail(200).mean()) if len(closes) >= 200 else None

            # 52w — usar High/Low reales si están disponibles
            highs_s = hist["High"] if "High" in hist.columns else closes
            lows_s  = hist["Low"]  if "Low"  in hist.columns else closes
            n_52w   = min(252, len(closes))
            high_52w = float(highs_s.tail(n_52w).max())
            low_52w  = float(lows_s.tail(n_52w).min())
            rng_52w  = high_52w - low_52w
            pos_52w  = ((precio - low_52w) / rng_52w * 100) if rng_52w > 0 else 50

            # Momentum
            mom_5d  = (precio / float(closes.iloc[-6])  - 1) * 100 if len(closes) > 5  else 0
            mom_20d = (precio / float(closes.iloc[-21]) - 1) * 100 if len(closes) > 20 else 0

            # Consolidación
            rng_60  = closes.tail(60)
            consol  = 60 if (rng_60.max() / rng_60.min() - 1) * 100 < 15 else 0

            # RSI
            rsi_val = None
            try:
                rsi_s   = calc_rsi(closes)
                rsi_val = float(rsi_s.iloc[-1]) if not pd.isna(rsi_s.iloc[-1]) else None
            except Exception:
                pass

            # ATR 14 (para plan operativo)
            atr_val = None
            try:
                tr_d = pd.concat([highs_s - lows_s,
                                  (highs_s - closes.shift(1)).abs(),
                                  (lows_s  - closes.shift(1)).abs()], axis=1).max(axis=1)
                atr_val = float(tr_d.rolling(14).mean().iloc[-1])
            except Exception:
                pass

            # Alpha vs SPY
            alpha = round(ret_d - spy_chg, 2) if spy_chg is not None else None

            # Gap apertura: open de hoy vs cierre de ayer
            open_h  = float(hist["Open"].iloc[-1]) if "Open" in hist.columns else precio
            # Si Open == Close, los datos son sólo cierre — gap no fiable
            if "Open" in hist.columns and abs(open_h - precio) > 0.001:
                gap_pct = (open_h / prev_c - 1) * 100 if prev_c > 0 else 0
            else:
                gap_pct = 0  # sin datos OHLC reales no calculamos gap

            # ── SCORES ──────────────────────────────────────────
            scores = {}

            if "VOL_EXTREMO" in señales_activas:
                if   vol_ratio >= 10: scores["VOL_EXTREMO"] = 100
                elif vol_ratio >= 5:  scores["VOL_EXTREMO"] = 75
                elif vol_ratio >= 3:  scores["VOL_EXTREMO"] = 50

            if "GAP_CONTINUATION" in señales_activas:
                if (abs(gap_pct) > 2 and abs(ret_d) > 2 and
                        np.sign(gap_pct) == np.sign(ret_d)):
                    scores["GAP_CONTINUATION"] = min(100, abs(gap_pct) * 15)

            if "NEW_52W_HIGH" in señales_activas:
                if precio >= high_52w * 0.995 and vol_ratio >= 1.5:
                    scores["NEW_52W_HIGH"] = min(100, vol_ratio * 20)

            if "CONSOLIDATION_BREAK" in señales_activas:
                if consol >= 40 and abs(ret_d) > 3 and vol_ratio >= 2:
                    scores["CONSOLIDATION_BREAK"] = min(100, abs(ret_d)*10 + vol_ratio*5)

            if "DIVERGENCIA" in señales_activas and spy_chg is not None and alpha is not None:
                if abs(alpha) > 3 and np.sign(ret_d) != np.sign(spy_chg):
                    scores["DIVERGENCIA"] = min(100, abs(alpha) * 8)

            if not scores:
                return None

            n_sig = len(scores)
            score = min(100, max(scores.values()) + 10 * (n_sig - 1))

            return {
                "Ticker":       ticker_d,
                "Precio":       round(precio, 2),
                "Cambio %":     round(ret_d, 2),
                "Alpha":        alpha,
                "Vol×":         round(vol_ratio, 2),
                "RSI":          round(rsi_val, 1) if rsi_val else None,
                "Pos 52w %":    round(pos_52w, 1),
                "Mom 5d %":     round(mom_5d, 1),
                "Mom 20d %":    round(mom_20d, 1),
                "vs MA50 %":    round((precio/ma50_val-1)*100, 1) if ma50_val else None,
                "Score":        round(score, 1),
                "ATR":          round(atr_val, 2) if atr_val else None,
                "Señales":      " + ".join(scores.keys()),
                "Principal":    max(scores.items(), key=lambda x: x[1])[0],
                "_scores":      scores,
                "_n_signals":   n_sig,
            }
        except Exception:
            return None

    # ── Estado inicial ───────────────────────────────────────────
    if not analizar_btn:
        # Mostrar info de índices disponibles
        st.markdown("### 📋 Índices disponibles")
        cols_idx = st.columns(4)
        for i, (k, (nombre, _)) in enumerate(INDICES.items()):
            cols_idx[i % 4].markdown(f"**{nombre}**")

        st.divider()
        st.info("⬅️ Configura los parámetros en el panel izquierdo y pulsa **🚀 Analizar ahora**")

        st.markdown("### 🔍 ¿Qué detecta cada señal?")
        señal_info = {
            "🔊 VOL_EXTREMO":         "Volumen ≥3x/5x/10x la media de 20 días. Indica interés institucional inusual.",
            "⚡ GAP_CONTINUATION":    "Brecha de apertura >2% que continúa en la misma dirección. Reacción fuerte a noticia.",
            "🏆 NEW_52W_HIGH":        "Precio en zona de máximo de 52 semanas con volumen. Ruptura de resistencia clave.",
            "🚀 CONSOLIDATION_BREAK": "Ruptura tras 40+ días en rango estrecho (<15%). Energía acumulada liberada.",
            "🌊 DIVERGENCIA":         "Sube >3% sobre el SPY cuando el mercado cae (o viceversa). Catalizador idiosincrático.",
        }
        for señal, desc in señal_info.items():
            st.markdown(f"**{señal}:** {desc}")

    else:
        # ── ANÁLISIS ────────────────────────────────────────────
        if not indices_sel:
            st.warning("Selecciona al menos un índice en el panel izquierdo.")
            st.stop()
        if not señales_sel:
            st.warning("Selecciona al menos un tipo de anomalía.")
            st.stop()

        # 1. Construir universo con diagnóstico por índice
        with st.spinner("📋 Construyendo universo de tickers..."):
            universo_raw = []
            indices_breakdown = []
            for idx_key in indices_sel:
                nombre, fn = INDICES[idx_key]
                tks = fn()
                indices_breakdown.append(f"{nombre}: **{len(tks)}**")
                universo_raw.extend(tks)

            universo_raw = list(dict.fromkeys(universo_raw))  # deduplicar

            if excluir_conocidos:
                universo = [t for t in universo_raw if t not in EXCLUIR_MEGACAPS]
            else:
                universo = universo_raw

            # Limitar al máximo seleccionado
            universo = universo[:n_activos]

        st.info(
            f"🌐 **Universo construido:** " + " · ".join(indices_breakdown) + "  \n"
            f"Total únicos: **{len(universo_raw)}** "
            f"→ **{len(universo)} a analizar** "
            f"({'excl. megacaps S&P 100' if excluir_conocidos else 'incl. megacaps'})"
        )

        # 2. SPY como referencia
        spy_chg_d = None
        try:
            spy_h, _ = descargar("^GSPC", "5d")
            if not spy_h.empty and len(spy_h) >= 2:
                spy_chg_d = round((spy_h["Close"].iloc[-1] / spy_h["Close"].iloc[-2] - 1) * 100, 2)
        except Exception:
            pass

        spy_col = "green" if (spy_chg_d or 0) >= 0 else "red"
        st.markdown(f"**S&P 500 hoy:** :{spy_col}[{spy_chg_d:+.2f}%]" if spy_chg_d else "**S&P 500:** N/A")

        # 3. Descarga bulk
        st.markdown("### ⏳ Descargando datos...")
        prog_bar = st.progress(0, text="Iniciando descarga bulk...")

        resultados_d = []
        n_ok_d = 0
        n_err_d = 0

        # Bulk download en lotes de 100 para eficiencia
        LOTE = 100
        n_lotes = (len(universo) + LOTE - 1) // LOTE

        all_closes  = {}
        all_volumes = {}
        all_opens   = {}
        all_highs   = {}
        all_lows    = {}

        for i_lote in range(n_lotes):
            batch = universo[i_lote * LOTE : (i_lote + 1) * LOTE]
            prog_bar.progress(
                int((i_lote / n_lotes) * 60),
                text=f"Descargando lote {i_lote+1}/{n_lotes} ({len(batch)} tickers)..."
            )
            try:
                raw = yf.download(
                    batch, period="6mo",
                    auto_adjust=True, progress=False,
                )
                if raw.empty:
                    n_err_d += len(batch)
                    continue

                for t in batch:
                    try:
                        if len(batch) == 1:
                            # Un solo ticker — columnas planas
                            c = raw["Close"].dropna() if "Close" in raw.columns else pd.Series(dtype=float)
                            v = raw["Volume"].dropna() if "Volume" in raw.columns else pd.Series(dtype=float)
                            o = raw["Open"].dropna()  if "Open"  in raw.columns else c
                            h = raw["High"].dropna()  if "High"  in raw.columns else c
                            lo = raw["Low"].dropna()  if "Low"   in raw.columns else c
                        else:
                            # Múltiples tickers — MultiIndex (field, ticker)
                            if isinstance(raw.columns, pd.MultiIndex):
                                tickers_available = raw.columns.get_level_values(1).unique()
                                if t not in tickers_available:
                                    continue
                                c = raw["Close"][t].dropna()
                                v = raw["Volume"][t].dropna()
                                o = raw["Open"][t].dropna()
                                h = raw["High"][t].dropna()
                                lo = raw["Low"][t].dropna()
                            else:
                                continue

                        if len(c) >= 30:
                            all_closes[t]  = c
                            all_volumes[t] = v
                            all_opens[t]   = o
                            all_highs[t]   = h
                            all_lows[t]    = lo
                        else:
                            n_err_d += 1
                    except Exception:
                        n_err_d += 1
            except Exception as e_batch:
                n_err_d += len(batch)
            time.sleep(0.3)

        prog_bar.progress(65, text="Detectando anomalías...")

        # 4. Detectar anomalías
        min_vol_abs = min_vol * 1_000

        for i_t, ticker_d in enumerate(all_closes.keys()):
            if i_t % 20 == 0:
                pct = 65 + int((i_t / max(len(all_closes), 1)) * 30)
                prog_bar.progress(pct, text=f"Analizando {ticker_d}... ({i_t}/{len(all_closes)})")

            closes_s  = all_closes[ticker_d]
            volumes_s = all_volumes.get(ticker_d, pd.Series(dtype=float))

            if len(closes_s) < 30 or closes_s.iloc[-1] < min_precio:
                continue
            if volumes_s.empty or float(volumes_s.tail(20).mean()) < min_vol_abs:
                continue

            # Construir hist DataFrame con OHLC real
            hist_d = pd.DataFrame({
                "Close":  closes_s,
                "Volume": volumes_s,
                "Open":   all_opens.get(ticker_d, closes_s),
                "High":   all_highs.get(ticker_d, closes_s),
                "Low":    all_lows.get(ticker_d, closes_s),
            }).dropna()

            r = detectar_anomalia(hist_d, {}, spy_chg_d, señales_sel)
            if r:
                resultados_d.append(r)
                n_ok_d += 1

        prog_bar.progress(100, text="¡Análisis completado!")
        time.sleep(0.3)
        prog_bar.empty()

        # 5. Mostrar resultados
        st.markdown(f"### 🏆 Resultados — {n_ok_d} anomalías detectadas en {len(all_closes)} tickers")

        if not resultados_d:
            st.warning("No se detectaron anomalías con los parámetros actuales. "
                       "Prueba reduciendo los filtros o cambiando el índice.")
        else:
            # Ordenar por score
            resultados_d.sort(key=lambda x: x["Score"], reverse=True)
            top_res = resultados_d[:top_n]

            # Guardar para que Outlook pueda mostrarlos
            st.session_state["descubrimiento_resultados"] = resultados_d
            st.session_state["descubrimiento_fecha"]      = datetime.now()

            # Métricas resumen
            col_m1, col_m2, col_m3, col_m4 = st.columns(4)
            col_m1.metric("Tickers analizados", len(all_closes))
            col_m2.metric("Anomalías detectadas", n_ok_d)
            col_m3.metric("Top mostrados", len(top_res))
            col_m4.metric("Score máx.", top_res[0]["Score"] if top_res else 0)

            st.divider()

            # Cards por ticker
            SEÑAL_EMOJI = {
                "VOL_EXTREMO":         "🔊",
                "GAP_CONTINUATION":    "⚡",
                "NEW_52W_HIGH":        "🏆",
                "CONSOLIDATION_BREAK": "🚀",
                "DIVERGENCIA":         "🌊",
            }
            SEÑAL_DESC = {
                "VOL_EXTREMO":         "Volumen extremo — institucional posiblemente entrando",
                "GAP_CONTINUATION":    "Brecha + continuación — reacción fuerte a noticia",
                "NEW_52W_HIGH":        "Nuevo máximo 52 semanas con volumen",
                "CONSOLIDATION_BREAK": "Ruptura tras consolidación larga",
                "DIVERGENCIA":         "Diverge del mercado — catalizador idiosincrático",
            }

            for i, r in enumerate(top_res, 1):
                primary  = r["Principal"]
                emoji    = SEÑAL_EMOJI.get(primary, "📊")
                cambio_c = "🟢" if r["Cambio %"] > 0 else "🔴"
                ticker_r = r["Ticker"]

                with st.expander(
                    f"#{i} {emoji} **{ticker_r}** — {cambio_c} {r['Cambio %']:+.2f}%  "
                    f"| Vol×{r['Vol×']}  | Score: {r['Score']}/100",
                    expanded=(i <= 3)
                ):
                    col_a, col_b, col_c = st.columns(3)
                    with col_a:
                        st.metric("Precio", f"${r['Precio']}")
                        st.metric("Cambio día", f"{r['Cambio %']:+.2f}%")
                        if r["Alpha"] is not None:
                            st.metric("Alpha vs SPY", f"{r['Alpha']:+.2f}%")
                    with col_b:
                        st.metric("Volumen relativo", f"{r['Vol×']}×")
                        st.metric("RSI", r["RSI"] or "N/A")
                        st.metric("Posición 52w", f"{r['Pos 52w %']}%")
                    with col_c:
                        st.metric("Mom 5 días", f"{r['Mom 5d %']:+.1f}%")
                        st.metric("Mom 20 días", f"{r['Mom 20d %']:+.1f}%")
                        if r["vs MA50 %"] is not None:
                            st.metric("vs MA50", f"{r['vs MA50 %']:+.1f}%")

                    st.markdown(f"**Señales detectadas:** {r['Señales']}")
                    st.caption(f"💡 {SEÑAL_DESC.get(primary, '')}")

                    # ── PLAN OPERATIVO — convierte la señal en algo invertible ──
                    if r.get("ATR"):
                        cap_disc  = st.session_state.get("disc_capital", 10000)
                        rsk_disc  = st.session_state.get("disc_riesgo", 1.0)
                        plan_d = _plan_operativo(r["Precio"], r["ATR"],
                                                  cap_disc, rsk_disc)
                        if plan_d and plan_d["shares"] > 0:
                            st.markdown(
                                f"<div style='background:#16213e;padding:10px;border-radius:6px;"
                                f"margin:8px 0;border-left:3px solid #f1fa8c'>"
                                f"<b style='color:#f1fa8c'>📋 Plan operativo</b> "
                                f"<span style='color:#888;font-size:11px'>(capital ${cap_disc:,.0f}, riesgo {rsk_disc}%)</span><br>"
                                f"<span style='color:#e8e8e8;font-size:13px'>"
                                f"Comprar <b>{plan_d['shares']} acciones</b> a {plan_d['entrada']:.2f} "
                                f"(inversión ${plan_d['inversion']:,.0f}) · "
                                f"Stop: <b style='color:#ff5555'>{plan_d['stop']:.2f}</b> ({plan_d['stop_pct']:.1f}%) · "
                                f"TP1: <b style='color:#50fa7b'>{plan_d['tp1']:.2f}</b> · "
                                f"TP2: <b style='color:#50fa7b'>{plan_d['tp2']:.2f}</b><br>"
                                f"Si salta el stop pierdes <b>${plan_d['riesgo_eur']:,.0f}</b>"
                                f"</span></div>",
                                unsafe_allow_html=True)

                    # Links de investigación
                    ticker_clean = ticker_r.split(".")[0]
                    col_l1, col_l2, col_l3, col_l4 = st.columns(4)
                    col_l1.link_button("📊 Finviz",
                        f"https://finviz.com/quote.ashx?t={ticker_clean}",
                        use_container_width=True)
                    col_l2.link_button("📰 Noticias",
                        f"https://finance.yahoo.com/quote/{ticker_r}/news",
                        use_container_width=True)
                    col_l3.link_button("📈 Fundamentales",
                        f"https://stockanalysis.com/stocks/{ticker_clean.lower()}/",
                        use_container_width=True)
                    col_l4.link_button("🕯️ Chart",
                        f"https://www.tradingview.com/chart/?symbol={ticker_r}",
                        use_container_width=True)

                    # Botón añadir a watchlist
                    if st.button(f"⭐ Seguir {ticker_r}",
                                 key=f"wl_disc_{ticker_r}_{i}",
                                 use_container_width=True):
                        err = _get_watchlist_error()
                        if err:
                            st.error(f"⚠️ {err}")
                        else:
                            signal_str = r["Señales"]
                            notes = f"Detectado por agente — {signal_str} | Score: {r['Score']}/100"
                            if watchlist_add(ticker_r, r["Precio"], notes):
                                st.success(f"✅ {ticker_r} añadido")
                                st.balloons()
                            else:
                                st.info(f"ℹ️ {ticker_r} ya estaba")

            st.divider()

            # Tabla resumen
            st.markdown("### 📋 Tabla resumen")
            df_res = pd.DataFrame([{
                "Ticker":    r["Ticker"],
                "Precio":    r["Precio"],
                "Cambio %":  r["Cambio %"],
                "Alpha %":   r["Alpha"],
                "Vol×":      r["Vol×"],
                "RSI":       r["RSI"],
                "Pos 52w %": r["Pos 52w %"],
                "Mom 5d %":  r["Mom 5d %"],
                "Score":     r["Score"],
                "Señales":   r["Señales"],
            } for r in top_res])


            st.dataframe(
                df_res.style
                    .map(_color_pct, subset=["Cambio %","Alpha %","Mom 5d %"])
                    .map(_color_score,  subset=["Score"]),
                use_container_width=True,
                hide_index=True
            )

            st.caption(
                f"📡 Datos: yfinance | "
                f"Universo: {', '.join([INDICES[k][0] for k in indices_sel])} | "
                f"Señales: {', '.join(señales_sel)} | "
                f"Filtros: precio>${min_precio}, vol>{min_vol}k | "
                f"Actualizado: {datetime.now().strftime('%H:%M UTC')}"
            )

            # ── Descarga CSV de resultados ─────────────────────────
            st.divider()
            col_dl1, col_dl2 = st.columns(2)
            with col_dl1:
                st.download_button(
                    f"📥 Descargar TOP {len(top_res)} (CSV)",
                    df_res.to_csv(index=False).encode("utf-8"),
                    f"descubrimiento_top_{datetime.now().strftime('%Y-%m-%d')}.csv",
                    "text/csv",
                    use_container_width=True,
                    type="primary"
                )
            with col_dl2:
                # Descargar TODAS las anomalías (no solo top N)
                df_all = pd.DataFrame([{
                    "Ticker":    r["Ticker"],
                    "Precio":    r["Precio"],
                    "Cambio %":  r["Cambio %"],
                    "Alpha %":   r["Alpha"],
                    "Vol×":      r["Vol×"],
                    "RSI":       r["RSI"],
                    "Pos 52w %": r["Pos 52w %"],
                    "Mom 5d %":  r["Mom 5d %"],
                    "Mom 20d %": r["Mom 20d %"],
                    "vs MA50 %": r["vs MA50 %"],
                    "Score":     r["Score"],
                    "Señales":   r["Señales"],
                } for r in resultados_d])
                st.download_button(
                    f"📥 Descargar TODAS ({len(resultados_d)}) (CSV)",
                    df_all.to_csv(index=False).encode("utf-8"),
                    f"descubrimiento_all_{datetime.now().strftime('%Y-%m-%d')}.csv",
                    "text/csv",
                    use_container_width=True
                )


# ╔═══════════════════════════════════════════════════════════════╗
# ║  WATCHLIST — Tickers seguidos con tracking histórico          ║
# ╚═══════════════════════════════════════════════════════════════╝
elif pagina == "⭐ Watchlist":
    st.header("⭐ Mi Watchlist")
    st.markdown("Tickers que estás siguiendo de cerca con tracking de su evolución desde que los añadiste.")

    # ═══════════════════════════════════════════════════════════════
    # PANEL DE DIAGNÓSTICO (siempre visible)
    # ═══════════════════════════════════════════════════════════════
    with st.expander("🔧 Estado de la conexión", expanded=False):
        col_d1, col_d2, col_d3 = st.columns(3)

        # 1. Librerías
        with col_d1:
            if GSPREAD_AVAILABLE:
                st.success("✅ Librerías OK")
                st.caption("gspread + google-auth instaladas")
            else:
                st.error("❌ Librerías NO instaladas")
                st.caption("Falta gspread/google-auth en requirements.txt")

        # 2. Secret
        with col_d2:
            try:
                if "gcp_service_account" in st.secrets:
                    creds_d = dict(st.secrets["gcp_service_account"])
                    if creds_d.get("client_email"):
                        st.success("✅ Secret OK")
                        email = creds_d["client_email"]
                        st.caption(f"📧 {email[:30]}...")
                    else:
                        st.error("❌ Secret incompleto")
                        st.caption("Falta client_email")
                else:
                    st.error("❌ Secret NO configurado")
                    st.caption("Falta [gcp_service_account]")
            except Exception as e:
                st.error("❌ Error en secrets")
                st.caption(str(e)[:50])

        # 3. Conexión real al Sheet
        with col_d3:
            try:
                ws = get_watchlist_ws()
                if ws is not None:
                    st.success("✅ Sheet conectado")
                    st.caption(f"Pestaña: {WATCHLIST_TAB}")
                else:
                    st.error("❌ No conecta al Sheet")
                    last_err = st.session_state.get("_watchlist_last_error", "Desconocido")
                    st.caption(str(last_err)[:50])
            except Exception as e:
                st.error("❌ Error conexión")
                st.caption(str(e)[:50])

        st.caption(
            "💡 **¿Algo en rojo?** Streamlit Cloud → ⋮ → Settings → "
            "Secrets — añade `[gcp_service_account]` con las credenciales del "
            "Service Account. Email del Service Account debe tener acceso al Google Sheet."
        )

    # Diagnóstico de conexión
    err = _get_watchlist_error()
    if err:
        st.error(f"⚠️ **Watchlist no disponible:** {err}")
        with st.expander("🔧 Cómo solucionarlo"):
            st.markdown("""
            **Si dice 'gspread no instalada':**
            - Asegúrate de que `gspread` y `google-auth` están en `requirements.txt`
            - Reinicia la app desde Streamlit Cloud → Manage app → Reboot

            **Si dice 'Falta secret gcp_service_account':**
            - Ve a Streamlit Cloud → Settings → Secrets
            - Añade el bloque `[gcp_service_account]` con las credenciales del Service Account
            - El email `tfm-agent@tfm-agent.iam.gserviceaccount.com` debe tener acceso al Sheet
            """)
        st.stop()

    # ── Cargar watchlist desde Google Sheets ─────────────────────
    df_wl_db = watchlist_load()

    # ── AUTO-REPARACIÓN: deja que la función de reparación decida qué arreglar ──
    # (detecta: precio <=0, >10000, o ratio >5x respecto precio actual)
    if not df_wl_db.empty:
        # Llamar siempre — la función internamente solo repara lo que necesita
        with st.spinner("🔧 Verificando integridad de precios en watchlist..."):
            n_ok, _, errs_rep = watchlist_reparar_precios()
        if n_ok > 0:
            st.success(f"✅ Auto-reparados {n_ok} precios desde histórico")
            df_wl_db = watchlist_load()  # recargar tras reparación
        elif errs_rep:
            with st.expander(f"⚠️ {len(errs_rep)} precios no se pudieron reparar"):
                for e in errs_rep:
                    st.caption(f"• {e}")

    # Mostrar error si hubo problema en la carga
    last_err = st.session_state.get("_watchlist_last_error")
    if last_err:
        st.warning(f"⚠️ Hubo un error en la última operación: {last_err}")
        if st.button("Limpiar error"):
            st.session_state.pop("_watchlist_last_error", None)
            st.rerun()

    with st.sidebar:
        st.markdown("### ➕ Añadir ticker")
        new_t = st.text_input("Ticker (ej: AAPL, NESN.SW)", key="add_wl_input")
        new_n = st.text_area("Nota (opcional)", key="notes_wl_input",
                              placeholder="¿Por qué estás siguiendo este ticker?")

        if st.button("Añadir a watchlist", type="primary", use_container_width=True):
            if new_t and len(new_t.strip()) > 0:
                t_clean = new_t.strip().upper()
                precio_inicial, error_p = _get_precio_actual(t_clean)
                if precio_inicial is not None:
                    if watchlist_add(t_clean, precio_inicial, new_n):
                        st.success(f"✅ {t_clean} añadido (${precio_inicial:.2f})")
                        st.rerun()
                    else:
                        st.warning(f"⚠️ {t_clean} ya está en la watchlist")
                else:
                    st.error(f"No se pudo obtener precio de {t_clean}")
                    st.caption(f"Detalles: {error_p}")
            else:
                st.error("Introduce un ticker válido")

        st.divider()
        st.caption(f"📋 {len(df_wl_db)} tickers en seguimiento")

        if not df_wl_db.empty:
            st.download_button(
                "📥 Descargar watchlist",
                df_wl_db.to_csv(index=False).encode("utf-8"),
                "watchlist.csv",
                "text/csv",
                use_container_width=True
            )

            with st.expander("🗑️ Vaciar watchlist completa"):
                st.warning("Esto borrará todos los tickers seguidos.")
                if st.button("Confirmar vaciado", type="secondary"):
                    if watchlist_clear():
                        st.success("Watchlist vaciada")
                        st.rerun()

    if df_wl_db.empty:
        st.info(
            "👋 **Tu watchlist está vacía.**\n\n"
            "Añade tickers desde el panel lateral, o desde:\n"
            "- 🔎 **Descubrimiento** — botón ⭐ Seguir en cada anomalía\n"
            "- 📈 **Análisis Individual** — botón ⭐ Añadir tras analizar"
        )
        st.stop()

    # ── Descargar precios actuales ───────────────────────────────
    st.markdown(f"### 📊 Estado actual de tus {len(df_wl_db)} tickers")

    col_top1, col_top2 = st.columns([1, 4])
    with col_top1:
        refresh_btn = st.button("🔄 Actualizar", type="primary", use_container_width=True)

    if refresh_btn or "wl_data" not in st.session_state:
        with st.spinner(f"Descargando datos de {len(df_wl_db)} tickers..."):
            tickers_list = df_wl_db["ticker"].tolist()
            try:
                # SPY de referencia
                spy_h, _ = descargar("^GSPC", "3mo")
                st.session_state["wl_spy"] = spy_h

                if len(tickers_list) == 1:
                    h, _ = descargar(tickers_list[0], "3mo")
                    if not h.empty:
                        st.session_state["wl_data"] = {tickers_list[0]: h}
                    else:
                        st.session_state["wl_data"] = {}
                else:
                    bulk = yf.download(tickers_list, period="3mo",
                                       auto_adjust=True, progress=False)
                    wl_data = {}
                    for t in tickers_list:
                        try:
                            if isinstance(bulk.columns, pd.MultiIndex):
                                if t in bulk.columns.get_level_values(1):
                                    h = pd.DataFrame({
                                        "Close": bulk["Close"][t],
                                        "Volume": bulk["Volume"][t] if "Volume" in bulk.columns.get_level_values(0) else pd.Series()
                                    }).dropna()
                                    if not h.empty:
                                        wl_data[t] = h
                        except Exception:
                            continue
                    st.session_state["wl_data"] = wl_data

                st.session_state["wl_last_refresh"] = datetime.now()
            except Exception as e:
                st.error(f"Error descargando: {e}")
                st.stop()

    wl_data = st.session_state.get("wl_data", {})
    spy_h   = st.session_state.get("wl_spy", pd.DataFrame())
    last_refresh = st.session_state.get("wl_last_refresh", datetime.now())

    with col_top2:
        st.caption(f"🕐 Última actualización: {last_refresh.strftime('%H:%M %d-%m-%Y')}")

    if not wl_data:
        st.warning("No se pudieron obtener datos. Pulsa Actualizar.")
        st.stop()

    # ── Construir tabla de tracking ──────────────────────────────
    rows = []
    for _, row_db in df_wl_db.iterrows():
        t          = row_db["ticker"]
        d_alta_str = row_db["fecha_anadido"]

        # Parser ROBUSTO de precio_inicial — maneja strings, comas decimales, etc.
        precio_alta = None
        try:
            raw = row_db["precio_inicial"]
            if raw is not None and raw != "" and not pd.isna(raw):
                # Si es string, normalizar (sustituir coma por punto si aplica)
                if isinstance(raw, str):
                    raw_clean = raw.strip().replace(",", ".")
                    if raw_clean:
                        precio_alta = float(raw_clean)
                else:
                    precio_alta = float(raw)
                # Validar que el valor es sensato
                if precio_alta is not None and precio_alta <= 0:
                    precio_alta = None
                # Defensa contra bug regional: si el precio es absurdamente grande, descartar
                # (BRK.A es la única excepción legítima >10000)
                if (precio_alta is not None and precio_alta > 10000
                    and t.upper() not in ("BRK.A", "BRK-A")):
                    precio_alta = None
        except Exception:
            precio_alta = None

        if t not in wl_data:
            continue
        h = wl_data[t]
        if h.empty or len(h) < 2:
            continue

        try:
            closes  = h["Close"]
            precio  = float(closes.iloc[-1])
            ayer    = float(closes.iloc[-2])
            chg_dia = (precio / ayer - 1) * 100

            # Retorno desde alta
            ret_alta = None
            if precio_alta and precio_alta > 0:
                ret_alta = (precio / precio_alta - 1) * 100

            # Días en watchlist
            dias = ""
            try:
                f_alta = datetime.strptime(d_alta_str, "%Y-%m-%d")
                dias = (datetime.now() - f_alta).days
            except Exception:
                pass

            # Alpha vs SPY desde alta
            alpha_spy = None
            try:
                if not spy_h.empty and d_alta_str:
                    f_alta_dt = pd.to_datetime(d_alta_str)
                    spy_desde = spy_h[spy_h.index >= f_alta_dt]
                    if len(spy_desde) >= 2 and ret_alta is not None:
                        spy_ret = (float(spy_desde["Close"].iloc[-1]) /
                                   float(spy_desde["Close"].iloc[0]) - 1) * 100
                        alpha_spy = ret_alta - spy_ret
            except Exception:
                pass

            # Mom y pos 52w
            mom_5d  = (precio/float(closes.iloc[-6])-1)*100  if len(closes)>5  else None
            mom_20d = (precio/float(closes.iloc[-21])-1)*100 if len(closes)>20 else None

            high_52w = float(closes.tail(min(252, len(closes))).max())
            low_52w  = float(closes.tail(min(252, len(closes))).min())
            rng_52w  = high_52w - low_52w
            pos_52w  = ((precio - low_52w)/rng_52w*100) if rng_52w > 0 else 50

            rows.append({
                "Ticker":       t,
                "Precio":       round(precio, 2),
                "Precio alta":  round(precio_alta, 2) if precio_alta is not None else None,
                "Hoy %":        round(chg_dia, 2),
                "Desde alta %": round(ret_alta, 2) if ret_alta is not None else None,
                "Alpha SPY %":  round(alpha_spy, 2) if alpha_spy is not None else None,
                "Mom 5d %":     round(mom_5d, 1)  if mom_5d  is not None else None,
                "Mom 20d %":    round(mom_20d, 1) if mom_20d is not None else None,
                "Pos 52w %":    round(pos_52w, 1),
                "Días":         dias,
                "Alta":         d_alta_str,
                "Nota":         row_db.get("nota", "") or "",
            })
        except Exception:
            continue

    if not rows:
        st.warning("No se pudieron procesar datos.")
        st.stop()

    df_wl_view = pd.DataFrame(rows)

    # Métricas resumen
    col_m1, col_m2, col_m3, col_m4 = st.columns(4)
    col_m1.metric("Tickers", len(df_wl_view))
    col_m2.metric("Subidas hoy", int((df_wl_view["Hoy %"] > 0).sum()))
    if "Desde alta %" in df_wl_view.columns:
        winners = df_wl_view["Desde alta %"].dropna()
        if len(winners) > 0:
            col_m3.metric(
                "En verde desde alta",
                f"{int((winners > 0).sum())}/{len(winners)}"
            )
    if "Alpha SPY %" in df_wl_view.columns:
        alpha_pos = df_wl_view["Alpha SPY %"].dropna()
        if len(alpha_pos) > 0:
            col_m4.metric(
                "Batiendo al SPY",
                f"{int((alpha_pos > 0).sum())}/{len(alpha_pos)}"
            )

    # Tabla con coloreado
    st.markdown("### 📊 Tabla de seguimiento")


    st.dataframe(
        df_wl_view.style.map(
            _color_pct,
            subset=["Hoy %","Desde alta %","Alpha SPY %","Mom 5d %","Mom 20d %"]
        ),
        use_container_width=True,
        hide_index=True,
        height=min(500, 60 + 36 * len(df_wl_view))
    )

    # ── Detalle individual ───────────────────────────────────────
    st.divider()
    st.markdown("### 🔍 Detalle individual")

    sel_ticker = st.selectbox(
        "Selecciona un ticker para ver detalles",
        options=df_wl_db["ticker"].tolist(),
        format_func=lambda x: f"⭐ {x}"
    )

    if sel_ticker and sel_ticker in wl_data:
        col_d1, col_d2 = st.columns([3, 1])
        h = wl_data[sel_ticker]

        with col_d1:
            try:
                fig = px.line(x=h.index, y=h["Close"].values,
                              title=f"📈 {sel_ticker} — últimos 3 meses",
                              labels={"x": "Fecha", "y": "Precio ($)"})

                row_db = df_wl_db[df_wl_db["ticker"] == sel_ticker].iloc[0]
                d_alta_str = row_db["fecha_anadido"]
                if d_alta_str:
                    try:
                        f_alta = pd.to_datetime(d_alta_str)
                        if f_alta >= h.index.min():
                            fig.add_vline(
                                x=f_alta, line_dash="dash",
                                line_color="#8be9fd",
                                annotation_text="Añadido",
                                annotation_position="top"
                            )
                    except Exception:
                        pass

                fig.update_layout(
                    template="plotly_dark", height=400,
                    paper_bgcolor="#12121f", plot_bgcolor="#1e1e2e"
                )
                st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.error(f"Error gráfico: {e}")

        with col_d2:
            row_db = df_wl_db[df_wl_db["ticker"] == sel_ticker].iloc[0]
            st.markdown(f"**📅 Añadido:** {row_db['fecha_anadido']}")
            st.markdown(f"**💰 Precio inicial:** ${row_db['precio_inicial']}")

            nota = row_db.get("nota", "")
            if nota and str(nota).strip():
                st.markdown("**📝 Nota:**")
                st.info(nota)

            st.divider()
            ticker_clean = sel_ticker.split(".")[0]
            st.link_button("📊 Finviz",
                f"https://finviz.com/quote.ashx?t={ticker_clean}",
                use_container_width=True)
            st.link_button("📰 Noticias",
                f"https://finance.yahoo.com/quote/{sel_ticker}/news",
                use_container_width=True)

            st.divider()
            if st.button("🗑️ Quitar de watchlist",
                         type="secondary",
                         use_container_width=True,
                         key=f"del_{sel_ticker}"):
                if watchlist_remove(sel_ticker):
                    st.success(f"❌ {sel_ticker} eliminado")
                    st.session_state.pop("wl_data", None)
                    st.rerun()


# ╔═══════════════════════════════════════════════════════════════╗
# ║  ANÁLISIS INDIVIDUAL                                          ║
# ╚═══════════════════════════════════════════════════════════════╝
elif pagina == "📈 Análisis Individual":
    # ── Sidebar ────────────────────────────────────────────────────
    with st.sidebar:
        ticker_in = st.text_input("Ticker", value="AAPL",
                                   help="Ej: AAPL, NESN.SW, 7203.T")
        st.markdown("**💰 Para el plan de entrada**")
        ai_importe = st.number_input("Importe a invertir ($)", 100, 10000000, 1000,
                                      step=100, key="ai_cap",
                                      help="Lo que piensas meter en esta operación. "
                                           "La app te dirá cuánto ganas o pierdes en cada nivel.")
        st.divider()
        go_btn = st.button("🚀 Analizar", type="primary", use_container_width=True)

        if GSPREAD_AVAILABLE and ticker_in:
            t_clean_btn = ticker_in.upper().strip()
            if st.button(f"⭐ Seguir {t_clean_btn}",
                          use_container_width=True, key="wl_ai_btn"):
                err = _get_watchlist_error()
                if err:
                    st.error(f"⚠️ Watchlist no disponible: {err}")
                else:
                    precio_inicial, error_p = _get_precio_actual(t_clean_btn)
                    if precio_inicial is not None:
                        if watchlist_add(t_clean_btn, precio_inicial,
                                         "Añadido desde Análisis Individual"):
                            st.success(f"✅ {t_clean_btn} añadido (${precio_inicial:.2f})")
                        else:
                            st.info(f"ℹ️ {t_clean_btn} ya estaba en watchlist")
                    else:
                        st.error(f"No se pudo obtener precio de {t_clean_btn}")

    # ── Pantalla de bienvenida ─────────────────────────────────────
    if not go_btn or not ticker_in:
        st.header("📈 Análisis Individual de Acciones")
        st.markdown(
            "**Análisis 360°** — técnico + fundamental + macro + peers para tomar "
            "decisiones informadas con un veredicto claro."
        )
        cols_intro = st.columns(5)
        intros = [
            ("📋", "Veredicto", "Recomendación clara"),
            ("📊", "Score 0-100", "5 dimensiones"),
            ("🌍", "Macro", "Ciclo económico LEI"),
            ("🎯", "Niveles", "Entrada · Stop · TP"),
            ("🆚", "Peers", "Compara con sector"),
        ]
        for i, (emoji, title, desc) in enumerate(intros):
            cols_intro[i].markdown(
                f"<div style='background:#1e1e2e;padding:14px;border-radius:8px;text-align:center;height:110px'>"
                f"<div style='font-size:26px'>{emoji}</div>"
                f"<div style='color:#8be9fd;font-weight:bold;margin-top:6px;font-size:13px'>{title}</div>"
                f"<div style='font-size:11px;color:#bbb;margin-top:4px'>{desc}</div>"
                f"</div>", unsafe_allow_html=True)

        st.info("⬅️ Introduce un ticker y pulsa **🚀 Analizar**")
        st.stop()

    # ═══════════════════════════════════════════════════════════════
    # ANÁLISIS COMPLETO
    # ═══════════════════════════════════════════════════════════════
    ticker_in = ticker_in.upper().strip()

    # CSS responsive móvil
    st.markdown("""
    <style>
    @media (max-width: 768px) {
        .stMarkdown div[style*="padding:20px"] { padding: 14px !important; }
        .stMarkdown div[style*="font-size:28px"] { font-size: 22px !important; }
        [data-testid="stMetricValue"] { font-size: 18px !important; }
        .stTabs [data-baseweb="tab-list"] { overflow-x: auto !important; }
        .stTabs [data-baseweb="tab"] { padding: 8px 12px !important; font-size: 12px !important; min-width: auto !important; }
    }
    </style>
    """, unsafe_allow_html=True)

    with st.spinner(f"Analizando {ticker_in}..."):
        hist, info = descargar(ticker_in, "2y")
        fin, bs, cf = descargar_financials(ticker_in)

    if hist.empty:
        st.error(f"❌ Sin datos para **{ticker_in}**")
        st.markdown("""
        **Posibles causas:**
        - El ticker no existe o tiene un formato incorrecto
        - APIs rate-limited en este momento
        - Es un ticker muy reciente

        **Qué hacer:**
        1. Verifica en [Yahoo Finance](https://finance.yahoo.com/lookup)
        2. Espera 1-2 minutos y reintenta
        3. Limpia caché en la barra lateral
        """)
        st.stop()

    # ── Datos básicos ──────────────────────────────────────────────
    nombre  = info.get("longName") or info.get("shortName", ticker_in)
    precio  = float(hist["Close"].iloc[-1])
    moneda  = info.get("currency", "USD")
    sector  = info.get("sector", "N/A")
    mcap    = info.get("marketCap", 0)
    exch    = info.get("_fmp_exchange", info.get("exchange", ""))

    # ── Cabecera ───────────────────────────────────────────────────
    st.markdown(f"### 📈 {nombre}")
    sub = [f"`{ticker_in}`"]
    if sector != "N/A": sub.append(f"🏭 {sector}")
    if exch:            sub.append(f"📍 {exch}")
    st.caption(" · ".join(sub))

    col_h1, col_h2, col_h3 = st.columns(3)
    col_h1.metric("💰 Precio", f"{precio:,.2f} {moneda}")
    # Cambio del día
    if len(hist) >= 2:
        cambio_dia = (precio / float(hist["Close"].iloc[-2]) - 1) * 100
        col_h2.metric("📊 Hoy", f"{cambio_dia:+.2f}%",
                      delta=f"{cambio_dia:+.2f}%", delta_color="normal" if cambio_dia >= 0 else "inverse")
    if mcap > 0:
        mcap_str = f"${mcap/1e9:,.1f}B" if mcap >= 1e9 else f"${mcap/1e6:,.0f}M"
        col_h3.metric("🏢 Market Cap", mcap_str)

    # ═══════════════════════════════════════════════════════════════
    # CALCULAR INDICADORES Y SCORES (5 DIMENSIONES)
    # ═══════════════════════════════════════════════════════════════
    closes  = hist["Close"]

    # Indicadores técnicos (con manejo de tuplas correcto)
    rsi_s = calc_rsi(closes); rsi_v = float(rsi_s.iloc[-1]) if not rsi_s.empty else None
    macd_line, macd_sig, macd_hist = calc_macd(closes)
    macd_v = float(macd_hist.iloc[-1]) if not macd_hist.empty else None

    try:
        bb_up, bb_mid, bb_low, _ = calc_bb(closes)
        if not bb_up.empty:
            bb_low_v, bb_up_v = float(bb_low.iloc[-1]), float(bb_up.iloc[-1])
            bb_pct = ((precio - bb_low_v) / (bb_up_v - bb_low_v) * 100) if bb_up_v > bb_low_v else 50
        else:
            bb_pct = 50
    except Exception:
        bb_up, bb_mid, bb_low, bb_pct = pd.Series(), pd.Series(), pd.Series(), 50

    try:
        adx_s, _, _ = calc_adx(hist); adx_v = float(adx_s.iloc[-1]) if not adx_s.empty else None
    except Exception:
        adx_v = None

    try:
        atr_s, _ = calc_atr(hist); atr_v = float(atr_s.iloc[-1]) if not atr_s.empty else None
    except Exception:
        atr_v = None

    ma50  = float(closes.tail(50).mean())  if len(closes) >= 50  else None
    ma200 = float(closes.tail(200).mean()) if len(closes) >= 200 else None

    high_52w = float(hist["High"].tail(min(252, len(hist))).max()) if "High" in hist.columns else float(closes.tail(min(252, len(closes))).max())
    low_52w  = float(hist["Low"].tail(min(252, len(hist))).min())  if "Low"  in hist.columns else float(closes.tail(min(252, len(closes))).min())
    pos_52w  = ((precio - low_52w) / (high_52w - low_52w) * 100) if high_52w > low_52w else 50

    mom_5d  = (precio/float(closes.iloc[-6])  - 1)*100 if len(closes) > 5  else 0
    mom_20d = (precio/float(closes.iloc[-21]) - 1)*100 if len(closes) > 20 else 0
    mom_3m  = (precio/float(closes.iloc[-63]) - 1)*100 if len(closes) > 62 else 0
    mom_6m  = (precio/float(closes.iloc[-126])- 1)*100 if len(closes) > 125 else 0

    # ── SCORES POR DIMENSIÓN ──────────────────────────────────────
    sb = {}  # score_breakdown

    # 1. VALORACIÓN
    val_score, val_notes = 50, []
    pe = info.get("trailingPE")
    if pe and pe > 0:
        if pe < 15:    val_score = 85; val_notes.append(f"PER {pe:.1f} bajo")
        elif pe < 25:  val_score = 65; val_notes.append(f"PER {pe:.1f}")
        elif pe < 40:  val_score = 35; val_notes.append(f"PER {pe:.1f} elevado")
        else:          val_score = 15; val_notes.append(f"PER {pe:.1f} muy alto")
    pb = info.get("priceToBook")
    if pb and pb > 0:
        if pb < 1:   val_score = min(100, val_score + 15); val_notes.append(f"P/B {pb:.1f} bajo")
        elif pb > 5: val_score = max(0, val_score - 10);  val_notes.append(f"P/B {pb:.1f} alto")
    sb["💰 Valoración"] = (val_score, " · ".join(val_notes) or "Sin datos")

    # 2. CALIDAD
    qual_score, qual_notes = 50, []
    roe = info.get("returnOnEquity")
    if roe is not None:
        roe_pct = roe * 100 if abs(roe) < 5 else roe
        if roe_pct > 20:   qual_score = 90; qual_notes.append(f"ROE {roe_pct:.1f}% excelente")
        elif roe_pct > 10: qual_score = 70; qual_notes.append(f"ROE {roe_pct:.1f}%")
        elif roe_pct > 0:  qual_score = 40; qual_notes.append(f"ROE {roe_pct:.1f}% bajo")
        else:              qual_score = 10; qual_notes.append("ROE negativo")
    margins = info.get("profitMargins")
    if margins is not None:
        m_pct = margins * 100 if abs(margins) < 5 else margins
        if m_pct > 15:   qual_score = min(100, qual_score + 10); qual_notes.append(f"Margen {m_pct:.0f}%")
        elif m_pct < 5:  qual_score = max(0, qual_score - 10);  qual_notes.append(f"Margen bajo {m_pct:.0f}%")
    debt = info.get("debtToEquity")
    if debt is not None and debt > 150:
        qual_score = max(0, qual_score - 15); qual_notes.append(f"Deuda alta")
    sb["🏆 Calidad"] = (qual_score, " · ".join(qual_notes) or "Sin datos")

    # 3. MOMENTUM
    mom_score, mom_notes = 50, []
    if mom_3m > 15:    mom_score = 85; mom_notes.append(f"3M +{mom_3m:.0f}%")
    elif mom_3m > 5:   mom_score = 70; mom_notes.append(f"3M +{mom_3m:.0f}%")
    elif mom_3m > -5:  mom_score = 50; mom_notes.append(f"3M {mom_3m:+.0f}% neutro")
    elif mom_3m > -15: mom_score = 30; mom_notes.append(f"3M {mom_3m:.0f}%")
    else:              mom_score = 15; mom_notes.append(f"3M {mom_3m:.0f}%")
    if ma50 and ma200:
        if precio > ma50 > ma200:
            mom_score = min(100, mom_score + 10); mom_notes.append("Tendencia alcista")
        elif precio < ma50 < ma200:
            mom_score = max(0, mom_score - 10);  mom_notes.append("Tendencia bajista")
    sb["🚀 Momentum"] = (mom_score, " · ".join(mom_notes))

    # 4. SENTIMIENTO TÉCNICO
    sent_score, sent_notes = 50, []
    if rsi_v is not None:
        if rsi_v > 70:   sent_score = 25; sent_notes.append(f"RSI {rsi_v:.0f} sobrecomprado")
        elif rsi_v > 60: sent_score = 50; sent_notes.append(f"RSI {rsi_v:.0f} fuerte")
        elif rsi_v > 40: sent_score = 65; sent_notes.append(f"RSI {rsi_v:.0f} neutro")
        elif rsi_v > 30: sent_score = 75; sent_notes.append(f"RSI {rsi_v:.0f} débil")
        else:            sent_score = 85; sent_notes.append(f"RSI {rsi_v:.0f} sobrevendido")
    if macd_v is not None:
        sent_notes.append("MACD+" if macd_v > 0 else "MACD-")
    if pos_52w > 90:
        sent_score = max(0, sent_score - 10); sent_notes.append("Cerca máx 52w")
    elif pos_52w < 20:
        sent_score = min(100, sent_score + 10); sent_notes.append("Cerca mín 52w")
    sb["📊 Sentimiento"] = (sent_score, " · ".join(sent_notes))

    # 5. CONTEXTO MACRO (LEI proxy) - DIMENSIÓN NUEVA
    df_lei, info_fase, raw_lei = calcular_lei_proxy()
    if info_fase and sector != "N/A":
        macro_score, macro_note = macro_score_for_sector(sector, info_fase)
    else:
        macro_score, macro_note = 50, "Datos macro no disponibles"
    sb["🌍 Contexto Macro"] = (macro_score, macro_note)

    # ── Score global ponderado (5 dimensiones) ─────────────────────
    weights = {
        "💰 Valoración":      0.25,
        "🏆 Calidad":          0.25,
        "🚀 Momentum":         0.18,
        "📊 Sentimiento":      0.17,
        "🌍 Contexto Macro":   0.15,
    }
    score_global = sum(sb[k][0] * w for k, w in weights.items())

    # ── Veredicto ──────────────────────────────────────────────────
    if   score_global >= 75: verdict, verdict_color, verdict_lvl = "🟢 COMPRA", "#50fa7b", "Convicción alta"
    elif score_global >= 60: verdict, verdict_color, verdict_lvl = "🟢 COMPRA MODERADA", "#50fa7b", "Convicción media"
    elif score_global >= 45: verdict, verdict_color, verdict_lvl = "🟡 NEUTRAL", "#f1fa8c", "Sin convicción clara"
    elif score_global >= 30: verdict, verdict_color, verdict_lvl = "🔴 EVITAR", "#ff5555", "Señales débiles"
    else:                    verdict, verdict_color, verdict_lvl = "🔴 VENDER", "#ff5555", "Señales muy negativas"

    best_dim  = max(sb.items(), key=lambda x: x[1][0])
    worst_dim = min(sb.items(), key=lambda x: x[1][0])

    narrative = (
        f"**{nombre}** obtiene un score global de **{score_global:.0f}/100**. "
        f"Punto fuerte: **{best_dim[0]}** ({best_dim[1][0]:.0f}) — {best_dim[1][1]}. "
        f"Punto débil: **{worst_dim[0]}** ({worst_dim[1][0]:.0f}) — {worst_dim[1][1]}."
    )

    # ── VEREDICTO VISIBLE ─────────────────────────────────────────
    st.markdown(
        f"<div style='background:#1a1a2e;border-left:5px solid {verdict_color};"
        f"padding:16px 18px;border-radius:8px;margin:14px 0;"
        f"box-shadow:0 2px 12px rgba(0,0,0,0.4)'>"
        f"<div style='font-size:24px;font-weight:bold;color:{verdict_color};line-height:1.2'>{verdict}</div>"
        f"<div style='font-size:13px;color:#bbb;margin:6px 0 10px 0'>"
        f"{verdict_lvl} — Score: <b style='color:#ffffff'>{score_global:.0f}/100</b></div>"
        f"<div style='font-size:13px;color:#e8e8e8;line-height:1.5'>{narrative}</div>"
        f"</div>",
        unsafe_allow_html=True
    )

    # ═══════════════════════════════════════════════════════════════
    # TABS
    # ═══════════════════════════════════════════════════════════════
    tab_resumen, tab_entrada, tab_tecnico, tab_macro, tab_fundamental, tab_peers, tab_noticias = st.tabs([
        "📋 Resumen", "🎯 Entrada", "📊 Técnico", "🌍 Macro", "💼 Fund.", "🆚 Peers", "📰 News"
    ])

    # ─────────────────────────────────────────────────────────────
    # TAB ENTRADA: entradas propuestas ESTUDIANDO la estructura del gráfico
    # ─────────────────────────────────────────────────────────────
    with tab_entrada:
        if atr_v is None or atr_v <= 0:
            st.warning("Sin ATR calculable para este ticker — no se puede generar plan de entrada.")
        else:
            st.markdown("#### 🗺️ Entradas propuestas según la estructura del gráfico")
            st.caption(f"Niveles detectados sobre las últimas 252 sesiones · "
                       f"Importe a invertir: ${ai_importe:,.0f}")

            hist_est = hist.tail(252)
            sop_e, res_e = detectar_soportes_resistencias(hist_est)
            closes_e = hist["Close"].astype(float)
            highs_e  = hist["High"].astype(float) if "High" in hist.columns else closes_e
            ma50_e   = float(closes_e.tail(50).mean())
            high60_e = float(highs_e.tail(61).iloc[:-1].max()) if len(highs_e) > 61 else None

            def _stop_estructural(entrada):
                """Tu regla: max(entrada-2ATR, soporte_bajo_entrada-0.5ATR), cap -15%."""
                stop_atr = entrada - 2 * atr_v
                sops_below = [s for s in sop_e if s["nivel"] < entrada * 0.995]
                if sops_below:
                    s_rel = max(sops_below, key=lambda s: s["nivel"])
                    stop_sop = s_rel["nivel"] - 0.5 * atr_v
                    if stop_sop > stop_atr:
                        stop, razon = stop_sop, f"bajo soporte {s_rel['nivel']:.2f} ({s_rel['toques']}t) − 0.5×ATR"
                    else:
                        stop, razon = stop_atr, "2×ATR (el soporte queda más lejos)"
                else:
                    stop, razon = stop_atr, "2×ATR (sin soporte estructural debajo)"
                if stop < entrada * 0.85:
                    stop, razon = entrada * 0.85, "cap máximo -15%"
                return stop, razon

            def _tps_estructurales(entrada, stop):
                """TPs en resistencias reales (si distan ≥1R); si no, múltiplos de R."""
                r = entrada - stop
                res_above = sorted([x for x in res_e if x["nivel"] > entrada * 1.005],
                                   key=lambda x: x["nivel"])
                tps, razones = [], []
                for x in res_above:
                    if x["nivel"] >= entrada + r:
                        tps.append(x["nivel"])
                        razones.append(f"resistencia {x['nivel']:.2f} ({x['toques']}t)")
                    if len(tps) == 2:
                        break
                while len(tps) < 2:
                    mult = 2 if not tps else 4
                    tps.append(entrada + mult * r)
                    razones.append(f"{mult}R (sin resistencia a esa altura)")
                return tps[0], tps[1], razones

            propuestas = []  # (nombre, tipo_orden, plan, explicacion, toques_nivel)

            # ── 1. A MERCADO — con stop estructural y TP en resistencia real
            stop_m, razon_sm = _stop_estructural(precio)
            tp1_m, tp2_m, raz_tp_m = _tps_estructurales(precio, stop_m)
            plan_m = _plan_estructural(precio, stop_m, tp1_m, tp2_m, ai_importe)
            if plan_m:
                propuestas.append((
                    "🟢 A mercado (ahora)", "Mercado", plan_m,
                    f"Stop {razon_sm} · TP1 en {raz_tp_m[0]}", 0))

            # ── 2. REBOTE EN SOPORTE — orden limitada en el soporte más cercano
            if sop_e:
                s1 = sop_e[0]
                dist_s1 = (s1["nivel"] / precio - 1) * 100
                if -12 <= dist_s1 < -0.5:
                    entrada_s = s1["nivel"] + 0.1 * atr_v  # ligera anticipación
                    stop_s = max(s1["nivel"] - 0.75 * atr_v, entrada_s * 0.85)
                    tp1_s, tp2_s, raz_tp_s = _tps_estructurales(entrada_s, stop_s)
                    plan_s = _plan_estructural(entrada_s, stop_s, tp1_s, tp2_s,
                                                ai_importe)
                    if plan_s:
                        confluencia = ""
                        if abs(ma50_e - s1["nivel"]) < atr_v:
                            confluencia = " · ⭐ CONFLUENCIA con MA50 (nivel reforzado)"
                        propuestas.append((
                            f"🔵 Rebote en soporte {s1['nivel']:.2f} ({dist_s1:+.1f}%)",
                            "Limitada", plan_s,
                            f"Soporte tocado {s1['toques']} veces · stop bajo el nivel · "
                            f"TP1 en {raz_tp_s[0]}{confluencia}", s1["toques"]))

            # ── 3. PULLBACK A MA50 — solo si no coincide con el soporte ya propuesto
            if ma50_e < precio * 0.995:
                coincide = sop_e and abs(ma50_e - sop_e[0]["nivel"]) < atr_v
                if not coincide:
                    entrada_p = ma50_e
                    stop_p, razon_sp = _stop_estructural(entrada_p)
                    tp1_p, tp2_p, raz_tp_p = _tps_estructurales(entrada_p, stop_p)
                    plan_p = _plan_estructural(entrada_p, stop_p, tp1_p, tp2_p,
                                                ai_importe)
                    if plan_p:
                        dist_p = (ma50_e / precio - 1) * 100
                        propuestas.append((
                            f"🟣 Pullback a MA50 ({dist_p:+.1f}%)", "Limitada", plan_p,
                            f"Media de 50 sesiones como zona de demanda · stop {razon_sp} · "
                            f"TP1 en {raz_tp_p[0]}", 0))

            # ── 4. RUPTURA DE RESISTENCIA — stop-buy sobre R1
            if res_e:
                r1 = res_e[0]
                dist_r1 = (r1["nivel"] / precio - 1) * 100
                if 0.5 < dist_r1 <= 12:
                    entrada_b = r1["nivel"] + 0.25 * atr_v
                    # Tras la ruptura, R1 se convierte en soporte → stop bajo R1
                    stop_b = max(r1["nivel"] - 0.75 * atr_v, entrada_b * 0.85)
                    # TPs: resistencias por ENCIMA de R1, o múltiplos
                    r_b = entrada_b - stop_b
                    res_above_b = sorted([x for x in res_e
                                          if x["nivel"] > entrada_b * 1.005],
                                         key=lambda x: x["nivel"])
                    if res_above_b and res_above_b[0]["nivel"] >= entrada_b + r_b:
                        tp1_b = res_above_b[0]["nivel"]
                        raz_b = f"siguiente resistencia {tp1_b:.2f} ({res_above_b[0]['toques']}t)"
                    else:
                        tp1_b = entrada_b + 2 * r_b
                        raz_b = "2R (ruptura a máximos: sin resistencias arriba)"
                    tp2_b = entrada_b + 4 * r_b
                    plan_b = _plan_estructural(entrada_b, stop_b, tp1_b, tp2_b,
                                                ai_importe)
                    if plan_b:
                        propuestas.append((
                            f"🟠 Ruptura de {r1['nivel']:.2f} ({dist_r1:+.1f}%)",
                            "Stop-buy", plan_b,
                            f"Resistencia tocada {r1['toques']} veces — si rompe, se "
                            f"convierte en soporte (stop bajo ella) · TP1 en {raz_b} · "
                            f"⚠️ Exigir volumen ≥1.5× en la ruptura", r1["toques"]))
            elif high60_e and high60_e > precio * 1.005:
                # Sin resistencias detectadas pero hay máximo 60d por encima
                entrada_b = high60_e + 0.25 * atr_v
                stop_b, razon_sb = _stop_estructural(entrada_b)
                r_b = entrada_b - stop_b
                plan_b = _plan_estructural(entrada_b, stop_b, entrada_b + 2 * r_b,
                                            entrada_b + 4 * r_b, ai_importe)
                if plan_b:
                    propuestas.append((
                        f"🟠 Breakout máx. 60d ({(entrada_b/precio-1)*100:+.1f}%)",
                        "Stop-buy", plan_b,
                        f"Sin resistencias estructurales — ruptura del máximo de 60 días "
                        f"con confirmación · stop {razon_sb}", 0))

            if not propuestas:
                st.info("No se pudieron generar propuestas (estructura insuficiente).")
            else:
                # Tabla comparativa
                filas_e = []
                for nombre_e, orden_e, p, _, _ in propuestas:
                    filas_e.append({
                        "Propuesta":     nombre_e,
                        "Orden":         orden_e,
                        "Entrada":       round(p["entrada"], 2),
                        "Stop":          round(p["stop"], 2),
                        "TP1":           round(p["tp1"], 2),
                        "TP2":           round(p["tp2"], 2),
                        "R/R":           round(p["rr1"], 2) if p["rr1"] else None,
                        "Acciones":      p["shares"],
                        "Inversión $":   round(p["inversion"], 0),
                        "Si toca Stop":  f"−${abs(p['pnl_stop']):,.0f} ({p['pct_stop']:+.1f}%)",
                        "Si toca TP1":   f"+${p['pnl_tp1']:,.0f} ({p['pct_tp1']:+.1f}%)",
                        "Si toca TP2":   f"+${p['pnl_tp2']:,.0f} ({p['pct_tp2']:+.1f}%)",
                    })
                st.dataframe(pd.DataFrame(filas_e), use_container_width=True,
                             hide_index=True)

                # Aviso si el importe no alcanza para 1 acción
                if any(p["shares"] == 0 for _, _, p, _, _ in propuestas):
                    precio_min = min(p["entrada"] for _, _, p, _, _ in propuestas)
                    st.warning(f"⚠️ Con ${ai_importe:,.0f} no alcanzas ni 1 acción "
                               f"(la entrada más barata es {precio_min:.2f}). Sube el "
                               f"importe o usa un broker con acciones fraccionadas.")

                for nombre_e, _, _, exp_e, _ in propuestas:
                    st.markdown(f"**{nombre_e}** — {exp_e}")

                # ── Veredicto: la mejor entrada según R/R real y calidad del nivel
                validas = [(n, o, p, e, t) for n, o, p, e, t in propuestas
                           if p["rr1"] and p["rr1"] >= 1.3]
                st.divider()
                if validas:
                    mejor = max(validas, key=lambda x: (x[2]["rr1"], x[4]))
                    st.success(
                        f"🏆 **Mejor entrada según la estructura: {mejor[0]}** — "
                        f"R/R {mejor[2]['rr1']:.1f}:1 hasta el primer objetivo. {mejor[3]}")
                    if mejor[2]["rr1"] < 2:
                        st.caption("ℹ️ R/R entre 1.3 y 2 es aceptable pero no excelente — "
                                   "considera esperar mejor precio.")
                else:
                    mejor_rr = max((p["rr1"] or 0) for _, _, p, _, _ in propuestas)
                    st.error(
                        f"⛔ **Ninguna entrada tiene R/R ≥ 1.3 ahora mismo** "
                        f"(mejor: {mejor_rr:.1f}:1). El precio está mal posicionado "
                        f"respecto a su estructura — la decisión de más calidad es **esperar**: "
                        f"o un retroceso al soporte, o una ruptura confirmada de la resistencia.")

            st.divider()

            # ── Backtest de las reglas de entrada en ESTE valor ──────
            st.markdown("#### 🧪 ¿Funcionaron estas entradas en este valor?")
            st.caption("Simulación sobre los últimos 2 años: entrada al cierre de la señal, "
                       "stop 2×ATR gestionado con el mínimo diario, salida a 21 sesiones. "
                       "Sin comisiones. Pocas señales = poca significancia estadística.")

            col_bt1, col_bt2 = st.columns(2)
            for col_bt, modo_bt, label_bt in [
                (col_bt1, "pullback", "🟣 Pullback a MA50"),
                (col_bt2, "breakout", "🟠 Breakout 60d"),
            ]:
                with col_bt:
                    bt = _backtest_entradas(hist, modo=modo_bt)
                    st.markdown(f"**{label_bt}**")
                    if bt is None:
                        st.caption("Histórico insuficiente (<260 sesiones)")
                    elif bt["n"] == 0:
                        st.caption("0 señales en 2 años — esta regla no aplica a este valor")
                    else:
                        c1, c2, c3 = st.columns(3)
                        c1.metric("Señales", bt["n"])
                        c2.metric("Aciertos", f"{bt['win_rate']:.0f}%")
                        c3.metric("Ret. medio", f"{bt['ret_medio']:+.1f}%")
                        st.caption(f"Mejor: {bt['mejor']:+.1f}% · Peor: {bt['peor']:+.1f}%")
                        if bt["n"] < 5:
                            st.caption("⚠️ Menos de 5 señales — anécdota, no estadística")

    # ─────────────────────────────────────────────────────────────
    # TAB 1: RESUMEN (decision-ready)
    # ─────────────────────────────────────────────────────────────
    with tab_resumen:
        # GAUGE + DESGLOSE
        col_g1, col_g2 = st.columns([1, 2])

        with col_g1:
            fig_gauge = go.Figure(go.Indicator(
                mode="gauge+number",
                value=score_global,
                domain={"x":[0,1], "y":[0,1]},
                number={"font":{"size":48, "color":"#ffffff"}, "suffix":""},
                gauge={
                    "axis": {"range":[0,100], "tickcolor":"#888", "tickwidth":1,
                             "tickfont":{"color":"#bbb","size":11}},
                    "bar":  {"color": verdict_color, "thickness": 0.75},
                    "bgcolor": "#0d0d1a",
                    "borderwidth": 0,
                    "steps": [
                        {"range":[0,30],   "color":"rgba(255,85,85,0.25)"},
                        {"range":[30,45],  "color":"rgba(255,184,108,0.25)"},
                        {"range":[45,60],  "color":"rgba(241,250,140,0.25)"},
                        {"range":[60,75],  "color":"rgba(80,250,123,0.20)"},
                        {"range":[75,100], "color":"rgba(80,250,123,0.40)"},
                    ],
                }
            ))
            fig_gauge.update_layout(
                template="plotly_dark", height=220,
                margin=dict(l=10, r=10, t=10, b=10),
                paper_bgcolor="rgba(0,0,0,0)"
            )
            st.plotly_chart(fig_gauge, use_container_width=True,
                            config={"displayModeBar": False})

        with col_g2:
            st.markdown("**Desglose del score**")
            for dim_name, (dim_score, dim_note) in sb.items():
                if   dim_score >= 70: bar_color = "#50fa7b"
                elif dim_score >= 50: bar_color = "#f1fa8c"
                elif dim_score >= 30: bar_color = "#ffb86c"
                else:                 bar_color = "#ff5555"
                weight_pct = int(weights[dim_name] * 100)
                st.markdown(
                    f"<div style='margin-bottom:10px;background:#1a1a2e;padding:10px;border-radius:6px'>"
                    f"<div style='display:flex;justify-content:space-between;margin-bottom:5px'>"
                    f"<span style='color:#ffffff;font-weight:bold;font-size:13px'>{dim_name}"
                    f" <span style=\'color:#888;font-size:11px;font-weight:normal\'>({weight_pct}%)</span></span>"
                    f"<span style='color:{bar_color};font-weight:bold;font-size:13px'>{dim_score:.0f}/100</span>"
                    f"</div>"
                    f"<div style='background:#0d0d1a;border-radius:4px;height:7px;overflow:hidden'>"
                    f"<div style='background:{bar_color};width:{dim_score}%;height:100%;"
                    f"transition:width 0.6s ease'></div>"
                    f"</div>"
                    f"<div style='font-size:11px;color:#bbb;margin-top:5px'>{dim_note}</div>"
                    f"</div>",
                    unsafe_allow_html=True
                )

        st.divider()

        # ═══════════════════════════════════════════════════════════
        # PLAN DE ENTRADA — soportes reales, stop inteligente, sizing
        # ═══════════════════════════════════════════════════════════
        st.markdown("### 🎯 Plan de entrada")

        if atr_v and ma50:
            # Soportes y resistencias por pivots
            soportes, resistencias = _detectar_sr(hist, precio)
            sop1 = soportes[0]["nivel"] if soportes else None

            # STOP INTELIGENTE: el mayor entre (2×ATR bajo precio) y
            # (0.5×ATR bajo el soporte más cercano) — así el stop queda
            # protegido DETRÁS de un nivel real, no en tierra de nadie.
            stop_atr = precio - 2 * atr_v
            stop_sop = (sop1 - 0.5 * atr_v) if sop1 else None
            if stop_sop and stop_sop > stop_atr:
                stop_loss, stop_tipo = stop_sop, f"soporte {sop1:.2f} − 0.5×ATR"
            else:
                stop_loss, stop_tipo = stop_atr, "2× ATR"
            stop_loss = max(stop_loss, precio * 0.85)  # cap -15%
            if stop_loss >= precio:
                stop_loss = precio - 2 * atr_v

            riesgo_accion = precio - stop_loss
            objetivo_1 = precio + 2 * riesgo_accion   # R/R 2:1
            objetivo_2 = precio + 4 * riesgo_accion   # R/R 4:1
            res1 = resistencias[0]["nivel"] if resistencias else None
            target_consenso = info.get("targetMeanPrice")

            # Position sizing
            cap_ai = st.session_state.get("ai_cap", 10000)
            rsk_ai = st.session_state.get("ai_rsk", 1.0)
            riesgo_eur = cap_ai * rsk_ai / 100
            shares = int(riesgo_eur / riesgo_accion) if riesgo_accion > 0 else 0
            inversion = shares * precio
            if inversion > cap_ai * 0.25 and precio > 0:
                shares = int(cap_ai * 0.25 / precio)
                inversion = shares * precio
            riesgo_real = shares * riesgo_accion

            rr_ratio = (objetivo_1 - precio) / riesgo_accion if riesgo_accion > 0 else 0

            # ── Fila 1: niveles ──
            col_n1, col_n2, col_n3, col_n4 = st.columns(4)
            col_n1.metric("🟢 Entrada", f"{precio:.2f}",
                          help="Precio actual. Entrada alternativa: test de la MA50 "
                               f"en {ma50:.2f} ({(ma50/precio-1)*100:+.1f}%)")
            col_n2.metric("🛑 Stop loss", f"{stop_loss:.2f}",
                          delta=f"{(stop_loss/precio-1)*100:.1f}%",
                          delta_color="off",
                          help=f"Anclado a: {stop_tipo} (cap -15%)")
            col_n3.metric("🎯 TP1 (R/R 2:1)", f"{objetivo_1:.2f}",
                          delta=f"{(objetivo_1/precio-1)*100:+.1f}%")
            col_n4.metric("🎯 TP2 (R/R 4:1)", f"{objetivo_2:.2f}",
                          delta=f"{(objetivo_2/precio-1)*100:+.1f}%")

            # ── Fila 2: tu posición en dinero real ──
            st.markdown(
                f"<div style='background:#16213e;padding:12px 14px;border-radius:8px;"
                f"margin:8px 0;border-left:4px solid #f1fa8c'>"
                f"<b style='color:#f1fa8c'>📋 Tu posición</b> "
                f"<span style='color:#888;font-size:11px'>(capital ${cap_ai:,.0f} · riesgo {rsk_ai}%)</span><br>"
                f"<span style='color:#e8e8e8;font-size:14px'>"
                f"Comprar <b>{shares} acciones</b> = <b>${inversion:,.0f}</b> "
                f"({inversion/cap_ai*100:.0f}% del capital)<br>"
                f"🔴 Si salta el stop: <b>−${riesgo_real:,.0f}</b> · "
                f"🟢 En TP1: <b>+${shares*(objetivo_1-precio):,.0f}</b> · "
                f"🟢 En TP2: <b>+${shares*(objetivo_2-precio):,.0f}</b>"
                f"</span></div>", unsafe_allow_html=True)

            # ── Fila 3: niveles estructurales detectados ──
            col_s1, col_s2 = st.columns(2)
            with col_s1:
                st.markdown("**🟦 Soportes detectados** *(pivots con más toques = más fuertes)*")
                if soportes:
                    for s in soportes:
                        st.markdown(f"- `{s['nivel']:.2f}` ({(s['nivel']/precio-1)*100:+.1f}%) · {s['toques']} toques")
                else:
                    st.caption("Sin soportes claros bajo el precio")
            with col_s2:
                st.markdown("**🟥 Resistencias detectadas**")
                if resistencias:
                    for r_ in resistencias:
                        st.markdown(f"- `{r_['nivel']:.2f}` ({(r_['nivel']/precio-1)*100:+.1f}%) · {r_['toques']} toques")
                else:
                    st.caption("Sin resistencias — precio en máximos (cielo abierto)")

            # ── Checklist de entrada ──
            st.markdown("#### ✅ Checklist de entrada")
            checks = []
            checks.append(("Tendencia alcista (precio > MA50 > MA200)",
                           bool(ma200 and precio > ma50 > ma200) if ma200 else None))
            if rsi_v is not None:
                checks.append((f"RSI en zona operativa 40-70 (actual: {rsi_v:.0f})",
                               40 <= rsi_v <= 70))
            if res1:
                dist_res = (res1 / precio - 1) * 100
                checks.append((f"Recorrido a resistencia ≥5% (hay {dist_res:.1f}%)",
                               dist_res >= 5))
            else:
                checks.append(("Sin resistencia cercana (en máximos)", True))
            checks.append((f"R/R a TP1 ≥ 1.5 (actual: {rr_ratio:.1f})", rr_ratio >= 1.5))
            atr_pct_v = atr_v / precio * 100
            checks.append((f"Volatilidad controlada ATR% < 6 (actual: {atr_pct_v:.1f}%)",
                           atr_pct_v < 6))
            checks.append((f"Stop asumible > −12% (actual: {(stop_loss/precio-1)*100:.1f}%)",
                           (stop_loss/precio-1)*100 > -12))

            n_ok = sum(1 for _, ok in checks if ok is True)
            n_tot = sum(1 for _, ok in checks if ok is not None)
            for label, ok in checks:
                icon = "✅" if ok is True else ("❌" if ok is False else "⚪")
                st.markdown(f"{icon} {label}")

            if n_ok == n_tot:
                st.success(f"**{n_ok}/{n_tot}** — setup completo. Si el resto del análisis acompaña, es una entrada de libro.")
            elif n_ok >= n_tot - 1:
                st.info(f"**{n_ok}/{n_tot}** — setup aceptable. Valora esperar a que se complete el punto que falta.")
            else:
                st.warning(f"**{n_ok}/{n_tot}** — setup incompleto. La paciencia también es una posición.")

            if target_consenso:
                tc_pct = (target_consenso / precio - 1) * 100
                st.caption(f"📊 Target medio de analistas: {target_consenso:.2f} ({tc_pct:+.1f}%) — referencia, no objetivo operativo")
        else:
            st.info("Datos insuficientes para el plan de entrada")

        st.divider()

        # RIESGOS
        st.markdown("### ⚠️ Riesgos identificados")
        riesgos = []
        if rsi_v and rsi_v > 70:
            riesgos.append(f"RSI sobrecomprado ({rsi_v:.0f}) — riesgo de corrección")
        if rsi_v and rsi_v < 30:
            riesgos.append(f"RSI sobrevendido ({rsi_v:.0f}) — investigar la causa")
        if pos_52w > 95:
            riesgos.append(f"En máximos 52w ({pos_52w:.0f}%) — posible resistencia")
        if pos_52w < 10:
            riesgos.append(f"Cerca mín 52w ({pos_52w:.0f}%) — analizar el motivo")
        if pe and pe > 40:
            riesgos.append(f"Valoración exigente (PER {pe:.0f})")
        if sb["🏆 Calidad"][0] < 35:
            riesgos.append("Calidad fundamental débil")
        if sb["🚀 Momentum"][0] < 30:
            riesgos.append("Tendencia bajista marcada")
        if mcap and mcap < 2e9:
            riesgos.append(f"Small cap (${mcap/1e9:.1f}B) — mayor volatilidad")
        if info.get("beta") and info["beta"] > 1.5:
            riesgos.append(f"Beta elevado ({info['beta']:.1f}) — más volátil")
        # Macro
        if info_fase and sector in SECTOR_CICLO:
            fase = info_fase["fase"]
            mult = SECTOR_CICLO[sector].get(fase, 1.0)
            if mult < 0.85:
                fase_n, _ = FASE_NOMBRES[fase]
                riesgos.append(f"Contexto macro adverso: {fase_n} desfavorable a {sector}")

        if riesgos:
            for r in riesgos:
                st.markdown(f"- ⚠️ {r}")
        else:
            st.success("✅ Sin riesgos críticos identificados")

    # ─────────────────────────────────────────────────────────────
    # TAB 2: TÉCNICO — Gráfico simplificado + indicadores
    # ─────────────────────────────────────────────────────────────
    with tab_tecnico:
        # Selector de período (más user-friendly que mostrar todo)
        col_p1, col_p2 = st.columns([3, 1])
        with col_p2:
            periodo_chart = st.selectbox(
                "Período", ["3M", "6M", "1Y", "2Y"], index=2,
                key="periodo_chart_ai"
            )

        period_days = {"3M": 63, "6M": 126, "1Y": 252, "2Y": 504}
        n_days = min(period_days[periodo_chart], len(hist))
        hist_view = hist.tail(n_days)
        closes_view = hist_view["Close"]

        # GRÁFICO PRINCIPAL: Solo precio + MA50 + MA200 (limpio)
        fig_main = go.Figure()

        # Velas
        fig_main.add_trace(go.Candlestick(
            x=hist_view.index,
            open=hist_view["Open"]   if "Open"  in hist_view.columns else closes_view,
            high=hist_view["High"]   if "High"  in hist_view.columns else closes_view,
            low=hist_view["Low"]     if "Low"   in hist_view.columns else closes_view,
            close=closes_view,
            name="Precio",
            increasing_line_color="#50fa7b",
            decreasing_line_color="#ff5555",
            increasing_fillcolor="#50fa7b",
            decreasing_fillcolor="#ff5555",
        ))

        # MAs solo si hay datos suficientes
        if len(closes) >= 50:
            ma50_series = closes.rolling(50).mean().tail(n_days)
            fig_main.add_trace(go.Scatter(
                x=hist_view.index, y=ma50_series,
                name="MA50", line=dict(color="#8be9fd", width=2),
                hovertemplate="MA50: %{y:.2f}<extra></extra>"
            ))
        if len(closes) >= 200 and periodo_chart in ["1Y", "2Y"]:
            ma200_series = closes.rolling(200).mean().tail(n_days)
            fig_main.add_trace(go.Scatter(
                x=hist_view.index, y=ma200_series,
                name="MA200", line=dict(color="#ff79c6", width=2),
                hovertemplate="MA200: %{y:.2f}<extra></extra>"
            ))

        # ── Soportes y resistencias detectados (del periodo visible) ──
        soportes_v, resist_v = detectar_soportes_resistencias(hist_view)
        for s in soportes_v:
            fig_main.add_hline(
                y=s["nivel"], line_color="#50fa7b", line_width=1,
                line_dash="dot", opacity=0.75,
                annotation_text=f"S {s['nivel']:.2f} ({s['toques']}t)",
                annotation_position="left",
                annotation_font=dict(size=10, color="#50fa7b"))
        for r in resist_v:
            fig_main.add_hline(
                y=r["nivel"], line_color="#ff5555", line_width=1,
                line_dash="dot", opacity=0.75,
                annotation_text=f"R {r['nivel']:.2f} ({r['toques']}t)",
                annotation_position="left",
                annotation_font=dict(size=10, color="#ff5555"))

        fig_main.update_layout(
            template="plotly_dark",
            paper_bgcolor="#12121f", plot_bgcolor="#12121f",
            height=420,
            margin=dict(l=10, r=10, t=30, b=10),
            xaxis_rangeslider_visible=False,
            hovermode="x unified",
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02,
                xanchor="center", x=0.5,
                bgcolor="rgba(0,0,0,0)",
                font=dict(size=11, color="#bbb")
            ),
            xaxis=dict(gridcolor="rgba(255,255,255,0.05)", showspikes=True,
                       spikecolor="#888", spikethickness=1),
            yaxis=dict(gridcolor="rgba(255,255,255,0.05)", side="right",
                       tickfont=dict(color="#bbb"))
        )
        st.plotly_chart(fig_main, use_container_width=True,
                        config={"displayModeBar": False})

        # ── Panel de volumen (mismo periodo) ──────────────────────
        if "Volume" in hist_view.columns and hist_view["Volume"].sum() > 0:
            vols_view = hist_view["Volume"].astype(float)
            rets_view = closes_view.pct_change().fillna(0)
            colores_v = ["#50fa7b" if r >= 0 else "#ff5555" for r in rets_view]
            fig_vol = go.Figure()
            fig_vol.add_trace(go.Bar(
                x=hist_view.index, y=vols_view, marker_color=colores_v,
                name="Volumen", opacity=0.85,
                hovertemplate="Vol: %{y:,.0f}<extra></extra>"))
            fig_vol.add_trace(go.Scatter(
                x=hist_view.index, y=vols_view.rolling(20).mean(),
                name="Media 20", line=dict(color="#f1fa8c", width=1.5),
                hovertemplate="Media 20: %{y:,.0f}<extra></extra>"))
            fig_vol.update_layout(
                template="plotly_dark",
                paper_bgcolor="#12121f", plot_bgcolor="#12121f",
                height=160, margin=dict(l=10, r=10, t=5, b=10),
                showlegend=False, hovermode="x unified",
                xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
                yaxis=dict(gridcolor="rgba(255,255,255,0.05)", side="right",
                           tickfont=dict(color="#bbb", size=10)))
            st.plotly_chart(fig_vol, use_container_width=True,
                            config={"displayModeBar": False})

        # ── Lectura automática de la gráfica ──────────────────────
        st.markdown("**📖 Lectura de la gráfica** *(periodo seleccionado)*")
        lecturas = []

        if soportes_v:
            s0 = soportes_v[0]
            dist_s = (s0["nivel"] / precio - 1) * 100
            lecturas.append(
                f"🟢 **Soporte más cercano: {s0['nivel']:.2f}** ({dist_s:+.1f}%), "
                f"tocado {s0['toques']} veces — "
                + ("nivel muy respetado, zona lógica para stop." if s0["toques"] >= 3
                   else "nivel moderado."))
        else:
            lecturas.append("🟢 Sin soportes claros debajo en este periodo — "
                            "el precio está en zona baja del rango (cuidado con stops).")

        if resist_v:
            r0 = resist_v[0]
            dist_r = (r0["nivel"] / precio - 1) * 100
            lecturas.append(
                f"🔴 **Resistencia más cercana: {r0['nivel']:.2f}** ({dist_r:+.1f}%), "
                f"tocada {r0['toques']} veces — "
                + ("barrera fuerte: esperar ruptura con volumen antes de perseguir."
                   if r0["toques"] >= 3 else "barrera moderada."))
        else:
            lecturas.append("🔴 **Sin resistencias por encima en este periodo — máximos "
                            "del rango.** Subida libre: sin vendedores atrapados arriba.")

        if soportes_v and resist_v:
            rango = resist_v[0]["nivel"] - soportes_v[0]["nivel"]
            if rango > 0:
                pos_rango = (precio - soportes_v[0]["nivel"]) / rango * 100
                lecturas.append(
                    f"📍 El precio está al **{pos_rango:.0f}%** del rango "
                    f"soporte-resistencia — "
                    + ("cerca del soporte: mejor ratio riesgo/beneficio para comprar."
                       if pos_rango < 35 else
                       "cerca de la resistencia: el recorrido fácil ya está hecho."
                       if pos_rango > 65 else "zona media, sin ventaja posicional."))

        av = analizar_volumen(hist_view)
        if av:
            if av.get("vol_rel") is not None:
                vr = av["vol_rel"]
                lecturas.append(
                    f"📊 **Volumen hoy: ×{vr:.1f}** la media de 20 sesiones — "
                    + ("actividad excepcional, hay interés institucional." if vr >= 2
                       else "por encima de lo normal." if vr >= 1.3
                       else "sesión tranquila." if vr >= 0.7
                       else "volumen muy bajo: movimientos poco fiables."))
            if av.get("ratio_ud") is not None:
                ru = av["ratio_ud"]
                lecturas.append(
                    f"⚖️ Volumen en días alcistas vs bajistas: **×{ru:.2f}** — "
                    + ("**acumulación**: se compra con más fuerza de la que se vende."
                       if ru >= 1.15 else
                       "**distribución**: las caídas llevan más volumen que las subidas — "
                       "señal de venta institucional." if ru <= 0.85
                       else "equilibrado."))
            if av.get("obv_confirma") is not None:
                if av["obv_confirma"]:
                    lecturas.append(
                        f"✅ **El volumen confirma el precio** (OBV {av['obv_dir']} y "
                        f"precio {av['precio_dir']} en 20 sesiones): el movimiento "
                        f"actual tiene respaldo real.")
                else:
                    lecturas.append(
                        f"⚠️ **Divergencia precio-volumen** (precio {av['precio_dir']} "
                        f"pero OBV {av['obv_dir']}): el movimiento no está respaldado "
                        f"por flujo de dinero — desconfiar de su continuidad.")
            if av.get("corr_mov_vol") is not None:
                cv = av["corr_mov_vol"]
                lecturas.append(
                    f"🔗 Correlación movimiento-volumen: **{cv:+.2f}** — "
                    + ("sana: los días de movimiento fuerte van con volumen (participación real)."
                       if cv >= 0.2 else
                       "baja: los movimientos grandes ocurren sin volumen — más ruido que tendencia."))

        for lect in lecturas:
            st.markdown(f"- {lect}")

        st.divider()

        # Indicadores en grid 2x4
        st.markdown("**Indicadores clave**")
        col_i1, col_i2, col_i3, col_i4 = st.columns(4)
        col_i1.metric("RSI 14", f"{rsi_v:.0f}" if rsi_v else "—",
                      help=">70 sobrecomprado · <30 sobrevendido")
        col_i2.metric("MACD", "▲" if (macd_v or 0) > 0 else "▼" if macd_v else "—",
                      f"{macd_v:+.2f}" if macd_v else None,
                      delta_color="normal" if (macd_v or 0) >= 0 else "inverse",
                      help="Positivo: alcista")
        col_i3.metric("ADX", f"{adx_v:.0f}" if adx_v else "—",
                      help=">25 tendencia fuerte")
        col_i4.metric("Pos. 52w", f"{pos_52w:.0f}%")

        col_j1, col_j2, col_j3, col_j4 = st.columns(4)
        col_j1.metric("Mom 1M", f"{mom_20d:+.1f}%")
        col_j2.metric("Mom 3M", f"{mom_3m:+.1f}%")
        col_j3.metric("Mom 6M", f"{mom_6m:+.1f}%")
        if ma50:
            col_j4.metric("vs MA50", f"{(precio/ma50-1)*100:+.1f}%")

    # ─────────────────────────────────────────────────────────────
    # TAB 3: MACRO (NUEVA) — LEI proxy
    # ─────────────────────────────────────────────────────────────
    with tab_macro:
        st.markdown("### 🌍 Contexto macro — LEI Proxy")
        st.caption(
            "Reconstrucción del Leading Economic Index (Conference Board) "
            "usando 6 componentes de FRED. Indica fase del ciclo económico actual."
        )

        if info_fase is None:
            st.warning(
                "⚠️ No se pudo calcular el LEI. Verifica que `FRED_KEY` está "
                "configurada en Streamlit secrets."
            )
        else:
            fase = info_fase["fase"]
            fase_nombre, fase_desc = FASE_NOMBRES[fase]

            # Métrica grande de la fase
            col_f1, col_f2, col_f3 = st.columns(3)
            col_f1.metric("Fase actual", fase_nombre)
            col_f2.metric("LEI proxy (base 100)", f"{info_fase['lei_base']:.1f}")
            col_f3.metric("Cambio 6M", f"{info_fase['cambio_6m']:+.2f}σ",
                         delta_color="normal" if info_fase['cambio_6m'] >= 0 else "inverse")

            st.info(f"💡 **{fase_desc}**")

            # Gráfico del LEI
            lei_serie = info_fase["serie"].tail(60)  # 5 años
            fig_lei = go.Figure()
            fig_lei.add_trace(go.Scatter(
                x=lei_serie.index, y=lei_serie.values,
                name="LEI proxy",
                line=dict(color="#8be9fd", width=2.5),
                fill="tozeroy",
                fillcolor="rgba(139,233,253,0.1)",
                hovertemplate="LEI: %{y:.2f}<extra></extra>"
            ))
            # Línea de 100 (neutro)
            fig_lei.add_hline(y=100, line_dash="dash",
                              line_color="rgba(255,255,255,0.3)",
                              annotation_text="Neutro = 100",
                              annotation_position="right")

            fig_lei.update_layout(
                template="plotly_dark",
                paper_bgcolor="#12121f", plot_bgcolor="#12121f",
                height=320,
                margin=dict(l=10, r=10, t=30, b=10),
                title=dict(text="LEI Proxy — últimos 5 años",
                          font=dict(size=14, color="#bbb")),
                xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
                yaxis=dict(gridcolor="rgba(255,255,255,0.05)",
                          tickfont=dict(color="#bbb")),
                showlegend=False
            )
            st.plotly_chart(fig_lei, use_container_width=True,
                            config={"displayModeBar": False})

            # Score macro para este sector
            st.markdown(f"### 📊 Impacto en **{sector}**")

            col_m1, col_m2 = st.columns([1, 2])
            with col_m1:
                # Mini gauge del score macro
                fig_mg = go.Figure(go.Indicator(
                    mode="gauge+number",
                    value=macro_score,
                    domain={"x":[0,1], "y":[0,1]},
                    number={"font":{"size":36, "color":"#ffffff"}, "suffix":"/100"},
                    gauge={
                        "axis": {"range":[0,100], "tickfont":{"color":"#bbb","size":10}},
                        "bar":  {"color": "#50fa7b" if macro_score >= 65 else "#ff5555" if macro_score < 35 else "#f1fa8c"},
                        "bgcolor": "#0d0d1a",
                        "borderwidth": 0,
                    }
                ))
                fig_mg.update_layout(
                    template="plotly_dark", height=180,
                    margin=dict(l=10,r=10,t=10,b=10),
                    paper_bgcolor="rgba(0,0,0,0)"
                )
                st.plotly_chart(fig_mg, use_container_width=True,
                                config={"displayModeBar": False})

            with col_m2:
                st.markdown(f"**Score macro:** {macro_score:.0f}/100")
                st.markdown(f"_{macro_note}_")
                st.markdown("")
                if sector in SECTOR_CICLO:
                    mult = SECTOR_CICLO[sector].get(fase, 1.0)
                    if mult > 1.15:
                        st.success(f"✅ El sector **{sector}** suele beneficiarse de la fase **{fase_nombre}**")
                    elif mult < 0.85:
                        st.error(f"⚠️ El sector **{sector}** suele sufrir en la fase **{fase_nombre}** — preferir defensivos")
                    else:
                        st.info(f"➖ El sector **{sector}** se comporta neutral en la fase **{fase_nombre}**")
                else:
                    st.caption("Sector no mapeado en el modelo cíclico")

            # Tabla de componentes del LEI
            with st.expander("🔍 Ver componentes del LEI proxy"):
                if df_lei is not None and not df_lei.empty:
                    # Último valor de cada componente (z-score)
                    last_z = df_lei.iloc[-1].sort_values(ascending=False)
                    for comp_nombre, z_val in last_z.items():
                        col_x1, col_x2 = st.columns([2, 1])
                        col_x1.write(comp_nombre)
                        col_color = "#50fa7b" if z_val > 0.3 else "#ff5555" if z_val < -0.3 else "#f1fa8c"
                        col_x2.markdown(
                            f"<span style='color:{col_color};font-weight:bold'>{z_val:+.2f}σ</span>",
                            unsafe_allow_html=True
                        )
                    st.caption("σ = desviaciones estándar vs media histórica. Positivo = mejor que media.")

    # ─────────────────────────────────────────────────────────────
    # TAB 4: FUNDAMENTAL
    # ─────────────────────────────────────────────────────────────
    with tab_fundamental:
        st.markdown("### 💼 Análisis fundamental")

        col_f1, col_f2, col_f3 = st.columns(3)
        with col_f1:
            st.markdown("**Valoración**")
            if pe:        st.metric("PER (TTM)", f"{pe:.1f}")
            fpe = info.get("forwardPE")
            if fpe:       st.metric("PER Forward", f"{fpe:.1f}")
            if pb:        st.metric("P/B", f"{pb:.1f}")
            ps = info.get("priceToSalesTrailing12Months")
            if ps:        st.metric("P/S", f"{ps:.1f}")

        with col_f2:
            st.markdown("**Rentabilidad**")
            if roe is not None:
                roe_pct = roe * 100 if abs(roe) < 5 else roe
                st.metric("ROE", f"{roe_pct:.1f}%")
            roa = info.get("returnOnAssets")
            if roa is not None:
                roa_pct = roa * 100 if abs(roa) < 5 else roa
                st.metric("ROA", f"{roa_pct:.1f}%")
            if margins is not None:
                m_pct = margins * 100 if abs(margins) < 5 else margins
                st.metric("Margen neto", f"{m_pct:.1f}%")
            gm = info.get("grossMargins")
            if gm is not None:
                gm_pct = gm * 100 if abs(gm) < 5 else gm
                st.metric("Margen bruto", f"{gm_pct:.1f}%")

        with col_f3:
            st.markdown("**Solidez & Dividendos**")
            if debt is not None:
                st.metric("Deuda/Equity", f"{debt:.0f}")
            cr = info.get("currentRatio")
            if cr:        st.metric("Current Ratio", f"{cr:.2f}")
            dy = info.get("dividendYield")
            if dy:
                dy_pct = dy * 100 if abs(dy) < 1 else dy
                st.metric("Dividend Yield", f"{dy_pct:.2f}%")
            beta = info.get("beta")
            if beta:      st.metric("Beta", f"{beta:.2f}")

        st.divider()
        st.markdown("### 🎓 Modelos clásicos")

        col_p1, col_p2, col_p3, col_p4 = st.columns(4)

        try:
            piotr_r = calc_piotroski(fin, bs, cf)
            piotr_s = piotr_r[0] if isinstance(piotr_r, tuple) else piotr_r
            piotr_det = piotr_r[1] if isinstance(piotr_r, tuple) else {}
            if piotr_s is not None:
                # Mostrar cuántos criterios fueron evaluables
                meta = piotr_det.get("_meta", {}).get("val", "")
                col_p1.metric("Piotroski", f"{piotr_s}/9",
                              help=f"≥7 fuerte · 4-6 medio · ≤3 débil. {meta}")
            else:
                motivo = piotr_det.get("_error", {}).get("val", "Sin datos suficientes")
                col_p1.metric("Piotroski", "N/A", help=motivo)
        except Exception:
            col_p1.metric("Piotroski", "N/A")

        try:
            altman_r = calc_altman(info, fin, bs)
            altman_z    = altman_r[0] if isinstance(altman_r, tuple) else altman_r
            altman_zona = altman_r[1] if isinstance(altman_r, tuple) else ""
            if altman_z is not None:
                col_p2.metric("Altman Z", f"{altman_z:.2f}",
                              help=f">3 sano · 1.8-3 gris · <1.8 distress. {altman_zona}")
            else:
                col_p2.metric("Altman Z", "N/A", help=altman_zona or "Sin datos")
        except Exception:
            col_p2.metric("Altman Z", "N/A")

        try:
            graham = calc_graham(info)
            if graham is not None:
                graham_disc = (precio/graham - 1) * 100
                col_p3.metric("Graham", f"{graham:.2f}",
                              delta=f"{graham_disc:+.1f}%",
                              help="Valor intrínseco. delta = precio vs Graham")
            else:
                col_p3.metric("Graham", "N/A",
                              help="Falta EPS o Book Value, o son negativos")
        except Exception:
            col_p3.metric("Graham", "N/A")

        try:
            fcf_y = calc_fcf_yield(info, cf)
            if fcf_y is not None:
                col_p4.metric("FCF Yield", f"{fcf_y:.1f}%",
                              help=">5% atractivo · >8% excelente")
            else:
                col_p4.metric("FCF Yield", "N/A", help="Sin datos de FCF o Market Cap")
        except Exception:
            col_p4.metric("FCF Yield", "N/A")

    # ─────────────────────────────────────────────────────────────
    # TAB 5: PEERS
    # ─────────────────────────────────────────────────────────────
    with tab_peers:
        st.markdown(f"### 🆚 Comparativa con peers del sector **{sector}**")

        peers_map = {
            "Technology":             ["AAPL","MSFT","NVDA","GOOGL","META","AMZN"],
            "Consumer Cyclical":      ["AMZN","TSLA","HD","NKE","SBUX","MCD"],
            "Communication Services": ["GOOGL","META","NFLX","DIS","T","VZ"],
            "Financial Services":     ["JPM","BAC","WFC","GS","MS","C"],
            "Healthcare":             ["JNJ","UNH","PFE","ABBV","MRK","LLY"],
            "Industrials":            ["HON","UNP","UPS","RTX","BA","CAT"],
            "Consumer Defensive":     ["WMT","PG","KO","PEP","COST","CL"],
            "Energy":                 ["XOM","CVX","COP","SLB","EOG","PSX"],
            "Basic Materials":        ["LIN","SHW","ECL","APD","FCX","NEM"],
            "Utilities":              ["NEE","DUK","SO","D","AEP","XEL"],
            "Real Estate":            ["PLD","AMT","EQIX","PSA","O","WELL"],
        }
        peer_list = [p for p in peers_map.get(sector, []) if p != ticker_in][:5]

        if not peer_list:
            st.info(f"Lista de peers no disponible para sector **{sector}**")
        else:
            tickers_cmp = [ticker_in] + peer_list

            with st.spinner(f"Cargando peers: {', '.join(peer_list)}..."):
                peer_rows = []
                for t in tickers_cmp:
                    try:
                        h_p, info_p = descargar(t, "1y")
                        if h_p.empty: continue
                        precio_p = float(h_p["Close"].iloc[-1])
                        mom3_p   = (precio_p/float(h_p["Close"].iloc[-63]) - 1)*100 if len(h_p) > 62 else 0
                        mom1y_p  = (precio_p/float(h_p["Close"].iloc[0])    - 1)*100
                        roe_p    = info_p.get("returnOnEquity")
                        roe_pct_p= (roe_p*100 if roe_p and abs(roe_p)<5 else roe_p) if roe_p else None
                        m_p      = info_p.get("profitMargins")
                        m_pct_p  = (m_p*100 if m_p and abs(m_p)<5 else m_p) if m_p else None
                        peer_rows.append({
                            "Ticker":  t + (" 👈" if t == ticker_in else ""),
                            "Precio":  round(precio_p, 2),
                            "PER":     round(info_p.get("trailingPE"), 1) if info_p.get("trailingPE") else None,
                            "ROE %":   round(roe_pct_p, 1) if roe_pct_p else None,
                            "Mom 3M":  round(mom3_p, 1),
                            "Mom 1Y":  round(mom1y_p, 1),
                        })
                    except Exception:
                        continue

            if peer_rows:
                df_peers = pd.DataFrame(peer_rows)
                st.dataframe(
                    df_peers.style.map(_color_pct, subset=["Mom 3M","Mom 1Y"]),
                    use_container_width=True, hide_index=True
                )

                # Insights
                target_row = next((r for r in peer_rows if "👈" in r["Ticker"]), None)
                if target_row:
                    insights = []
                    pers = [r["PER"] for r in peer_rows if r["PER"] is not None and "👈" not in r["Ticker"]]
                    if pers and target_row["PER"]:
                        avg_per = sum(pers)/len(pers)
                        if target_row["PER"] < avg_per * 0.85:
                            insights.append(f"💰 PER {target_row['PER']:.1f} **por debajo** de peers ({avg_per:.1f})")
                        elif target_row["PER"] > avg_per * 1.15:
                            insights.append(f"💸 PER {target_row['PER']:.1f} **por encima** de peers ({avg_per:.1f})")
                    moms = [r["Mom 3M"] for r in peer_rows if "👈" not in r["Ticker"]]
                    if moms:
                        avg_mom = sum(moms)/len(moms)
                        if target_row["Mom 3M"] > avg_mom + 5:
                            insights.append(f"🚀 Mejor momentum que peers ({target_row['Mom 3M']:+.1f}% vs {avg_mom:+.1f}%)")
                    if insights:
                        st.markdown("**Insights:**")
                        for ins in insights: st.markdown(f"- {ins}")

    # ─────────────────────────────────────────────────────────────
    # TAB 6: NOTICIAS
    # ─────────────────────────────────────────────────────────────
    with tab_noticias:
        if fh_client is None:
            st.info("📭 Finnhub no configurado")
        elif "." in ticker_in:
            st.info("Finnhub solo soporta tickers US.")
            st.link_button(f"Ver noticias en Yahoo Finance",
                           f"https://finance.yahoo.com/quote/{ticker_in}/news")
        else:
            try:
                today_str    = datetime.now().strftime("%Y-%m-%d")
                week_ago_str = (datetime.now() - timedelta(days=14)).strftime("%Y-%m-%d")
                news = _news_company_cached(ticker_in, week_ago_str, today_str)
                if news:
                    for n in news[:10]:
                        fecha_n = datetime.fromtimestamp(n.get("datetime", 0)).strftime("%d-%m %H:%M") if n.get("datetime") else ""
                        st.markdown(
                            f"<div style='background:#1e1e2e;padding:12px;margin-bottom:8px;"
                            f"border-radius:6px;border-left:3px solid #ff79c6'>"
                            f"<div style='display:flex;justify-content:space-between;flex-wrap:wrap;gap:6px'>"
                            f"<span style='color:#bbb;font-size:11px'>{n.get('source','')} · {fecha_n}</span>"
                            f"<a href='{n.get('url','')}' target='_blank' "
                            f"style='color:#8be9fd;font-size:12px;text-decoration:none'>Leer →</a>"
                            f"</div>"
                            f"<div style='margin-top:6px;font-size:14px;color:#ffffff;font-weight:bold'>"
                            f"{n.get('headline','')}</div>"
                            f"<div style='margin-top:4px;font-size:12px;color:#bbb'>"
                            f"{(n.get('summary','') or '')[:200]}"
                            f"{'...' if len(n.get('summary','') or '') >= 200 else ''}"
                            f"</div></div>",
                            unsafe_allow_html=True
                        )
                else:
                    st.info("Sin noticias recientes")
            except Exception as e:
                st.error(f"Error: {e}")

    # ── Footer ─────────────────────────────────────────────────────
    st.divider()
    ticker_clean = ticker_in.split(".")[0]
    col_l1, col_l2, col_l3, col_l4 = st.columns(4)
    col_l1.link_button("📊 Finviz", f"https://finviz.com/quote.ashx?t={ticker_clean}", use_container_width=True)
    col_l2.link_button("📰 Yahoo News", f"https://finance.yahoo.com/quote/{ticker_in}/news", use_container_width=True)
    col_l3.link_button("📈 StockAnalysis", f"https://stockanalysis.com/stocks/{ticker_clean.lower()}/", use_container_width=True)
    col_l4.link_button("🕯️ TradingView", f"https://www.tradingview.com/chart/?symbol={ticker_in}", use_container_width=True)

    st.caption(f"🕐 {datetime.now().strftime('%H:%M %d-%m-%Y')} · Datos: yfinance + FMP + Finnhub + FRED")

elif pagina == "💼 Cartera":
    TD = 252
    with st.sidebar:
        periodo_c = st.selectbox("Período", ["6mo","1y","2y"], index=1)
        rf        = st.number_input("Tasa libre riesgo %", value=4.5, step=0.25) / 100

    st.header("💼 Análisis de Cartera")
    st.markdown("Edita la tabla con tus posiciones:")

    if "cart_df" not in st.session_state:
        st.session_state["cart_df"] = pd.DataFrame({
            "Ticker":       ["AAPL","MSFT","GOOGL","GLD","TLT"],
            "Cantidad":     [10, 5, 3, 20, 15],
            "Precio Compra":[150.0, 240.0, 130.0, 180.0, 95.0],
            "Divisa":       ["USD"] * 5,
        })

    edited = st.data_editor(
        st.session_state["cart_df"], num_rows="dynamic", use_container_width=True,
        column_config={
            "Ticker":       st.column_config.TextColumn("Ticker", required=True),
            "Cantidad":     st.column_config.NumberColumn("Cantidad", min_value=0, required=True),
            "Precio Compra":st.column_config.NumberColumn("Precio Compra", min_value=0.0, format="%.2f"),
            "Divisa":       st.column_config.SelectboxColumn("Divisa", options=["USD","EUR","CHF","GBP","JPY"]),
        }, key="cart_ed")
    st.session_state["cart_df"] = edited

    if st.button("🚀 Analizar Cartera", type="primary", use_container_width=True):
        dc = edited.dropna(subset=["Ticker"]).copy()
        dc = dc[dc["Cantidad"] > 0]
        if len(dc) < 2:
            st.error("Mínimo 2 posiciones.")
            st.stop()

        precios    = {}
        valores    = {}
        info_cache = {}   # sector diversification data
        errs       = []
        with st.spinner("Descargando datos..."):
            for _, row in dc.iterrows():
                t  = row["Ticker"].upper().strip()
                ht, it = descargar(t, periodo_c)
                if not ht.empty and len(ht) > 20:
                    precios[t]    = ht["Close"]
                    valores[t]    = row["Cantidad"] * ht["Close"].iloc[-1]
                    info_cache[t] = it
                else:
                    errs.append(t)
                time.sleep(0.5)
        if errs:
            st.warning(f"Sin datos: {', '.join(errs)}")
        if len(precios) < 2:
            st.error("Datos insuficientes.")
            st.stop()

        dfp  = pd.DataFrame(precios).dropna()
        dfr  = dfp.pct_change().dropna()
        vt   = sum(valores.values())
        pesos = {t: v / vt for t, v in valores.items()}
        tv   = [t for t in pesos if t in dfr.columns]
        w    = np.array([pesos[t] for t in tv])
        w    = w / w.sum()
        rc   = dfr[tv].dot(w)

        ret_a  = rc.mean() * TD * 100
        vol_a  = rc.std() * np.sqrt(TD) * 100
        rfd    = rf / TD
        sharpe = (rc.mean() - rfd) * TD / (rc.std() * np.sqrt(TD))
        ds     = rc[rc < 0]
        vol_d  = np.std(ds) * np.sqrt(TD) if len(ds) > 0 else np.nan
        sortino= ((rc.mean() - rfd) * TD) / vol_d if vol_d and vol_d > 0 else np.nan
        cum    = (1 + rc).cumprod()
        pk     = cum.cummax()
        dd     = (cum - pk) / pk
        mdd    = dd.min()
        var95  = np.percentile(rc.dropna(), 5)
        cvar95 = rc[rc <= var95].mean()

        try:
            sph, _ = descargar("^GSPC", periodo_c)
            spr  = sph["Close"].pct_change().dropna()
            al   = pd.concat([rc, spr], axis=1).dropna()
            al.columns = ["c","s"]
            beta = np.cov(al["c"], al["s"])[0,1] / np.var(al["s"])
        except Exception:
            beta = np.nan; al = None

        hhi      = round(np.sum(w ** 2), 4)
        cm       = dfr[tv].corr()
        cv       = cm.values.copy(); np.fill_diagonal(cv, np.nan)
        corr_avg = round(np.nanmean(cv), 3)
        sigma    = dfr[tv].cov().values * TD
        pv       = np.sqrt(w @ sigma @ w)
        mrc_v    = sigma @ w
        cr_pct   = (w * mrc_v) / pv * 100
        cr_dict  = dict(zip(tv, np.round(cr_pct, 2)))

        # ── Resumen métricas ──
        st.subheader("📊 Resumen")
        st.metric("Valor Total", f"${vt:,.2f}")
        st.caption("📡 Precios: Finnhub / Yahoo Finance | Benchmark: S&P 500")
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown(f"**Retorno:** {ret_a:+.2f}%")
            st.markdown(f"**Volatilidad:** {vol_a:.2f}%")
        with c2:
            st.markdown(f"**Beta:** {beta:.3f}" if not np.isnan(beta) else "Beta: N/A")
            st.markdown(f"**Max DD:** {mdd*100:.2f}%")
            st.markdown(f"**VaR 95%:** {var95*100:.2f}%")
        with c3:
            st.markdown(f"**Sharpe:** {sharpe:.3f}")
            st.markdown(f"**Sortino:** {sortino:.3f}" if sortino and not np.isnan(sortino) else "Sortino: N/A")
            st.markdown(f"**HHI:** {hhi:.4f}")

        g1, g2 = st.columns(2)
        with g1:
            fig = go.Figure(data=go.Heatmap(
                z=cm.values, x=tv, y=tv, colorscale="RdYlGn_r", zmin=-1, zmax=1,
                text=np.round(cm.values, 2), texttemplate="%{text}"))
            fig.update_layout(title="Correlaciones", template="plotly_dark",
                              paper_bgcolor="#12121f", plot_bgcolor="#1e1e2e", height=400)
            st.plotly_chart(fig, use_container_width=True)
        with g2:
            crs = dict(sorted(cr_dict.items(), key=lambda x: x[1], reverse=True))
            fig2 = go.Figure(go.Bar(x=list(crs.keys()), y=list(crs.values()),
                marker_color=["#ff5555" if v > 20 else "#50fa7b" for v in crs.values()]))
            fig2.update_layout(title="Contrib. Riesgo", template="plotly_dark",
                               paper_bgcolor="#12121f", plot_bgcolor="#1e1e2e", height=400)
            st.plotly_chart(fig2, use_container_width=True)

        g3, g4 = st.columns(2)
        with g3:
            fig3 = go.Figure()
            fig3.add_trace(go.Scatter(x=dd.index, y=dd*100, fill="tozeroy",
                fillcolor="rgba(255,85,85,0.2)", line=dict(color="#ff5555", width=1.5)))
            fig3.update_layout(title="Drawdown", template="plotly_dark",
                               paper_bgcolor="#12121f", plot_bgcolor="#1e1e2e", height=400)
            st.plotly_chart(fig3, use_container_width=True)
        with g4:
            ra = (cum / cum.iloc[0] - 1) * 100
            fig4 = go.Figure()
            fig4.add_trace(go.Scatter(x=ra.index, y=ra, name="Cartera",
                                       line=dict(color="#8be9fd", width=2)))
            if al is not None:
                try:
                    sc2 = (1 + al["s"]).cumprod()
                    sa  = (sc2 / sc2.iloc[0] - 1) * 100
                    fig4.add_trace(go.Scatter(x=sa.index, y=sa, name="S&P 500",
                                               line=dict(color="#ffb86c", dash="dash")))
                except Exception:
                    pass
            fig4.update_layout(title="Retorno vs S&P 500", template="plotly_dark",
                               paper_bgcolor="#12121f", plot_bgcolor="#1e1e2e", height=400)
            st.plotly_chart(fig4, use_container_width=True)

        # Alertas
        issues = []
        if hhi > 0.15:           issues.append(f"⚠️ Concentración alta (HHI={hhi:.3f})")
        if corr_avg > 0.50:      issues.append(f"⚠️ Correlación elevada ({corr_avg:.2f})")
        if sharpe < 0:           issues.append(f"🔴 Sharpe negativo ({sharpe:.2f})")
        if abs(mdd) > 0.25:      issues.append(f"🔴 Max DD severo ({mdd*100:.1f}%)")
        if issues:
            st.subheader("⚠️ Alertas")
            for issue in issues: st.markdown(issue)
        else:
            st.success("✅ Indicadores de riesgo aceptables.")

        # ══════════════════════════════════════════════════════════
        # NUEVA SECCIÓN: DIVERSIFICACIÓN SECTORIAL
        # ══════════════════════════════════════════════════════════
        st.divider()
        st.subheader("🏗️ Diversificación Sectorial & Recomendaciones")

        # Calcular pesos por sector SPDR
        sw_pct = sector_weights_from_portfolio(tv, w, info_cache)

        # Donut de exposición sectorial
        spdr_labels  = []
        spdr_weights = []
        for etf, weight_pct in sorted(sw_pct.items(), key=lambda x: -x[1]):
            if etf == "OTRO":
                label = "Sin clasificar"
            else:
                label = f"{SPDR_INFO[etf][1]} {SPDR_INFO[etf][0]}" if etf in SPDR_INFO else etf
            spdr_labels.append(label)
            spdr_weights.append(round(weight_pct, 1))

        col_donut, col_table = st.columns([1, 1])
        with col_donut:
            fig_donut = go.Figure(go.Pie(
                labels=spdr_labels, values=spdr_weights, hole=0.5,
                marker=dict(colors=px.colors.qualitative.Plotly),
                textinfo="label+percent", hovertemplate="%{label}<br>%{value:.1f}%<extra></extra>"))
            fig_donut.update_layout(
                title="Exposición sectorial actual",
                template="plotly_dark", paper_bgcolor="#12121f",
                height=380, showlegend=False,
                margin=dict(t=40, b=10, l=10, r=10))
            st.plotly_chart(fig_donut, use_container_width=True)

        with col_table:
            sector_rows = []
            for etf in SPDR_INFO:
                w_pct = sw_pct.get(etf, 0)
                nombre, emoji = SPDR_INFO[etf]
                estado = "✅ Presente" if w_pct >= 5 else ("🟡 Bajo" if w_pct > 0 else "⭕ Ausente")
                sector_rows.append({"Sector": f"{emoji} {nombre}", "ETF": etf,
                                     "Peso %": round(w_pct, 1), "Estado": estado})
            df_sec = pd.DataFrame(sector_rows).sort_values("Peso %", ascending=False)
            st.dataframe(df_sec, use_container_width=True, hide_index=True, height=380)

        # Obtener régimen de mercado
        with st.spinner("Analizando condiciones de mercado..."):
            regime, vix_val, spread_val = get_market_regime()

        # Mostrar régimen
        regime_info = {
            "risk_on":  ("🟢 Risk ON — Mercado optimista",    "VIX bajo, apetito por riesgo elevado"),
            "risk_off": ("🔴 Risk OFF — Mercado defensivo",   "VIX alto, búsqueda de seguridad"),
            "neutral":  ("🟡 Régimen Neutral",                "Sin señales claras de dirección"),
        }
        r_title, r_desc = regime_info[regime]
        st.markdown(f"**Régimen detectado:** {r_title}")
        rc_cols = st.columns(3)
        rc_cols[0].metric("VIX",      f"{vix_val:.1f}" if vix_val else "N/A",
                           delta="Alto" if vix_val and vix_val > 20 else "Normal",
                           delta_color="inverse")
        rc_cols[1].metric("Spread 10Y-2Y", f"{spread_val:+.2f}%" if spread_val else "N/A")
        rc_cols[2].caption(r_desc)

        # Botones de perfil
        st.markdown("#### 🎯 Selecciona tu perfil para recibir recomendaciones de diversificación:")
        pb1, pb2, pb3 = st.columns(3)
        with pb1: btn_agresivo   = st.button("🚀 Agresivo",   use_container_width=True,
                                              help="Sectores growth y cíclicos. Mayor riesgo/retorno.")
        with pb2: btn_neutro     = st.button("⚖️ Neutro",     use_container_width=True,
                                              help="Balance entre crecimiento y estabilidad.")
        with pb3: btn_balanceado = st.button("🛡️ Balanceado", use_container_width=True,
                                              help="Sectores defensivos. Menor volatilidad.")

        active_profile = None
        if btn_agresivo:   active_profile = "agresivo"
        if btn_neutro:     active_profile = "neutro"
        if btn_balanceado: active_profile = "balanceado"
        if "cartera_profile" in st.session_state and not active_profile:
            active_profile = st.session_state["cartera_profile"]
        if active_profile:
            st.session_state["cartera_profile"] = active_profile

        if active_profile:
            profile_labels = {
                "agresivo":   "🚀 Agresivo — Growth & Cíclicos",
                "neutro":     "⚖️ Neutro — Equilibrado",
                "balanceado": "🛡️ Balanceado — Defensivo",
            }
            st.markdown(f"##### Recomendaciones para perfil **{profile_labels[active_profile]}**:")

            recs = recomendar_sectores(sw_pct, active_profile, regime)

            if not recs:
                st.success("✅ Tu cartera ya tiene cobertura sectorial suficiente para este perfil.")
            else:
                rec_cols = st.columns(len(recs))
                for i, rec in enumerate(recs):
                    with rec_cols[i]:
                        etf_ticker = rec["etf"]
                        # Obtener datos del ETF recomendado
                        h_rec, i_rec = descargar(etf_ticker, "3mo") if etf_ticker else (pd.DataFrame(), {})
                        precio_rec = h_rec["Close"].iloc[-1] if not h_rec.empty else None
                        chg_rec    = ((h_rec["Close"].iloc[-1]/h_rec["Close"].iloc[-22]-1)*100
                                      if not h_rec.empty and len(h_rec)>22 else None)

                        with st.container(border=True):
                            st.markdown(f"### {rec['emoji']} {rec['nombre']}")
                            st.markdown(f"**Vehículo:** `{etf_ticker}` — ETF SPDR")
                            if precio_rec:
                                st.metric("Precio actual", f"${precio_rec:.2f}",
                                          f"{chg_rec:+.1f}% 1M" if chg_rec else None)
                            peso_txt = (f"Ausente en cartera" if rec["peso_actual"] == 0
                                        else f"Solo {rec['peso_actual']:.1f}% en cartera")
                            st.caption(peso_txt)

                            # Descripción por perfil + régimen
                            if active_profile == "agresivo":
                                razon = "Sector de alto crecimiento con momentum potencial"
                            elif active_profile == "neutro":
                                razon = "Equilibra exposición sin sesgar riesgo"
                            else:
                                razon = "Sector defensivo — protege en caídas"
                            if regime == "risk_off" and etf_ticker in ("XLV","XLP","XLU"):
                                razon += " — especialmente relevante con VIX alto"
                            elif regime == "risk_on" and etf_ticker in ("XLK","XLY","XLC"):
                                razon += " — favorecido en entorno risk-on"
                            st.caption(f"💡 {razon}")

                st.caption(f"📡 Precios ETFs: Finnhub / Yahoo Finance | Régimen: VIX vía yfinance + FRED")
        else:
            st.info("👆 Selecciona un perfil para ver recomendaciones personalizadas.")


# ╔═══════════════════════════════════════════════════════════════╗
# ║  MACRO DASHBOARD                                              ║
# ╚═══════════════════════════════════════════════════════════════╝
elif pagina == "📊 Macro":
    st.header("📊 Macro Dashboard — Datos Económicos")

    tab_macro, tab_rf = st.tabs(["🇺🇸 US Macro (FRED)", "📉 Renta Fija"])

    # ── TAB 1: US MACRO ────────────────────────────────────────────
    with tab_macro:
        if not fred_client:
            st.error("Configura la API key de FRED en Settings → Secrets.")
            st.stop()

        with st.sidebar:
            start_date = st.date_input("Desde", value=datetime(2020, 1, 1))
            series_config = {
                "Fed Funds Rate":    {"id":"FEDFUNDS",  "desc":"Tipo interés Fed",               "unit":"%"},
                "US CPI (YoY)":      {"id":"CPIAUCSL",  "desc":"Índice precios consumo",          "unit":"Index"},
                "US 10Y Treasury":   {"id":"DGS10",     "desc":"Bono US 10 años",                 "unit":"%"},
                "US 2Y Treasury":    {"id":"DGS2",      "desc":"Bono US 2 años",                  "unit":"%"},
                "Desempleo US":      {"id":"UNRATE",    "desc":"Tasa desempleo EEUU",             "unit":"%"},
                "VIX":               {"id":"VIXCLS",    "desc":"Índice volatilidad (miedo)",      "unit":"Pts"},
                "DXY (Dollar Index)":{"id":"DTWEXBGS",  "desc":"Fortaleza dólar vs cesta",        "unit":"Index"},
                "US GDP":            {"id":"GDP",       "desc":"PIB EEUU",                        "unit":"$B"},
            }
            selected = st.multiselect("Series", list(series_config.keys()),
                                       default=["Fed Funds Rate","US 10Y Treasury","VIX","Desempleo US"])

        if not selected:
            st.info("Selecciona al menos una serie en la barra lateral.")
        else:
            for name in selected:
                cfg = series_config[name]
                try:
                    data = fred_client.get_series(cfg["id"],
                            observation_start=start_date.strftime("%Y-%m-%d"))
                    if not data.empty:
                        data = data.dropna()
                        last_val = data.iloc[-1]; prev_val = data.iloc[-2] if len(data) > 1 else last_val
                        st.subheader(name)
                        st.caption(f"{cfg['desc']} — 📡 [{cfg['id']}](https://fred.stlouisfed.org/series/{cfg['id']})")
                        m1, m2, m3 = st.columns([1, 1, 2])
                        with m1: st.metric("Último", f"{last_val:.2f} {cfg['unit']}", f"{last_val-prev_val:+.2f}")
                        with m2:
                            st.metric("Mín período", f"{data.min():.2f}")
                            st.metric("Máx período", f"{data.max():.2f}")
                        fig = go.Figure()
                        fig.add_trace(go.Scatter(x=data.index, y=data, name=name,
                                                  line=dict(color="#8be9fd", width=2),
                                                  fill="tozeroy", fillcolor="rgba(139,233,253,0.05)"))
                        fig.update_layout(template="plotly_dark", paper_bgcolor="#12121f",
                                          plot_bgcolor="#1e1e2e", height=300,
                                          yaxis_title=cfg["unit"], margin=dict(t=10,b=30))
                        st.plotly_chart(fig, use_container_width=True)
                        st.divider()
                except Exception as e:
                    st.warning(f"Error cargando {name}: {e}")

        # Curva de tipos
        if fred_client:
            try:
                y10 = fred_client.get_series("DGS10", observation_start="2022-01-01").dropna()
                y2  = fred_client.get_series("DGS2",  observation_start="2022-01-01").dropna()
                if not y10.empty and not y2.empty:
                    spread = (y10 - y2).dropna()
                    if not spread.empty:
                        st.subheader("📉 Curva de Tipos (10Y − 2Y Spread)")
                        st.caption("📡 [DGS10](https://fred.stlouisfed.org/series/DGS10) − [DGS2](https://fred.stlouisfed.org/series/DGS2)")
                        fig_sp = go.Figure()
                        fig_sp.add_trace(go.Scatter(x=spread.index, y=spread,
                            line=dict(color="#8be9fd", width=2), fill="tozeroy",
                            fillcolor="rgba(139,233,253,0.1)"))
                        fig_sp.add_hline(y=0, line_dash="dash", line_color="#ff5555", opacity=0.7,
                            annotation_text="Inversión (recesión)", annotation_font_color="#ff5555")
                        fig_sp.update_layout(template="plotly_dark", paper_bgcolor="#12121f",
                            plot_bgcolor="#1e1e2e", height=350, yaxis_title="Spread (%)")
                        st.plotly_chart(fig_sp, use_container_width=True)
                        ls = spread.iloc[-1]
                        if ls < 0: st.warning(f"⚠️ Curva invertida ({ls:.2f}%). Señal histórica de recesión.")
                        else:      st.success(f"✅ Curva normal ({ls:.2f}%).")
            except Exception:
                pass

    # ── TAB 2: RENTA FIJA ──────────────────────────────────────────
    with tab_rf:
        st.subheader("📉 Renta Fija — ETF Proxy & Curva de Tipos")
        st.markdown("Seguimiento del mercado de bonos a través de los principales ETFs de renta fija.")

        # Curva de tipos spot (puntos de la curva)
        if fred_client:
            st.markdown("### 📐 Curva de Tipos US (actual vs hace 1 año)")
            maturities = ["1M","3M","6M","1A","2A","5A","10A","30A"]
            curve_now  = []
            curve_1y   = []
            for mat in maturities:
                sid = YIELD_CURVE_SERIES[mat]
                try:
                    s = fred_client.get_series(sid, observation_start="2023-01-01").dropna()
                    if not s.empty:
                        curve_now.append(float(s.iloc[-1]))
                        idx_1y = max(0, len(s) - 252)
                        curve_1y.append(float(s.iloc[idx_1y]))
                    else:
                        curve_now.append(None); curve_1y.append(None)
                except Exception:
                    curve_now.append(None); curve_1y.append(None)

            fig_curve = go.Figure()
            c_now = [v for v in curve_now if v is not None]
            m_now = [maturities[i] for i,v in enumerate(curve_now) if v is not None]
            c_1y  = [v for v in curve_1y if v is not None]
            m_1y  = [maturities[i] for i,v in enumerate(curve_1y) if v is not None]
            if c_now:
                fig_curve.add_trace(go.Scatter(x=m_now, y=c_now, name="Actual",
                    mode="lines+markers", line=dict(color="#8be9fd", width=2.5),
                    marker=dict(size=7)))
            if c_1y:
                fig_curve.add_trace(go.Scatter(x=m_1y, y=c_1y, name="Hace ~1 año",
                    mode="lines+markers", line=dict(color="#ffb86c", width=1.5, dash="dash"),
                    marker=dict(size=5)))
            fig_curve.update_layout(
                title="Curva de Tipos US Treasuries (%)",
                template="plotly_dark", paper_bgcolor="#12121f", plot_bgcolor="#1e1e2e",
                height=380, yaxis_title="Yield (%)", xaxis_title="Vencimiento",
                legend=dict(orientation="h"))
            st.plotly_chart(fig_curve, use_container_width=True)
            st.caption("📡 FRED — Federal Reserve Economic Data | [fred.stlouisfed.org](https://fred.stlouisfed.org)")

            # Spreads clave
            try:
                st.markdown("### 📊 Spreads de Crédito")
                spr_ids = {
                    "High Yield OAS (BAML)":     "BAMLH0A0HYM2",
                    "Investment Grade OAS (BAML)":"BAMLC0A0CM",
                }
                sc1, sc2 = st.columns(2)
                for i, (name, sid) in enumerate(spr_ids.items()):
                    try:
                        s = fred_client.get_series(sid, observation_start="2022-01-01").dropna()
                        if not s.empty:
                            last_v = s.iloc[-1]; prev_v = s.iloc[-2] if len(s)>1 else last_v
                            with (sc1 if i==0 else sc2):
                                st.metric(name, f"{last_v:.2f} bps", f"{last_v-prev_v:+.2f}")
                                fig_spr = go.Figure()
                                fig_spr.add_trace(go.Scatter(x=s.index[-252:], y=s.iloc[-252:],
                                    fill="tozeroy", fillcolor="rgba(255,184,108,0.15)",
                                    line=dict(color="#ffb86c", width=1.5)))
                                fig_spr.update_layout(template="plotly_dark", paper_bgcolor="#12121f",
                                    plot_bgcolor="#1e1e2e", height=200,
                                    margin=dict(t=5,b=20,l=0,r=0))
                                st.plotly_chart(fig_spr, use_container_width=True)
                    except Exception:
                        pass
                st.caption("📡 Bank of America Merrill Lynch indices via FRED")
            except Exception:
                pass
        else:
            st.info("Conecta la API de FRED en Settings → Secrets para ver la curva de tipos.")

        # ETF Proxy table
        st.markdown("### 📋 Principales ETFs de Renta Fija")
        with st.spinner("Descargando ETFs de renta fija..."):
            df_rf = get_rf_etf_data("1y")

        if not df_rf.empty:
            # Color condicional para rendimientos
            def _color_pct(val):
                if pd.isna(val): return ""
                color = "#50fa7b" if val >= 0 else "#ff5555"
                return f"color: {color}"

            st.dataframe(
                df_rf.style.map(_color_pct, subset=["1D %","1M %","YTD %"]),
                use_container_width=True, hide_index=True)

            # Gráfico comparativo
            st.markdown("### 📈 Retorno YTD comparado")
            df_plot = df_rf.sort_values("YTD %")
            colors  = ["#50fa7b" if v >= 0 else "#ff5555" for v in df_plot["YTD %"]]
            fig_rf  = go.Figure(go.Bar(
                x=df_plot["ETF"], y=df_plot["YTD %"], marker_color=colors,
                text=[f"{v:+.1f}%" for v in df_plot["YTD %"]],
                textposition="outside",
                customdata=df_plot["Nombre"].values,
                hovertemplate="<b>%{x}</b> — %{customdata}<br>YTD: %{y:.2f}%<extra></extra>"))
            fig_rf.update_layout(
                title="Retorno YTD — ETFs Renta Fija",
                template="plotly_dark", paper_bgcolor="#12121f",
                plot_bgcolor="#1e1e2e", height=400, yaxis_title="%")
            st.plotly_chart(fig_rf, use_container_width=True)

            # Gráfico duración vs rendimiento
            df_dur = df_rf.dropna(subset=["YTD %"])
            if not df_dur.empty:
                fig_dur = px.scatter(df_dur, x="Duración (A)", y="YTD %",
                                      text="ETF", color="Categoría",
                                      title="Duración vs Retorno YTD",
                                      labels={"Duración (A)":"Duración modificada (años)",
                                              "YTD %":"Retorno YTD (%)"})
                fig_dur.update_traces(textposition="top center", textfont_size=9)
                fig_dur.update_layout(template="plotly_dark", paper_bgcolor="#12121f",
                                       plot_bgcolor="#1e1e2e", height=400)
                st.plotly_chart(fig_dur, use_container_width=True)

            st.caption("📡 Precios ETFs: Finnhub / Yahoo Finance | Datos duración: estimaciones estándar de mercado")
            st.markdown("""
            **Guía de categorías:**
            - **Gobierno:** Bonos soberanos US. Máxima calidad crediticia. Sensibles a tipos de interés.
            - **TIPS:** Bonos ligados a inflación. Protegen ante subida de precios.
            - **Investment Grade:** Deuda corporativa de alta calidad. Spread sobre treasury.
            - **High Yield:** Deuda corporativa sub-investment grade. Mayor rentabilidad, mayor riesgo.
            - **Emergentes:** Bonos soberanos/corporativos mercados emergentes en USD.
            - **Global:** Diversificación internacional de renta fija.
            """)
        else:
            st.warning("No se pudieron cargar los ETFs de renta fija. Reintenta en unos momentos.")


# ╔═══════════════════════════════════════════════════════════════╗
# ║  RESEARCH ASSISTANT (Claude API)                              ║
# ╚═══════════════════════════════════════════════════════════════╝
