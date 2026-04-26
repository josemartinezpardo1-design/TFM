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

def calc_obv(df):
    d   = np.sign(df["Close"].diff())
    obv = (d * df["Volume"]).fillna(0).cumsum()
    osm = obv.rolling(20).mean()
    ot  = pd.Series(np.where(obv > osm, 1, -1), index=df.index)
    p20 = df["Close"].pct_change(20)
    o20 = obv.pct_change(20)
    div = pd.Series(np.where((p20 < 0) & (o20 > 0), "Alcista",
                   np.where((p20 > 0) & (o20 < 0), "Bajista", "Neutral")),
                   index=df.index)
    return obv, osm, ot, div

def calc_stoch(df, kp=14, dp=3):
    lm = df["Low"].rolling(kp).min()
    hm = df["High"].rolling(kp).max()
    k  = 100 * (df["Close"] - lm) / (hm - lm).replace(0, np.nan)
    return k, k.rolling(dp).mean()

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
    sc = 0; det = {}
    try:
        ta0  = _sf(bs, "Total Assets", 0, 1)
        ta1  = _sf(bs, "Total Assets", 1, 1)
        ni0  = _sf(fin, "Net Income", 0)
        cfo  = _sf(cf, "Operating Cash Flow", 0)
        ca0  = _sf(bs, "Current Assets", 0)
        cl0  = _sf(bs, "Current Liabilities", 0) or 1
        ca1  = _sf(bs, "Current Assets", 1)
        cl1  = _sf(bs, "Current Liabilities", 1) or 1
        ltd0 = _sf(bs, "Long Term Debt", 0)
        ltd1 = _sf(bs, "Long Term Debt", 1)
        rev0 = _sf(fin, "Total Revenue", 0) or 1
        rev1 = _sf(fin, "Total Revenue", 1) or 1
        gp0  = _sf(fin, "Gross Profit", 0)
        gp1  = _sf(fin, "Gross Profit", 1)
        ni1  = _sf(fin, "Net Income", 1)
        tests = [
            ("F1 ROA positivo",  ni0 / ta0 > 0,           f"{ni0/ta0*100:.2f}%"),
            ("F2 CFO positivo",  cfo > 0,                  f"${cfo/1e6:.0f}M"),
            ("F3 ROA mejora",    ni0/ta0 > ni1/ta1,        f"{ni0/ta0*100:.2f}% vs {ni1/ta1*100:.2f}%"),
            ("F4 CFO > NI",      cfo > ni0,                f"CFO {cfo/1e6:.0f}M > NI {ni0/1e6:.0f}M"),
            ("F5 Menor deuda",   ltd0/ta0 < ltd1/ta1,      f"{ltd0/ta0:.3f} vs {ltd1/ta1:.3f}"),
            ("F6 Mejor liquidez",ca0/cl0  > ca1/cl1,       f"{ca0/cl0:.2f} vs {ca1/cl1:.2f}"),
            ("F7 Sin dilución",  True,                     "(manual)"),
            ("F8 Margen bruto+", gp0/rev0 > gp1/rev1,      f"{gp0/rev0*100:.1f}% vs {gp1/rev1*100:.1f}%"),
            ("F9 Rot activos+",  rev0/ta0 > rev1/ta1,      f"{rev0/ta0:.3f} vs {rev1/ta1:.3f}"),
        ]
        for n, c, v in tests:
            pt = 1 if c else 0
            sc += pt
            det[n] = {"ok": bool(c), "val": v}
    except Exception as e:
        det["_error"] = {"ok": False, "val": str(e)}
    return sc, det

def calc_altman(info, fin, bs):
    try:
        ta   = _sf(bs, "Total Assets", 0, 1)
        ca   = _sf(bs, "Current Assets", 0)
        cl   = _sf(bs, "Current Liabilities", 0)
        re   = _sf(bs, "Retained Earnings", 0)
        ebit = _sf(fin, "EBIT", 0) or _sf(fin, "Operating Income", 0)
        tl   = _sf(bs, "Total Liabilities Net Minority Interest", 0) or _sf(bs, "Total Debt", 0) or 1
        rev  = _sf(fin, "Total Revenue", 0)
        mc   = info.get("marketCap", 0)
        z    = (1.2 * ((ca - cl) / ta) + 1.4 * (re / ta) + 3.3 * (ebit / ta)
               + 0.6 * (mc / tl) + 1.0 * (rev / ta))
        zona = "🟢 SEGURA" if z > 2.99 else ("🟡 GRIS" if z > 1.81 else "🔴 PELIGRO")
        return round(z, 2), zona
    except Exception:
        return None, "Sin datos"

def calc_graham(info):
    try:
        eps = info.get("trailingEps") or info.get("forwardEps")
        bv  = info.get("bookValue")
        if eps and bv and eps > 0 and bv > 0:
            return round((22.5 * eps * bv) ** 0.5, 2)
    except Exception:
        pass
    return None

def calc_fcf_yield(info, cf):
    try:
        fcf = info.get("freeCashflow")
        if not fcf:
            fcf = _sf(cf, "Operating Cash Flow", 0) - abs(_sf(cf, "Capital Expenditure", 0))
        mc = info.get("marketCap")
        if fcf and mc and mc > 0:
            return round(fcf / mc * 100, 2)
    except Exception:
        pass
    return None

def calc_dupont(fin, bs):
    try:
        ni  = _sf(fin, "Net Income", 0)
        rev = _sf(fin, "Total Revenue", 0) or 1
        ta  = _sf(bs, "Total Assets", 0) or 1
        eq  = _sf(bs, "Stockholders Equity", 0) or 1
        nm  = ni / rev; at = rev / ta; lv = ta / eq
        return {"ROE": round(nm*at*lv*100, 2), "Margen_Neto": round(nm*100, 2),
                "Rot_Activos": round(at, 3), "Apalanc": round(lv, 2)}
    except Exception:
        return None

def calc_cagr(fin):
    try:
        if fin.empty or len(fin.columns) < 2:
            return None, None
        def cg(s):
            v = s.dropna()
            if len(v) < 2: return None
            vi, vf, n = v.iloc[-1], v.iloc[0], len(v) - 1
            if vi <= 0 or vf <= 0: return None
            return round(((vf / vi) ** (1 / n) - 1) * 100, 1)
        rv = fin.loc["Total Revenue"] if "Total Revenue" in fin.index else None
        ni = fin.loc["Net Income"]    if "Net Income"    in fin.index else None
        return (cg(rv) if rv is not None else None,
                cg(ni) if ni is not None else None)
    except Exception:
        return None, None

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
# ║  WATCHLIST — Persistencia en Google Sheets                    ║
# ╚═══════════════════════════════════════════════════════════════╝
WATCHLIST_SHEET_ID = "1Yj2KkMypva14ZzpbnP9hDMexhzDGljWU6yhtnsVN980"
WATCHLIST_TAB      = "watchlist"
WATCHLIST_COLS     = ["fecha_anadido", "ticker", "precio_inicial", "nota"]

@st.cache_resource
def get_watchlist_ws():
    """Conecta a Google Sheets y retorna el worksheet de watchlist (lo crea si no existe)."""
    if not GSPREAD_AVAILABLE:
        return None
    try:
        # Las credenciales vienen de Streamlit secrets
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
    """Añade un ticker a la watchlist. Retorna True si tuvo éxito, False si ya existía."""
    ws = get_watchlist_ws()
    if ws is None:
        return False
    try:
        df = watchlist_load()
        if ticker.upper() in df["ticker"].astype(str).str.upper().values:
            return False  # ya existe
        fecha = datetime.now().strftime("%Y-%m-%d")
        ws.append_row([fecha, ticker.upper(), float(precio_inicial), str(nota)])
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

def analizar_screener(ticker):
    try:
        hist, info = descargar(ticker, "1y")
        if hist.empty or len(hist) < 20:
            return None
        last   = hist.iloc[-1]
        precio = last["Close"]
        if precio <= 0: return None
        mc_b  = round(info.get("marketCap", 0) / 1e9, 2)
        mom3  = ((precio / hist["Close"].iloc[-63] - 1) * 100) if len(hist) > 63 else np.nan
        sma50 = hist["Close"].rolling(50).mean().iloc[-1] if len(hist) >= 50 else np.nan
        vs50  = round((precio / sma50 - 1) * 100, 2) if pd.notna(sma50) else np.nan
        dmx   = round((precio / hist["High"].max() - 1) * 100, 2)
        vh    = last["Volume"]
        va20  = hist["Volume"].rolling(20).mean().iloc[-1]
        vr20  = round(vh / va20, 2) if va20 > 0 else np.nan
        per   = info.get("trailingPE")
        roe   = info.get("returnOnEquity")
        mn    = info.get("profitMargins")
        de    = info.get("debtToEquity")
        dy    = info.get("dividendYield")
        tm    = info.get("targetMeanPrice")
        pot   = round((tm / precio - 1) * 100, 1) if tm and precio > 0 else np.nan
        r = {
            "Ticker": ticker, "Precio": round(precio, 2), "MktCap (B$)": mc_b,
            "Mom 3M %": round(mom3, 2) if pd.notna(mom3) else np.nan,
            "vs SMA50 %": vs50, "Dist Max52W %": dmx, "Vol/Avg 20d": vr20,
            "PER":        round(per, 1) if per else np.nan,
            "ROE %":      round(roe * 100, 1) if roe else np.nan,
            "Margen Net %": round(mn * 100, 1) if mn else np.nan,
            "D/E":        round(de, 1) if de else np.nan,
            "Div Yield %":round(dy * 100, 2) if dy else 0,
            "Potencial %": pot,
            "Consenso":   info.get("recommendationKey", "N/A"),
        }
        r["Score"] = score_screener(r)
        r["Label"] = label_sc(r["Score"])
        return r
    except Exception:
        return None

def filtrar(df, modo):
    d = df.copy()
    if   modo == "VALUE":     mask = d["PER"].between(0, 20) & (d["Margen Net %"] > 8)
    elif modo == "MOMENTUM":  mask = (d["Mom 3M %"] > 10) & (d["vs SMA50 %"] > 0)
    elif modo == "QUALITY":   mask = (d["ROE %"] > 15) & (d["Margen Net %"] > 12)
    elif modo == "DIVIDENDOS":mask = (d["Div Yield %"] > 2.5) & (d["Margen Net %"] > 5)
    else:                     mask = pd.Series([True] * len(d), index=d.index)
    return d[mask].copy()


# ╔═══════════════════════════════════════════════════════════════╗
# ║  RENTA FIJA — ETF PROXY & CURVA DE TIPOS                     ║
# ╚═══════════════════════════════════════════════════════════════╝
RF_ETFS = {
    "SHY":  ("Tesoros US 1-3A",   "Gobierno",       1.8),
    "IEF":  ("Tesoros US 7-10A",  "Gobierno",       7.5),
    "TLT":  ("Tesoros US 20A+",   "Gobierno",      17.0),
    "TIP":  ("TIPS — Inflación",  "Inflación",      6.5),
    "LQD":  ("Corp IG USD",       "Inv. Grade",     8.5),
    "HYG":  ("Corp HY USD",       "High Yield",     3.8),
    "EMB":  ("Emergentes USD",    "Emergentes",     7.0),
    "BNDX": ("Intl ex-US",        "Global",         8.0),
}

YIELD_CURVE_SERIES = {
    "1M": "DGS1MO", "3M": "DGS3MO", "6M": "DGS6MO",
    "1A": "DGS1",   "2A": "DGS2",   "5A": "DGS5",
    "10A":"DGS10",  "30A":"DGS30",
}

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
        "🤖 Research",
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
if pagina == "🌅 Outlook":
    st.header("🌅 Resumen Ejecutivo del Mercado")
    st.markdown("Vista de 30 segundos: estado del mercado, tu watchlist, anomalías recientes y eventos macro de la semana.")

    refresh_outlook = st.button("🔄 Actualizar", type="primary")

    # ═══════════════════════════════════════════════════════════════
    # 1. ESTADO DEL MERCADO — métricas clave en una fila
    # ═══════════════════════════════════════════════════════════════
    st.markdown("### 📊 Estado del mercado")

    market_metrics = {
        "S&P 500":      ("^GSPC",  "📈"),
        "VIX":          ("^VIX",   "⚡"),
        "Dólar (DXY)":  ("DX-Y.NYB","💵"),
        "10Y Treasury": ("^TNX",   "🏦"),
        "Oro":          ("GC=F",   "🥇"),
        "Bitcoin":      ("BTC-USD","₿"),
    }

    with st.spinner("Descargando datos del mercado..."):
        market_data = {}
        for nombre, (ticker, emoji) in market_metrics.items():
            try:
                h, _ = descargar(ticker, "5d")
                if not h.empty and len(h) >= 2:
                    precio = float(h["Close"].iloc[-1])
                    ayer   = float(h["Close"].iloc[-2])
                    chg    = (precio / ayer - 1) * 100
                    market_data[nombre] = {
                        "precio": precio,
                        "cambio": chg,
                        "emoji":  emoji
                    }
            except Exception:
                continue

    if market_data:
        cols_market = st.columns(len(market_data))
        for i, (nombre, datos) in enumerate(market_data.items()):
            with cols_market[i]:
                color = "#50fa7b" if datos["cambio"] >= 0 else "#ff5555"
                st.markdown(f"""
                <div style="background:#1e1e2e;padding:14px;border-radius:8px;text-align:center;border-left:3px solid {color}">
                  <div style="font-size:22px">{datos['emoji']}</div>
                  <div style="font-size:12px;color:#888;margin-top:4px">{nombre}</div>
                  <div style="font-size:18px;font-weight:bold;color:#f8f8f2;margin-top:4px">{datos['precio']:.2f}</div>
                  <div style="font-size:13px;color:{color};margin-top:2px">{datos['cambio']:+.2f}%</div>
                </div>
                """, unsafe_allow_html=True)

    # ── Sectores líderes/débiles del día (ETFs sectoriales) ─────────
    st.markdown("#### 🏭 Sectores hoy")

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

    sector_changes = []
    with st.spinner("Analizando sectores..."):
        for nombre, ticker in sector_etfs.items():
            try:
                h, _ = descargar(ticker, "5d")
                if not h.empty and len(h) >= 2:
                    chg = (float(h["Close"].iloc[-1]) / float(h["Close"].iloc[-2]) - 1) * 100
                    sector_changes.append({"Sector": nombre, "Cambio %": round(chg, 2)})
            except Exception:
                continue

    if sector_changes:
        df_sect = pd.DataFrame(sector_changes).sort_values("Cambio %", ascending=False)

        col_sec1, col_sec2 = st.columns(2)
        with col_sec1:
            st.markdown("**🟢 Líderes**")
            top3 = df_sect.head(3)
            for _, row in top3.iterrows():
                st.markdown(f"<div style='padding:6px;background:#1e3a1e;border-radius:4px;margin-bottom:4px'>"
                            f"<b>{row['Sector']}</b> <span style='color:#50fa7b;float:right'>+{row['Cambio %']:.2f}%</span></div>",
                            unsafe_allow_html=True)
        with col_sec2:
            st.markdown("**🔴 Más débiles**")
            bot3 = df_sect.tail(3).iloc[::-1]
            for _, row in bot3.iterrows():
                st.markdown(f"<div style='padding:6px;background:#3a1e1e;border-radius:4px;margin-bottom:4px'>"
                            f"<b>{row['Sector']}</b> <span style='color:#ff5555;float:right'>{row['Cambio %']:.2f}%</span></div>",
                            unsafe_allow_html=True)

    st.divider()

    # ═══════════════════════════════════════════════════════════════
    # 2. TU WATCHLIST RESUMIDA
    # ═══════════════════════════════════════════════════════════════
    st.markdown("### ⭐ Tu watchlist")

    df_wl_o = watchlist_load() if GSPREAD_AVAILABLE else pd.DataFrame()
    if df_wl_o.empty:
        st.info("📋 No tienes tickers en watchlist. Ve a **⭐ Watchlist** para añadir alguno.")
    else:
        with st.spinner(f"Cargando {len(df_wl_o)} tickers..."):
            try:
                tickers_wl = df_wl_o["ticker"].tolist()

                if len(tickers_wl) == 1:
                    h_t, _ = descargar(tickers_wl[0], "5d")
                    closes_dict = {tickers_wl[0]: h_t["Close"]} if not h_t.empty else {}
                else:
                    bulk = yf.download(tickers_wl, period="5d",
                                       auto_adjust=True, progress=False)
                    closes_dict = {}
                    if isinstance(bulk.columns, pd.MultiIndex):
                        for t in tickers_wl:
                            if t in bulk.columns.get_level_values(1):
                                c = bulk["Close"][t].dropna()
                                if not c.empty:
                                    closes_dict[t] = c

                wl_rows = []
                for _, row_db in df_wl_o.iterrows():
                    t = row_db["ticker"]
                    if t not in closes_dict: continue
                    c = closes_dict[t]
                    if len(c) < 2: continue
                    precio = float(c.iloc[-1])
                    chg    = (precio / float(c.iloc[-2]) - 1) * 100
                    ret_alta = None
                    try:
                        p_inicial = float(row_db["precio_inicial"])
                        if p_inicial > 0:
                            ret_alta = (precio / p_inicial - 1) * 100
                    except Exception:
                        pass
                    wl_rows.append({
                        "Ticker":      t,
                        "Precio":      round(precio, 2),
                        "Hoy %":       round(chg, 2),
                        "Desde alta %": round(ret_alta, 2) if ret_alta is not None else None,
                        "Alta":        row_db["fecha_anadido"],
                    })

                if wl_rows:
                    df_wlo = pd.DataFrame(wl_rows).sort_values("Hoy %", ascending=False)

                    col_w1, col_w2, col_w3 = st.columns(3)
                    n_up = int((df_wlo["Hoy %"] > 0).sum())
                    col_w1.metric("Subiendo hoy", f"{n_up}/{len(df_wlo)}")

                    if "Desde alta %" in df_wlo.columns:
                        winners = df_wlo["Desde alta %"].dropna()
                        if len(winners) > 0:
                            col_w2.metric("En verde", f"{int((winners>0).sum())}/{len(winners)}")

                    avg_chg = float(df_wlo["Hoy %"].mean())
                    col_w3.metric("Cambio medio", f"{avg_chg:+.2f}%")

                    def color_chg(v):
                        if pd.isna(v): return ""
                        return "color: #50fa7b" if v > 0 else "color: #ff5555"

                    st.dataframe(
                        df_wlo.style.applymap(color_chg,
                            subset=["Hoy %","Desde alta %"]),
                        use_container_width=True, hide_index=True
                    )
            except Exception as e:
                st.error(f"Error cargando watchlist: {e}")

    st.divider()

    # ═══════════════════════════════════════════════════════════════
    # 3. TOP 3 ANOMALÍAS RECIENTES (de la última ejecución de Descubrimiento)
    # ═══════════════════════════════════════════════════════════════
    st.markdown("### 🔎 Anomalías recientes")

    last_disc = st.session_state.get("descubrimiento_resultados", None)
    if last_disc and len(last_disc) > 0:
        st.caption(f"De tu última ejecución de Descubrimiento — {len(last_disc)} anomalías totales detectadas")

        top3 = last_disc[:3]
        for i, r in enumerate(top3, 1):
            ticker_r  = r["Ticker"]
            chg_color = "#50fa7b" if r["Cambio %"] > 0 else "#ff5555"
            ticker_clean = ticker_r.split(".")[0]

            st.markdown(f"""
            <div style="background:#1e1e2e;border-left:3px solid #8be9fd;padding:12px;margin-bottom:8px;border-radius:4px">
              <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap">
                <div>
                  <span style="font-size:16px;font-weight:bold;color:#f8f8f2">#{i} {ticker_r}</span>
                  <span style="color:{chg_color};font-weight:bold;margin-left:10px">{r['Cambio %']:+.2f}%</span>
                  <span style="color:#888;font-size:12px;margin-left:8px">${r['Precio']}</span>
                </div>
                <div style="background:#44475a;padding:3px 8px;border-radius:10px;font-size:12px;color:#8be9fd">
                  Score: {r['Score']}/100
                </div>
              </div>
              <div style="margin-top:6px;font-size:12px;color:#bbb">
                <b>Señales:</b> {r['Señales']} &nbsp;|&nbsp; Vol×{r['Vol×']}
              </div>
            </div>
            """, unsafe_allow_html=True)

        st.caption("💡 Para ver las anomalías completas → ve a **🔎 Descubrimiento**")
    else:
        st.info("📋 No has ejecutado **🔎 Descubrimiento** en esta sesión. Ve allí para detectar anomalías del día.")

    st.divider()

    # ═══════════════════════════════════════════════════════════════
    # 4. CALENDARIO MACRO DE LA SEMANA
    # ═══════════════════════════════════════════════════════════════
    st.markdown("### 📅 Calendario macro de la semana")

    today        = datetime.now()
    weekday      = today.weekday()  # 0=lunes, 6=domingo
    monday       = today - timedelta(days=weekday)
    nombre_dias  = ["Lunes","Martes","Miércoles","Jueves","Viernes"]
    week_dates   = [monday + timedelta(days=d) for d in range(5)]

    # Eventos macro hardcoded — ocurrencias mensuales/trimestrales típicas
    eventos_macro = []
    for fecha in week_dates:
        dia_mes  = fecha.day
        dia_sem  = fecha.weekday()  # 0=lunes ... 4=viernes

        # Primer viernes del mes — Non-Farm Payrolls (US)
        if dia_sem == 4 and dia_mes <= 7:
            eventos_macro.append({
                "fecha":    fecha,
                "evento":   "📊 Non-Farm Payrolls (US)",
                "hora":     "14:30 UTC",
                "impacto":  "🔴 Alto",
                "desc":     "Datos de empleo no agrícola — clave para la Fed"
            })

        # Día 10-15: CPI mensual (US, normalmente miércoles o jueves)
        if 10 <= dia_mes <= 15 and dia_sem in [2, 3]:
            eventos_macro.append({
                "fecha":    fecha,
                "evento":   "💰 CPI — Inflación (US)",
                "hora":     "14:30 UTC",
                "impacto":  "🔴 Alto",
                "desc":     "Índice de Precios al Consumo — indicador de inflación"
            })

        # Día 15-20: PPI (Productor)
        if 15 <= dia_mes <= 20 and dia_sem == 3:
            eventos_macro.append({
                "fecha":    fecha,
                "evento":   "🏭 PPI — Inflación productor (US)",
                "hora":     "14:30 UTC",
                "impacto":  "🟡 Medio",
                "desc":     "Precios al productor — anticipa CPI"
            })

        # Reuniones FOMC (~cada 6 semanas, miércoles)
        # Aproximación: si miércoles y día_mes entre 20-25 alguna semana
        if dia_sem == 2 and 20 <= dia_mes <= 25:
            eventos_macro.append({
                "fecha":    fecha,
                "evento":   "🏦 Reunión FOMC (Fed)",
                "hora":     "20:00 UTC",
                "impacto":  "🔴 Alto",
                "desc":     "Decisión de tipos de interés y press conference Powell"
            })

        # Jueves: Jobless Claims (semanal)
        if dia_sem == 3:
            eventos_macro.append({
                "fecha":    fecha,
                "evento":   "👥 Jobless Claims (US)",
                "hora":     "14:30 UTC",
                "impacto":  "🟢 Bajo",
                "desc":     "Solicitudes semanales de subsidio por desempleo"
            })

        # ECB suele anunciar tipos jueves cada ~6 semanas (aproximación)
        if dia_sem == 3 and 12 <= dia_mes <= 18:
            eventos_macro.append({
                "fecha":    fecha,
                "evento":   "🇪🇺 BCE — Decisión de tipos",
                "hora":     "14:15 UTC",
                "impacto":  "🟡 Medio",
                "desc":     "Banco Central Europeo — política monetaria EUR"
            })

        # GDP trimestral (último mes de cada trimestre, día 25-30)
        if today.month in [3, 6, 9, 12] and 25 <= dia_mes <= 30 and dia_sem == 3:
            eventos_macro.append({
                "fecha":    fecha,
                "evento":   "📈 GDP trimestral (US)",
                "hora":     "14:30 UTC",
                "impacto":  "🔴 Alto",
                "desc":     "Producto Interior Bruto — crecimiento de la economía"
            })

    # Mostrar eventos por día
    if eventos_macro:
        eventos_macro.sort(key=lambda x: x["fecha"])

        for fecha_dia in week_dates:
            eventos_dia = [e for e in eventos_macro if e["fecha"].date() == fecha_dia.date()]
            if eventos_dia:
                dia_label = nombre_dias[fecha_dia.weekday()]
                fecha_str = fecha_dia.strftime("%d-%m")
                es_hoy    = fecha_dia.date() == today.date()
                color_bg  = "#1a3a1a" if es_hoy else "#1e1e2e"
                marcador  = " 👈 HOY" if es_hoy else ""

                st.markdown(f"**{dia_label} {fecha_str}**{marcador}")
                for ev in eventos_dia:
                    st.markdown(f"""
                    <div style="background:{color_bg};padding:10px;margin-bottom:6px;border-radius:6px;border-left:3px solid #8be9fd">
                      <div style="display:flex;justify-content:space-between;flex-wrap:wrap">
                        <div>
                          <b style="color:#f8f8f2">{ev['evento']}</b>
                          <span style="color:#888;font-size:12px;margin-left:6px">{ev['hora']}</span>
                        </div>
                        <span style="font-size:12px">{ev['impacto']}</span>
                      </div>
                      <div style="font-size:11px;color:#aaa;margin-top:4px">{ev['desc']}</div>
                    </div>
                    """, unsafe_allow_html=True)
    else:
        st.info("📭 No hay eventos macro destacables esta semana — semana relativamente tranquila.")

    st.caption(
        "ℹ️ Eventos calculados según patrones típicos. Verifica fechas exactas en "
        "[Trading Economics](https://tradingeconomics.com/calendar) o "
        "[Investing.com](https://www.investing.com/economic-calendar/)"
    )

    st.divider()

    st.caption(f"🕐 Última actualización: {datetime.now().strftime('%H:%M %d-%m-%Y')}")

elif pagina == "🔍 Screener":
    with st.sidebar:
        indice  = st.selectbox("Índice", list(INDICES.keys()), format_func=lambda x: INDICES[x][0])
        modo    = st.selectbox("Modo", ["VALUE","MOMENTUM","QUALITY","DIVIDENDOS","TODO"])
        limite  = st.slider("Tickers a analizar", 10, 500, 50, step=10,
                            help="Más tickers = análisis más completo pero más lento")
        st.divider()
        ejecutar = st.button("🚀 Ejecutar análisis", type="primary", use_container_width=True)

        # Descargar lista de tickers del índice seleccionado
        st.divider()
        st.caption("📋 Lista de tickers del índice")
        tickers_idx = INDICES[indice][1]()
        df_lista = pd.DataFrame({"Ticker": tickers_idx})
        st.download_button(
            f"📥 Descargar {len(tickers_idx)} tickers",
            df_lista.to_csv(index=False).encode("utf-8"),
            f"tickers_{indice}.csv",
            "text/csv",
            use_container_width=True
        )

    st.header(f"🔍 Screener: {INDICES[indice][0]} — {modo}")
    if ejecutar:
        with st.spinner("Obteniendo tickers..."):
            tickers = INDICES[indice][1]()
        if not tickers:
            st.error("No se pudieron obtener tickers.")
        else:
            ta = tickers[:limite]
            res = []
            pb = st.progress(0)
            for i, t in enumerate(ta):
                pb.progress((i + 1) / len(ta), text=f"{t} ({i+1}/{len(ta)})")
                r = analizar_screener(t)
                if r: res.append(r)
                if i % 3 == 2: time.sleep(1)
            pb.empty()
            if not res:
                st.error("Sin resultados. Yahoo Finance puede estar limitando. Intenta con menos tickers.")
            else:
                df_raw = pd.DataFrame(res)
                df_f   = filtrar(df_raw, modo).sort_values("Score", ascending=False).reset_index(drop=True)
                st.subheader(f"{len(df_f)} de {len(df_raw)} activos")
                if df_f.empty:
                    st.warning("Ningún activo cumple los filtros.")
                else:
                    cols_show = ["Ticker","Precio","Score","Label","Mom 3M %","Vol/Avg 20d",
                                 "PER","ROE %","Margen Net %","Potencial %","MktCap (B$)"]
                    cols_show = [c for c in cols_show if c in df_f.columns]
                    st.dataframe(df_f[cols_show], use_container_width=True, height=500)
                    st.caption("📡 Precios: Finnhub / Yahoo Finance | Tickers: Wikipedia")
                    dv = df_f.dropna(subset=["Mom 3M %"]).head(25)
                    if not dv.empty:
                        fig = px.scatter(dv, x="Mom 3M %", y="Score", color="Label",
                                         color_discrete_map=CL, hover_data=["Ticker","PER"],
                                         text="Ticker", title="Momentum vs Score")
                        fig.update_traces(textposition="top center", textfont_size=9)
                        fig.update_layout(template="plotly_dark", height=450,
                                          paper_bgcolor="#12121f", plot_bgcolor="#1e1e2e")
                        st.plotly_chart(fig, use_container_width=True)
                    st.session_state["screener_results"] = df_f
                    st.download_button("📥 CSV", df_f.to_csv(index=False).encode("utf-8"),
                                       f"screener_{indice}_{modo}.csv", "text/csv")
    else:
        st.info("👈 Configura y pulsa **Ejecutar**.")




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
                        signal_str = r["Señales"]
                        notes = f"Detectado por agente — {signal_str} | Score: {r['Score']}/100"
                        if watchlist_add(ticker_r, r["Precio"], notes):
                            st.success(f"✅ {ticker_r} añadido a watchlist")
                        else:
                            st.info(f"ℹ️ {ticker_r} ya estaba en watchlist")

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

            def color_cambio(val):
                if pd.isna(val): return ""
                return "color: #50fa7b" if val > 0 else "color: #ff5555"

            def color_score(val):
                if pd.isna(val): return ""
                if val >= 80: return "color: #50fa7b; font-weight: bold"
                if val >= 60: return "color: #f1fa8c"
                return ""

            st.dataframe(
                df_res.style
                    .applymap(color_cambio, subset=["Cambio %","Alpha %","Mom 5d %"])
                    .applymap(color_score,  subset=["Score"]),
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

    if not GSPREAD_AVAILABLE:
        st.error("⚠️ La librería `gspread` no está instalada. Añade `gspread` y `google-auth` a `requirements.txt`")
        st.stop()

    # ── Cargar watchlist desde Google Sheets ─────────────────────
    df_wl_db = watchlist_load()

    with st.sidebar:
        st.markdown("### ➕ Añadir ticker")
        new_t = st.text_input("Ticker (ej: AAPL, NESN.SW)", key="add_wl_input")
        new_n = st.text_area("Nota (opcional)", key="notes_wl_input",
                              placeholder="¿Por qué estás siguiendo este ticker?")

        if st.button("Añadir a watchlist", type="primary", use_container_width=True):
            if new_t and len(new_t.strip()) > 0:
                t_clean = new_t.strip().upper()
                # Obtener precio actual antes de guardar
                try:
                    h_t, _ = descargar(t_clean, "5d")
                    if not h_t.empty:
                        precio_inicial = float(h_t["Close"].iloc[-1])
                        if watchlist_add(t_clean, precio_inicial, new_n):
                            st.success(f"✅ {t_clean} añadido a watchlist")
                            st.rerun()
                        else:
                            st.warning(f"⚠️ {t_clean} ya está en la watchlist")
                    else:
                        st.error(f"No se pudo obtener precio de {t_clean}")
                except Exception as e:
                    st.error(f"Error: {e}")
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
        precio_alta = float(row_db["precio_inicial"]) if row_db["precio_inicial"] else None

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

    def color_pct(v):
        if pd.isna(v): return ""
        return "color: #50fa7b" if v > 0 else "color: #ff5555"

    st.dataframe(
        df_wl_view.style.applymap(
            color_pct,
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
    with st.sidebar:
        ticker_in = st.text_input("Ticker", value="AAPL")
        tab       = st.radio("Vista", ["🔧 Técnico","📋 Fundamental","🔧+📋 Completo"])
        st.divider()
        go_btn    = st.button("🚀 Analizar", type="primary", use_container_width=True)

        # Botón añadir a watchlist
        if GSPREAD_AVAILABLE and ticker_in:
            if st.button(f"⭐ Seguir {ticker_in.upper().strip()}",
                          use_container_width=True, key="wl_ai_btn"):
                t_clean = ticker_in.upper().strip()
                try:
                    h_t, _ = descargar(t_clean, "5d")
                    if not h_t.empty:
                        precio_inicial = float(h_t["Close"].iloc[-1])
                        nota = "Añadido desde Análisis Individual"
                        if watchlist_add(t_clean, precio_inicial, nota):
                            st.success(f"✅ {t_clean} añadido a watchlist")
                        else:
                            st.info(f"ℹ️ {t_clean} ya estaba en watchlist")
                    else:
                        st.error(f"No se pudo obtener precio de {t_clean}")
                except Exception as e:
                    st.error(f"Error: {e}")

    if go_btn and ticker_in:
        ticker_in = ticker_in.upper().strip()
        with st.spinner(f"Analizando {ticker_in}..."):
            hist, info = descargar(ticker_in, "2y")
            fin, bs, cf = descargar_financials(ticker_in)

        if hist.empty:
            st.error(f"Sin datos para {ticker_in}. Espera 1-2 minutos y reintenta.")
            st.stop()
        if len(hist) < 50:
            st.warning(f"Solo {len(hist)} sesiones disponibles. Algunos indicadores pueden ser parciales.")

        nombre  = info.get("longName") or info.get("shortName", ticker_in)
        precio  = hist["Close"].iloc[-1]
        moneda  = info.get("currency", "")
        pais    = info.get("_fmp_country", "")
        exch    = info.get("_fmp_exchange", "")

        st.header(f"🏢 {nombre} ({ticker_in})")
        h1, h2, h3, h4 = st.columns(4)
        h1.metric("Precio", f"{precio:.2f} {moneda}")
        h2.metric("Sector", info.get("sector","N/A"))
        h3.metric("MktCap", f"${info.get('marketCap',0)/1e9:.2f}B")
        h4.metric("País / Bolsa", f"{pais} {exch}" if pais else exch or "—")

        src_p = info.get("_source_prices","Yahoo Finance")
        src_m = info.get("_source_metrics","Yahoo Finance")
        src_t = info.get("_source_target","Yahoo Finance")
        st.caption(f"📡 Precios: **{src_p}** | Métricas: **{src_m}** | Target: **{src_t}**")

        # ── TÉCNICO ────────────────────────────────────────────────
        if tab in ["🔧 Técnico","🔧+📋 Completo"]:
            st.divider()
            st.subheader("🔧 Análisis Técnico — 7 Indicadores")
            hist["RSI"]   = calc_rsi(hist["Close"])
            hist["SMA_50"]  = hist["Close"].rolling(50).mean()
            hist["SMA_200"] = hist["Close"].rolling(200).mean()
            bu, bm, bl, bp = calc_bb(hist["Close"])
            hist["BB_Up"] = bu; hist["BB_Mid"] = bm; hist["BB_Low"] = bl; hist["BB_PctB"] = bp
            mc2, sg, mh = calc_macd(hist["Close"])
            hist["MACD"] = mc2; hist["Signal"] = sg; hist["MACD_Hist"] = mh
            hist["MACD_Hist_prev"] = mh.shift(1)
            ax, dp, dm = calc_adx(hist)
            hist["ADX"] = ax.values; hist["DI_Plus"] = dp.values; hist["DI_Minus"] = dm.values
            ov, osm, ot, od = calc_obv(hist)
            hist["OBV"] = ov; hist["OBV_SMA"] = osm
            sk, sd = calc_stoch(hist)
            hist["Stoch_K"] = sk; hist["Stoch_D"] = sd
            at_v, at_p = calc_atr(hist)
            hist["ATR_PCT"] = at_p

            last2  = hist.iloc[-1]
            sc, det = score_tecnico(last2, ot.iloc[-1], od.iloc[-1])
            verd, _ = interpretar(sc)

            s1, s2, s3 = st.columns([1, 2, 1])
            s1.metric("SCORE", f"{sc}/10")
            s2.markdown(f"### {verd}")
            s3.caption(f"SMA200: {'✅' if precio > last2['SMA_200'] else '🔴'} | ATR%: {last2['ATR_PCT']:.2f}%")

            for ind, d in det.items():
                pct = d["pts"] / d["max"] if d["max"] > 0 else 0
                ic  = "✅" if pct >= 0.7 else ("🟡" if pct >= 0.3 else "🔴")
                st.markdown(f"{ic} **{ind}** — {d['val']} — `{d['pts']:.1f}/{d['max']:.1f}` — {d['msg']}")

            # Niveles operativos
            st.markdown("---")
            st.markdown("### 🎯 Niveles Operativos")
            nv = niveles_op(hist, info)
            n1, n2, n3 = st.columns(3)
            with n1:
                st.markdown("**ENTRADAS**")
                st.markdown(f"🟢 **Agresiva:** {nv['entrada_agresiva']:.2f}")
                st.markdown(f"🔵 **Óptima:** {nv['entrada_optima']:.2f}")
            with n2:
                st.markdown("**STOP LOSS**")
                st.markdown(f"🔴 **SL:** {nv['stop_loss']:.2f} (−{nv['riesgo_pct']:.1f}%)")
                st.caption(f"ATR: {nv['atr']:.2f} | {nv['sl_nota']}")
            with n3:
                st.markdown("**TAKE PROFIT**")
                st.markdown(f"🎯 **TP1 (2:1):** {nv['tp1']:.2f} (+{((nv['tp1']/precio-1)*100):.1f}%)")
                st.markdown(f"🎯 **TP2 (3:1):** {nv['tp2']:.2f} (+{((nv['tp2']/precio-1)*100):.1f}%)")
                if nv["tp3"]:
                    st.markdown(f"🎯 **TP3:** {nv['tp3']:.2f} (+{((nv['tp3']/precio-1)*100):.1f}%)")

            # Gráfico
            n_pts = min(len(hist), 252); hg = hist.iloc[-n_pts:]
            fig = make_subplots(rows=5, cols=1, shared_xaxes=True, vertical_spacing=0.02,
                                row_heights=[0.35, 0.15, 0.15, 0.15, 0.15])
            fig.add_trace(go.Scatter(x=hg.index, y=hg["Close"],  name="Precio",
                                      line=dict(color="#f8f8f2", width=1.8)), row=1, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["SMA_50"], name="SMA50",
                                      line=dict(color="#ffb86c", width=1, dash="dash")), row=1, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["SMA_200"],name="SMA200",
                                      line=dict(color="#ff5555", width=1, dash="dash")), row=1, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["BB_Up"],  showlegend=False,
                                      line=dict(color="#8be9fd", width=0.5)), row=1, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["BB_Low"], name="BB",
                                      line=dict(color="#8be9fd", width=0.5),
                                      fill="tonexty", fillcolor="rgba(139,233,253,0.08)"), row=1, col=1)
            fig.add_hline(y=nv["stop_loss"], line_dash="solid", line_color="#ff5555", opacity=0.7,
                          row=1, col=1, annotation_text=f"SL {nv['stop_loss']:.2f}",
                          annotation_font_color="#ff5555", annotation_font_size=9)
            fig.add_hline(y=nv["tp1"], line_dash="dot", line_color="#f1fa8c", opacity=0.6,
                          row=1, col=1, annotation_text=f"TP1 {nv['tp1']:.2f}",
                          annotation_font_color="#f1fa8c", annotation_font_size=9)
            ch = ["#50fa7b" if v >= 0 else "#ff5555" for v in hg["MACD_Hist"]]
            fig.add_trace(go.Bar(x=hg.index, y=hg["MACD_Hist"], marker_color=ch, showlegend=False), row=2, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["MACD"],  name="MACD",
                                      line=dict(color="#50fa7b", width=1)), row=2, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["Signal"],name="Signal",
                                      line=dict(color="#ff79c6", width=1)), row=2, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["RSI"],    name="RSI",
                                      line=dict(color="#bd93f9", width=1)), row=3, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["Stoch_K"],name="%K",
                                      line=dict(color="#f1fa8c", width=1, dash="dot")), row=3, col=1)
            fig.add_hline(y=70, line_dash="dash", line_color="#ff5555", opacity=0.4, row=3, col=1)
            fig.add_hline(y=30, line_dash="dash", line_color="#50fa7b", opacity=0.4, row=3, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["ADX"],   name="ADX",
                                      line=dict(color="#ffb86c", width=1.4)), row=4, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["DI_Plus"],name="DI+",
                                      line=dict(color="#50fa7b", width=0.8)), row=4, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["DI_Minus"],name="DI−",
                                      line=dict(color="#ff5555", width=0.8)), row=4, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["OBV"],    name="OBV",
                                      line=dict(color="#8be9fd", width=1)), row=5, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["OBV_SMA"],name="OBV SMA",
                                      line=dict(color="#ff79c6", width=1, dash="dash")), row=5, col=1)
            fig.update_layout(
                title=f"{ticker_in} | Score: {sc}/10 | {verd}",
                template="plotly_dark", paper_bgcolor="#12121f", plot_bgcolor="#1e1e2e",
                height=1000, legend=dict(orientation="h", y=-0.02, font=dict(size=9)),
                hovermode="x unified")
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"📡 Datos OHLCV: **{src_p}** | Indicadores: cálculo propio")

        # ── FUNDAMENTAL ────────────────────────────────────────────
        if tab in ["📋 Fundamental","🔧+📋 Completo"]:
            st.divider()
            st.subheader("📋 Análisis Fundamental")
            per = info.get("trailingPE"); pfw = info.get("forwardPE"); peg = info.get("pegRatio")
            pb  = info.get("priceToBook"); ps  = info.get("priceToSalesTrailing12Months")
            eve = info.get("enterpriseToEbitda")
            gn  = calc_graham(info); fy = calc_fcf_yield(info, cf)

            st.markdown("### 💰 Valoración")
            st.caption(f"📡 Múltiplos: **{src_m}** | Target: **{src_t}**")
            v1, v2, v3 = st.columns(3)
            with v1:
                st.markdown(mf("PER",    per, ".1f", lambda x: x < 15,  lambda x: x > 30))
                st.markdown(mf("PER Fwd",pfw, ".1f", lambda x: x < 12,  lambda x: x > 25))
            with v2:
                st.markdown(mf("PEG",    peg, ".2f", lambda x: x < 1,   lambda x: x > 2))
                st.markdown(mf("P/Book", pb,  ".2f", lambda x: x < 1.5, lambda x: x > 5))
            with v3:
                st.markdown(mf("P/Ventas",ps, ".2f", lambda x: x < 2,   lambda x: x > 10))
                st.markdown(mf("EV/EBITDA",eve,".1f",lambda x: x < 10,  lambda x: x > 20))
            if fy:
                ic = "✅" if fy > 5 else ("🔴" if fy < 0 else "🟡")
                st.markdown(f"{ic} **FCF Yield:** {fy:.2f}%")
            if gn and precio:
                dif = (precio / gn - 1) * 100
                st.markdown(f"**Graham:** {gn:.2f} → {dif:+.1f}% — "
                            f"{'✅ INFRAVALORADO' if precio < gn else '⚠️ SOBREVALORADO'}")
            tm = info.get("targetMeanPrice")
            if tm and precio:
                up = (tm / precio - 1) * 100
                st.markdown(f"{'✅' if up > 10 else '🟡'} **Target:** {tm:.2f} ({up:+.1f}%)")

            st.markdown("### 📈 Rentabilidad")
            roe = info.get("returnOnEquity"); roa = info.get("returnOnAssets")
            pm  = info.get("profitMargins");  gm  = info.get("grossMargins")
            dupont = calc_dupont(fin, bs); cagr_r, cagr_n = calc_cagr(fin)
            r1, r2, r3 = st.columns(3)
            with r1:
                st.markdown(mf("ROE",   roe*100 if roe else None,".1f%",lambda x:x>15,lambda x:x<5))
                st.markdown(mf("ROA",   roa*100 if roa else None,".1f%",lambda x:x>8, lambda x:x<2))
            with r2:
                st.markdown(mf("M.Bruto",gm*100 if gm else None, ".1f%",lambda x:x>40,lambda x:x<20))
                st.markdown(mf("M.Neto", pm*100 if pm else None, ".1f%",lambda x:x>15,lambda x:x<3))
            with r3:
                if cagr_r is not None:
                    st.markdown(f"{'✅' if cagr_r>7 else '🟡'} **CAGR Rev:** {cagr_r:+.1f}%")
                if cagr_n is not None:
                    st.markdown(f"{'✅' if cagr_n>7 else '🟡'} **CAGR BN:** {cagr_n:+.1f}%")
            if dupont:
                d1,d2,d3,d4 = st.columns(4)
                d1.metric("ROE DuPont",f"{dupont['ROE']:.2f}%")
                d2.metric("Margen",    f"{dupont['Margen_Neto']:.2f}%")
                d3.metric("Rot.Act",   f"{dupont['Rot_Activos']:.3f}x")
                d4.metric("Apalanc",   f"{dupont['Apalanc']:.2f}x")

            st.markdown("### 🏥 Salud Financiera")
            cr = info.get("currentRatio"); de = info.get("debtToEquity")
            s1, s2, s3 = st.columns(3)
            with s1: st.markdown(mf("R.Corriente",cr,".2f",lambda x:x>1.5,lambda x:x<1))
            with s2: st.markdown(mf("D/E",de,".1f",lambda x:x<80,lambda x:x>200))
            with s3:
                if not bs.empty and not fin.empty:
                    z, zz = calc_altman(info, fin, bs)
                    if z: st.markdown(f"**Altman Z:** {z} → {zz}")

            fs = 0
            if not fin.empty and not bs.empty and not cf.empty:
                st.markdown("### 🔢 Piotroski F-Score")
                fs, fd = calc_piotroski(fin, bs, cf)
                ifs = "🟢" if fs >= 7 else ("🟡" if fs >= 4 else "🔴")
                st.markdown(f"### {ifs} F-Score: {fs}/9")
                for c, v in fd.items():
                    if c.startswith("_"): continue
                    st.markdown(f"{'✅' if v['ok'] else '❌'} {c} — `{v['val']}`")

            dy = info.get("dividendYield")
            if dy and dy > 0:
                st.markdown("### 💵 Dividendos")
                st.markdown(f"{'✅' if dy>0.03 else '🟡'} **Yield:** {dy*100:.2f}%")
                dr = info.get("dividendRate")
                if dr: st.markdown(f"**Pago anual/acción:** {dr:.2f} {moneda}")
                pay = info.get("payoutRatio")
                if pay: st.markdown(f"**Payout:** {pay*100:.1f}% — {'✅ Sostenible' if pay<0.6 else '⚠️ Elevado'}")

            st.markdown("### 🏆 Veredicto Fundamental")
            pts_v = 0; mx_v = 0
            if per:    mx_v+=2; pts_v+=(2 if per<15 else 1 if per<25 else 0)
            if roe:    mx_v+=2; pts_v+=(2 if roe>0.20 else 1 if roe>0.10 else 0)
            if pm:     mx_v+=2; pts_v+=(2 if pm>0.15 else 1 if pm>0.05 else 0)
            if de is not None: mx_v+=2; pts_v+=(2 if de<80 else 1 if de<150 else 0)
            if not fin.empty and not bs.empty and not cf.empty:
                mx_v+=2; pts_v+=(2 if fs>=7 else 1 if fs>=4 else 0)
            if mx_v > 0:
                pf = pts_v / mx_v
                if   pf >= 0.75: vf_txt = "🟢 FUNDAMENTALMENTE SÓLIDA"
                elif pf >= 0.45: vf_txt = "🟡 FUNDAMENTALMENTE ACEPTABLE"
                else:            vf_txt = "🔴 FUNDAMENTALMENTE DÉBIL"
                st.markdown(f"**{vf_txt}** — Puntuación: {pts_v}/{mx_v} ({pf*100:.0f}%)")
                st.progress(pf)
            else:
                st.warning("Datos insuficientes para el veredicto.")
    else:
        st.info("👈 Introduce un ticker y pulsa **Analizar**.")


# ╔═══════════════════════════════════════════════════════════════╗
# ║  CARTERA                                                      ║
# ╚═══════════════════════════════════════════════════════════════╝
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
            def color_pct(val):
                if pd.isna(val): return ""
                color = "#50fa7b" if val >= 0 else "#ff5555"
                return f"color: {color}"

            st.dataframe(
                df_rf.style.applymap(color_pct, subset=["1D %","1M %","YTD %"]),
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
elif pagina == "🤖 Research":
    st.header("🤖 Research Assistant")
    st.markdown("Pregunta sobre cualquier activo, sector o tema de mercado. El asistente busca datos reales y genera un análisis.")

    pregunta = st.text_area("Tu pregunta:",
        placeholder="Ej: ¿Qué opinan los analistas sobre NVDA? ¿Es buen momento para bonos US 10Y? ¿Cómo afectan los aranceles a MAIRE.MI?",
        height=100)

    if st.button("🔍 Investigar", type="primary", use_container_width=True) and pregunta:
        with st.spinner("Investigando..."):
            import re
            contexto_parts = []
            posibles = re.findall(r'\b[A-Z]{1,5}\b', pregunta.upper())
            stop_words = {"QUE","LOS","LAS","DEL","POR","CON","UNA","COMO","PARA","MAS",
                          "SER","HAY","SON","EST","THE","AND","FOR","LAS","LES"}
            tickers_v = [t for t in posibles if t not in stop_words][:3]

            for t in tickers_v:
                h, info_r = descargar(t, "6mo")
                if not h.empty:
                    p_r  = h["Close"].iloc[-1]
                    c_1m = ((p_r / h["Close"].iloc[-22] - 1)*100) if len(h)>22 else 0
                    pais = info_r.get("_fmp_country","")
                    src  = info_r.get("_source_metrics","Yahoo Finance")
                    contexto_parts.append(
                        f"**{t}**: Precio {p_r:.2f}, cambio 1M: {c_1m:+.1f}%, "
                        f"PE: {info_r.get('trailingPE','N/A')}, "
                        f"Sector: {info_r.get('sector','N/A')}, "
                        f"País: {pais if pais else 'US'}, "
                        f"Target: {info_r.get('targetMeanPrice','N/A')}, "
                        f"Consenso: {info_r.get('recommendationKey','N/A')}, "
                        f"Fuente métricas: {src}"
                    )

            noticias_ctx = []
            if fh_client and tickers_v:
                for t in tickers_v[:2]:
                    try:
                        today    = datetime.now().strftime("%Y-%m-%d")
                        week_ago = (datetime.now()-timedelta(days=7)).strftime("%Y-%m-%d")
                        news     = fh_client.company_news(t, _from=week_ago, to=today)
                        if news:
                            for n in news[:3]:
                                noticias_ctx.append(f"- [{n.get('source','')}] {n.get('headline','')}")
                    except Exception:
                        pass

            macro_ctx = []
            if fred_client:
                for name, sid in [("Fed Funds Rate","FEDFUNDS"),("US 10Y","DGS10"),("VIX","VIXCLS")]:
                    v = get_last_fred(sid)
                    if v: macro_ctx.append(f"{name}: {v:.2f}")

            contexto = "DATOS DE MERCADO REALES (hoy):\n"
            if contexto_parts: contexto += "\n".join(contexto_parts) + "\n"
            if macro_ctx:      contexto += "\nMACRO: " + " | ".join(macro_ctx) + "\n"
            if noticias_ctx:   contexto += "\nNOTICIAS RECIENTES:\n" + "\n".join(noticias_ctx[:6]) + "\n"

            try:
                response = requests.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={"Content-Type": "application/json"},
                    json={
                        "model": "claude-sonnet-4-20250514",
                        "max_tokens": 1500,
                        "messages": [{"role": "user", "content": f"""Eres un analista financiero senior de banca privada.
El cliente te hace esta pregunta: "{pregunta}"

Tienes acceso a estos datos reales actualizados:
{contexto}

Responde de forma profesional pero accesible, como un informe breve de research. Incluye:
1. Resumen ejecutivo (2-3 frases)
2. Datos clave que apoyan tu análisis
3. Riesgos a considerar
4. Conclusión con recomendación clara

Responde en español."""}]
                    }, timeout=30)
                if response.status_code == 200:
                    data = response.json()
                    respuesta = "".join(b["text"] for b in data.get("content", []) if b.get("type") == "text")
                    st.markdown("---")
                    st.subheader("📋 Informe de Research")
                    st.markdown(respuesta)
                    st.markdown("---")
                    st.caption("📡 **Fuentes:** Precios: Finnhub / Yahoo Finance / FMP | "
                               "Macro: FRED | Noticias: Finnhub | Análisis: Claude API (Anthropic)")
                    with st.expander("📊 Datos brutos utilizados"):
                        st.text(contexto)
                else:
                    st.error(f"Error API: {response.status_code}")
                    st.markdown(contexto)
            except Exception as e:
                st.warning(f"No se pudo conectar con Claude API: {e}")
                st.markdown(contexto)
    else:
        st.info("Escribe una pregunta arriba y pulsa **Investigar**.")
        st.markdown("""
        **Ejemplos de preguntas:**
        - ¿Qué opinan los analistas sobre NVDA?
        - ¿Es buen momento para entrar en AAPL?
        - ¿Cómo está el mercado de bonos US?
        - Análisis del sector tecnológico europeo
        - ¿Cómo afecta la subida de tipos a los REITs?
        - Dame un resumen de las noticias de TSLA esta semana
        - ¿Cuál es el riesgo de invertir en bonos emergentes ahora?
        """)
