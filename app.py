"""
TFM — PLATAFORMA DE INVERSIÓN INTELIGENTE v4
Master IA Sector Financiero — VIU 2025/26

Novedades v4:
  · FMP (Financial Modeling Prep) como 3ª fuente de datos — mejora cobertura no-US
  · Renta Fija vía ETF proxy — curva tipos, spreads, duración
  · Diversificación sectorial en Cartera con 3 perfiles: Agresivo / Neutro / Balanceado
"""

import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import requests, time, json, math as _math
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")

try:
    import torch
    import torch.nn as nn
    TORCH_OK = True
except Exception:
    TORCH_OK = False

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
@st.cache_data(ttl=86400)
def get_sp500():
    try:
        html = requests.get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
                            headers={"User-Agent": "Mozilla/5.0"}, timeout=10).text
        t = pd.read_html(html)
        return [x.replace(".", "-") for x in t[0]["Symbol"].tolist()]
    except Exception:
        return ["AAPL","MSFT","GOOGL","AMZN","NVDA","META","TSLA","BRK-B","JPM","V",
                "JNJ","UNH","PG","MA","HD","DIS","NFLX","PFE","KO","PEP","MRK","ABBV",
                "AVGO","COST","WMT","CSCO","TMO","ABT","CRM","ACN","NKE","MCD","LLY",
                "DHR","TXN","QCOM","INTC","AMGN","PM","UPS","MS","GS","BLK","AXP",
                "CAT","BA","GE","IBM","MMM","CVX"]

@st.cache_data(ttl=86400)
def get_ibex():
    return ["SAN.MC","BBVA.MC","ITX.MC","IBE.MC","TEF.MC","FER.MC","AMS.MC",
            "REP.MC","CABK.MC","ACS.MC","GRF.MC","MAP.MC","ENG.MC","RED.MC",
            "IAG.MC","FDR.MC","MEL.MC","COL.MC","CLNX.MC","SAB.MC"]

@st.cache_data(ttl=86400)
def get_dax():
    return ["SAP.DE","SIE.DE","ALV.DE","DTE.DE","AIR.DE","MBG.DE","DHL.DE",
            "BAS.DE","BMW.DE","IFX.DE","BEI.DE","BAYN.DE","ADS.DE","VOW3.DE",
            "DB1.DE","RWE.DE","CON.DE","DBK.DE","MRK.DE","SHL.DE"]

INDICES = {
    "SP500":  ("S&P 500", get_sp500),
    "IBEX35": ("IBEX 35", get_ibex),
    "DAX40":  ("DAX 40",  get_dax),
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
# ║  PREDICCIÓN IA — ARQUITECTURAS & FUNCIONES                   ║
# ╚═══════════════════════════════════════════════════════════════╝
if TORCH_OK:
    class _LSTMModel(nn.Module):
        def __init__(self, n_feat, hidden, n_layers, dropout):
            super().__init__()
            self.lstm = nn.LSTM(n_feat, hidden, num_layers=n_layers,
                                batch_first=True,
                                dropout=dropout if n_layers > 1 else 0)
            self.fc = nn.Linear(hidden, 1)
        def forward(self, x):
            out, _ = self.lstm(x)
            return self.fc(out[:, -1, :]).squeeze(-1)

    class _GRUModel(nn.Module):
        def __init__(self, n_feat, hidden, n_layers, dropout):
            super().__init__()
            self.gru = nn.GRU(n_feat, hidden, num_layers=n_layers,
                              batch_first=True,
                              dropout=dropout if n_layers > 1 else 0)
            self.fc = nn.Linear(hidden, 1)
        def forward(self, x):
            out, _ = self.gru(x)
            return self.fc(out[:, -1, :]).squeeze(-1)

    class _RNNModel(nn.Module):
        def __init__(self, n_feat, hidden, n_layers, dropout):
            super().__init__()
            self.rnn = nn.RNN(n_feat, hidden, num_layers=n_layers,
                              batch_first=True,
                              dropout=dropout if n_layers > 1 else 0)
            self.fc = nn.Linear(hidden, 1)
        def forward(self, x):
            out, _ = self.rnn(x)
            return self.fc(out[:, -1, :]).squeeze(-1)

    class _PositionalEncoding(nn.Module):
        def __init__(self, d_model, max_len=500):
            super().__init__()
            pe  = torch.zeros(max_len, d_model)
            pos = torch.arange(0, max_len).unsqueeze(1).float()
            div = torch.exp(torch.arange(0, d_model, 2).float()
                            * (-_math.log(10000.0) / d_model))
            pe[:, 0::2] = torch.sin(pos * div)
            pe[:, 1::2] = torch.cos(pos * div)
            self.register_buffer('pe', pe.unsqueeze(0))
        def forward(self, x):
            return x + self.pe[:, :x.size(1), :]

    class _TransformerModel(nn.Module):
        def __init__(self, n_feat, hidden, n_layers, dropout, n_heads):
            super().__init__()
            self.proj    = nn.Linear(n_feat, hidden)
            self.pe      = _PositionalEncoding(hidden)
            enc_layer    = nn.TransformerEncoderLayer(
                d_model=hidden, nhead=n_heads, dim_feedforward=hidden * 4,
                dropout=dropout, batch_first=True, activation='gelu')
            self.encoder = nn.TransformerEncoder(enc_layer, num_layers=n_layers)
            self.fc      = nn.Linear(hidden, 1)
        def forward(self, x):
            x    = self.pe(self.proj(x))
            mask = nn.Transformer.generate_square_subsequent_mask(x.size(1)).to(x.device)
            x    = self.encoder(x, mask=mask)
            return self.fc(x[:, -1, :]).squeeze(-1)

    class _MLPModel(nn.Module):
        def __init__(self, input_dim, hidden, dropout):
            super().__init__()
            self.net = nn.Sequential(
                nn.Linear(input_dim, hidden), nn.ReLU(), nn.Dropout(dropout),
                nn.Linear(hidden, hidden),    nn.ReLU(), nn.Dropout(dropout),
                nn.Linear(hidden, 1)
            )
        def forward(self, x):
            return self.net(x).squeeze(-1)


@st.cache_resource
def cargar_registry():
    try:
        with open("model_registry.json", "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return None


@st.cache_resource
def cargar_modelo(ticker: str, registry_str: str):
    """registry_str es json.dumps(registry) para que cache_resource pueda hashear."""
    if not TORCH_OK:
        return None, "PyTorch no disponible"
    registry = json.loads(registry_str)
    if ticker not in registry:
        return None, f"{ticker} no encontrado en registry"
    meta = registry[ticker]
    mn   = meta["model_name"]
    h    = meta["hidden"]
    nl   = meta["n_layers"]
    do   = meta["dropout"]
    nh   = meta["n_heads"]
    nf   = meta["n_features"]
    try:
        if   mn == "MLP":         model = _MLPModel(meta["win_mlp"] * nf, h, do)
        elif mn == "Transformer": model = _TransformerModel(nf, h, nl, do, nh)
        elif mn == "LSTM":        model = _LSTMModel(nf, h, nl, do)
        elif mn == "GRU":         model = _GRUModel(nf, h, nl, do)
        elif mn == "RNN":         model = _RNNModel(nf, h, nl, do)
        else: return None, f"Arquitectura {mn} desconocida"
        state = torch.load(meta["file"], map_location="cpu")
        model.load_state_dict(state)
        model.eval()
        return model, meta
    except Exception as e:
        return None, str(e)


def build_features_app(ticker: str):
    """
    Replica exactamente build_features() del notebook.
    Retorna (DataFrame con 10 features, None) o (None, mensaje_error).
    """
    FEATURE_COLS = [
        'log_ret', 'ma_5_ratio', 'ma_10_ratio', 'ma_20_ratio',
        'vol_norm', 'rsi_norm', 'realized_vol', 'momentum_10',
        'VIX', 'SPREAD'
    ]
    WIN = 30
    try:
        h, _ = descargar(ticker, "6mo")
        if h.empty or len(h) < 60:
            return None, f"Datos insuficientes ({len(h)} sesiones)"
        df = h[["Open","High","Low","Close","Volume"]].copy()
        df.columns = ["open","high","low","close","volume"]

        df["log_ret"]      = np.log(df["close"] / df["close"].shift(1))
        for w in [5, 10, 20]:
            df[f"ma_{w}_ratio"] = df["close"] / df["close"].rolling(w).mean() - 1
        df["vol_norm"]     = df["volume"] / df["volume"].rolling(20).mean()
        delta = df["close"].diff()
        gain  = delta.clip(lower=0).rolling(14).mean()
        loss  = (-delta.clip(upper=0)).rolling(14).mean()
        rs    = gain / loss.replace(0, 1e-10)
        df["rsi_norm"]     = (100 - 100 / (1 + rs)) / 100
        df["realized_vol"] = df["log_ret"].rolling(20).std() * np.sqrt(252)
        df["momentum_10"]  = df["close"].pct_change(10)

        # VIX
        try:
            h_vix, _ = descargar("^VIX", "6mo")
            df = df.join(h_vix["Close"].rename("VIX"), how="left")
            df["VIX"] = df["VIX"].ffill()
        except Exception:
            df["VIX"] = 20.0

        # SPREAD 10Y-2Y
        spread_ok = False
        if fred_client:
            try:
                y10 = fred_client.get_series("DGS10", observation_start="2024-01-01").dropna()
                y2  = fred_client.get_series("DGS2",  observation_start="2024-01-01").dropna()
                if not y10.empty and not y2.empty:
                    df = df.join((y10 - y2).rename("SPREAD"), how="left")
                    df["SPREAD"] = df["SPREAD"].ffill()
                    spread_ok = True
            except Exception:
                pass
        if not spread_ok:
            try:
                tnx, _ = descargar("^TNX", "6mo")
                irx, _ = descargar("^IRX", "6mo")
                if not tnx.empty and not irx.empty:
                    df = df.join(
                        (tnx["Close"] / 10 - irx["Close"] / 10).rename("SPREAD"), how="left")
                    df["SPREAD"] = df["SPREAD"].ffill()
                else:
                    df["SPREAD"] = 0.5
            except Exception:
                df["SPREAD"] = 0.5

        df = df[FEATURE_COLS].dropna()
        if len(df) < WIN:
            return None, f"Solo {len(df)} filas tras limpiar NaN (mínimo {WIN})"
        return df, None
    except Exception as e:
        return None, str(e)


def predecir_senal(ticker: str, model, meta: dict, features_df: pd.DataFrame):
    """Normaliza con scaler guardado y corre inferencia. Retorna dict con señal."""
    FEATURE_COLS = meta["feature_cols"]
    WIN          = meta["win_seq"]
    X_raw  = features_df[FEATURE_COLS].values[-WIN:]
    mean   = np.array(meta["scaler_mean"])
    scale  = np.array(meta["scaler_scale"])
    X_norm = (X_raw - mean) / scale
    X_t    = torch.FloatTensor(X_norm).unsqueeze(0)
    with torch.no_grad():
        logit = model(X_t).item()
    prob_alc  = 1 / (1 + np.exp(-logit))
    confianza = abs(prob_alc - 0.5) * 2
    signal    = "📈 ALCISTA" if prob_alc > 0.5 else "📉 BAJISTA"
    return {
        "prob_alcista": round(prob_alc, 4),
        "prob_bajista": round(1 - prob_alc, 4),
        "signal":       signal,
        "confianza":    round(confianza, 4),
        "logit":        round(logit, 4),
    }


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
        "📈 Análisis Individual",
        "💼 Cartera",
        "📊 Macro",
        "🔮 Predicción IA",
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
    st.header("🌅 Morning Outlook — Resumen de Mercado")
    st.caption(f"📅 {datetime.now().strftime('%d/%m/%Y %H:%M')} UTC")

    with st.spinner("Cargando datos de mercado..."):
        indices = {
            "^GSPC":"S&P 500","^IXIC":"Nasdaq","^DJI":"Dow Jones",
            "^STOXX50E":"Euro Stoxx 50","^IBEX":"IBEX 35","^GDAXI":"DAX 40",
            "^N225":"Nikkei 225","^HSI":"Hang Seng",
        }
        idx_data = []
        for sym, name in indices.items():
            h, _ = descargar(sym, "6mo")
            if not h.empty and len(h) > 2:
                last = h["Close"].iloc[-1]; prev = h["Close"].iloc[-2]
                chg   = (last / prev - 1) * 100
                chg_m = (last / h["Close"].iloc[-22] - 1) * 100 if len(h) > 22 else np.nan
                chg_y = (last / h["Close"].iloc[0] - 1) * 100
                idx_data.append({"Índice": name, "Último": round(last, 2),
                                  "Día %": round(chg, 2),
                                  "Mes %": round(chg_m, 2) if pd.notna(chg_m) else None,
                                  "YTD %": round(chg_y, 2)})
            time.sleep(0.3)

    if idx_data:
        st.subheader("🌍 Índices Principales")
        df_idx = pd.DataFrame(idx_data)
        cols = st.columns(4)
        for i, row in df_idx.iterrows():
            with cols[i % 4]:
                st.metric(row["Índice"], f"{row['Último']:,.2f}", f"{row['Día %']:+.2f}%")
        st.dataframe(df_idx, use_container_width=True, hide_index=True)
        st.caption("📡 Fuente: Finnhub / Yahoo Finance")

    # Sectores
    st.subheader("📊 Rendimiento Sectorial (1 día)")
    sectores = {
        "XLK":"Tecnología","XLF":"Financiero","XLE":"Energía","XLV":"Salud",
        "XLY":"Cons. Discrecional","XLP":"Cons. Básico","XLI":"Industrial",
        "XLU":"Utilities","XLB":"Materiales","XLRE":"Inmobiliario","XLC":"Comunicación",
    }
    sec_data = []
    with st.spinner("Cargando sectores..."):
        for sym, name in sectores.items():
            h, _ = descargar(sym, "6mo")
            if not h.empty and len(h) > 2:
                chg = (h["Close"].iloc[-1] / h["Close"].iloc[-2] - 1) * 100
                sec_data.append({"Sector": name, "Ticker": sym, "Cambio %": round(chg, 2)})
            time.sleep(0.2)
    if sec_data:
        df_sec = pd.DataFrame(sec_data).sort_values("Cambio %", ascending=False)
        colors = ["#50fa7b" if v >= 0 else "#ff5555" for v in df_sec["Cambio %"]]
        fig_sec = go.Figure(go.Bar(
            x=df_sec["Sector"], y=df_sec["Cambio %"], marker_color=colors,
            text=[f"{v:+.2f}%" for v in df_sec["Cambio %"]], textposition="outside"))
        fig_sec.update_layout(title="Mapa Sectorial — Cambio Diario", template="plotly_dark",
                               paper_bgcolor="#12121f", plot_bgcolor="#1e1e2e",
                               height=400, yaxis_title="%")
        st.plotly_chart(fig_sec, use_container_width=True)
        st.caption("📡 ETFs sectoriales SPDR vía Finnhub / Yahoo Finance")

    # Macro rápido
    if fred_client:
        st.subheader("📈 Indicadores Macro (FRED)")
        macro_ids = {"Fed Funds Rate":"FEDFUNDS","US 10Y":"DGS10","US 2Y":"DGS2",
                     "CPI YoY":"CPIAUCSL","Desempleo":"UNRATE","VIX":"VIXCLS"}
        mc = st.columns(3); i = 0
        for name, sid in macro_ids.items():
            v = get_last_fred(sid)
            if v is not None:
                with mc[i % 3]:
                    st.metric(name, f"{v:.2f}")
                i += 1

    # Noticias
    if fh_client:
        st.subheader("📰 Noticias del Mercado")
        st.caption("📡 Finnhub News API")
        try:
            news = fh_client.general_news("general", min_id=0)
            if news:
                for a in news[:8]:
                    c1, c2 = st.columns([3, 1])
                    with c1:
                        st.markdown(f"**{a.get('headline', 'Sin título')}**")
                        st.caption(f"{a.get('source','')} — {datetime.fromtimestamp(a.get('datetime',0)).strftime('%d/%m %H:%M')}")
                        s = a.get("summary", "")
                        if s: st.caption(s[:200] + "..." if len(s) > 200 else s)
                    with c2:
                        if a.get("url"): st.link_button("Leer →", a["url"])
                    st.divider()
        except Exception:
            st.info("No se pudieron cargar noticias.")


# ╔═══════════════════════════════════════════════════════════════╗
# ║  SCREENER                                                     ║
# ╚═══════════════════════════════════════════════════════════════╝
elif pagina == "🔍 Screener":
    with st.sidebar:
        indice  = st.selectbox("Índice", list(INDICES.keys()), format_func=lambda x: INDICES[x][0])
        modo    = st.selectbox("Modo", ["VALUE","MOMENTUM","QUALITY","DIVIDENDOS","TODO"])
        limite  = st.slider("Tickers", 10, 100, 30, step=10)
        st.divider()
        ejecutar = st.button("🚀 Ejecutar", type="primary", use_container_width=True)

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
# ║  ANÁLISIS INDIVIDUAL                                          ║
# ╚═══════════════════════════════════════════════════════════════╝
elif pagina == "📈 Análisis Individual":
    with st.sidebar:
        ticker_in = st.text_input("Ticker", value="AAPL")
        tab       = st.radio("Vista", ["🔧 Técnico","📋 Fundamental","🔧+📋 Completo"])
        st.divider()
        go_btn    = st.button("🚀 Analizar", type="primary", use_container_width=True)

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
# ║  PREDICCIÓN IA                                                ║
# ╚═══════════════════════════════════════════════════════════════╝
elif pagina == "🔮 Predicción IA":
    st.header("🔮 Predicción IA — Señal Direccional")
    st.markdown(
        "Predicción de dirección del precio para el **día siguiente** "
        "usando modelos de redes neuronales entrenados sobre datos 2010–2018 "
        "y evaluados en test 2021–2023."
    )

    if not TORCH_OK:
        st.error("PyTorch no está instalado. Añade `torch` a requirements.txt.")
        st.stop()

    registry = cargar_registry()

    if registry is None:
        st.error("No se encontró `model_registry.json` en el repositorio.")
        st.info(
            "**Pasos para activar este módulo:**\n"
            "1. Ejecuta el Bloque 12 en tu notebook de Colab\n"
            "2. Descarga `models_export.zip`\n"
            "3. Sube los archivos `.pt` y `model_registry.json` a la raíz del repo GitHub\n"
            "4. Commit & push → el módulo se activa automáticamente"
        )
        st.stop()

    TICKERS_DISP = list(registry.keys())

    with st.sidebar:
        ticker_pred      = st.selectbox("Ticker", TICKERS_DISP)
        mostrar_features = st.checkbox("Ver features de entrada", value=False)
        st.divider()
        run_pred = st.button("🔮 Predecir", type="primary", use_container_width=True)

    meta_ticker = registry.get(ticker_pred, {})

    c1, c2, c3 = st.columns(3)
    c1.metric("Arquitectura",        meta_ticker.get("model_name", "—"))
    c2.metric("Dir. Accuracy test",  f"{meta_ticker.get('test_dir_accuracy', 0)*100:.1f}%")
    c3.metric("Sharpe test",         f"{meta_ticker.get('test_sharpe', 0):+.3f}")
    st.caption(
        f"📊 Entrenado: {meta_ticker.get('train_period','—')} | "
        f"Evaluado: {meta_ticker.get('test_period','—')} | "
        f"Ventana: {meta_ticker.get('win_seq', 30)} días | "
        f"Features: {meta_ticker.get('n_features', 10)}"
    )

    if run_pred:
        with st.spinner(f"Cargando modelo {meta_ticker.get('model_name','—')} para {ticker_pred}..."):
            model, meta_o = cargar_modelo(ticker_pred, json.dumps(registry))

        if model is None:
            st.error(f"No se pudo cargar el modelo: {meta_o}")
            st.stop()

        with st.spinner("Descargando datos y calculando features..."):
            features_df, err = build_features_app(ticker_pred)

        if features_df is None:
            st.error(f"Error construyendo features: {err}")
            st.stop()

        resultado = predecir_senal(ticker_pred, model, meta_o, features_df)

        st.divider()
        st.subheader(f"📡 Señal para {ticker_pred} — próximo día hábil")

        prob_alc = resultado["prob_alcista"]
        conf     = resultado["confianza"]
        signal   = resultado["signal"]

        if   prob_alc > 0.65: color, emoji = "#50fa7b", "🟢"
        elif prob_alc > 0.5:  color, emoji = "#8be9fd", "🔵"
        elif prob_alc > 0.35: color, emoji = "#ffb86c", "🟡"
        else:                  color, emoji = "#ff5555", "🔴"

        st.markdown(
            f"<div style='text-align:center; padding:20px; background:#1e1e2e; "
            f"border-radius:12px; border: 2px solid {color};'>"
            f"<h1 style='color:{color}; margin:0'>{emoji} {signal}</h1>"
            f"<p style='color:#ccc; margin:8px 0 0 0; font-size:1.1em'>"
            f"Probabilidad alcista: <b style='color:{color}'>{prob_alc*100:.1f}%</b>"
            f"</p></div>",
            unsafe_allow_html=True
        )
        st.markdown("")

        m1, m2, m3 = st.columns(3)
        m1.metric("Prob. Alcista", f"{prob_alc*100:.1f}%")
        m2.metric("Prob. Bajista", f"{resultado['prob_bajista']*100:.1f}%")
        m3.metric("Confianza",     f"{conf*100:.1f}%",
                   help="0% = máxima incertidumbre · 100% = señal muy clara")

        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=prob_alc * 100,
            title={"text": "Probabilidad Alcista (%)"},
            gauge={
                "axis":       {"range": [0, 100], "tickwidth": 1},
                "bar":        {"color": color},
                "bgcolor":    "#1e1e2e",
                "bordercolor":"#44475a",
                "steps": [
                    {"range": [0,  35],  "color": "rgba(255,85,85,0.3)"},
                    {"range": [35, 50],  "color": "rgba(255,184,108,0.2)"},
                    {"range": [50, 65],  "color": "rgba(139,233,253,0.2)"},
                    {"range": [65, 100], "color": "rgba(80,250,123,0.3)"},
                ],
                "threshold": {"line": {"color": "#f8f8f2", "width": 3},
                              "thickness": 0.8, "value": 50}
            },
            number={"suffix": "%", "font": {"size": 32}},
        ))
        fig_gauge.update_layout(
            template="plotly_dark", paper_bgcolor="#12121f",
            height=300, margin=dict(t=40, b=20, l=30, r=30))
        st.plotly_chart(fig_gauge, use_container_width=True)

        if mostrar_features:
            st.subheader("🔬 Últimos 10 días de features (entrada al modelo)")
            WIN   = meta_o.get("win_seq", 30)
            mean  = np.array(meta_o["scaler_mean"])
            scale = np.array(meta_o["scaler_scale"])
            FCOLS = meta_o["feature_cols"]
            X_norm = (features_df[FCOLS].values[-WIN:] - mean) / scale
            df_feat = pd.DataFrame(X_norm[-10:], columns=FCOLS)
            df_feat.index = features_df.index[-10:].strftime("%d/%m/%y")
            st.dataframe(df_feat.style.background_gradient(cmap="RdYlGn", axis=None),
                         use_container_width=True)
            st.caption("Valores normalizados con StandardScaler entrenado en datos 2010-2018")

        st.divider()
        with st.expander("ℹ️ Cómo interpretar esta señal"):
            st.markdown(f"""
**¿Qué predice el modelo?**
El modelo {meta_ticker.get('model_name','—')} genera una probabilidad de que el
**retorno logarítmico del día siguiente sea positivo** (precio sube).

**Features utilizadas (10 variables):**
Log-retorno diario, ratios MA-5/10/20, volumen normalizado, RSI normalizado,
volatilidad realizada 20d, momentum 10d, VIX, spread 10Y-2Y.

**Contexto del entrenamiento:**
- Train: 2010–2018 | Val: 2019–2020 | Test: 2021–2023
- Directional Accuracy en test: **{meta_ticker.get('test_dir_accuracy',0)*100:.1f}%**
- Sharpe de la estrategia basada en la señal: **{meta_ticker.get('test_sharpe',0):+.3f}**

**⚠️ Limitaciones importantes:**
- Esta es una señal estadística, no una recomendación de inversión
- El modelo fue entrenado hasta 2018; el mercado ha cambiado desde entonces
- Úsala como uno más de los inputs del análisis junto con el técnico y el fundamental
            """)
        st.caption(
            f"📡 Datos: Finnhub / Yahoo Finance / FRED | "
            f"Modelo: {meta_ticker.get('model_name','—')} entrenado en Colab (PyTorch) | "
            f"Inferencia: Streamlit"
        )

    else:
        st.info("👈 Selecciona un ticker y pulsa **Predecir**.")
        st.subheader("📋 Modelos disponibles")
        rows = []
        for tk, meta in registry.items():
            rows.append({
                "Ticker":        tk,
                "Arquitectura":  meta.get("model_name","—"),
                "Dir. Accuracy": f"{meta.get('test_dir_accuracy',0)*100:.1f}%",
                "Sharpe test":   f"{meta.get('test_sharpe',0):+.3f}",
                "Max DD test":   f"{meta.get('test_max_dd',0)*100:.1f}%",
                "Período test":  meta.get("test_period","—"),
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        st.caption(
            "Modelos entrenados en el curso Redes Neuronales y Aplicaciones Financieras — VIU | "
            "Selección automática del mejor modelo por ticker según Sharpe en test"
        )


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
