"""
TFM — PLATAFORMA DE INVERSIÓN INTELIGENTE
Master IA Sector Financiero — VIU 2025/26
Screener · Análisis Individual · Cartera
Fuentes: Finnhub (primario) + yfinance (fallback) + FRED (macro)
"""
import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from collections import Counter
import requests
import time
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")

try:
    import finnhub
    HAS_FINNHUB = True
except ImportError:
    HAS_FINNHUB = False

try:
    from fredapi import Fred
    HAS_FRED = True
except ImportError:
    HAS_FRED = False

st.set_page_config(page_title="TFM — Investment Intelligence", page_icon="📊", layout="wide", initial_sidebar_state="expanded")

# ── API keys ──
FINNHUB_KEY = ""
FRED_KEY = ""
finnhub_client = None
fred_client = None
try:
    FINNHUB_KEY = st.secrets["FINNHUB_KEY"]
    if HAS_FINNHUB and FINNHUB_KEY:
        finnhub_client = finnhub.Client(api_key=FINNHUB_KEY)
except Exception:
    pass
try:
    FRED_KEY = st.secrets["FRED_KEY"]
    if HAS_FRED and FRED_KEY:
        fred_client = Fred(api_key=FRED_KEY)
except Exception:
    pass


# ╔═══════════════════════════════════════════════════════════════╗
# ║  DATA LAYER                                                   ║
# ╚═══════════════════════════════════════════════════════════════╝
@st.cache_data(ttl=1800, show_spinner=False)
def finnhub_candles(ticker, days=365):
    if not finnhub_client:
        return pd.DataFrame()
    try:
        now = int(datetime.now().timestamp())
        start = int((datetime.now() - timedelta(days=days)).timestamp())
        r = finnhub_client.stock_candles(ticker, "D", start, now)
        if r.get("s") != "ok" or not r.get("c"):
            return pd.DataFrame()
        return pd.DataFrame({"Open": r["o"], "High": r["h"], "Low": r["l"], "Close": r["c"], "Volume": r["v"]},
                            index=pd.to_datetime(r["t"], unit="s"))
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def finnhub_info(ticker):
    if not finnhub_client:
        return {}
    info = {}
    try:
        p = finnhub_client.company_profile2(symbol=ticker)
        if p:
            info["longName"] = p.get("name", ticker)
            info["sector"] = p.get("finnhubIndustry", "N/A")
            info["industry"] = p.get("finnhubIndustry", "N/A")
            info["marketCap"] = (p.get("marketCapitalization") or 0) * 1e6
            info["currency"] = p.get("currency", "USD")
    except Exception:
        pass
    try:
        m = finnhub_client.company_basic_financials(ticker, "all").get("metric", {})
        if m:
            info["trailingPE"] = m.get("peBasicExclExtraTTM")
            info["forwardPE"] = m.get("peTTM")
            info["priceToBook"] = m.get("pbAnnual")
            info["priceToSalesTrailing12Months"] = m.get("psTTM")
            info["returnOnEquity"] = (m.get("roeTTM") or 0) / 100 if m.get("roeTTM") else None
            info["returnOnAssets"] = (m.get("roaTTM") or 0) / 100 if m.get("roaTTM") else None
            info["profitMargins"] = (m.get("netProfitMarginTTM") or 0) / 100 if m.get("netProfitMarginTTM") else None
            info["grossMargins"] = (m.get("grossMarginTTM") or 0) / 100 if m.get("grossMarginTTM") else None
            info["dividendYield"] = (m.get("dividendYieldIndicatedAnnual") or 0) / 100 if m.get("dividendYieldIndicatedAnnual") else None
            info["beta"] = m.get("beta")
            info["debtToEquity"] = m.get("totalDebt/totalEquityAnnual")
    except Exception:
        pass
    try:
        t = finnhub_client.price_target(ticker)
        if t:
            info["targetMeanPrice"] = t.get("targetMean")
            info["targetHighPrice"] = t.get("targetHigh")
            info["targetLowPrice"] = t.get("targetLow")
    except Exception:
        pass
    try:
        recs = finnhub_client.recommendation_trends(ticker)
        if recs:
            r = recs[0]
            buys = r.get("strongBuy", 0) + r.get("buy", 0)
            sells = r.get("strongSell", 0) + r.get("sell", 0)
            info["recommendationKey"] = "BUY" if buys > sells else ("SELL" if sells > buys else "HOLD")
    except Exception:
        pass
    return info

@st.cache_data(ttl=1800, show_spinner=False)
def yf_download(ticker, period="2y"):
    try:
        s = yf.Ticker(ticker)
        h = s.history(period=period)
        i = s.info or {}
        if h.empty or len(i) < 3:
            raise ValueError("empty")
        try:
            f = s.financials
        except Exception:
            f = pd.DataFrame()
        try:
            b = s.balance_sheet
        except Exception:
            b = pd.DataFrame()
        try:
            c = s.cashflow
        except Exception:
            c = pd.DataFrame()
        return h, i, f, b, c
    except Exception:
        raise

def yf_safe(ticker, period="2y"):
    try:
        return yf_download(ticker, period)
    except Exception:
        return pd.DataFrame(), {}, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

def smart_download(ticker, period="1y"):
    days = {"6mo": 180, "1y": 365, "2y": 730}.get(period, 365)
    hist = finnhub_candles(ticker, days)
    info = finnhub_info(ticker)
    if not hist.empty and len(hist) > 30:
        return hist, info
    h2, i2, _, _, _ = yf_safe(ticker, period)
    merged = {**i2, **{k: v for k, v in info.items() if v is not None and v != 0}}
    return h2, merged

@st.cache_data(ttl=3600, show_spinner=False)
def fred_series(series_id, start="2020-01-01"):
    if not fred_client:
        return pd.Series(dtype=float)
    try:
        return fred_client.get_series(series_id, observation_start=start)
    except Exception:
        return pd.Series(dtype=float)

FRED_IDS = {"Fed Funds Rate": "FEDFUNDS", "CPI": "CPIAUCSL", "US 10Y": "DGS10",
            "US 2Y": "DGS2", "Unemployment": "UNRATE", "VIX": "VIXCLS"}


# ╔═══════════════════════════════════════════════════════════════╗
# ║  INDICADORES TÉCNICOS                                        ║
# ╚═══════════════════════════════════════════════════════════════╝
def calc_rsi(s, p=14):
    d = s.diff(); g = d.where(d > 0, 0).rolling(p).mean(); l = (-d.where(d < 0, 0)).rolling(p).mean()
    return 100 - (100 / (1 + g / l))

def calc_macd(s, f=12, sl=26, sg=9):
    ef = s.ewm(span=f, adjust=False).mean(); es = s.ewm(span=sl, adjust=False).mean()
    m = ef - es; si = m.ewm(span=sg, adjust=False).mean(); return m, si, m - si

def calc_adx(df, p=14):
    h, l, c = df["High"], df["Low"], df["Close"]
    tr = pd.concat([h-l, (h-c.shift(1)).abs(), (l-c.shift(1)).abs()], axis=1).max(axis=1)
    up = h - h.shift(1); dn = l.shift(1) - l
    pdm = pd.Series(np.where((up > dn) & (up > 0), up, 0), index=df.index)
    mdm = pd.Series(np.where((dn > up) & (dn > 0), dn, 0), index=df.index)
    atr = tr.rolling(p).mean()
    pdi = 100 * (pdm.rolling(p).mean() / atr); mdi = 100 * (mdm.rolling(p).mean() / atr)
    dx = 100 * (pdi - mdi).abs() / (pdi + mdi).replace(0, np.nan)
    return dx.rolling(p).mean(), pdi, mdi

def calc_obv(df):
    d = np.sign(df["Close"].diff()); obv = (d * df["Volume"]).fillna(0).cumsum()
    osma = obv.rolling(20).mean()
    ot = pd.Series(np.where(obv > osma, 1, -1), index=df.index)
    p20 = df["Close"].pct_change(20); o20 = obv.pct_change(20)
    div = pd.Series(np.where((p20 < 0) & (o20 > 0), "Alcista", np.where((p20 > 0) & (o20 < 0), "Bajista", "Neutral")), index=df.index)
    return obv, osma, ot, div

def calc_stoch(df, kp=14, dp=3):
    lm = df["Low"].rolling(kp).min(); hm = df["High"].rolling(kp).max()
    k = 100 * (df["Close"] - lm) / (hm - lm).replace(0, np.nan); return k, k.rolling(dp).mean()

def calc_bb(s, p=20, std=2):
    m = s.rolling(p).mean(); sg = s.rolling(p).std()
    u = m + std * sg; l = m - std * sg; pb = (s - l) / (u - l).replace(0, np.nan)
    return u, m, l, pb

def calc_atr(df, p=14):
    tr = pd.concat([df["High"]-df["Low"], (df["High"]-df["Close"].shift(1)).abs(), (df["Low"]-df["Close"].shift(1)).abs()], axis=1).max(axis=1)
    a = tr.rolling(p).mean(); return a, a / df["Close"] * 100


# ╔═══════════════════════════════════════════════════════════════╗
# ║  SCORING TÉCNICO 0–10                                        ║
# ╚═══════════════════════════════════════════════════════════════╝
def score_tecnico(row, obv_t, obv_d):
    sc = 0.0; det = {}
    rsi = row["RSI"]
    if rsi < 30: p, m = 2.5, f"Sobrevendido ({rsi:.0f})"
    elif rsi < 45: p, m = 2.2, f"Zona acumulación ({rsi:.0f})"
    elif rsi < 55: p, m = 2.0, f"Neutral ({rsi:.0f})"
    elif rsi < 65: p, m = 1.5, f"Momentum+ ({rsi:.0f})"
    elif rsi < 75: p, m = 0.5, f"Zona caliente ({rsi:.0f})"
    else: p, m = 0.0, f"Sobrecomprado ({rsi:.0f})"
    sc += p; det["RSI"] = {"pts": p, "max": 2.5, "val": f"{rsi:.1f}", "msg": m}

    mv, sv, hv = row["MACD"], row["Signal"], row["MACD_Hist"]
    ph = row.get("MACD_Hist_prev", 0) or 0; al = mv > sv; ac = hv > ph
    if al and ac: p, m = 2.0, "Cruce alcista acelerando"
    elif al: p, m = 1.5, "Por encima de señal"
    elif not al and ac: p, m = 0.7, "Bajista perdiendo fuerza"
    else: p, m = 0.0, "Bajista acelerando"
    sc += p; det["MACD"] = {"pts": p, "max": 2.0, "val": f"{mv:.4f}", "msg": m}

    av, dp, dm = row["ADX"], row["DI_Plus"], row["DI_Minus"]
    if av > 30 and dp > dm: p, m = 2.0, f"Alcista FUERTE (ADX={av:.0f})"
    elif av > 20 and dp > dm: p, m = 1.5, f"Alcista moderada (ADX={av:.0f})"
    elif av > 20 and dp < dm: p, m = 0.3, f"Bajista activa (ADX={av:.0f})"
    elif av < 20: p, m = 1.0, f"Lateralización (ADX={av:.0f})"
    else: p, m = 0.7, f"Débil (ADX={av:.0f})"
    sc += p; det["ADX"] = {"pts": p, "max": 2.0, "val": f"{av:.1f}", "msg": m}

    if obv_t == 1 and obv_d == "Alcista": p, m = 1.5, "Compradora + div alcista"
    elif obv_t == 1: p, m = 1.2, "Compradora (OBV>SMA20)"
    elif obv_t == -1 and obv_d == "Bajista": p, m = 0.0, "Vendedora + div bajista"
    else: p, m = 0.3, "Vendedora"
    sc += p; det["OBV"] = {"pts": p, "max": 1.5, "val": "Alcista" if obv_t == 1 else "Bajista", "msg": m}

    kv, dv = row["Stoch_K"], row["Stoch_D"]
    if kv < 20 and kv > dv: p, m = 1.0, f"Sobrevendido+cruce (%K={kv:.0f})"
    elif kv < 25: p, m = 0.8, f"Sobrevendida (%K={kv:.0f})"
    elif kv > 80: p, m = 0.0, f"Sobrecomprada (%K={kv:.0f})"
    elif kv > dv: p, m = 0.6, f"Momentum+ (%K={kv:.0f}>%D)"
    else: p, m = 0.2, f"Momentum- (%K={kv:.0f}<%D)"
    sc += p; det["Stochastic"] = {"pts": p, "max": 1.0, "val": f"{kv:.1f}", "msg": m}

    bb = row["BB_PctB"]
    if bb < 0: p, m = 1.0, f"Debajo banda inf (%B={bb:.2f})"
    elif bb < 0.35: p, m = 1.0, f"Zona inferior (%B={bb:.2f})"
    elif bb < 0.65: p, m = 0.5, f"Zona media (%B={bb:.2f})"
    elif bb < 1.0: p, m = 0.1, f"Zona superior (%B={bb:.2f})"
    else: p, m = 0.0, f"Encima banda sup (%B={bb:.2f})"
    sc += p; det["Bollinger%B"] = {"pts": p, "max": 1.0, "val": f"{bb:.2f}", "msg": m}
    return round(min(sc, 10.0), 1), det

def interpretar_score(sc):
    if sc >= 8.0: return "🟢 COMPRAR / ACUMULAR", "#50fa7b"
    elif sc >= 6.0: return "🔵 MANTENER / VIGILAR", "#8be9fd"
    elif sc >= 4.0: return "🟡 NEUTRO / ESPERAR", "#f1fa8c"
    else: return "🔴 REDUCIR / VENDER", "#ff5555"


# ╔═══════════════════════════════════════════════════════════════╗
# ║  NIVELES OPERATIVOS                                          ║
# ╚═══════════════════════════════════════════════════════════════╝
def niveles_operativos(hist, info):
    last = hist.iloc[-1]; precio = last["Close"]
    tr = pd.concat([hist["High"]-hist["Low"], (hist["High"]-hist["Close"].shift(1)).abs(), (hist["Low"]-hist["Close"].shift(1)).abs()], axis=1).max(axis=1)
    atr_val = tr.rolling(14).mean().iloc[-1]
    bb_low = last.get("BB_Low", np.nan); sma50 = last.get("SMA_50", np.nan)
    soportes = [v for v in [bb_low, sma50] if pd.notna(v) and v < precio]
    entrada_opt = round(max(soportes), 2) if soportes else round(precio * 0.97, 2)
    sl_atr = precio - 1.5 * atr_val; sop_20d = hist["Low"].iloc[-20:].min()
    if sop_20d > sl_atr and sop_20d < precio:
        sl = round(sop_20d, 2); sl_nota = f"Soporte 20d ({sop_20d:.2f}) > SL ATR ({sl_atr:.2f})"
    else:
        sl = round(sl_atr, 2); sl_nota = f"ATR×1.5 ({sl_atr:.2f})"
    riesgo = precio - sl; riesgo_pct = round(riesgo / precio * 100, 2)
    tp1 = round(precio + 2 * riesgo, 2); tp2 = round(precio + 3 * riesgo, 2)
    tm = info.get("targetMeanPrice"); tp3 = round(tm, 2) if tm and tm > precio else None
    return {"precio": round(precio, 2), "entrada_agresiva": round(precio, 2), "entrada_optima": entrada_opt,
            "stop_loss": sl, "sl_nota": sl_nota, "riesgo": round(riesgo, 2), "riesgo_pct": riesgo_pct,
            "atr": round(atr_val, 2), "tp1": tp1, "tp2": tp2, "tp3": tp3, "soporte_20d": round(sop_20d, 2)}


# ╔═══════════════════════════════════════════════════════════════╗
# ║  FUNDAMENTALES                                                ║
# ╚═══════════════════════════════════════════════════════════════╝
def _safe(df, key, col=0, default=0):
    try:
        v = df.loc[key].iloc[col] if key in df.index else default
        return v if not pd.isna(v) else default
    except Exception:
        return default

def calc_piotroski(fin, bs, cf):
    sc = 0; det = {}
    try:
        ta0 = _safe(bs, "Total Assets", 0, 1); ta1 = _safe(bs, "Total Assets", 1, 1)
        ni0 = _safe(fin, "Net Income", 0); ni1 = _safe(fin, "Net Income", 1)
        cfo = _safe(cf, "Operating Cash Flow", 0)
        ca0 = _safe(bs, "Current Assets", 0); cl0 = _safe(bs, "Current Liabilities", 0) or 1
        ca1 = _safe(bs, "Current Assets", 1); cl1 = _safe(bs, "Current Liabilities", 1) or 1
        ltd0 = _safe(bs, "Long Term Debt", 0); ltd1 = _safe(bs, "Long Term Debt", 1)
        rev0 = _safe(fin, "Total Revenue", 0) or 1; rev1 = _safe(fin, "Total Revenue", 1) or 1
        gp0 = _safe(fin, "Gross Profit", 0); gp1 = _safe(fin, "Gross Profit", 1)
        tests = [
            ("F1 ROA positivo", ni0/ta0 > 0, f"{ni0/ta0*100:.2f}%"),
            ("F2 CFO positivo", cfo > 0, f"${cfo/1e6:.0f}M"),
            ("F3 ROA mejora", ni0/ta0 > ni1/ta1, f"{ni0/ta0*100:.2f}% vs {ni1/ta1*100:.2f}%"),
            ("F4 CFO > NI", cfo > ni0, f"CFO {cfo/1e6:.0f}M > NI {ni0/1e6:.0f}M"),
            ("F5 Menor deuda", ltd0/ta0 < ltd1/ta1, f"{ltd0/ta0:.3f} vs {ltd1/ta1:.3f}"),
            ("F6 Mejor liquidez", ca0/cl0 > ca1/cl1, f"{ca0/cl0:.2f} vs {ca1/cl1:.2f}"),
            ("F7 Sin dilución", True, "(manual)"),
            ("F8 Margen bruto+", gp0/rev0 > gp1/rev1, f"{gp0/rev0*100:.1f}% vs {gp1/rev1*100:.1f}%"),
            ("F9 Rot activos+", rev0/ta0 > rev1/ta1, f"{rev0/ta0:.3f} vs {rev1/ta1:.3f}"),
        ]
        for n, c, v in tests:
            p = 1 if c else 0; sc += p; det[n] = {"ok": bool(c), "val": v}
    except Exception as e:
        det["_error"] = {"ok": False, "val": str(e)}
    return sc, det

def calc_altman(info, fin, bs):
    try:
        ta = _safe(bs, "Total Assets", 0, 1); ca = _safe(bs, "Current Assets", 0); cl = _safe(bs, "Current Liabilities", 0)
        re = _safe(bs, "Retained Earnings", 0); ebit = _safe(fin, "EBIT", 0) or _safe(fin, "Operating Income", 0)
        tl = _safe(bs, "Total Liabilities Net Minority Interest", 0) or _safe(bs, "Total Debt", 0) or 1
        rev = _safe(fin, "Total Revenue", 0); mc = info.get("marketCap", 0)
        z = 1.2*((ca-cl)/ta) + 1.4*(re/ta) + 3.3*(ebit/ta) + 0.6*(mc/tl) + 1.0*(rev/ta)
        zona = "🟢 SEGURA" if z > 2.99 else ("🟡 GRIS" if z > 1.81 else "🔴 PELIGRO")
        return round(z, 2), zona
    except Exception:
        return None, "Sin datos"

def calc_graham(info):
    try:
        eps = info.get("trailingEps") or info.get("forwardEps"); bv = info.get("bookValue")
        if eps and bv and eps > 0 and bv > 0: return round((22.5 * eps * bv) ** 0.5, 2)
    except Exception:
        pass
    return None

def calc_fcf_yield(info, cf):
    try:
        fcf = info.get("freeCashflow")
        if not fcf: fcf = _safe(cf, "Operating Cash Flow", 0) - abs(_safe(cf, "Capital Expenditure", 0))
        mc = info.get("marketCap")
        if fcf and mc and mc > 0: return round(fcf / mc * 100, 2)
    except Exception:
        pass
    return None

def calc_dupont(fin, bs):
    try:
        ni = _safe(fin, "Net Income", 0); rev = _safe(fin, "Total Revenue", 0) or 1
        ta = _safe(bs, "Total Assets", 0) or 1; eq = _safe(bs, "Stockholders Equity", 0) or 1
        nm = ni/rev; at = rev/ta; lv = ta/eq
        return {"ROE": round(nm*at*lv*100, 2), "Margen_Neto": round(nm*100, 2), "Rot_Activos": round(at, 3), "Apalanc": round(lv, 2)}
    except Exception:
        return None

def calc_cagr(fin):
    try:
        if fin.empty or len(fin.columns) < 2: return None, None
        def cg(s):
            v = s.dropna()
            if len(v) < 2: return None
            vi, vf, n = v.iloc[-1], v.iloc[0], len(v) - 1
            if vi <= 0 or vf <= 0: return None
            return round(((vf/vi)**(1/n)-1)*100, 1)
        rv = fin.loc["Total Revenue"] if "Total Revenue" in fin.index else None
        ni = fin.loc["Net Income"] if "Net Income" in fin.index else None
        return (cg(rv) if rv is not None else None, cg(ni) if ni is not None else None)
    except Exception:
        return None, None

def mf(nombre, val, fmt, bueno, malo):
    if val is None: return f"**{nombre}:** N/A"
    ic = "✅" if bueno(val) else ("🔴" if malo(val) else "🟡")
    if fmt.endswith("%"):
        return f"{ic} **{nombre}:** {val:{fmt[:-1]}}%"
    return f"{ic} **{nombre}:** {val:{fmt}}"


# ╔═══════════════════════════════════════════════════════════════╗
# ║  SCREENER FUNCTIONS                                           ║
# ╚═══════════════════════════════════════════════════════════════╝
@st.cache_data(ttl=86400)
def get_sp500():
    try:
        t = pd.read_html(requests.get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", headers={"User-Agent": "Mozilla/5.0"}).text)
        return [x.replace(".", "-") for x in t[0]["Symbol"].tolist()]
    except Exception:
        return []

@st.cache_data(ttl=86400)
def get_sp600():
    try:
        t = pd.read_html(requests.get("https://en.wikipedia.org/wiki/List_of_S%26P_600_companies", headers={"User-Agent": "Mozilla/5.0"}).text)
        df = t[0] if "Symbol" in t[0].columns else t[1]
        col = "Symbol" if "Symbol" in df.columns else "Ticker symbol"
        return [x.replace(".", "-") for x in df[col].tolist()]
    except Exception:
        return []

@st.cache_data(ttl=86400)
def get_ibex():
    return ["SAN.MC","BBVA.MC","ITX.MC","IBE.MC","TEF.MC","FER.MC","AMS.MC","REP.MC","CABK.MC","ACS.MC","GRF.MC","MAP.MC","ENG.MC","RED.MC","IAG.MC","FDR.MC","MEL.MC","COL.MC","CLNX.MC","SAB.MC","BKT.MC","AENA.MC","LOG.MC","CIE.MC","ACX.MC","MRL.MC","PHM.MC","ROVI.MC","VIS.MC","ALM.MC","SGRE.MC","SLR.MC","UNI.MC","EDR.MC","SOL.MC"]

@st.cache_data(ttl=86400)
def get_dax():
    return ["SAP.DE","SIE.DE","ALV.DE","DTE.DE","AIR.DE","MBG.DE","MUV2.DE","DHL.DE","BAS.DE","BMW.DE","IFX.DE","BEI.DE","BAYN.DE","HEN3.DE","ADS.DE","VOW3.DE","DB1.DE","SY1.DE","RWE.DE","FRE.DE","CON.DE","MTX.DE","ENR.DE","PAH3.DE","ZAL.DE","HEI.DE","QIA.DE","MRK.DE","SHL.DE","DBK.DE"]

@st.cache_data(ttl=86400)
def get_ftse():
    return ["AZN.L","SHEL.L","HSBA.L","ULVR.L","BP.L","GSK.L","RIO.L","REL.L","DGE.L","LSEG.L","NG.L","BA.L","GLEN.L","VOD.L","PRU.L","CPG.L","AAL.L","LLOY.L","BARC.L","BATS.L"]

INDICES = {"SP500": ("S&P 500", get_sp500), "SP600": ("S&P 600", get_sp600), "IBEX35": ("IBEX 35", get_ibex), "DAX40": ("DAX 40", get_dax), "FTSE100": ("FTSE 100", get_ftse)}
CL = {"COMPRAR": "#50fa7b", "VIGILAR": "#8be9fd", "NEUTRO": "#f1fa8c", "EVITAR": "#ff5555"}

def score_screener(r):
    s = 0.0
    mom = r.get("Mom 3M %", np.nan)
    if pd.notna(mom) and mom > 20: s += 1.0
    elif pd.notna(mom) and mom > 10: s += 0.7
    elif pd.notna(mom) and mom > 0: s += 0.4
    if pd.notna(r.get("vs SMA50 %")) and r["vs SMA50 %"] > 0: s += 0.5
    if pd.notna(r.get("vs SMA200 %")) and r["vs SMA200 %"] > 0: s += 0.5
    vr = r.get("Vol/Avg 20d", np.nan)
    if pd.notna(vr) and vr > 2.0: s += 1.0
    elif pd.notna(vr) and vr > 1.3: s += 0.5
    per = r.get("PER", np.nan)
    if pd.notna(per) and 0 < per < 12: s += 1.5
    elif pd.notna(per) and 0 < per < 20: s += 1.0
    elif pd.notna(per) and 0 < per < 30: s += 0.3
    roe = r.get("ROE %", np.nan)
    if pd.notna(roe) and roe > 25: s += 1.0
    elif pd.notna(roe) and roe > 15: s += 0.7
    elif pd.notna(roe) and roe > 8: s += 0.3
    mg = r.get("Margen Net %", np.nan)
    if pd.notna(mg) and mg > 20: s += 1.0
    elif pd.notna(mg) and mg > 10: s += 0.7
    elif pd.notna(mg) and mg > 3: s += 0.3
    de = r.get("D/E", np.nan)
    if pd.notna(de) and de < 50: s += 0.5
    elif pd.notna(de) and de < 100: s += 0.3
    sig = str(r.get("Señales", ""))
    if "BREAKOUT" in sig: s += 0.75
    if "VOL_CONFIRM" in sig: s += 0.5
    if "ACUMULACION" in sig: s += 0.5
    if "TRAMPA_ALC" in sig: s -= 0.5
    pot = r.get("Potencial %", np.nan)
    if pd.notna(pot) and pot > 20: s += 0.5
    elif pd.notna(pot) and pot > 10: s += 0.3
    return round(min(max(s, 0), 10), 1)

def label_score(sc):
    if sc >= 7.5: return "COMPRAR"
    elif sc >= 6.0: return "VIGILAR"
    elif sc >= 4.0: return "NEUTRO"
    return "EVITAR"

def analizar_ticker(ticker):
    try:
        hist, info = smart_download(ticker, "1y")
        if hist.empty or len(hist) < 60: return None
        last = hist.iloc[-1]; prev = hist.iloc[-2] if len(hist) > 1 else last
        precio = last["Close"]
        if precio <= 0: return None
        mc_b = round(info.get("marketCap", 0) / 1e9, 2)
        mom1 = ((precio / hist["Close"].iloc[-21] - 1) * 100) if len(hist) > 21 else np.nan
        mom3 = ((precio / hist["Close"].iloc[-63] - 1) * 100) if len(hist) > 63 else np.nan
        sma50 = hist["Close"].rolling(50).mean().iloc[-1] if len(hist) >= 50 else np.nan
        sma200 = hist["Close"].rolling(200).mean().iloc[-1] if len(hist) >= 200 else np.nan
        vs50 = round((precio / sma50 - 1) * 100, 2) if pd.notna(sma50) else np.nan
        vs200 = round((precio / sma200 - 1) * 100, 2) if pd.notna(sma200) else np.nan
        mx52 = hist["High"].max(); mn52 = hist["Low"].min()
        dmx = round((precio / mx52 - 1) * 100, 2); dmn = round((precio / mn52 - 1) * 100, 2)
        vh = last["Volume"]; va20 = hist["Volume"].rolling(20).mean().iloc[-1]
        vr20 = round(vh / va20, 2) if va20 > 0 else np.nan
        v5 = hist["Volume"].iloc[-5:].mean(); v20m = hist["Volume"].iloc[-20:].mean()
        vt = "Creciente" if v20m > 0 and v5/v20m > 1.15 else ("Decreciente" if v20m > 0 and v5/v20m < 0.85 else "Estable")
        señ = []; rd = (precio / prev["Close"] - 1) if prev["Close"] > 0 else 0
        if rd > 0 and pd.notna(vr20) and vr20 > 1.5: señ.append("VOL_CONFIRM")
        if rd > 0 and pd.notna(vr20) and vr20 < 0.7: señ.append("TRAMPA_ALC")
        if pd.notna(mom3) and mom3 > 15 and vt == "Creciente": señ.append("MOMENTUM")
        per = info.get("trailingPE"); roe = info.get("returnOnEquity"); mn = info.get("profitMargins")
        de = info.get("debtToEquity"); dy = info.get("dividendYield")
        tm = info.get("targetMeanPrice"); pot = round((tm/precio-1)*100, 1) if tm and precio > 0 else np.nan
        r = {"Ticker": ticker, "Precio": round(precio, 2), "MktCap (B$)": mc_b,
             "Mom 3M %": round(mom3, 2) if pd.notna(mom3) else np.nan, "vs SMA50 %": vs50, "vs SMA200 %": vs200,
             "Dist Max52W %": dmx, "Vol/Avg 20d": vr20, "Vol Tendencia": vt,
             "Señales": " | ".join(señ) if señ else "-",
             "PER": round(per, 1) if per else np.nan, "ROE %": round(roe * 100, 1) if roe else np.nan,
             "Margen Net %": round(mn * 100, 1) if mn else np.nan, "D/E": round(de, 1) if de else np.nan,
             "Div Yield %": round(dy * 100, 2) if dy else 0, "Potencial %": pot,
             "Consenso": info.get("recommendationKey", "N/A")}
        r["Score"] = score_screener(r); r["Label"] = label_score(r["Score"])
        return r
    except Exception:
        return None

def filtrar(df, modo):
    d = df.copy()
    if modo == "VALUE":
        mask = d["PER"].between(0, 20) & (d["Margen Net %"] > 8) & (d["D/E"] < 150)
    elif modo == "MOMENTUM":
        mask = (d["Mom 3M %"] > 10) & (d["vs SMA50 %"] > 0)
    elif modo == "QUALITY":
        mask = (d["ROE %"] > 15) & (d["Margen Net %"] > 12)
    elif modo == "DIVIDENDOS":
        mask = (d["Div Yield %"] > 2.5) & (d["Margen Net %"] > 5)
    else:
        mask = pd.Series([True] * len(d), index=d.index)
    return d[mask].copy()


# ╔═══════════════════════════════════════════════════════════════╗
# ║  SIDEBAR                                                      ║
# ╚═══════════════════════════════════════════════════════════════╝
with st.sidebar:
    st.title("📊 TFM Investment App")
    st.caption("Master IA Sector Financiero — VIU")
    st.divider()
    pagina = st.radio("Módulo", ["🔍 Screener", "📈 Análisis Individual", "💼 Cartera"])
    st.divider()
    st.markdown("**APIs:**")
    st.caption(f"{'✅' if finnhub_client else '❌'} Finnhub")
    st.caption(f"{'✅' if fred_client else '❌'} FRED")
    st.caption("✅ yfinance (fallback)")


# ╔═══════════════════════════════════════════════════════════════╗
# ║  SCREENER                                                     ║
# ╚═══════════════════════════════════════════════════════════════╝
if pagina == "🔍 Screener":
    with st.sidebar:
        indice = st.selectbox("Índice", list(INDICES.keys()), format_func=lambda x: INDICES[x][0])
        modo = st.selectbox("Modo", ["VALUE", "MOMENTUM", "QUALITY", "DIVIDENDOS", "TODO"])
        limite = st.slider("Tickers", 10, 200, 50, step=10)
        min_mc = st.number_input("MktCap mín ($B)", value=0.5, step=0.5, min_value=0.0)
        st.divider()
        ejecutar = st.button("🚀 Ejecutar", type="primary", use_container_width=True)

    st.header(f"🔍 Screener: {INDICES[indice][0]} — {modo}")

    if ejecutar:
        with st.spinner("Obteniendo tickers..."):
            tickers = INDICES[indice][1]()
        if not tickers:
            st.error("No se pudieron obtener tickers.")
        else:
            ta = tickers[:limite]; res = []; pb = st.progress(0)
            for i, t in enumerate(ta):
                pb.progress((i+1)/len(ta), text=f"{t} ({i+1}/{len(ta)})")
                r = analizar_ticker(t)
                if r and r.get("MktCap (B$)", 0) >= min_mc:
                    res.append(r)
                if i % 5 == 4:
                    time.sleep(0.5)
            pb.empty()
            if not res:
                st.error("Sin resultados.")
            else:
                df_raw = pd.DataFrame(res)
                df_f = filtrar(df_raw, modo).sort_values("Score", ascending=False).reset_index(drop=True)
                st.subheader(f"{len(df_f)} de {len(df_raw)} activos")
                if df_f.empty:
                    st.warning("Ningún activo cumple los filtros.")
                else:
                    cols = ["Ticker", "Precio", "Score", "Label", "Señales", "Mom 3M %", "Vol/Avg 20d", "PER", "ROE %", "Margen Net %", "Potencial %", "MktCap (B$)"]
                    cols = [c for c in cols if c in df_f.columns]
                    st.dataframe(df_f[cols], use_container_width=True, height=500)
                    dv = df_f.dropna(subset=["Mom 3M %", "Vol/Avg 20d"]).head(25)
                    if not dv.empty:
                        fig = px.scatter(dv, x="Mom 3M %", y="Vol/Avg 20d", color="Label", color_discrete_map=CL, size="MktCap (B$)", hover_data=["Ticker", "Score"], text="Ticker", title="Momentum vs Volumen")
                        fig.update_traces(textposition="top center", textfont_size=9)
                        fig.update_layout(template="plotly_dark", height=500, paper_bgcolor="#12121f", plot_bgcolor="#1e1e2e")
                        st.plotly_chart(fig, use_container_width=True)
                    st.session_state["screener_results"] = df_f
                    st.download_button("📥 CSV", df_f.to_csv(index=False).encode("utf-8"), f"screener_{indice}_{modo}.csv", "text/csv")
    elif "screener_results" in st.session_state:
        st.dataframe(st.session_state["screener_results"], use_container_width=True, height=400)
    else:
        st.info("👈 Configura y pulsa **Ejecutar**.")


# ╔═══════════════════════════════════════════════════════════════╗
# ║  ANÁLISIS INDIVIDUAL                                          ║
# ╚═══════════════════════════════════════════════════════════════╝
elif pagina == "📈 Análisis Individual":
    with st.sidebar:
        scr = st.session_state.get("screener_results")
        scr_t = scr["Ticker"].tolist() if scr is not None and not scr.empty else []
        ticker_in = st.text_input("Ticker", value=scr_t[0] if scr_t else "MSFT")
        if scr_t:
            alt = st.selectbox("Del Screener:", ["—"] + scr_t)
            if alt != "—": ticker_in = alt
        tab = st.radio("Vista", ["🔧 Técnico", "📋 Fundamental", "🔧+📋 Completo"])
        st.divider()
        go_btn = st.button("🚀 Analizar", type="primary", use_container_width=True)

    if go_btn and ticker_in:
        ticker_in = ticker_in.upper().strip()
        with st.spinner(f"Analizando {ticker_in}..."):
            hist, info = smart_download(ticker_in, "2y")
            _, info_yf, fin, bs, cf = yf_safe(ticker_in, "2y")
            info = {**info_yf, **{k: v for k, v in info.items() if v is not None and v != 0 and v != "N/A"}}

        if hist.empty:
            st.error(f"Sin datos para {ticker_in}. Reintenta en unos minutos.")
            if st.button("🔄 Limpiar caché"): st.cache_data.clear(); st.rerun()
            st.stop()

        nombre = info.get("longName") or info.get("shortName", ticker_in)
        precio = hist["Close"].iloc[-1]; moneda = info.get("currency", "")
        st.header(f"🏢 {nombre} ({ticker_in})")
        h1, h2, h3, h4 = st.columns(4)
        h1.metric("Precio", f"{precio:.2f} {moneda}"); h2.metric("Sector", info.get("sector", "N/A"))
        h3.metric("MktCap", f"${info.get('marketCap', 0)/1e9:.2f}B"); h4.metric("Industria", info.get("industry", "N/A"))

        # ── TÉCNICO ──
        if tab in ["🔧 Técnico", "🔧+📋 Completo"]:
            st.divider(); st.subheader("🔧 Análisis Técnico")
            hist["RSI"] = calc_rsi(hist["Close"]); hist["SMA_50"] = hist["Close"].rolling(50).mean(); hist["SMA_200"] = hist["Close"].rolling(200).mean()
            bu, bm, bl, bp = calc_bb(hist["Close"]); hist["BB_Up"], hist["BB_Mid"], hist["BB_Low"], hist["BB_PctB"] = bu, bm, bl, bp
            mc, sg, mh = calc_macd(hist["Close"]); hist["MACD"], hist["Signal"], hist["MACD_Hist"] = mc, sg, mh; hist["MACD_Hist_prev"] = mh.shift(1)
            ax, dp, dm = calc_adx(hist); hist["ADX"], hist["DI_Plus"], hist["DI_Minus"] = ax.values, dp.values, dm.values
            ov, os, ot, od = calc_obv(hist); hist["OBV"], hist["OBV_SMA"] = ov, os
            sk, sd = calc_stoch(hist); hist["Stoch_K"], hist["Stoch_D"] = sk, sd
            at, ap = calc_atr(hist); hist["ATR_PCT"] = ap

            last = hist.iloc[-1]; sc, det = score_tecnico(last, ot.iloc[-1], od.iloc[-1])
            verd, colv = interpretar_score(sc)
            s1, s2, s3 = st.columns([1, 2, 1])
            s1.metric("SCORE", f"{sc}/10"); s2.markdown(f"### {verd}")
            s3.caption(f"SMA200: {'✅' if precio > last['SMA_200'] else '🔴'} | SMA50: {'✅' if precio > last['SMA_50'] else '🔴'} | ATR%: {last['ATR_PCT']:.2f}%")
            for ind, d in det.items():
                pct = d["pts"]/d["max"] if d["max"] > 0 else 0
                ic = "✅" if pct >= 0.7 else ("🟡" if pct >= 0.3 else "🔴")
                st.markdown(f"{ic} **{ind}** — {d['val']} — `{d['pts']:.1f}/{d['max']:.1f}` — {d['msg']}")

            # Niveles operativos
            st.markdown("---"); st.markdown("### 🎯 Niveles Operativos")
            nv = niveles_operativos(hist, info)
            n1, n2, n3 = st.columns(3)
            with n1: st.markdown("**ENTRADAS**"); st.markdown(f"🟢 **Agresiva:** {nv['entrada_agresiva']:.2f}"); st.markdown(f"🔵 **Óptima:** {nv['entrada_optima']:.2f}")
            with n2: st.markdown("**STOP LOSS**"); st.markdown(f"🔴 **SL:** {nv['stop_loss']:.2f} (−{nv['riesgo_pct']:.1f}%)"); st.caption(f"ATR: {nv['atr']:.2f} | Sop 20d: {nv['soporte_20d']:.2f}")
            with n3:
                st.markdown("**TAKE PROFIT**")
                st.markdown(f"🎯 **TP1 (2:1):** {nv['tp1']:.2f} (+{((nv['tp1']/precio-1)*100):.1f}%)")
                st.markdown(f"🎯 **TP2 (3:1):** {nv['tp2']:.2f} (+{((nv['tp2']/precio-1)*100):.1f}%)")
                if nv['tp3']: st.markdown(f"🎯 **TP3:** {nv['tp3']:.2f} (+{((nv['tp3']/precio-1)*100):.1f}%)")

            # Gráfico 5 paneles
            n = min(len(hist), 252); h = hist.iloc[-n:]
            fig = make_subplots(rows=5, cols=1, shared_xaxes=True, vertical_spacing=0.02, row_heights=[0.35, 0.15, 0.15, 0.15, 0.15])
            fig.add_trace(go.Scatter(x=h.index, y=h["Close"], name="Precio", line=dict(color="#f8f8f2", width=1.8)), row=1, col=1)
            fig.add_trace(go.Scatter(x=h.index, y=h["SMA_50"], name="SMA50", line=dict(color="#ffb86c", width=1.2, dash="dash")), row=1, col=1)
            fig.add_trace(go.Scatter(x=h.index, y=h["SMA_200"], name="SMA200", line=dict(color="#ff5555", width=1.2, dash="dash")), row=1, col=1)
            fig.add_trace(go.Scatter(x=h.index, y=h["BB_Up"], line=dict(color="#8be9fd", width=0.6), showlegend=False), row=1, col=1)
            fig.add_trace(go.Scatter(x=h.index, y=h["BB_Low"], name="BB", line=dict(color="#8be9fd", width=0.6), fill="tonexty", fillcolor="rgba(139,233,253,0.08)"), row=1, col=1)
            fig.add_hline(y=nv["stop_loss"], line_dash="solid", line_color="#ff5555", opacity=0.7, row=1, col=1, annotation_text=f"SL {nv['stop_loss']:.2f}", annotation_font_color="#ff5555", annotation_font_size=9)
            fig.add_hline(y=nv["tp1"], line_dash="dot", line_color="#f1fa8c", opacity=0.6, row=1, col=1, annotation_text=f"TP1 {nv['tp1']:.2f}", annotation_font_color="#f1fa8c", annotation_font_size=9)
            fig.add_hline(y=nv["tp2"], line_dash="dot", line_color="#ffb86c", opacity=0.6, row=1, col=1, annotation_text=f"TP2 {nv['tp2']:.2f}", annotation_font_color="#ffb86c", annotation_font_size=9)
            ch = ["#50fa7b" if v >= 0 else "#ff5555" for v in h["MACD_Hist"]]
            fig.add_trace(go.Bar(x=h.index, y=h["MACD_Hist"], marker_color=ch, showlegend=False), row=2, col=1)
            fig.add_trace(go.Scatter(x=h.index, y=h["MACD"], name="MACD", line=dict(color="#50fa7b", width=1.2)), row=2, col=1)
            fig.add_trace(go.Scatter(x=h.index, y=h["Signal"], name="Signal", line=dict(color="#ff79c6", width=1.2)), row=2, col=1)
            fig.add_trace(go.Scatter(x=h.index, y=h["RSI"], name="RSI", line=dict(color="#bd93f9", width=1.2)), row=3, col=1)
            fig.add_trace(go.Scatter(x=h.index, y=h["Stoch_K"], name="%K", line=dict(color="#f1fa8c", width=1, dash="dot")), row=3, col=1)
            fig.add_hline(y=70, line_dash="dash", line_color="#ff5555", opacity=0.5, row=3, col=1)
            fig.add_hline(y=30, line_dash="dash", line_color="#50fa7b", opacity=0.5, row=3, col=1)
            fig.add_trace(go.Scatter(x=h.index, y=h["ADX"], name="ADX", line=dict(color="#ffb86c", width=1.4)), row=4, col=1)
            fig.add_trace(go.Scatter(x=h.index, y=h["DI_Plus"], name="DI+", line=dict(color="#50fa7b", width=0.9)), row=4, col=1)
            fig.add_trace(go.Scatter(x=h.index, y=h["DI_Minus"], name="DI−", line=dict(color="#ff5555", width=0.9)), row=4, col=1)
            fig.add_trace(go.Scatter(x=h.index, y=h["OBV"], name="OBV", line=dict(color="#8be9fd", width=1.2)), row=5, col=1)
            fig.add_trace(go.Scatter(x=h.index, y=h["OBV_SMA"], name="OBV SMA", line=dict(color="#ff79c6", width=1, dash="dash")), row=5, col=1)
            fig.update_layout(title=f"{ticker_in} | Score: {sc}/10 | {verd}", template="plotly_dark", paper_bgcolor="#12121f", plot_bgcolor="#1e1e2e", height=1000, legend=dict(orientation="h", y=-0.02, font=dict(size=9)), hovermode="x unified")
            st.plotly_chart(fig, use_container_width=True)

        # ── FUNDAMENTAL ──
        if tab in ["📋 Fundamental", "🔧+📋 Completo"]:
            st.divider(); st.subheader("📋 Análisis Fundamental")
            per = info.get("trailingPE"); pfw = info.get("forwardPE"); peg = info.get("pegRatio")
            pb = info.get("priceToBook"); ps = info.get("priceToSalesTrailing12Months"); eve = info.get("enterpriseToEbitda")
            gn = calc_graham(info); fy = calc_fcf_yield(info, cf)
            st.markdown("### 💰 Valoración")
            v1, v2, v3 = st.columns(3)
            with v1: st.markdown(mf("PER", per, ".1f", lambda x: x<15, lambda x: x>30)); st.markdown(mf("PER Fwd", pfw, ".1f", lambda x: x<12, lambda x: x>25))
            with v2: st.markdown(mf("PEG", peg, ".2f", lambda x: x<1, lambda x: x>2)); st.markdown(mf("P/Book", pb, ".2f", lambda x: x<1.5, lambda x: x>5))
            with v3: st.markdown(mf("P/Ventas", ps, ".2f", lambda x: x<2, lambda x: x>10)); st.markdown(mf("EV/EBITDA", eve, ".1f", lambda x: x<10, lambda x: x>20))
            if fy:
                ic = "✅" if fy > 5 else ("🔴" if fy < 0 else "🟡"); st.markdown(f"{ic} **FCF Yield:** {fy:.2f}%")
            if gn and precio:
                dif = (precio/gn-1)*100; st.markdown(f"**Graham:** {gn:.2f} → {dif:+.1f}% — {'✅ INFRAVALORADO' if precio<gn else '⚠️ SOBREVALORADO'}")
            tm = info.get("targetMeanPrice")
            if tm and precio:
                up = (tm/precio-1)*100; st.markdown(f"{'✅' if up>10 else '🟡'} **Target:** {tm:.2f} ({up:+.1f}%)")

            st.markdown("### 📈 Rentabilidad")
            roe = info.get("returnOnEquity"); roa = info.get("returnOnAssets"); pm = info.get("profitMargins"); gm = info.get("grossMargins")
            dupont = calc_dupont(fin, bs); cagr_r, cagr_n = calc_cagr(fin)
            r1, r2, r3 = st.columns(3)
            with r1: st.markdown(mf("ROE", roe*100 if roe else None, ".1f%", lambda x: x>15, lambda x: x<5)); st.markdown(mf("ROA", roa*100 if roa else None, ".1f%", lambda x: x>8, lambda x: x<2))
            with r2: st.markdown(mf("M.Bruto", gm*100 if gm else None, ".1f%", lambda x: x>40, lambda x: x<20)); st.markdown(mf("M.Neto", pm*100 if pm else None, ".1f%", lambda x: x>15, lambda x: x<3))
            with r3:
                if cagr_r is not None: st.markdown(f"{'✅' if cagr_r>7 else '🟡'} **CAGR Rev:** {cagr_r:+.1f}%")
                if cagr_n is not None: st.markdown(f"{'✅' if cagr_n>7 else '🟡'} **CAGR BN:** {cagr_n:+.1f}%")
            if dupont:
                d1, d2, d3, d4 = st.columns(4)
                d1.metric("ROE DuPont", f"{dupont['ROE']:.2f}%"); d2.metric("Margen", f"{dupont['Margen_Neto']:.2f}%")
                d3.metric("Rot.Act", f"{dupont['Rot_Activos']:.3f}x"); d4.metric("Apalanc", f"{dupont['Apalanc']:.2f}x")

            st.markdown("### 🏥 Salud Financiera")
            cr = info.get("currentRatio"); qr = info.get("quickRatio"); de = info.get("debtToEquity"); pay = info.get("payoutRatio")
            s1, s2, s3 = st.columns(3)
            with s1: st.markdown(mf("R.Corriente", cr, ".2f", lambda x: x>1.5, lambda x: x<1))
            with s2: st.markdown(mf("D/E", de, ".1f", lambda x: x<80, lambda x: x>200))
            with s3:
                if not bs.empty and not fin.empty:
                    z, zz = calc_altman(info, fin, bs)
                    if z: st.markdown(f"**Altman Z:** {z} → {zz}")

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

            st.markdown("### 🏆 Veredicto")
            pts = 0; mx = 0
            if per: mx += 2; pts += (2 if per < 15 else 1 if per < 25 else 0)
            if roe: mx += 2; pts += (2 if roe > 0.20 else 1 if roe > 0.10 else 0)
            if pm: mx += 2; pts += (2 if pm > 0.15 else 1 if pm > 0.05 else 0)
            if mx > 0:
                pf = pts/mx
                vf = "🟢 SÓLIDA" if pf >= 0.75 else ("🟡 ACEPTABLE" if pf >= 0.45 else "🔴 DÉBIL")
                st.markdown(f"**{vf}** — {pts}/{mx} ({pf*100:.0f}%)"); st.progress(pf)
    else:
        st.info("👈 Introduce un ticker y pulsa **Analizar**.")


# ╔═══════════════════════════════════════════════════════════════╗
# ║  CARTERA                                                      ║
# ╚═══════════════════════════════════════════════════════════════╝
elif pagina == "💼 Cartera":
    TD = 252
    with st.sidebar:
        periodo_c = st.selectbox("Período", ["6mo", "1y", "2y"], index=1)
        rf = st.number_input("Tasa libre riesgo %", value=4.5, step=0.25) / 100

    st.header("💼 Análisis de Cartera")
    if "cart_df" not in st.session_state:
        st.session_state["cart_df"] = pd.DataFrame({"Ticker": ["AAPL", "MSFT", "GOOGL", "GLD", "TLT"], "Cantidad": [10, 5, 3, 20, 15], "Precio Compra": [150.0, 240.0, 130.0, 180.0, 95.0], "Divisa": ["USD"]*5})
    edited = st.data_editor(st.session_state["cart_df"], num_rows="dynamic", use_container_width=True,
        column_config={"Ticker": st.column_config.TextColumn("Ticker", required=True), "Cantidad": st.column_config.NumberColumn("Cantidad", min_value=0, required=True),
                       "Precio Compra": st.column_config.NumberColumn("Precio Compra", min_value=0.0, format="%.2f"), "Divisa": st.column_config.SelectboxColumn("Divisa", options=["USD","EUR","CHF","GBP","JPY"])},
        key="cart_ed")
    st.session_state["cart_df"] = edited

    if st.button("🚀 Analizar Cartera", type="primary", use_container_width=True):
        dc = edited.dropna(subset=["Ticker"]).copy(); dc = dc[dc["Cantidad"] > 0]
        if len(dc) < 2: st.error("Mínimo 2 posiciones."); st.stop()
        precios = {}; valores = {}; errs = []
        with st.spinner("Descargando..."):
            for _, row in dc.iterrows():
                t = row["Ticker"].upper().strip()
                try:
                    ht, _ = smart_download(t, periodo_c)
                    if not ht.empty and len(ht) > 30:
                        precios[t] = ht["Close"]; valores[t] = row["Cantidad"] * ht["Close"].iloc[-1]
                    else: errs.append(t)
                except Exception: errs.append(t)
                time.sleep(0.3)
        if errs: st.warning(f"Sin datos: {', '.join(errs)}")
        if len(precios) < 2: st.error("Datos insuficientes."); st.stop()

        dfp = pd.DataFrame(precios).dropna(); dfr = dfp.pct_change().dropna()
        vt = sum(valores.values()); pesos = {t: v/vt for t, v in valores.items()}
        tv = [t for t in pesos if t in dfr.columns]; w = np.array([pesos[t] for t in tv]); w = w/w.sum()
        rc = dfr[tv].dot(w)
        ret_a = rc.mean()*TD*100; vol_a = rc.std()*np.sqrt(TD)*100
        rfd = rf/TD; sharpe = (rc.mean()-rfd)*TD/(rc.std()*np.sqrt(TD))
        ds = rc[rc<0]; vol_d = np.std(ds)*np.sqrt(TD) if len(ds)>0 else np.nan
        sortino = ((rc.mean()-rfd)*TD)/vol_d if vol_d and vol_d>0 else np.nan
        cum = (1+rc).cumprod(); pk = cum.cummax(); dd = (cum-pk)/pk; mdd = dd.min()
        calmar = (ret_a/100)/abs(mdd) if mdd != 0 else np.nan
        var95 = np.percentile(rc.dropna(), 5); cvar95 = rc[rc<=var95].mean()
        try:
            sph, _ = smart_download("^GSPC", periodo_c); spr = sph["Close"].pct_change().dropna()
            al = pd.concat([rc, spr], axis=1).dropna(); al.columns = ["c", "s"]
            beta = np.cov(al["c"], al["s"])[0,1]/np.var(al["s"])
        except Exception: beta = np.nan
        hhi = round(np.sum(w**2), 4); neff = round(1/hhi, 1) if hhi > 0 else np.nan
        cm = dfr[tv].corr(); cv = cm.values.copy(); np.fill_diagonal(cv, np.nan); corr_avg = round(np.nanmean(cv), 3)
        sigma = dfr[tv].cov().values*TD; pv = np.sqrt(w@sigma@w); mrc = sigma@w; ca = w*mrc; cp = ca/pv*100
        cr_dict = dict(zip(tv, np.round(cp, 2)))

        st.subheader("📊 Resumen"); st.metric("Valor Total", f"${vt:,.2f}")
        c1, c2, c3 = st.columns(3)
        with c1: st.markdown(f"{'✅' if ret_a>8 else '🟡'} **Retorno:** {ret_a:+.2f}%"); st.markdown(f"**Volatilidad:** {vol_a:.2f}%")
        with c2: st.markdown(f"**Beta:** {beta:.3f}" if not np.isnan(beta) else "Beta: N/A"); st.markdown(f"**Max DD:** {mdd*100:.2f}%"); st.markdown(f"**VaR 95%:** {var95*100:.2f}%")
        with c3: st.markdown(f"**Sharpe:** {sharpe:.3f}"); st.markdown(f"**Sortino:** {sortino:.3f}" if sortino and not np.isnan(sortino) else "Sortino: N/A"); st.markdown(f"**HHI:** {hhi:.4f} ({neff} pos.ef.)")

        g1, g2 = st.columns(2)
        with g1:
            fig = go.Figure(data=go.Heatmap(z=cm.values, x=tv, y=tv, colorscale="RdYlGn_r", zmin=-1, zmax=1, text=np.round(cm.values, 2), texttemplate="%{text}"))
            fig.update_layout(title="Correlaciones", template="plotly_dark", paper_bgcolor="#12121f", plot_bgcolor="#1e1e2e", height=400)
            st.plotly_chart(fig, use_container_width=True)
        with g2:
            crs = dict(sorted(cr_dict.items(), key=lambda x: x[1], reverse=True))
            fig2 = go.Figure(go.Bar(x=list(crs.keys()), y=list(crs.values()), marker_color=["#ff5555" if v>20 else "#ffb86c" if v>10 else "#50fa7b" for v in crs.values()]))
            fig2.update_layout(title="Contrib. Riesgo", template="plotly_dark", paper_bgcolor="#12121f", plot_bgcolor="#1e1e2e", height=400)
            st.plotly_chart(fig2, use_container_width=True)
        g3, g4 = st.columns(2)
        with g3:
            fig3 = go.Figure(); fig3.add_trace(go.Scatter(x=dd.index, y=dd*100, fill="tozeroy", fillcolor="rgba(255,85,85,0.2)", line=dict(color="#ff5555", width=1.5)))
            fig3.update_layout(title="Drawdown", template="plotly_dark", paper_bgcolor="#12121f", plot_bgcolor="#1e1e2e", height=400)
            st.plotly_chart(fig3, use_container_width=True)
        with g4:
            ra = (cum/cum.iloc[0]-1)*100
            fig4 = go.Figure(); fig4.add_trace(go.Scatter(x=ra.index, y=ra, name="Cartera", line=dict(color="#8be9fd", width=2)))
            try:
                sc2 = (1+al["s"]).cumprod(); sa = (sc2/sc2.iloc[0]-1)*100
                fig4.add_trace(go.Scatter(x=sa.index, y=sa, name="S&P 500", line=dict(color="#ffb86c", dash="dash")))
            except Exception: pass
            fig4.update_layout(title="Retorno vs S&P 500", template="plotly_dark", paper_bgcolor="#12121f", plot_bgcolor="#1e1e2e", height=400)
            st.plotly_chart(fig4, use_container_width=True)

        issues = []
        if hhi > 0.15: issues.append(f"⚠️ Concentración alta (HHI={hhi:.3f})")
        if corr_avg > 0.50: issues.append(f"⚠️ Correlación elevada ({corr_avg:.2f})")
        if sharpe < 0: issues.append(f"🔴 Sharpe negativo ({sharpe:.2f})")
        if abs(mdd) > 0.25: issues.append(f"🔴 Max DD severo ({mdd*100:.1f}%)")
        if issues:
            st.subheader("⚠️ Alertas")
            for i in issues: st.markdown(i)
        else:
            st.success("✅ Indicadores de riesgo dentro de rangos aceptables.")
