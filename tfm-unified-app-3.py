"""
TFM — PLATAFORMA DE INVERSIÓN INTELIGENTE v3.1
Master IA Sector Financiero — VIU 2025/26
6 módulos: Outlook · Screener · Análisis · Cartera · Macro · Research
"""
import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from collections import Counter
import requests, time, re
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")

st.set_page_config(page_title="TFM — Investment Intelligence", page_icon="📊", layout="wide")

# ── APIs opcionales ──
fh = None
try:
    import finnhub
    k = st.secrets.get("FINNHUB_KEY", "")
    if k: fh = finnhub.Client(api_key=k)
except Exception: pass

fred = None
try:
    from fredapi import Fred
    k = st.secrets.get("FRED_KEY", "")
    if k: fred = Fred(api_key=k)
except Exception: pass


# ╔═══════════════════════════════════════════════════════════════╗
# ║  DESCARGA DE DATOS                                            ║
# ╚═══════════════════════════════════════════════════════════════╝
def descargar(ticker, period="1y"):
    """Retorna (DataFrame precios, dict info, str fuente_precios)."""
    days_map = {"6mo": 180, "1y": 365, "2y": 730, "5y": 1825}
    days = days_map.get(period, 365)
    hist = pd.DataFrame()
    info = {}
    fuente = "N/A"

    # 1) Finnhub precios
    if fh:
        try:
            now = int(datetime.now().timestamp())
            start = int((datetime.now() - timedelta(days=days)).timestamp())
            r = fh.stock_candles(ticker, "D", start, now)
            if r and r.get("s") == "ok" and r.get("c") and len(r["c"]) > 10:
                hist = pd.DataFrame({"Open": r["o"], "High": r["h"], "Low": r["l"],
                                     "Close": r["c"], "Volume": r["v"]},
                                    index=pd.to_datetime(r["t"], unit="s"))
                fuente = "Finnhub"
        except Exception: pass

    # 2) yfinance fallback precios
    if hist.empty:
        for _ in range(2):
            try:
                h = yf.Ticker(ticker).history(period=period)
                if not h.empty and len(h) > 10:
                    hist = h
                    fuente = "Yahoo Finance"
                    break
            except Exception: pass
            time.sleep(1)

    # 3) Info desde Finnhub
    if fh:
        try:
            p = fh.company_profile2(symbol=ticker)
            if p:
                info["longName"] = p.get("name", ticker)
                info["sector"] = p.get("finnhubIndustry", "N/A")
                info["industry"] = p.get("finnhubIndustry", "N/A")
                info["marketCap"] = (p.get("marketCapitalization") or 0) * 1e6
                info["currency"] = p.get("currency", "USD")
                info["_src_perfil"] = "Finnhub"
        except Exception: pass
        try:
            m = fh.company_basic_financials(ticker, "all").get("metric", {})
            if m:
                info["trailingPE"] = m.get("peBasicExclExtraTTM")
                info["forwardPE"] = m.get("peTTM")
                info["priceToBook"] = m.get("pbAnnual")
                info["priceToSalesTrailing12Months"] = m.get("psTTM")
                info["returnOnEquity"] = (m["roeTTM"] / 100) if m.get("roeTTM") else None
                info["returnOnAssets"] = (m["roaTTM"] / 100) if m.get("roaTTM") else None
                info["profitMargins"] = (m["netProfitMarginTTM"] / 100) if m.get("netProfitMarginTTM") else None
                info["grossMargins"] = (m["grossMarginTTM"] / 100) if m.get("grossMarginTTM") else None
                info["dividendYield"] = (m["dividendYieldIndicatedAnnual"] / 100) if m.get("dividendYieldIndicatedAnnual") else None
                info["beta"] = m.get("beta")
                info["debtToEquity"] = m.get("totalDebt/totalEquityAnnual")
                info["currentRatio"] = m.get("currentRatioAnnual")
                info["_src_metrics"] = "Finnhub"
        except Exception: pass
        try:
            pt = fh.price_target(ticker)
            if pt:
                info["targetMeanPrice"] = pt.get("targetMean")
                info["targetHighPrice"] = pt.get("targetHigh")
                info["targetLowPrice"] = pt.get("targetLow")
                info["_src_target"] = "Finnhub"
        except Exception: pass
        try:
            recs = fh.recommendation_trends(ticker)
            if recs:
                r = recs[0]
                b = r.get("strongBuy", 0) + r.get("buy", 0)
                s = r.get("strongSell", 0) + r.get("sell", 0)
                info["recommendationKey"] = "BUY" if b > s else ("SELL" if s > b else "HOLD")
                info["_src_rec"] = "Finnhub"
        except Exception: pass

    # 4) yfinance info (complementa)
    try:
        yi = yf.Ticker(ticker).info or {}
        if "_src_perfil" not in info and yi.get("longName"): info["_src_perfil"] = "Yahoo Finance"
        if "_src_metrics" not in info and yi.get("trailingPE"): info["_src_metrics"] = "Yahoo Finance"
        if "_src_target" not in info and yi.get("targetMeanPrice"): info["_src_target"] = "Yahoo Finance"
        if "_src_rec" not in info and yi.get("recommendationKey"): info["_src_rec"] = "Yahoo Finance"
        info = {**yi, **{k: v for k, v in info.items() if v is not None and v != 0 and v != "N/A"}}
    except Exception: pass

    return hist, info, fuente


def descargar_fin(ticker):
    try:
        t = yf.Ticker(ticker)
        return t.financials, t.balance_sheet, t.cashflow
    except Exception:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


def src_link(name, ticker=None, sid=None):
    if name == "Finnhub":
        return f"[Finnhub](https://finnhub.io)"
    if name == "Yahoo Finance" and ticker:
        return f"[Yahoo Finance](https://finance.yahoo.com/quote/{ticker})"
    if name == "Yahoo Finance":
        return f"[Yahoo Finance](https://finance.yahoo.com)"
    if name == "FRED" and sid:
        return f"[FRED: {sid}](https://fred.stlouisfed.org/series/{sid})"
    if name == "FRED":
        return f"[FRED](https://fred.stlouisfed.org)"
    return name


# ╔═══════════════════════════════════════════════════════════════╗
# ║  INDICADORES TÉCNICOS                                        ║
# ╚═══════════════════════════════════════════════════════════════╝
def calc_rsi(s, p=14):
    d = s.diff(); g = d.where(d > 0, 0).rolling(p).mean(); l = (-d.where(d < 0, 0)).rolling(p).mean()
    return 100 - (100 / (1 + g / l))

def calc_macd(s):
    ef = s.ewm(span=12, adjust=False).mean(); es = s.ewm(span=26, adjust=False).mean()
    m = ef - es; si = m.ewm(span=9, adjust=False).mean()
    return m, si, m - si

def calc_adx(df, p=14):
    h, l, c = df["High"], df["Low"], df["Close"]
    tr = pd.concat([h-l, (h-c.shift(1)).abs(), (l-c.shift(1)).abs()], axis=1).max(axis=1)
    up = h - h.shift(1); dn = l.shift(1) - l
    pdm = pd.Series(np.where((up > dn) & (up > 0), up, 0), index=df.index)
    mdm = pd.Series(np.where((dn > up) & (dn > 0), dn, 0), index=df.index)
    atr = tr.rolling(p).mean()
    pdi = 100 * pdm.rolling(p).mean() / atr; mdi = 100 * mdm.rolling(p).mean() / atr
    dx = 100 * (pdi - mdi).abs() / (pdi + mdi).replace(0, np.nan)
    return dx.rolling(p).mean(), pdi, mdi

def calc_obv(df):
    d = np.sign(df["Close"].diff()); obv = (d * df["Volume"]).fillna(0).cumsum()
    osma = obv.rolling(20).mean(); ot = pd.Series(np.where(obv > osma, 1, -1), index=df.index)
    p20 = df["Close"].pct_change(20); o20 = obv.pct_change(20)
    div = pd.Series(np.where((p20 < 0) & (o20 > 0), "Alcista", np.where((p20 > 0) & (o20 < 0), "Bajista", "Neutral")), index=df.index)
    return obv, osma, ot, div

def calc_stoch(df):
    lm = df["Low"].rolling(14).min(); hm = df["High"].rolling(14).max()
    k = 100 * (df["Close"] - lm) / (hm - lm).replace(0, np.nan)
    return k, k.rolling(3).mean()

def calc_bb(s):
    m = s.rolling(20).mean(); sg = s.rolling(20).std()
    u = m + 2 * sg; l = m - 2 * sg
    return u, m, l, (s - l) / (u - l).replace(0, np.nan)

def calc_atr(df):
    tr = pd.concat([df["High"]-df["Low"], (df["High"]-df["Close"].shift(1)).abs(), (df["Low"]-df["Close"].shift(1)).abs()], axis=1).max(axis=1)
    a = tr.rolling(14).mean()
    return a, a / df["Close"] * 100


# ╔═══════════════════════════════════════════════════════════════╗
# ║  SCORING TÉCNICO 0–10                                        ║
# ╚═══════════════════════════════════════════════════════════════╝
def score_tecnico(row, obv_t, obv_d):
    sc = 0.0; det = {}
    rsi = row["RSI"]
    if rsi < 30: p, m = 2.5, f"Sobrevendido ({rsi:.0f})"
    elif rsi < 45: p, m = 2.2, f"Acumulación ({rsi:.0f})"
    elif rsi < 55: p, m = 2.0, f"Neutral ({rsi:.0f})"
    elif rsi < 65: p, m = 1.5, f"Momentum+ ({rsi:.0f})"
    elif rsi < 75: p, m = 0.5, f"Caliente ({rsi:.0f})"
    else: p, m = 0.0, f"Sobrecomprado ({rsi:.0f})"
    sc += p; det["RSI"] = {"pts": p, "max": 2.5, "val": f"{rsi:.1f}", "msg": m}

    mv, sv, hv = row["MACD"], row["Signal"], row["MACD_Hist"]
    ph = row.get("MACD_Hist_prev", 0) or 0
    if mv > sv and hv > ph: p, m = 2.0, "Alcista acelerando"
    elif mv > sv: p, m = 1.5, "Encima de señal"
    elif mv <= sv and hv > ph: p, m = 0.7, "Bajista frenando"
    else: p, m = 0.0, "Bajista acelerando"
    sc += p; det["MACD"] = {"pts": p, "max": 2.0, "val": f"{mv:.4f}", "msg": m}

    av, dp, dm = row["ADX"], row["DI_Plus"], row["DI_Minus"]
    if av > 30 and dp > dm: p, m = 2.0, f"Alcista FUERTE ({av:.0f})"
    elif av > 20 and dp > dm: p, m = 1.5, f"Alcista moderada ({av:.0f})"
    elif av > 20 and dp < dm: p, m = 0.3, f"Bajista ({av:.0f})"
    elif av < 20: p, m = 1.0, f"Lateral ({av:.0f})"
    else: p, m = 0.7, f"Débil ({av:.0f})"
    sc += p; det["ADX"] = {"pts": p, "max": 2.0, "val": f"{av:.1f}", "msg": m}

    if obv_t == 1 and obv_d == "Alcista": p, m = 1.5, "Compradora+div alcista"
    elif obv_t == 1: p, m = 1.2, "Compradora"
    elif obv_t == -1 and obv_d == "Bajista": p, m = 0.0, "Vendedora+div bajista"
    else: p, m = 0.3, "Vendedora"
    sc += p; det["OBV"] = {"pts": p, "max": 1.5, "val": "Alc" if obv_t == 1 else "Baj", "msg": m}

    kv = row["Stoch_K"]
    if kv < 20 and kv > row["Stoch_D"]: p, m = 1.0, f"Sobrevendido+cruce ({kv:.0f})"
    elif kv < 25: p, m = 0.8, f"Sobrevendida ({kv:.0f})"
    elif kv > 80: p, m = 0.0, f"Sobrecomprada ({kv:.0f})"
    elif kv > row["Stoch_D"]: p, m = 0.6, f"Mom+ ({kv:.0f})"
    else: p, m = 0.2, f"Mom- ({kv:.0f})"
    sc += p; det["Stoch"] = {"pts": p, "max": 1.0, "val": f"{kv:.1f}", "msg": m}

    bb = row["BB_PctB"]
    if bb < 0: p, m = 1.0, f"Debajo BB ({bb:.2f})"
    elif bb < 0.35: p, m = 1.0, f"Zona inf ({bb:.2f})"
    elif bb < 0.65: p, m = 0.5, f"Media ({bb:.2f})"
    elif bb < 1.0: p, m = 0.1, f"Zona sup ({bb:.2f})"
    else: p, m = 0.0, f"Encima BB ({bb:.2f})"
    sc += p; det["BB%B"] = {"pts": p, "max": 1.0, "val": f"{bb:.2f}", "msg": m}
    return round(min(sc, 10.0), 1), det

def interpretar(sc):
    if sc >= 8.0: return "🟢 COMPRAR / ACUMULAR", "#50fa7b"
    if sc >= 6.0: return "🔵 MANTENER / VIGILAR", "#8be9fd"
    if sc >= 4.0: return "🟡 NEUTRO / ESPERAR", "#f1fa8c"
    return "🔴 REDUCIR / VENDER", "#ff5555"


# ╔═══════════════════════════════════════════════════════════════╗
# ║  NIVELES OPERATIVOS                                          ║
# ╚═══════════════════════════════════════════════════════════════╝
def niveles_op(hist, info):
    last = hist.iloc[-1]; precio = last["Close"]
    tr = pd.concat([hist["High"]-hist["Low"], (hist["High"]-hist["Close"].shift(1)).abs(), (hist["Low"]-hist["Close"].shift(1)).abs()], axis=1).max(axis=1)
    atr_v = tr.rolling(14).mean().iloc[-1]
    sops = [v for v in [last.get("BB_Low", np.nan), last.get("SMA_50", np.nan)] if pd.notna(v) and v < precio]
    ent_opt = round(max(sops), 2) if sops else round(precio * 0.97, 2)
    sl_atr = precio - 1.5 * atr_v; sop20 = hist["Low"].iloc[-20:].min()
    if sop20 > sl_atr and sop20 < precio: sl, sl_n = round(sop20, 2), f"Soporte 20d"
    else: sl, sl_n = round(sl_atr, 2), f"ATR×1.5"
    riesgo = precio - sl; rp = round(riesgo / precio * 100, 2)
    tp1 = round(precio + 2 * riesgo, 2); tp2 = round(precio + 3 * riesgo, 2)
    tm = info.get("targetMeanPrice"); tp3 = round(tm, 2) if tm and tm > precio else None
    return {"p": round(precio, 2), "ea": round(precio, 2), "eo": ent_opt, "sl": sl,
            "sl_n": sl_n, "r": round(riesgo, 2), "rp": rp, "atr": round(atr_v, 2),
            "tp1": tp1, "tp2": tp2, "tp3": tp3, "s20": round(sop20, 2)}


# ╔═══════════════════════════════════════════════════════════════╗
# ║  FUNDAMENTALES                                                ║
# ╚═══════════════════════════════════════════════════════════════╝
def _sf(df, key, col=0, d=0):
    try:
        v = df.loc[key].iloc[col] if key in df.index else d
        return v if not pd.isna(v) else d
    except Exception: return d

def calc_piotroski(fin, bs, cf):
    sc = 0; det = {}
    try:
        ta0=_sf(bs,"Total Assets",0,1); ta1=_sf(bs,"Total Assets",1,1)
        ni0=_sf(fin,"Net Income",0); ni1=_sf(fin,"Net Income",1); cfo=_sf(cf,"Operating Cash Flow",0)
        ca0=_sf(bs,"Current Assets",0); cl0=_sf(bs,"Current Liabilities",0) or 1
        ca1=_sf(bs,"Current Assets",1); cl1=_sf(bs,"Current Liabilities",1) or 1
        ltd0=_sf(bs,"Long Term Debt",0); ltd1=_sf(bs,"Long Term Debt",1)
        rev0=_sf(fin,"Total Revenue",0) or 1; rev1=_sf(fin,"Total Revenue",1) or 1
        gp0=_sf(fin,"Gross Profit",0); gp1=_sf(fin,"Gross Profit",1)
        tests = [("F1 ROA+", ni0/ta0>0, f"{ni0/ta0*100:.2f}%"), ("F2 CFO+", cfo>0, f"${cfo/1e6:.0f}M"),
                 ("F3 ROA↑", ni0/ta0>ni1/ta1, f"{ni0/ta0*100:.2f}% vs {ni1/ta1*100:.2f}%"),
                 ("F4 CFO>NI", cfo>ni0, f"CFO {cfo/1e6:.0f}M vs NI {ni0/1e6:.0f}M"),
                 ("F5 Deuda↓", ltd0/ta0<ltd1/ta1, f"{ltd0/ta0:.3f} vs {ltd1/ta1:.3f}"),
                 ("F6 Liquidez↑", ca0/cl0>ca1/cl1, f"{ca0/cl0:.2f} vs {ca1/cl1:.2f}"),
                 ("F7 Sin dilución", True, "manual"), ("F8 Margen↑", gp0/rev0>gp1/rev1, f"{gp0/rev0*100:.1f}% vs {gp1/rev1*100:.1f}%"),
                 ("F9 Rot.Act↑", rev0/ta0>rev1/ta1, f"{rev0/ta0:.3f} vs {rev1/ta1:.3f}")]
        for n, c, v in tests: p = 1 if c else 0; sc += p; det[n] = {"ok": bool(c), "val": v}
    except Exception as e: det["err"] = {"ok": False, "val": str(e)}
    return sc, det

def calc_altman(info, fin, bs):
    try:
        ta=_sf(bs,"Total Assets",0,1); ca=_sf(bs,"Current Assets",0); cl=_sf(bs,"Current Liabilities",0)
        re=_sf(bs,"Retained Earnings",0); ebit=_sf(fin,"EBIT",0) or _sf(fin,"Operating Income",0)
        tl=_sf(bs,"Total Liabilities Net Minority Interest",0) or _sf(bs,"Total Debt",0) or 1
        rev=_sf(fin,"Total Revenue",0); mc=info.get("marketCap",0)
        z=1.2*((ca-cl)/ta)+1.4*(re/ta)+3.3*(ebit/ta)+0.6*(mc/tl)+1.0*(rev/ta)
        return round(z,2), "🟢 SEGURA" if z>2.99 else ("🟡 GRIS" if z>1.81 else "🔴 PELIGRO")
    except Exception: return None, "N/A"

def calc_graham(info):
    try:
        eps = info.get("trailingEps") or info.get("forwardEps"); bv = info.get("bookValue")
        if eps and bv and eps > 0 and bv > 0: return round((22.5 * eps * bv) ** 0.5, 2)
    except Exception: pass
    return None

def calc_fcf_yield(info, cf):
    try:
        fcf = info.get("freeCashflow") or (_sf(cf,"Operating Cash Flow",0) - abs(_sf(cf,"Capital Expenditure",0)))
        mc = info.get("marketCap")
        if fcf and mc and mc > 0: return round(fcf / mc * 100, 2)
    except Exception: pass
    return None

def calc_dupont(fin, bs):
    try:
        ni=_sf(fin,"Net Income",0); rev=_sf(fin,"Total Revenue",0) or 1
        ta=_sf(bs,"Total Assets",0) or 1; eq=_sf(bs,"Stockholders Equity",0) or 1
        nm=ni/rev; at=rev/ta; lv=ta/eq
        return {"ROE": round(nm*at*lv*100,2), "Margen": round(nm*100,2), "Rot": round(at,3), "Apal": round(lv,2)}
    except Exception: return None

def calc_cagr(fin):
    try:
        if fin.empty or len(fin.columns) < 2: return None, None
        def cg(s):
            v=s.dropna()
            if len(v)<2: return None
            vi,vf,n=v.iloc[-1],v.iloc[0],len(v)-1
            if vi<=0 or vf<=0: return None
            return round(((vf/vi)**(1/n)-1)*100,1)
        rv=fin.loc["Total Revenue"] if "Total Revenue" in fin.index else None
        ni=fin.loc["Net Income"] if "Net Income" in fin.index else None
        return (cg(rv) if rv is not None else None, cg(ni) if ni is not None else None)
    except Exception: return None, None

def mf(nombre, val, fmt, bueno, malo):
    if val is None: return f"**{nombre}:** N/A"
    ic = "✅" if bueno(val) else ("🔴" if malo(val) else "🟡")
    if fmt.endswith("%"): return f"{ic} **{nombre}:** {val:{fmt[:-1]}}%"
    return f"{ic} **{nombre}:** {val:{fmt}}"


# ╔═══════════════════════════════════════════════════════════════╗
# ║  SCREENER                                                     ║
# ╚═══════════════════════════════════════════════════════════════╝
@st.cache_data(ttl=86400)
def get_sp500():
    try:
        t = pd.read_html(requests.get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", headers={"User-Agent": "Mozilla/5.0"}, timeout=10).text)
        return [x.replace(".", "-") for x in t[0]["Symbol"].tolist()]
    except Exception:
        return ["AAPL","MSFT","GOOGL","AMZN","NVDA","META","TSLA","BRK-B","JPM","V","JNJ","UNH","PG","MA","HD","DIS","NFLX","PFE","KO","PEP","MRK","ABBV","AVGO","COST","WMT","CSCO","TMO","ABT","CRM","ACN","NKE","MCD","LLY","DHR","TXN","QCOM","INTC","AMGN","CVX","BA"]

@st.cache_data(ttl=86400)
def get_ibex():
    return ["SAN.MC","BBVA.MC","ITX.MC","IBE.MC","TEF.MC","FER.MC","AMS.MC","REP.MC","CABK.MC","ACS.MC","GRF.MC","MAP.MC","ENG.MC","RED.MC","IAG.MC","FDR.MC","MEL.MC","COL.MC","CLNX.MC","SAB.MC"]

@st.cache_data(ttl=86400)
def get_dax():
    return ["SAP.DE","SIE.DE","ALV.DE","DTE.DE","AIR.DE","MBG.DE","DHL.DE","BAS.DE","BMW.DE","IFX.DE","BEI.DE","BAYN.DE","ADS.DE","VOW3.DE","DB1.DE","RWE.DE","CON.DE","DBK.DE","MRK.DE","SHL.DE"]

INDICES_MAP = {"SP500": ("S&P 500", get_sp500), "IBEX35": ("IBEX 35", get_ibex), "DAX40": ("DAX 40", get_dax)}
CL = {"COMPRAR": "#50fa7b", "VIGILAR": "#8be9fd", "NEUTRO": "#f1fa8c", "EVITAR": "#ff5555"}

def score_scr(r):
    s = 0
    mom = r.get("Mom 3M %", np.nan)
    if pd.notna(mom) and mom > 20: s += 1
    elif pd.notna(mom) and mom > 10: s += 0.7
    elif pd.notna(mom) and mom > 0: s += 0.4
    if pd.notna(r.get("vs SMA50 %")) and r["vs SMA50 %"] > 0: s += 0.5
    vr = r.get("Vol/Avg", np.nan)
    if pd.notna(vr) and vr > 2: s += 1
    elif pd.notna(vr) and vr > 1.3: s += 0.5
    per = r.get("PER", np.nan)
    if pd.notna(per) and 0 < per < 12: s += 1.5
    elif pd.notna(per) and 0 < per < 20: s += 1
    roe = r.get("ROE %", np.nan)
    if pd.notna(roe) and roe > 25: s += 1
    elif pd.notna(roe) and roe > 15: s += 0.7
    mg = r.get("Margen %", np.nan)
    if pd.notna(mg) and mg > 20: s += 1
    elif pd.notna(mg) and mg > 10: s += 0.7
    de = r.get("D/E", np.nan)
    if pd.notna(de) and de < 50: s += 0.5
    pot = r.get("Potencial %", np.nan)
    if pd.notna(pot) and pot > 20: s += 0.5
    return round(min(max(s, 0), 10), 1)

def label_sc(sc):
    if sc >= 7.5: return "COMPRAR"
    if sc >= 6: return "VIGILAR"
    if sc >= 4: return "NEUTRO"
    return "EVITAR"

def analizar_scr(ticker):
    try:
        hist, info, _ = descargar(ticker, "1y")
        if hist.empty or len(hist) < 20: return None
        precio = hist["Close"].iloc[-1]
        if precio <= 0: return None
        mc = round(info.get("marketCap", 0) / 1e9, 2)
        mom3 = ((precio / hist["Close"].iloc[-63] - 1) * 100) if len(hist) > 63 else np.nan
        sma50 = hist["Close"].rolling(50).mean().iloc[-1] if len(hist) >= 50 else np.nan
        vs50 = round((precio / sma50 - 1) * 100, 2) if pd.notna(sma50) else np.nan
        vh = hist.iloc[-1]["Volume"]; va20 = hist["Volume"].rolling(20).mean().iloc[-1]
        vr = round(vh / va20, 2) if va20 > 0 else np.nan
        per = info.get("trailingPE"); roe = info.get("returnOnEquity"); mn = info.get("profitMargins")
        de = info.get("debtToEquity"); dy = info.get("dividendYield")
        tm = info.get("targetMeanPrice"); pot = round((tm / precio - 1) * 100, 1) if tm and precio > 0 else np.nan
        r = {"Ticker": ticker, "Precio": round(precio, 2), "MktCap B$": mc,
             "Mom 3M %": round(mom3, 2) if pd.notna(mom3) else np.nan, "vs SMA50 %": vs50,
             "Vol/Avg": vr, "PER": round(per, 1) if per else np.nan,
             "ROE %": round(roe * 100, 1) if roe else np.nan,
             "Margen %": round(mn * 100, 1) if mn else np.nan,
             "D/E": round(de, 1) if de else np.nan,
             "Div %": round(dy * 100, 2) if dy else 0,
             "Potencial %": pot, "Consenso": info.get("recommendationKey", "N/A")}
        r["Score"] = score_scr(r); r["Label"] = label_sc(r["Score"])
        return r
    except Exception: return None

def filtrar(df, modo):
    d = df.copy()
    if modo == "VALUE": return d[d["PER"].between(0, 20) & (d["Margen %"] > 8)].copy()
    if modo == "MOMENTUM": return d[(d["Mom 3M %"] > 10) & (d["vs SMA50 %"] > 0)].copy()
    if modo == "QUALITY": return d[(d["ROE %"] > 15) & (d["Margen %"] > 12)].copy()
    if modo == "DIVIDENDOS": return d[(d["Div %"] > 2.5) & (d["Margen %"] > 5)].copy()
    return d.copy()


# ╔═══════════════════════════════════════════════════════════════╗
# ║  SIDEBAR                                                      ║
# ╚═══════════════════════════════════════════════════════════════╝
with st.sidebar:
    st.title("📊 TFM Investment App")
    st.caption("Master IA Sector Financiero — VIU")
    st.divider()
    pagina = st.radio("Módulo", ["🌅 Outlook", "🔍 Screener", "📈 Análisis Individual", "💼 Cartera", "📊 Macro", "🤖 Research"])
    st.divider()
    st.caption(f"{'✅' if fh else '❌'} Finnhub | {'✅' if fred else '❌'} FRED | ✅ yfinance")


# ╔═══════════════════════════════════════════════════════════════╗
# ║  OUTLOOK                                                      ║
# ╚═══════════════════════════════════════════════════════════════╝
if pagina == "🌅 Outlook":
    st.header("🌅 Morning Outlook — Resumen de Mercado")
    st.caption(f"📅 {datetime.now().strftime('%d/%m/%Y %H:%M')} UTC")

    indices_syms = {"^GSPC": "S&P 500", "^IXIC": "Nasdaq", "^DJI": "Dow Jones",
                    "^STOXX50E": "Euro Stoxx 50", "^IBEX": "IBEX 35", "^GDAXI": "DAX 40", "^N225": "Nikkei"}
    idx_data = []
    with st.spinner("Cargando índices..."):
        for sym, name in indices_syms.items():
            h, _, src = descargar(sym, "6mo")
            if not h.empty and len(h) > 2:
                last = h["Close"].iloc[-1]; prev = h["Close"].iloc[-2]
                chg = (last / prev - 1) * 100
                ytd = (last / h["Close"].iloc[0] - 1) * 100
                idx_data.append({"Índice": name, "Último": round(last, 2), "Día %": round(chg, 2), "YTD %": round(ytd, 2)})
            time.sleep(0.3)
    if idx_data:
        st.subheader("🌍 Índices Principales")
        cols = st.columns(4)
        for i, row in enumerate(idx_data):
            with cols[i % 4]: st.metric(row["Índice"], f"{row['Último']:,.2f}", f"{row['Día %']:+.2f}%")
        st.caption(f"📡 Fuente: {src_link('Finnhub')} / {src_link('Yahoo Finance')}")

    sect_syms = {"XLK": "Tech", "XLF": "Financiero", "XLE": "Energía", "XLV": "Salud",
                 "XLY": "Cons.Disc", "XLP": "Cons.Básico", "XLI": "Industrial", "XLU": "Utilities"}
    sec_data = []
    with st.spinner("Cargando sectores..."):
        for sym, name in sect_syms.items():
            h, _, _ = descargar(sym, "6mo")
            if not h.empty and len(h) > 2:
                sec_data.append({"Sector": name, "Cambio %": round((h["Close"].iloc[-1] / h["Close"].iloc[-2] - 1) * 100, 2)})
            time.sleep(0.2)
    if sec_data:
        st.subheader("📊 Rendimiento Sectorial")
        df_sec = pd.DataFrame(sec_data).sort_values("Cambio %", ascending=False)
        fig = go.Figure(go.Bar(x=df_sec["Sector"], y=df_sec["Cambio %"],
              marker_color=["#50fa7b" if v >= 0 else "#ff5555" for v in df_sec["Cambio %"]],
              text=[f"{v:+.2f}%" for v in df_sec["Cambio %"]], textposition="outside"))
        fig.update_layout(template="plotly_dark", paper_bgcolor="#12121f", plot_bgcolor="#1e1e2e", height=400)
        st.plotly_chart(fig, use_container_width=True)
        st.caption(f"📡 ETFs sectoriales SPDR — {src_link('Finnhub')} / {src_link('Yahoo Finance')}")

    if fred:
        st.subheader("📈 Indicadores Macro")
        st.caption(f"📡 {src_link('FRED')}")
        macro = {"Fed Funds": "FEDFUNDS", "US 10Y": "DGS10", "US 2Y": "DGS2", "VIX": "VIXCLS", "Desempleo": "UNRATE"}
        mc = st.columns(3); i = 0
        for name, sid in macro.items():
            try:
                s = fred.get_series(sid, observation_start="2024-01-01").dropna()
                if not s.empty:
                    with mc[i % 3]: st.metric(f"{name} [{sid}](https://fred.stlouisfed.org/series/{sid})", f"{s.iloc[-1]:.2f}", f"{s.iloc[-1]-s.iloc[-2]:+.2f}")
                    i += 1
            except Exception: pass
        try:
            y10 = fred.get_series("DGS10", observation_start="2022-01-01").dropna()
            y2 = fred.get_series("DGS2", observation_start="2022-01-01").dropna()
            spread = (y10 - y2).dropna()
            if not spread.empty:
                st.subheader("📉 Curva de Tipos (10Y-2Y)")
                st.caption(f"📡 {src_link('FRED', sid='DGS10')} − {src_link('FRED', sid='DGS2')}")
                fig = go.Figure(go.Scatter(x=spread.index, y=spread, line=dict(color="#8be9fd", width=2), fill="tozeroy", fillcolor="rgba(139,233,253,0.1)"))
                fig.add_hline(y=0, line_dash="dash", line_color="#ff5555", opacity=0.7)
                fig.update_layout(template="plotly_dark", paper_bgcolor="#12121f", plot_bgcolor="#1e1e2e", height=300)
                st.plotly_chart(fig, use_container_width=True)
                if spread.iloc[-1] < 0: st.warning(f"⚠️ Curva invertida ({spread.iloc[-1]:.2f}%)")
                else: st.success(f"✅ Curva normal ({spread.iloc[-1]:.2f}%)")
        except Exception: pass

    if fh:
        st.subheader("📰 Noticias")
        st.caption(f"📡 {src_link('Finnhub')}")
        try:
            news = fh.general_news("general", min_id=0)
            for a in (news or [])[:6]:
                st.markdown(f"**{a.get('headline', '')}**")
                st.caption(f"{a.get('source', '')} — {datetime.fromtimestamp(a.get('datetime', 0)).strftime('%d/%m %H:%M')} — [Leer →]({a.get('url', '')})")
                st.divider()
        except Exception: pass


# ╔═══════════════════════════════════════════════════════════════╗
# ║  SCREENER                                                     ║
# ╚═══════════════════════════════════════════════════════════════╝
elif pagina == "🔍 Screener":
    with st.sidebar:
        indice = st.selectbox("Índice", list(INDICES_MAP.keys()), format_func=lambda x: INDICES_MAP[x][0])
        modo = st.selectbox("Modo", ["VALUE", "MOMENTUM", "QUALITY", "DIVIDENDOS", "TODO"])
        limite = st.slider("Tickers", 10, 100, 30, step=10)
        ejecutar = st.button("🚀 Ejecutar", type="primary", use_container_width=True)
    st.header(f"🔍 Screener: {INDICES_MAP[indice][0]} — {modo}")
    if ejecutar:
        tickers = INDICES_MAP[indice][1]()
        if not tickers: st.error("Sin tickers."); st.stop()
        ta = tickers[:limite]; res = []; pb = st.progress(0)
        for i, t in enumerate(ta):
            pb.progress((i + 1) / len(ta), text=f"{t} ({i+1}/{len(ta)})")
            r = analizar_scr(t)
            if r: res.append(r)
            if i % 3 == 2: time.sleep(1)
        pb.empty()
        if not res: st.error("Sin resultados.")
        else:
            df = filtrar(pd.DataFrame(res), modo).sort_values("Score", ascending=False).reset_index(drop=True)
            if df.empty: st.warning("Ningún activo cumple filtros.")
            else:
                st.subheader(f"{len(df)} activos")
                cols = [c for c in ["Ticker","Precio","Score","Label","Mom 3M %","Vol/Avg","PER","ROE %","Margen %","Potencial %","MktCap B$"] if c in df.columns]
                st.dataframe(df[cols], use_container_width=True, height=500)
                st.caption(f"📡 Precios: {src_link('Finnhub')} | Fundamentales: {src_link('Yahoo Finance')} | Tickers: [Wikipedia](https://en.wikipedia.org/wiki/List_of_S%26P_500_companies)")
                dv = df.dropna(subset=["Mom 3M %"]).head(25)
                if not dv.empty:
                    fig = px.scatter(dv, x="Mom 3M %", y="Score", color="Label", color_discrete_map=CL, hover_data=["Ticker","PER"], text="Ticker")
                    fig.update_traces(textposition="top center", textfont_size=9)
                    fig.update_layout(template="plotly_dark", height=450, paper_bgcolor="#12121f", plot_bgcolor="#1e1e2e")
                    st.plotly_chart(fig, use_container_width=True)
                st.session_state["scr"] = df
                st.download_button("📥 CSV", df.to_csv(index=False).encode("utf-8"), f"screener.csv", "text/csv")
    else: st.info("👈 Configura y pulsa **Ejecutar**.")


# ╔═══════════════════════════════════════════════════════════════╗
# ║  ANÁLISIS INDIVIDUAL                                          ║
# ╚═══════════════════════════════════════════════════════════════╝
elif pagina == "📈 Análisis Individual":
    with st.sidebar:
        tk = st.text_input("Ticker", value="AAPL")
        tab = st.radio("Vista", ["🔧 Técnico", "📋 Fundamental", "🔧+📋 Completo"])
        go_btn = st.button("🚀 Analizar", type="primary", use_container_width=True)
    if go_btn and tk:
        tk = tk.upper().strip()
        with st.spinner(f"Analizando {tk}..."):
            hist, info, f_precio = descargar(tk, "2y")
            fin, bs, cf = descargar_fin(tk)
        if hist.empty: st.error(f"Sin datos para {tk}."); st.stop()
        if len(hist) < 50: st.warning(f"Solo {len(hist)} sesiones.")
        nombre = info.get("longName", tk); precio = hist["Close"].iloc[-1]; moneda = info.get("currency", "")
        st.header(f"🏢 {nombre} ({tk})")
        h1, h2, h3 = st.columns(3)
        h1.metric("Precio", f"{precio:.2f} {moneda}"); h2.metric("Sector", info.get("sector", "N/A")); h3.metric("MktCap", f"${info.get('marketCap',0)/1e9:.2f}B")
        f_met = info.get("_src_metrics", "Yahoo Finance"); f_tgt = info.get("_src_target", "Yahoo Finance"); f_prf = info.get("_src_perfil", "Yahoo Finance")
        st.caption(f"📡 Precios: {src_link(f_precio, tk)} | Perfil: {src_link(f_prf, tk)} | Métricas: {src_link(f_met, tk)}")

        if tab in ["🔧 Técnico", "🔧+📋 Completo"]:
            st.divider(); st.subheader("🔧 Análisis Técnico")
            hist["RSI"] = calc_rsi(hist["Close"]); hist["SMA_50"] = hist["Close"].rolling(50).mean(); hist["SMA_200"] = hist["Close"].rolling(200).mean()
            bu, bm, bl, bp = calc_bb(hist["Close"]); hist["BB_Up"]=bu; hist["BB_Low"]=bl; hist["BB_PctB"]=bp
            mc, sg, mh = calc_macd(hist["Close"]); hist["MACD"]=mc; hist["Signal"]=sg; hist["MACD_Hist"]=mh; hist["MACD_Hist_prev"]=mh.shift(1)
            ax, dp, dm = calc_adx(hist); hist["ADX"]=ax.values; hist["DI_Plus"]=dp.values; hist["DI_Minus"]=dm.values
            ov, os, ot, od = calc_obv(hist); hist["OBV"]=ov; hist["OBV_SMA"]=os
            sk, sd = calc_stoch(hist); hist["Stoch_K"]=sk; hist["Stoch_D"]=sd
            at_v, at_p = calc_atr(hist); hist["ATR_PCT"]=at_p
            last = hist.iloc[-1]; sc, det = score_tecnico(last, ot.iloc[-1], od.iloc[-1]); verd, colv = interpretar(sc)
            s1, s2, s3 = st.columns([1, 2, 1])
            s1.metric("SCORE", f"{sc}/10"); s2.markdown(f"### {verd}")
            s3.caption(f"SMA200: {'✅' if precio > last['SMA_200'] else '🔴'} | ATR%: {last['ATR_PCT']:.2f}%")
            for ind, d in det.items():
                pct = d["pts"]/d["max"] if d["max"]>0 else 0; ic = "✅" if pct>=0.7 else ("🟡" if pct>=0.3 else "🔴")
                st.markdown(f"{ic} **{ind}** — {d['val']} — `{d['pts']:.1f}/{d['max']:.1f}` — {d['msg']}")
            st.markdown("---"); st.markdown("### 🎯 Niveles Operativos")
            nv = niveles_op(hist, info)
            n1, n2, n3 = st.columns(3)
            with n1: st.markdown("**ENTRADAS**"); st.markdown(f"🟢 Agresiva: **{nv['ea']:.2f}**"); st.markdown(f"🔵 Óptima: **{nv['eo']:.2f}**")
            with n2: st.markdown("**STOP LOSS**"); st.markdown(f"🔴 SL: **{nv['sl']:.2f}** (−{nv['rp']:.1f}%)"); st.caption(f"ATR: {nv['atr']} | {nv['sl_n']}")
            with n3:
                st.markdown("**TAKE PROFIT**"); st.markdown(f"🎯 TP1 (2:1): **{nv['tp1']:.2f}** (+{((nv['tp1']/precio-1)*100):.1f}%)")
                st.markdown(f"🎯 TP2 (3:1): **{nv['tp2']:.2f}** (+{((nv['tp2']/precio-1)*100):.1f}%)")
                if nv["tp3"]: st.markdown(f"🎯 TP3: **{nv['tp3']:.2f}** (+{((nv['tp3']/precio-1)*100):.1f}%)")
            n_pts = min(len(hist), 252); hg = hist.iloc[-n_pts:]
            fig = make_subplots(rows=5, cols=1, shared_xaxes=True, vertical_spacing=0.02, row_heights=[.35,.15,.15,.15,.15])
            fig.add_trace(go.Scatter(x=hg.index, y=hg["Close"], name="Precio", line=dict(color="#f8f8f2", width=1.8)), row=1, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["SMA_50"], name="SMA50", line=dict(color="#ffb86c", width=1, dash="dash")), row=1, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["SMA_200"], name="SMA200", line=dict(color="#ff5555", width=1, dash="dash")), row=1, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["BB_Up"], showlegend=False, line=dict(color="#8be9fd", width=.5)), row=1, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["BB_Low"], name="BB", line=dict(color="#8be9fd", width=.5), fill="tonexty", fillcolor="rgba(139,233,253,0.08)"), row=1, col=1)
            fig.add_hline(y=nv["sl"], line_dash="solid", line_color="#ff5555", opacity=.7, row=1, col=1, annotation_text=f"SL {nv['sl']}", annotation_font_color="#ff5555", annotation_font_size=9)
            fig.add_hline(y=nv["tp1"], line_dash="dot", line_color="#f1fa8c", opacity=.6, row=1, col=1, annotation_text=f"TP1 {nv['tp1']}", annotation_font_color="#f1fa8c", annotation_font_size=9)
            ch = ["#50fa7b" if v>=0 else "#ff5555" for v in hg["MACD_Hist"]]
            fig.add_trace(go.Bar(x=hg.index, y=hg["MACD_Hist"], marker_color=ch, showlegend=False), row=2, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["MACD"], name="MACD", line=dict(color="#50fa7b", width=1)), row=2, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["Signal"], name="Signal", line=dict(color="#ff79c6", width=1)), row=2, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["RSI"], name="RSI", line=dict(color="#bd93f9", width=1)), row=3, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["Stoch_K"], name="%K", line=dict(color="#f1fa8c", width=1, dash="dot")), row=3, col=1)
            fig.add_hline(y=70, line_dash="dash", line_color="#ff5555", opacity=.4, row=3, col=1)
            fig.add_hline(y=30, line_dash="dash", line_color="#50fa7b", opacity=.4, row=3, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["ADX"], name="ADX", line=dict(color="#ffb86c", width=1.4)), row=4, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["DI_Plus"], name="DI+", line=dict(color="#50fa7b", width=.8)), row=4, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["DI_Minus"], name="DI−", line=dict(color="#ff5555", width=.8)), row=4, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["OBV"], name="OBV", line=dict(color="#8be9fd", width=1)), row=5, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["OBV_SMA"], name="OBV SMA", line=dict(color="#ff79c6", width=1, dash="dash")), row=5, col=1)
            fig.update_layout(title=f"{tk} | Score: {sc}/10 | {verd}", template="plotly_dark", paper_bgcolor="#12121f", plot_bgcolor="#1e1e2e", height=1000, legend=dict(orientation="h", y=-.02, font=dict(size=9)), hovermode="x unified")
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"📡 OHLCV: {src_link(f_precio, tk)} | Indicadores: cálculo propio")

        if tab in ["📋 Fundamental", "🔧+📋 Completo"]:
            st.divider(); st.subheader("📋 Análisis Fundamental")
            per=info.get("trailingPE"); pfw=info.get("forwardPE"); peg=info.get("pegRatio"); pb=info.get("priceToBook")
            ps=info.get("priceToSalesTrailing12Months"); eve=info.get("enterpriseToEbitda")
            gn=calc_graham(info); fy=calc_fcf_yield(info, cf)
            st.markdown("### 💰 Valoración")
            st.caption(f"📡 Múltiplos: {src_link(f_met, tk)} | Target: {src_link(f_tgt, tk)}")
            v1,v2,v3=st.columns(3)
            with v1: st.markdown(mf("PER",per,".1f",lambda x:x<15,lambda x:x>30)); st.markdown(mf("PER Fwd",pfw,".1f",lambda x:x<12,lambda x:x>25))
            with v2: st.markdown(mf("PEG",peg,".2f",lambda x:x<1,lambda x:x>2)); st.markdown(mf("P/Book",pb,".2f",lambda x:x<1.5,lambda x:x>5))
            with v3: st.markdown(mf("P/Ventas",ps,".2f",lambda x:x<2,lambda x:x>10)); st.markdown(mf("EV/EBITDA",eve,".1f",lambda x:x<10,lambda x:x>20))
            if fy: st.markdown(f"{'✅' if fy>5 else '🟡'} **FCF Yield:** {fy:.2f}%")
            if gn and precio: st.markdown(f"**Graham:** {gn:.2f} → {(precio/gn-1)*100:+.1f}% — {'✅ INFRAVALORADO' if precio<gn else '⚠️ SOBREVALORADO'}")
            tm=info.get("targetMeanPrice")
            if tm and precio: st.markdown(f"{'✅' if (tm/precio-1)*100>10 else '🟡'} **Target:** {tm:.2f} ({(tm/precio-1)*100:+.1f}%)")

            st.markdown("### 📈 Rentabilidad")
            roe=info.get("returnOnEquity"); roa=info.get("returnOnAssets"); pm=info.get("profitMargins"); gm=info.get("grossMargins")
            dupont=calc_dupont(fin,bs); cagr_r,cagr_n=calc_cagr(fin)
            r1,r2,r3=st.columns(3)
            with r1: st.markdown(mf("ROE",roe*100 if roe else None,".1f%",lambda x:x>15,lambda x:x<5)); st.markdown(mf("ROA",roa*100 if roa else None,".1f%",lambda x:x>8,lambda x:x<2))
            with r2: st.markdown(mf("M.Bruto",gm*100 if gm else None,".1f%",lambda x:x>40,lambda x:x<20)); st.markdown(mf("M.Neto",pm*100 if pm else None,".1f%",lambda x:x>15,lambda x:x<3))
            with r3:
                if cagr_r is not None: st.markdown(f"{'✅' if cagr_r>7 else '🟡'} **CAGR Rev:** {cagr_r:+.1f}%")
                if cagr_n is not None: st.markdown(f"{'✅' if cagr_n>7 else '🟡'} **CAGR BN:** {cagr_n:+.1f}%")
            if dupont:
                d1,d2,d3,d4=st.columns(4)
                d1.metric("ROE DuPont",f"{dupont['ROE']:.2f}%"); d2.metric("Margen",f"{dupont['Margen']:.2f}%"); d3.metric("Rot.Act",f"{dupont['Rot']:.3f}x"); d4.metric("Apal",f"{dupont['Apal']:.2f}x")

            st.markdown("### 🏥 Salud Financiera")
            st.caption(f"📡 Estados financieros: {src_link('Yahoo Finance', tk)} (fuente: [SEC EDGAR](https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&company={tk}&type=10-K))")
            cr=info.get("currentRatio"); de=info.get("debtToEquity")
            s1,s2,s3=st.columns(3)
            with s1: st.markdown(mf("R.Corriente",cr,".2f",lambda x:x>1.5,lambda x:x<1))
            with s2: st.markdown(mf("D/E",de,".1f",lambda x:x<80,lambda x:x>200))
            with s3:
                if not bs.empty and not fin.empty:
                    z,zz=calc_altman(info,fin,bs)
                    if z: st.markdown(f"**Altman Z:** {z} → {zz}")

            fs = 0
            if not fin.empty and not bs.empty and not cf.empty:
                st.markdown("### 🔢 Piotroski F-Score")
                fs, fd = calc_piotroski(fin, bs, cf)
                st.markdown(f"### {'🟢' if fs>=7 else ('🟡' if fs>=4 else '🔴')} F-Score: {fs}/9")
                for c, v in fd.items():
                    if not c.startswith("e"): st.markdown(f"{'✅' if v['ok'] else '❌'} {c} — `{v['val']}`")

            dy=info.get("dividendYield")
            if dy and dy>0:
                st.markdown("### 💵 Dividendos"); st.markdown(f"{'✅' if dy>.03 else '🟡'} **Yield:** {dy*100:.2f}%")
                pay=info.get("payoutRatio")
                if pay: st.markdown(f"**Payout:** {pay*100:.1f}% — {'✅' if pay<.6 else '⚠️'}")

            st.markdown("### 🏆 Veredicto Fundamental")
            pts=0; mx=0
            if per: mx+=2; pts+=(2 if per<15 else 1 if per<25 else 0)
            if roe: mx+=2; pts+=(2 if roe>.20 else 1 if roe>.10 else 0)
            if pm: mx+=2; pts+=(2 if pm>.15 else 1 if pm>.05 else 0)
            if de is not None: mx+=2; pts+=(2 if de<80 else 1 if de<150 else 0)
            if fs: mx+=2; pts+=(2 if fs>=7 else 1 if fs>=4 else 0)
            if mx > 0:
                pf=pts/mx; vf="🟢 SÓLIDA" if pf>=.75 else ("🟡 ACEPTABLE" if pf>=.45 else "🔴 DÉBIL")
                st.markdown(f"**{vf}** — {pts}/{mx} ({pf*100:.0f}%)"); st.progress(pf)
    else: st.info("👈 Introduce un ticker y pulsa **Analizar**.")


# ╔═══════════════════════════════════════════════════════════════╗
# ║  CARTERA                                                      ║
# ╚═══════════════════════════════════════════════════════════════╝
elif pagina == "💼 Cartera":
    TD=252
    with st.sidebar:
        periodo_c=st.selectbox("Período",["6mo","1y","2y"],index=1); rf=st.number_input("Rf %",value=4.5,step=.25)/100
    st.header("💼 Análisis de Cartera")
    if "cart" not in st.session_state:
        st.session_state["cart"]=pd.DataFrame({"Ticker":["AAPL","MSFT","GOOGL","GLD","TLT"],"Cantidad":[10,5,3,20,15],"Precio Compra":[150.,240.,130.,180.,95.],"Divisa":["USD"]*5})
    ed=st.data_editor(st.session_state["cart"],num_rows="dynamic",use_container_width=True,
        column_config={"Ticker":st.column_config.TextColumn("Ticker",required=True),"Cantidad":st.column_config.NumberColumn("Cantidad",min_value=0,required=True),
                       "Precio Compra":st.column_config.NumberColumn("Precio Compra",min_value=0.,format="%.2f"),"Divisa":st.column_config.SelectboxColumn("Divisa",options=["USD","EUR","CHF","GBP","JPY"])},key="ce")
    st.session_state["cart"]=ed
    if st.button("🚀 Analizar Cartera",type="primary",use_container_width=True):
        dc=ed.dropna(subset=["Ticker"]).copy(); dc=dc[dc["Cantidad"]>0]
        if len(dc)<2: st.error("Mínimo 2."); st.stop()
        precios={}; valores={}; errs=[]
        with st.spinner("Descargando..."):
            for _,row in dc.iterrows():
                t=row["Ticker"].upper().strip(); ht,_,_=descargar(t,periodo_c)
                if not ht.empty and len(ht)>20: precios[t]=ht["Close"]; valores[t]=row["Cantidad"]*ht["Close"].iloc[-1]
                else: errs.append(t)
                time.sleep(.3)
        if errs: st.warning(f"Sin datos: {', '.join(errs)}")
        if len(precios)<2: st.error("Insuficiente."); st.stop()
        dfp=pd.DataFrame(precios).dropna(); dfr=dfp.pct_change().dropna()
        vt=sum(valores.values()); pesos={t:v/vt for t,v in valores.items()}
        tv=[t for t in pesos if t in dfr.columns]; w=np.array([pesos[t] for t in tv]); w=w/w.sum()
        rc=dfr[tv].dot(w); ret_a=rc.mean()*TD*100; vol_a=rc.std()*np.sqrt(TD)*100
        rfd=rf/TD; sharpe=(rc.mean()-rfd)*TD/(rc.std()*np.sqrt(TD))
        ds=rc[rc<0]; vol_d=np.std(ds)*np.sqrt(TD) if len(ds)>0 else np.nan
        sortino=((rc.mean()-rfd)*TD)/vol_d if vol_d and vol_d>0 else np.nan
        cum=(1+rc).cumprod(); pk=cum.cummax(); dd=(cum-pk)/pk; mdd=dd.min()
        var95=np.percentile(rc.dropna(),5); cvar95=rc[rc<=var95].mean()
        al=None
        try:
            sph,_,_=descargar("^GSPC",periodo_c); spr=sph["Close"].pct_change().dropna()
            al=pd.concat([rc,spr],axis=1).dropna(); al.columns=["c","s"]
            beta=np.cov(al["c"],al["s"])[0,1]/np.var(al["s"])
        except Exception: beta=np.nan
        hhi=round(np.sum(w**2),4)
        cm=dfr[tv].corr(); cv=cm.values.copy(); np.fill_diagonal(cv,np.nan); corr_avg=round(np.nanmean(cv),3)
        sigma=dfr[tv].cov().values*TD; pv=np.sqrt(w@sigma@w); mrc=sigma@w; cp=(w*mrc)/pv*100; cr_d=dict(zip(tv,np.round(cp,2)))
        st.subheader("📊 Resumen"); st.metric("Valor Total",f"${vt:,.2f}")
        st.caption(f"📡 Precios: {src_link('Finnhub')} / {src_link('Yahoo Finance')} | Benchmark: [S&P 500](https://finance.yahoo.com/quote/%5EGSPC)")
        c1,c2,c3=st.columns(3)
        with c1: st.markdown(f"**Retorno:** {ret_a:+.2f}%"); st.markdown(f"**Volatilidad:** {vol_a:.2f}%")
        with c2: st.markdown(f"**Beta:** {beta:.3f}" if not np.isnan(beta) else "Beta: N/A"); st.markdown(f"**Max DD:** {mdd*100:.2f}%"); st.markdown(f"**VaR 95%:** {var95*100:.2f}%")
        with c3: st.markdown(f"**Sharpe:** {sharpe:.3f}"); st.markdown(f"**Sortino:** {sortino:.3f}" if sortino and not np.isnan(sortino) else "Sortino: N/A"); st.markdown(f"**HHI:** {hhi:.4f}")
        g1,g2=st.columns(2)
        with g1:
            fig=go.Figure(data=go.Heatmap(z=cm.values,x=tv,y=tv,colorscale="RdYlGn_r",zmin=-1,zmax=1,text=np.round(cm.values,2),texttemplate="%{text}"))
            fig.update_layout(title="Correlaciones",template="plotly_dark",paper_bgcolor="#12121f",plot_bgcolor="#1e1e2e",height=400); st.plotly_chart(fig,use_container_width=True)
        with g2:
            crs=dict(sorted(cr_d.items(),key=lambda x:x[1],reverse=True))
            fig2=go.Figure(go.Bar(x=list(crs.keys()),y=list(crs.values()),marker_color=["#ff5555" if v>20 else "#50fa7b" for v in crs.values()]))
            fig2.update_layout(title="Contrib. Riesgo",template="plotly_dark",paper_bgcolor="#12121f",plot_bgcolor="#1e1e2e",height=400); st.plotly_chart(fig2,use_container_width=True)
        g3,g4=st.columns(2)
        with g3:
            fig3=go.Figure(go.Scatter(x=dd.index,y=dd*100,fill="tozeroy",fillcolor="rgba(255,85,85,0.2)",line=dict(color="#ff5555",width=1.5)))
            fig3.update_layout(title="Drawdown",template="plotly_dark",paper_bgcolor="#12121f",plot_bgcolor="#1e1e2e",height=400); st.plotly_chart(fig3,use_container_width=True)
        with g4:
            ra=(cum/cum.iloc[0]-1)*100; fig4=go.Figure(go.Scatter(x=ra.index,y=ra,name="Cartera",line=dict(color="#8be9fd",width=2)))
            if al is not None:
                try: sc2=(1+al["s"]).cumprod(); sa=(sc2/sc2.iloc[0]-1)*100; fig4.add_trace(go.Scatter(x=sa.index,y=sa,name="S&P 500",line=dict(color="#ffb86c",dash="dash")))
                except Exception: pass
            fig4.update_layout(title="Retorno vs S&P 500",template="plotly_dark",paper_bgcolor="#12121f",plot_bgcolor="#1e1e2e",height=400); st.plotly_chart(fig4,use_container_width=True)
        issues=[]
        if hhi>.15: issues.append(f"⚠️ Concentración alta (HHI={hhi:.3f})")
        if corr_avg>.50: issues.append(f"⚠️ Correlación elevada ({corr_avg:.2f})")
        if sharpe<0: issues.append(f"🔴 Sharpe negativo ({sharpe:.2f})")
        if abs(mdd)>.25: issues.append(f"🔴 Max DD severo ({mdd*100:.1f}%)")
        if issues:
            st.subheader("⚠️ Alertas")
            for i in issues: st.markdown(i)
        else: st.success("✅ Riesgo aceptable.")


# ╔═══════════════════════════════════════════════════════════════╗
# ║  MACRO DASHBOARD                                              ║
# ╚═══════════════════════════════════════════════════════════════╝
elif pagina == "📊 Macro":
    st.header("📊 Macro Dashboard")
    if not fred: st.error("Configura FRED API key en Settings → Secrets."); st.stop()
    series_cfg = {"Fed Funds Rate": ("FEDFUNDS","%"), "CPI": ("CPIAUCSL","Idx"), "US 10Y": ("DGS10","%"),
                  "US 2Y": ("DGS2","%"), "Desempleo US": ("UNRATE","%"), "VIX": ("VIXCLS","Pts"),
                  "DXY": ("DTWEXBGS","Idx"), "GDP": ("GDP","$B")}
    with st.sidebar: start=st.date_input("Desde",value=datetime(2020,1,1)); sel=st.multiselect("Series",list(series_cfg.keys()),default=["Fed Funds Rate","US 10Y","VIX","Desempleo US"])
    for name in sel:
        sid, unit = series_cfg[name]
        try:
            data=fred.get_series(sid,observation_start=start.strftime("%Y-%m-%d")).dropna()
            if not data.empty:
                st.subheader(name); st.caption(f"📡 {src_link('FRED', sid=sid)}")
                m1,m2=st.columns([1,2])
                with m1: st.metric("Último",f"{data.iloc[-1]:.2f} {unit}",f"{data.iloc[-1]-data.iloc[-2]:+.2f}"); st.metric("Mín",f"{data.min():.2f}"); st.metric("Máx",f"{data.max():.2f}")
                fig=go.Figure(go.Scatter(x=data.index,y=data,line=dict(color="#8be9fd",width=2),fill="tozeroy",fillcolor="rgba(139,233,253,0.05)"))
                fig.update_layout(template="plotly_dark",paper_bgcolor="#12121f",plot_bgcolor="#1e1e2e",height=250,margin=dict(t=10,b=30))
                with m2: st.plotly_chart(fig,use_container_width=True)
                st.divider()
        except Exception as e: st.warning(f"Error {name}: {e}")


# ╔═══════════════════════════════════════════════════════════════╗
# ║  RESEARCH ASSISTANT                                           ║
# ╚═══════════════════════════════════════════════════════════════╝
elif pagina == "🤖 Research":
    st.header("🤖 Research Assistant")
    st.markdown("Pregunta sobre cualquier activo, sector o tema de mercado.")
    pregunta = st.text_area("Tu pregunta:", placeholder="Ej: ¿Qué opinan los analistas sobre NVDA?", height=100)
    if st.button("🔍 Investigar", type="primary", use_container_width=True) and pregunta:
        with st.spinner("Investigando..."):
            ctx = []; posibles = re.findall(r'\b[A-Z]{1,5}\b', pregunta.upper())
            excl = {"QUE","LOS","LAS","DEL","POR","CON","UNA","COMO","PARA","MAS","HAY","SON","THE","AND","FOR","HOW"}
            tks = [t for t in posibles if t not in excl][:3]
            for t in tks:
                h, inf, _ = descargar(t, "6mo")
                if not h.empty:
                    p = h["Close"].iloc[-1]; chg = ((p/h["Close"].iloc[-22]-1)*100) if len(h)>22 else 0
                    ctx.append(f"**{t}**: {p:.2f}, 1M: {chg:+.1f}%, PE: {inf.get('trailingPE','N/A')}, Sector: {inf.get('sector','N/A')}, Target: {inf.get('targetMeanPrice','N/A')}, Rec: {inf.get('recommendationKey','N/A')}")
            news_ctx = []
            if fh and tks:
                for t in tks[:2]:
                    try:
                        ns = fh.company_news(t, _from=(datetime.now()-timedelta(days=7)).strftime("%Y-%m-%d"), to=datetime.now().strftime("%Y-%m-%d"))
                        for n in (ns or [])[:3]: news_ctx.append(f"- [{n.get('source','')}] {n.get('headline','')}")
                    except Exception: pass
            macro_ctx = []
            if fred:
                for nm, sid in [("Fed Funds","FEDFUNDS"),("US 10Y","DGS10"),("VIX","VIXCLS")]:
                    try:
                        s = fred.get_series(sid, observation_start="2024-01-01").dropna()
                        if not s.empty: macro_ctx.append(f"{nm}: {s.iloc[-1]:.2f}")
                    except Exception: pass
            contexto = "DATOS REALES:\n" + "\n".join(ctx) + ("\nMACRO: " + " | ".join(macro_ctx) if macro_ctx else "") + ("\nNOTICIAS:\n" + "\n".join(news_ctx[:6]) if news_ctx else "")
            try:
                resp = requests.post("https://api.anthropic.com/v1/messages", headers={"Content-Type": "application/json"},
                    json={"model": "claude-sonnet-4-20250514", "max_tokens": 1500,
                          "messages": [{"role": "user", "content": f'Eres analista de banca privada. Pregunta: "{pregunta}"\n\nDatos:\n{contexto}\n\nResponde en español: 1) Resumen ejecutivo 2) Datos clave 3) Riesgos 4) Conclusión'}]}, timeout=30)
                if resp.status_code == 200:
                    txt = "".join([b["text"] for b in resp.json().get("content", []) if b.get("type") == "text"])
                    st.markdown("---"); st.subheader("📋 Informe"); st.markdown(txt)
                    st.markdown("---"); st.caption(f"📡 Datos: {src_link('Finnhub')} | {src_link('Yahoo Finance')} | {src_link('FRED')} | Análisis: [Claude API — Anthropic](https://anthropic.com)")
                    with st.expander("📊 Datos brutos"): st.text(contexto)
                else: st.warning(f"API status {resp.status_code}"); st.markdown(contexto); st.caption(f"📡 {src_link('Finnhub')} | {src_link('Yahoo Finance')} | {src_link('FRED')}")
            except Exception as e: st.warning(f"Claude API: {e}"); st.markdown(contexto); st.caption(f"📡 {src_link('Finnhub')} | {src_link('Yahoo Finance')} | {src_link('FRED')}")
    else:
        st.info("Escribe tu pregunta y pulsa **Investigar**.")
        st.markdown("**Ejemplos:** ¿Qué opinan de NVDA? · ¿Buen momento para bonos US? · Análisis sector tech · Noticias TSLA esta semana")
