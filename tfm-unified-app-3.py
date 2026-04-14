"""
TFM — PLATAFORMA DE INVERSIÓN INTELIGENTE v3
Master IA Sector Financiero — VIU 2025/26
"""
import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from collections import Counter
import requests, time
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")

st.set_page_config(page_title="TFM — Investment Intelligence", page_icon="📊", layout="wide")

# ── APIs opcionales ──
fh_client = None
try:
    import finnhub
    key = st.secrets.get("FINNHUB_KEY", "")
    if key:
        fh_client = finnhub.Client(api_key=key)
except Exception:
    pass

fred_client = None
try:
    from fredapi import Fred
    key = st.secrets.get("FRED_KEY", "")
    if key:
        fred_client = Fred(api_key=key)
except Exception:
    pass


# ╔═══════════════════════════════════════════════════════════════╗
# ║  DESCARGA DE DATOS — Simple y robusto                        ║
# ╚═══════════════════════════════════════════════════════════════╝
def descargar(ticker, period="1y"):
    """
    Descarga precios + info.
    1) Finnhub para precios (API oficial, no bloqueada)
    2) yfinance como fallback
    3) Info: combina ambas fuentes
    """
    days = {"6mo": 180, "1y": 365, "2y": 730, "5y": 1825}.get(period, 365)
    hist = pd.DataFrame()
    info = {}

    # ── PASO 1: Precios desde Finnhub (primario) ──
    if fh_client:
        try:
            now = int(datetime.now().timestamp())
            start = int((datetime.now() - timedelta(days=days)).timestamp())
            r = fh_client.stock_candles(ticker, "D", start, now)
            if r and r.get("s") == "ok" and r.get("c") and len(r["c"]) > 10:
                hist = pd.DataFrame({
                    "Open": r["o"], "High": r["h"], "Low": r["l"],
                    "Close": r["c"], "Volume": r["v"]
                }, index=pd.to_datetime(r["t"], unit="s"))
                hist.index.name = "Date"
        except Exception:
            pass

    # ── PASO 2: Si Finnhub no dio precios, intentar yfinance ──
    if hist.empty:
        for intento in range(2):
            try:
                t = yf.Ticker(ticker)
                h = t.history(period=period)
                if not h.empty and len(h) > 10:
                    hist = h
                    break
            except Exception:
                pass
            time.sleep(1)

    # ── PASO 3: Info desde Finnhub ──
    if fh_client:
        try:
            p = fh_client.company_profile2(symbol=ticker)
            if p:
                info["longName"] = p.get("name", ticker)
                info["sector"] = p.get("finnhubIndustry", "N/A")
                info["industry"] = p.get("finnhubIndustry", "N/A")
                info["marketCap"] = (p.get("marketCapitalization") or 0) * 1e6
                info["currency"] = p.get("currency", "USD")
        except Exception:
            pass
        try:
            m = fh_client.company_basic_financials(ticker, "all").get("metric", {})
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
                info["currentRatio"] = m.get("currentRatioAnnual")
        except Exception:
            pass
        try:
            pt = fh_client.price_target(ticker)
            if pt:
                info["targetMeanPrice"] = pt.get("targetMean")
                info["targetHighPrice"] = pt.get("targetHigh")
                info["targetLowPrice"] = pt.get("targetLow")
        except Exception:
            pass
        try:
            recs = fh_client.recommendation_trends(ticker)
            if recs and len(recs) > 0:
                r = recs[0]
                b = r.get("strongBuy", 0) + r.get("buy", 0)
                s = r.get("strongSell", 0) + r.get("sell", 0)
                info["recommendationKey"] = "BUY" if b > s else ("SELL" if s > b else "HOLD")
        except Exception:
            pass

    # ── PASO 4: Info desde yfinance (complementa lo que Finnhub no tenga) ──
    try:
        yf_info = yf.Ticker(ticker).info or {}
        # yfinance como base, Finnhub sobreescribe lo que tenga
        merged = {**yf_info, **{k: v for k, v in info.items() if v is not None and v != 0 and v != "N/A"}}
        info = merged
    except Exception:
        pass

    return hist, info


def descargar_financials(ticker):
    """Descarga estados financieros para análisis fundamental profundo."""
    try:
        t = yf.Ticker(ticker)
        return t.financials, t.balance_sheet, t.cashflow
    except Exception:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


def fuente_badge(nombre, ticker=None, series_id=None):
    """Genera un enlace a la fuente de datos."""
    links = {
        "Finnhub": f"[🔗 Finnhub](https://finnhub.io/api/v1/quote?symbol={ticker}&token=free)" if ticker else "[🔗 Finnhub](https://finnhub.io)",
        "Yahoo Finance": f"[🔗 Yahoo Finance](https://finance.yahoo.com/quote/{ticker})" if ticker else "[🔗 Yahoo Finance](https://finance.yahoo.com)",
        "FRED": f"[🔗 FRED](https://fred.stlouisfed.org/series/{series_id})" if series_id else "[🔗 FRED](https://fred.stlouisfed.org)",
        "SEC Filings": f"[🔗 SEC](https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&company={ticker}&type=10-K)" if ticker else "[🔗 SEC](https://www.sec.gov)",
        "Wikipedia": "[🔗 Wikipedia](https://en.wikipedia.org/wiki/List_of_S%26P_500_companies)",
    }
    link = links.get(nombre, f"[🔗 {nombre}]")
    return f"`📡 {nombre}` {link}"


def enriquecer_info(ticker, info):
    """Añade datos de Finnhub si está disponible (price targets, recomendaciones)."""
    if not fh_client:
        return info
    try:
        pt = fh_client.price_target(ticker)
        if pt:
            info.setdefault("targetMeanPrice", pt.get("targetMean"))
            info.setdefault("targetHighPrice", pt.get("targetHigh"))
            info.setdefault("targetLowPrice", pt.get("targetLow"))
    except Exception:
        pass
    try:
        recs = fh_client.recommendation_trends(ticker)
        if recs and len(recs) > 0:
            r = recs[0]
            b = r.get("strongBuy", 0) + r.get("buy", 0)
            s = r.get("strongSell", 0) + r.get("sell", 0)
            info.setdefault("recommendationKey", "BUY" if b > s else ("SELL" if s > b else "HOLD"))
    except Exception:
        pass
    return info


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
    tr = pd.concat([h - l, (h - c.shift(1)).abs(), (l - c.shift(1)).abs()], axis=1).max(axis=1)
    up = h - h.shift(1)
    dn = l.shift(1) - l
    pdm = pd.Series(np.where((up > dn) & (up > 0), up, 0), index=df.index)
    mdm = pd.Series(np.where((dn > up) & (dn > 0), dn, 0), index=df.index)
    atr = tr.rolling(p).mean()
    pdi = 100 * (pdm.rolling(p).mean() / atr)
    mdi = 100 * (mdm.rolling(p).mean() / atr)
    dx = 100 * (pdi - mdi).abs() / (pdi + mdi).replace(0, np.nan)
    return dx.rolling(p).mean(), pdi, mdi

def calc_obv(df):
    d = np.sign(df["Close"].diff())
    obv = (d * df["Volume"]).fillna(0).cumsum()
    osma = obv.rolling(20).mean()
    ot = pd.Series(np.where(obv > osma, 1, -1), index=df.index)
    p20 = df["Close"].pct_change(20)
    o20 = obv.pct_change(20)
    div = pd.Series(
        np.where((p20 < 0) & (o20 > 0), "Alcista",
                 np.where((p20 > 0) & (o20 < 0), "Bajista", "Neutral")),
        index=df.index)
    return obv, osma, ot, div

def calc_stoch(df, kp=14, dp=3):
    lm = df["Low"].rolling(kp).min()
    hm = df["High"].rolling(kp).max()
    k = 100 * (df["Close"] - lm) / (hm - lm).replace(0, np.nan)
    return k, k.rolling(dp).mean()

def calc_bb(s, p=20, std=2):
    m = s.rolling(p).mean()
    sg = s.rolling(p).std()
    u = m + std * sg
    l = m - std * sg
    pb = (s - l) / (u - l).replace(0, np.nan)
    return u, m, l, pb

def calc_atr(df, p=14):
    tr = pd.concat([df["High"] - df["Low"],
                     (df["High"] - df["Close"].shift(1)).abs(),
                     (df["Low"] - df["Close"].shift(1)).abs()], axis=1).max(axis=1)
    a = tr.rolling(p).mean()
    return a, a / df["Close"] * 100


# ╔═══════════════════════════════════════════════════════════════╗
# ║  SCORING TÉCNICO 0–10                                        ║
# ╚═══════════════════════════════════════════════════════════════╝
def score_tecnico(row, obv_t, obv_d):
    sc = 0.0
    det = {}

    rsi = row["RSI"]
    if rsi < 30: p, m = 2.5, f"Sobrevendido ({rsi:.0f})"
    elif rsi < 45: p, m = 2.2, f"Zona acumulación ({rsi:.0f})"
    elif rsi < 55: p, m = 2.0, f"Neutral ({rsi:.0f})"
    elif rsi < 65: p, m = 1.5, f"Momentum+ ({rsi:.0f})"
    elif rsi < 75: p, m = 0.5, f"Zona caliente ({rsi:.0f})"
    else: p, m = 0.0, f"Sobrecomprado ({rsi:.0f})"
    sc += p
    det["RSI"] = {"pts": p, "max": 2.5, "val": f"{rsi:.1f}", "msg": m}

    mv = row["MACD"]
    sv = row["Signal"]
    hv = row["MACD_Hist"]
    ph = row.get("MACD_Hist_prev", 0) or 0
    al = mv > sv
    ac = hv > ph
    if al and ac: p, m = 2.0, "Cruce alcista acelerando"
    elif al: p, m = 1.5, "Por encima de señal"
    elif not al and ac: p, m = 0.7, "Bajista perdiendo fuerza"
    else: p, m = 0.0, "Bajista acelerando"
    sc += p
    det["MACD"] = {"pts": p, "max": 2.0, "val": f"{mv:.4f}", "msg": m}

    av = row["ADX"]
    dp = row["DI_Plus"]
    dm = row["DI_Minus"]
    if av > 30 and dp > dm: p, m = 2.0, f"Alcista FUERTE (ADX={av:.0f})"
    elif av > 20 and dp > dm: p, m = 1.5, f"Alcista moderada (ADX={av:.0f})"
    elif av > 20 and dp < dm: p, m = 0.3, f"Bajista activa (ADX={av:.0f})"
    elif av < 20: p, m = 1.0, f"Lateralización (ADX={av:.0f})"
    else: p, m = 0.7, f"Débil (ADX={av:.0f})"
    sc += p
    det["ADX"] = {"pts": p, "max": 2.0, "val": f"{av:.1f}", "msg": m}

    if obv_t == 1 and obv_d == "Alcista": p, m = 1.5, "Compradora + div alcista"
    elif obv_t == 1: p, m = 1.2, "Compradora (OBV>SMA20)"
    elif obv_t == -1 and obv_d == "Bajista": p, m = 0.0, "Vendedora + div bajista"
    else: p, m = 0.3, "Vendedora"
    sc += p
    det["OBV"] = {"pts": p, "max": 1.5, "val": "Alcista" if obv_t == 1 else "Bajista", "msg": m}

    kv = row["Stoch_K"]
    if kv < 20 and kv > row["Stoch_D"]: p, m = 1.0, f"Sobrevendido+cruce (%K={kv:.0f})"
    elif kv < 25: p, m = 0.8, f"Sobrevendida (%K={kv:.0f})"
    elif kv > 80: p, m = 0.0, f"Sobrecomprada (%K={kv:.0f})"
    elif kv > row["Stoch_D"]: p, m = 0.6, f"Momentum+ (%K={kv:.0f}>%D)"
    else: p, m = 0.2, f"Momentum- (%K={kv:.0f}<%D)"
    sc += p
    det["Stochastic"] = {"pts": p, "max": 1.0, "val": f"{kv:.1f}", "msg": m}

    bb = row["BB_PctB"]
    if bb < 0: p, m = 1.0, f"Debajo banda inf (%B={bb:.2f})"
    elif bb < 0.35: p, m = 1.0, f"Zona inferior (%B={bb:.2f})"
    elif bb < 0.65: p, m = 0.5, f"Zona media (%B={bb:.2f})"
    elif bb < 1.0: p, m = 0.1, f"Zona superior (%B={bb:.2f})"
    else: p, m = 0.0, f"Encima banda sup (%B={bb:.2f})"
    sc += p
    det["Bollinger%B"] = {"pts": p, "max": 1.0, "val": f"{bb:.2f}", "msg": m}

    return round(min(sc, 10.0), 1), det

def interpretar(sc):
    if sc >= 8.0: return "🟢 COMPRAR / ACUMULAR", "#50fa7b"
    elif sc >= 6.0: return "🔵 MANTENER / VIGILAR", "#8be9fd"
    elif sc >= 4.0: return "🟡 NEUTRO / ESPERAR", "#f1fa8c"
    return "🔴 REDUCIR / VENDER", "#ff5555"


# ╔═══════════════════════════════════════════════════════════════╗
# ║  NIVELES OPERATIVOS                                          ║
# ╚═══════════════════════════════════════════════════════════════╝
def niveles_op(hist, info):
    last = hist.iloc[-1]
    precio = last["Close"]
    tr = pd.concat([hist["High"] - hist["Low"],
                     (hist["High"] - hist["Close"].shift(1)).abs(),
                     (hist["Low"] - hist["Close"].shift(1)).abs()], axis=1).max(axis=1)
    atr_val = tr.rolling(14).mean().iloc[-1]
    bb_low = last.get("BB_Low", np.nan)
    sma50 = last.get("SMA_50", np.nan)
    soportes = [v for v in [bb_low, sma50] if pd.notna(v) and v < precio]
    entrada_opt = round(max(soportes), 2) if soportes else round(precio * 0.97, 2)
    sl_atr = precio - 1.5 * atr_val
    sop_20d = hist["Low"].iloc[-20:].min()
    if sop_20d > sl_atr and sop_20d < precio:
        sl = round(sop_20d, 2)
        sl_nota = f"Soporte 20d ({sop_20d:.2f})"
    else:
        sl = round(sl_atr, 2)
        sl_nota = f"ATR×1.5 ({sl_atr:.2f})"
    riesgo = precio - sl
    riesgo_pct = round(riesgo / precio * 100, 2)
    tp1 = round(precio + 2 * riesgo, 2)
    tp2 = round(precio + 3 * riesgo, 2)
    tm = info.get("targetMeanPrice")
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
    sc = 0
    det = {}
    try:
        ta0 = _sf(bs, "Total Assets", 0, 1)
        ta1 = _sf(bs, "Total Assets", 1, 1)
        ni0 = _sf(fin, "Net Income", 0)
        ni1 = _sf(fin, "Net Income", 1)
        cfo = _sf(cf, "Operating Cash Flow", 0)
        ca0 = _sf(bs, "Current Assets", 0)
        cl0 = _sf(bs, "Current Liabilities", 0) or 1
        ca1 = _sf(bs, "Current Assets", 1)
        cl1 = _sf(bs, "Current Liabilities", 1) or 1
        ltd0 = _sf(bs, "Long Term Debt", 0)
        ltd1 = _sf(bs, "Long Term Debt", 1)
        rev0 = _sf(fin, "Total Revenue", 0) or 1
        rev1 = _sf(fin, "Total Revenue", 1) or 1
        gp0 = _sf(fin, "Gross Profit", 0)
        gp1 = _sf(fin, "Gross Profit", 1)
        tests = [
            ("F1 ROA positivo", ni0 / ta0 > 0, f"{ni0/ta0*100:.2f}%"),
            ("F2 CFO positivo", cfo > 0, f"${cfo/1e6:.0f}M"),
            ("F3 ROA mejora", ni0 / ta0 > ni1 / ta1, f"{ni0/ta0*100:.2f}% vs {ni1/ta1*100:.2f}%"),
            ("F4 CFO > NI", cfo > ni0, f"CFO {cfo/1e6:.0f}M > NI {ni0/1e6:.0f}M"),
            ("F5 Menor deuda", ltd0 / ta0 < ltd1 / ta1, f"{ltd0/ta0:.3f} vs {ltd1/ta1:.3f}"),
            ("F6 Mejor liquidez", ca0 / cl0 > ca1 / cl1, f"{ca0/cl0:.2f} vs {ca1/cl1:.2f}"),
            ("F7 Sin dilución", True, "(manual)"),
            ("F8 Margen bruto+", gp0 / rev0 > gp1 / rev1, f"{gp0/rev0*100:.1f}% vs {gp1/rev1*100:.1f}%"),
            ("F9 Rot activos+", rev0 / ta0 > rev1 / ta1, f"{rev0/ta0:.3f} vs {rev1/ta1:.3f}"),
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
        ta = _sf(bs, "Total Assets", 0, 1)
        ca = _sf(bs, "Current Assets", 0)
        cl = _sf(bs, "Current Liabilities", 0)
        re = _sf(bs, "Retained Earnings", 0)
        ebit = _sf(fin, "EBIT", 0) or _sf(fin, "Operating Income", 0)
        tl = _sf(bs, "Total Liabilities Net Minority Interest", 0) or _sf(bs, "Total Debt", 0) or 1
        rev = _sf(fin, "Total Revenue", 0)
        mc = info.get("marketCap", 0)
        z = 1.2 * ((ca - cl) / ta) + 1.4 * (re / ta) + 3.3 * (ebit / ta) + 0.6 * (mc / tl) + 1.0 * (rev / ta)
        zona = "🟢 SEGURA" if z > 2.99 else ("🟡 GRIS" if z > 1.81 else "🔴 PELIGRO")
        return round(z, 2), zona
    except Exception:
        return None, "Sin datos"

def calc_graham(info):
    try:
        eps = info.get("trailingEps") or info.get("forwardEps")
        bv = info.get("bookValue")
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
        ni = _sf(fin, "Net Income", 0)
        rev = _sf(fin, "Total Revenue", 0) or 1
        ta = _sf(bs, "Total Assets", 0) or 1
        eq = _sf(bs, "Stockholders Equity", 0) or 1
        nm = ni / rev
        at = rev / ta
        lv = ta / eq
        return {"ROE": round(nm * at * lv * 100, 2), "Margen_Neto": round(nm * 100, 2),
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
            vi = v.iloc[-1]
            vf = v.iloc[0]
            n = len(v) - 1
            if vi <= 0 or vf <= 0: return None
            return round(((vf / vi) ** (1 / n) - 1) * 100, 1)
        rv = fin.loc["Total Revenue"] if "Total Revenue" in fin.index else None
        ni = fin.loc["Net Income"] if "Net Income" in fin.index else None
        return (cg(rv) if rv is not None else None, cg(ni) if ni is not None else None)
    except Exception:
        return None, None


def calc_dcf(info, cf, fin):
    """
    Valoración DCF simplificada.
    Proyecta FCF 5 años, descuenta con WACC estimado, añade valor terminal.
    Retorna: precio justo estimado, detalle del cálculo.
    """
    try:
        fcf = info.get("freeCashflow")
        if not fcf or fcf <= 0:
            cfo = _sf(cf, "Operating Cash Flow", 0)
            capex = abs(_sf(cf, "Capital Expenditure", 0))
            fcf = cfo - capex
        if not fcf or fcf <= 0:
            return None, None

        shares = info.get("sharesOutstanding")
        if not shares or shares <= 0:
            return None, None

        # Tasa de crecimiento: CAGR de ingresos histórico, capped
        cagr_r, _ = calc_cagr(fin)
        if cagr_r is not None and cagr_r > 0:
            g = min(cagr_r / 100, 0.20)  # cap 20%
        else:
            g = 0.05  # default 5%

        # WACC estimado
        beta = info.get("beta") or 1.0
        risk_free = 0.04
        equity_premium = 0.055
        wacc = risk_free + beta * equity_premium
        wacc = max(wacc, 0.06)  # mínimo 6%

        # Tasa de crecimiento terminal
        g_terminal = 0.025  # 2.5%

        # Proyectar FCF 5 años
        fcf_proj = []
        fcf_current = fcf
        for yr in range(1, 6):
            fcf_current = fcf_current * (1 + g)
            fcf_proj.append(fcf_current)

        # Valor terminal (Gordon Growth)
        terminal_value = fcf_proj[-1] * (1 + g_terminal) / (wacc - g_terminal)

        # Descontar todo al presente
        pv_fcf = sum(f / (1 + wacc) ** i for i, f in enumerate(fcf_proj, 1))
        pv_terminal = terminal_value / (1 + wacc) ** 5
        enterprise_value = pv_fcf + pv_terminal

        # Ajustar por deuda y cash
        total_debt = info.get("totalDebt") or 0
        total_cash = info.get("totalCash") or 0
        equity_value = enterprise_value - total_debt + total_cash

        fair_price = equity_value / shares

        detalle = {
            "fcf_actual": fcf,
            "growth_rate": round(g * 100, 1),
            "wacc": round(wacc * 100, 1),
            "g_terminal": round(g_terminal * 100, 1),
            "pv_fcf": round(pv_fcf / 1e9, 2),
            "pv_terminal": round(pv_terminal / 1e9, 2),
            "enterprise_value": round(enterprise_value / 1e9, 2),
            "deuda": round(total_debt / 1e9, 2),
            "cash": round(total_cash / 1e9, 2),
            "equity_value": round(equity_value / 1e9, 2),
            "shares_m": round(shares / 1e6, 1),
            "fair_price": round(fair_price, 2),
            "beta": round(beta, 2),
        }
        return round(fair_price, 2), detalle
    except Exception:
        return None, None

def mf(nombre, val, fmt, bueno, malo):
    if val is None:
        return f"**{nombre}:** N/A"
    ic = "✅" if bueno(val) else ("🔴" if malo(val) else "🟡")
    if fmt.endswith("%"):
        return f"{ic} **{nombre}:** {val:{fmt[:-1]}}%"
    return f"{ic} **{nombre}:** {val:{fmt}}"


# ╔═══════════════════════════════════════════════════════════════╗
# ║  SCREENER TICKERS                                             ║
# ╚═══════════════════════════════════════════════════════════════╝
@st.cache_data(ttl=86400)
def get_sp500():
    try:
        html = requests.get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
                            headers={"User-Agent": "Mozilla/5.0"}, timeout=10).text
        t = pd.read_html(html)
        return [x.replace(".", "-") for x in t[0]["Symbol"].tolist()]
    except Exception:
        # Fallback: top 50 del S&P 500 hardcodeados
        return ["AAPL","MSFT","GOOGL","AMZN","NVDA","META","TSLA","BRK-B","JPM","V",
                "JNJ","UNH","PG","MA","HD","DIS","NFLX","PFE","KO","PEP","MRK","ABBV",
                "AVGO","COST","WMT","CSCO","TMO","ABT","CRM","ACN","NKE","MCD","LLY",
                "DHR","TXN","QCOM","INTC","AMGN","PM","UPS","MS","GS","BLK","AXP",
                "CAT","BA","GE","IBM","MMM","CVX"]

@st.cache_data(ttl=86400)
def get_ibex():
    return ["SAN.MC", "BBVA.MC", "ITX.MC", "IBE.MC", "TEF.MC", "FER.MC", "AMS.MC",
            "REP.MC", "CABK.MC", "ACS.MC", "GRF.MC", "MAP.MC", "ENG.MC", "RED.MC",
            "IAG.MC", "FDR.MC", "MEL.MC", "COL.MC", "CLNX.MC", "SAB.MC"]

@st.cache_data(ttl=86400)
def get_dax():
    return ["SAP.DE", "SIE.DE", "ALV.DE", "DTE.DE", "AIR.DE", "MBG.DE", "DHL.DE",
            "BAS.DE", "BMW.DE", "IFX.DE", "BEI.DE", "BAYN.DE", "ADS.DE", "VOW3.DE",
            "DB1.DE", "RWE.DE", "CON.DE", "DBK.DE", "MRK.DE", "SHL.DE"]

@st.cache_data(ttl=86400)
def get_etfs():
    return ["SPY", "QQQ", "IWM", "EFA", "EEM", "VTI", "VEA", "VWO", "GLD", "SLV",
            "TLT", "IEF", "HYG", "LQD", "XLE", "XLF", "XLK", "XLV", "XLY", "XLP",
            "XLI", "XLU", "XLB", "XLRE", "XLC", "VNQ", "ARKK", "KWEB", "SMH", "SOXX",
            "FXI", "EWZ", "EWJ", "EWG", "EWU", "PHO", "CGW", "ICLN", "TAN", "LIT"]

INDICES = {
    "SP500": ("S&P 500", get_sp500),
    "IBEX35": ("IBEX 35", get_ibex),
    "DAX40": ("DAX 40", get_dax),
    "ETFs": ("ETFs Globales", get_etfs),
}

CL = {"COMPRAR": "#50fa7b", "VIGILAR": "#8be9fd", "NEUTRO": "#f1fa8c", "EVITAR": "#ff5555"}

def score_screener(r):
    s = 0.0
    mom = r.get("Mom 3M %", np.nan)
    if pd.notna(mom) and mom > 20: s += 1.0
    elif pd.notna(mom) and mom > 10: s += 0.7
    elif pd.notna(mom) and mom > 0: s += 0.4
    if pd.notna(r.get("vs SMA50 %")) and r["vs SMA50 %"] > 0: s += 0.5
    vr = r.get("Vol/Avg 20d", np.nan)
    if pd.notna(vr) and vr > 2.0: s += 1.0
    elif pd.notna(vr) and vr > 1.3: s += 0.5
    per = r.get("PER", np.nan)
    if pd.notna(per) and 0 < per < 12: s += 1.5
    elif pd.notna(per) and 0 < per < 20: s += 1.0
    roe = r.get("ROE %", np.nan)
    if pd.notna(roe) and roe > 25: s += 1.0
    elif pd.notna(roe) and roe > 15: s += 0.7
    mg = r.get("Margen Net %", np.nan)
    if pd.notna(mg) and mg > 20: s += 1.0
    elif pd.notna(mg) and mg > 10: s += 0.7
    de = r.get("D/E", np.nan)
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
        hist, info, fuente = descargar(ticker, "1y")
        if hist.empty or len(hist) < 20:
            return None
        last = hist.iloc[-1]
        precio = last["Close"]
        if precio <= 0:
            return None
        mc_b = round(info.get("marketCap", 0) / 1e9, 2)
        mom3 = ((precio / hist["Close"].iloc[-63] - 1) * 100) if len(hist) > 63 else np.nan
        sma50 = hist["Close"].rolling(50).mean().iloc[-1] if len(hist) >= 50 else np.nan
        vs50 = round((precio / sma50 - 1) * 100, 2) if pd.notna(sma50) else np.nan
        dmx = round((precio / hist["High"].max() - 1) * 100, 2)
        vh = last["Volume"]
        va20 = hist["Volume"].rolling(20).mean().iloc[-1]
        vr20 = round(vh / va20, 2) if va20 > 0 else np.nan
        per = info.get("trailingPE")
        roe = info.get("returnOnEquity")
        mn = info.get("profitMargins")
        de = info.get("debtToEquity")
        dy = info.get("dividendYield")
        tm = info.get("targetMeanPrice")
        pot = round((tm / precio - 1) * 100, 1) if tm and precio > 0 else np.nan
        r = {
            "Ticker": ticker, "Precio": round(precio, 2), "MktCap (B$)": mc_b,
            "Mom 3M %": round(mom3, 2) if pd.notna(mom3) else np.nan,
            "vs SMA50 %": vs50, "Dist Max52W %": dmx,
            "Vol/Avg 20d": vr20,
            "PER": round(per, 1) if per else np.nan,
            "ROE %": round(roe * 100, 1) if roe else np.nan,
            "Margen Net %": round(mn * 100, 1) if mn else np.nan,
            "D/E": round(de, 1) if de else np.nan,
            "Div Yield %": round(dy * 100, 2) if dy else 0,
            "Potencial %": pot,
            "Consenso": info.get("recommendationKey", "N/A"),
        }
        r["Score"] = score_screener(r)
        r["Label"] = label_sc(r["Score"])
        return r
    except Exception:
        return None

def filtrar(df, modo):
    d = df.copy()
    if modo == "VALUE":
        mask = d["PER"].between(0, 20) & (d["Margen Net %"] > 8)
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
    pagina = st.radio("Módulo", ["🌅 Outlook", "🔍 Screener", "📈 Análisis Individual", "💼 Cartera", "📊 Macro", "🤖 Research"])
    st.divider()
    st.caption(f"{'✅' if fh_client else '❌'} Finnhub | {'✅' if fred_client else '❌'} FRED | ✅ yfinance")


# ╔═══════════════════════════════════════════════════════════════╗
# ║  MORNING OUTLOOK                                              ║
# ╚═══════════════════════════════════════════════════════════════╝
if pagina == "🌅 Outlook":
    st.header("🌅 Morning Outlook — Resumen de Mercado")
    st.caption(f"📅 {datetime.now().strftime('%d/%m/%Y %H:%M')} UTC")

    with st.spinner("Cargando datos de mercado..."):
        # ── Índices principales ──
        indices = {
            "^GSPC": "S&P 500", "^IXIC": "Nasdaq", "^DJI": "Dow Jones",
            "^STOXX50E": "Euro Stoxx 50", "^IBEX": "IBEX 35", "^GDAXI": "DAX 40",
            "^N225": "Nikkei 225", "^HSI": "Hang Seng",
        }
        idx_data = []
        for sym, name in indices.items():
            h, _, src = descargar(sym, "6mo")
            if not h.empty and len(h) > 2:
                last = h["Close"].iloc[-1]
                prev = h["Close"].iloc[-2]
                chg = (last / prev - 1) * 100
                chg_mtd = (last / h["Close"].iloc[-22] - 1) * 100 if len(h) > 22 else np.nan
                chg_ytd = (last / h["Close"].iloc[0] - 1) * 100
                idx_data.append({"Índice": name, "Último": round(last, 2),
                                  "Día %": round(chg, 2), "Mes %": round(chg_mtd, 2) if pd.notna(chg_mtd) else None,
                                  "YTD %": round(chg_ytd, 2)})
            time.sleep(0.3)

    if idx_data:
        st.subheader("🌍 Índices Principales")
        df_idx = pd.DataFrame(idx_data)

        # Mostrar como métricas en fila
        cols = st.columns(4)
        for i, row in df_idx.iterrows():
            with cols[i % 4]:
                delta = f"{row['Día %']:+.2f}%"
                st.metric(row["Índice"], f"{row['Último']:,.2f}", delta)

        st.dataframe(df_idx, use_container_width=True, hide_index=True)
        st.caption(f"📡 Fuente: Finnhub API / Yahoo Finance — [finnhub.io](https://finnhub.io) | [finance.yahoo.com](https://finance.yahoo.com)")
    else:
        st.warning("No se pudieron cargar los índices. Reintenta en unos minutos.")

    # ── Sectores (ETFs sectoriales) ──
    st.subheader("📊 Rendimiento Sectorial (1 día)")
    sectores = {
        "XLK": "Tecnología", "XLF": "Financiero", "XLE": "Energía",
        "XLV": "Salud", "XLY": "Cons. Discrecional", "XLP": "Cons. Básico",
        "XLI": "Industrial", "XLU": "Utilities", "XLB": "Materiales",
        "XLRE": "Inmobiliario", "XLC": "Comunicación",
    }
    sec_data = []
    with st.spinner("Cargando sectores..."):
        for sym, name in sectores.items():
            h, _, src = descargar(sym, "6mo")
            if not h.empty and len(h) > 2:
                chg = (h["Close"].iloc[-1] / h["Close"].iloc[-2] - 1) * 100
                sec_data.append({"Sector": name, "Ticker": sym, "Cambio %": round(chg, 2)})
            time.sleep(0.2)

    if sec_data:
        df_sec = pd.DataFrame(sec_data).sort_values("Cambio %", ascending=False)
        colors = ["#50fa7b" if v >= 0 else "#ff5555" for v in df_sec["Cambio %"]]
        fig_sec = go.Figure(go.Bar(
            x=df_sec["Sector"], y=df_sec["Cambio %"],
            marker_color=colors,
            text=[f"{v:+.2f}%" for v in df_sec["Cambio %"]],
            textposition="outside"))
        fig_sec.update_layout(title="Mapa Sectorial — Cambio Diario",
                               template="plotly_dark", paper_bgcolor="#12121f",
                               plot_bgcolor="#1e1e2e", height=400, yaxis_title="%")
        st.plotly_chart(fig_sec, use_container_width=True)
        st.caption("📡 Fuente: ETFs sectoriales SPDR vía Finnhub / Yahoo Finance — [sectorspdrs.com](https://www.sectorspdrs.com)")

    # ── Macro rápido (FRED) ──
    if fred_client:
        st.subheader("📈 Indicadores Macro (FRED)")
        st.caption("📡 Fuente: Federal Reserve Economic Data — [fred.stlouisfed.org](https://fred.stlouisfed.org)")
        macro_ids = {"Fed Funds Rate": "FEDFUNDS", "US 10Y Treasury": "DGS10",
                     "US 2Y Treasury": "DGS2", "CPI (inflación YoY)": "CPIAUCSL",
                     "Desempleo US": "UNRATE", "VIX": "VIXCLS"}
        mc = st.columns(3)
        i = 0
        for name, sid in macro_ids.items():
            try:
                s = fred_client.get_series(sid, observation_start="2024-01-01")
                if not s.empty:
                    last_val = s.dropna().iloc[-1]
                    prev_val = s.dropna().iloc[-2] if len(s.dropna()) > 1 else last_val
                    delta = last_val - prev_val
                    with mc[i % 3]:
                        st.metric(name, f"{last_val:.2f}", f"{delta:+.2f}")
                    i += 1
            except Exception:
                pass

        # Curva de tipos (2Y vs 10Y spread)
        try:
            y10 = fred_client.get_series("DGS10", observation_start="2022-01-01").dropna()
            y2 = fred_client.get_series("DGS2", observation_start="2022-01-01").dropna()
            if not y10.empty and not y2.empty:
                spread = y10 - y2
                spread = spread.dropna()
                if not spread.empty:
                    st.subheader("📉 Curva de Tipos (10Y - 2Y Spread)")
                    st.caption("📡 [DGS10](https://fred.stlouisfed.org/series/DGS10) − [DGS2](https://fred.stlouisfed.org/series/DGS2) — Federal Reserve Economic Data")
                    fig_spread = go.Figure()
                    fig_spread.add_trace(go.Scatter(x=spread.index, y=spread,
                        line=dict(color="#8be9fd", width=2), fill="tozeroy",
                        fillcolor="rgba(139,233,253,0.1)"))
                    fig_spread.add_hline(y=0, line_dash="dash", line_color="#ff5555", opacity=0.7,
                        annotation_text="Inversión (recesión)", annotation_font_color="#ff5555")
                    fig_spread.update_layout(template="plotly_dark", paper_bgcolor="#12121f",
                        plot_bgcolor="#1e1e2e", height=350, yaxis_title="Spread (%)")
                    st.plotly_chart(fig_spread, use_container_width=True)
                    last_spread = spread.iloc[-1]
                    if last_spread < 0:
                        st.warning(f"⚠️ Curva invertida ({last_spread:.2f}%). Históricamente señal de recesión.")
                    else:
                        st.success(f"✅ Curva normal ({last_spread:.2f}%).")
        except Exception:
            pass
    else:
        st.info("Conecta la API de FRED en Settings → Secrets para ver datos macro.")

    # ── Noticias (Finnhub) ──
    if fh_client:
        st.subheader("📰 Noticias del Mercado")
        st.caption("📡 Fuente: Finnhub News API — [finnhub.io](https://finnhub.io)")
        try:
            today = datetime.now().strftime("%Y-%m-%d")
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            news = fh_client.general_news("general", min_id=0)
            if news:
                for article in news[:8]:
                    col_n1, col_n2 = st.columns([3, 1])
                    with col_n1:
                        st.markdown(f"**{article.get('headline', 'Sin título')}**")
                        st.caption(f"{article.get('source', '')} — {datetime.fromtimestamp(article.get('datetime', 0)).strftime('%d/%m %H:%M')}")
                        summary = article.get("summary", "")
                        if summary:
                            st.caption(summary[:200] + "..." if len(summary) > 200 else summary)
                    with col_n2:
                        url = article.get("url", "")
                        if url:
                            st.link_button("Leer →", url)
                    st.divider()
        except Exception:
            st.info("No se pudieron cargar noticias.")

    # ── Escenarios macro (Claude API) ──
    st.subheader("🔮 Escenarios de Mercado")
    if st.button("Generar escenarios con IA", type="secondary"):
        with st.spinner("Generando escenarios..."):
            # Recopilar datos macro
            macro_str = ""
            if idx_data:
                macro_str += "ÍNDICES: " + " | ".join([f"{r['Índice']}: {r['Día %']:+.2f}% día, {r['YTD %']:+.2f}% YTD" for r in idx_data[:6]]) + "\n"
            if sec_data:
                tops = sorted(sec_data, key=lambda x: x["Cambio %"], reverse=True)
                macro_str += f"MEJORES SECTORES: {tops[0]['Sector']} ({tops[0]['Cambio %']:+.2f}%), {tops[1]['Sector']} ({tops[1]['Cambio %']:+.2f}%)\n"
                macro_str += f"PEORES SECTORES: {tops[-1]['Sector']} ({tops[-1]['Cambio %']:+.2f}%), {tops[-2]['Sector']} ({tops[-2]['Cambio %']:+.2f}%)\n"
            if fred_client:
                for name, sid in [("Fed Funds", "FEDFUNDS"), ("US10Y", "DGS10"), ("VIX", "VIXCLS"), ("Desempleo", "UNRATE")]:
                    try:
                        s = fred_client.get_series(sid, observation_start="2024-06-01").dropna()
                        if not s.empty:
                            macro_str += f"{name}: {s.iloc[-1]:.2f} (hace 3M: {s.iloc[-63]:.2f})\n" if len(s) > 63 else f"{name}: {s.iloc[-1]:.2f}\n"
                    except Exception:
                        pass
            try:
                resp = requests.post("https://api.anthropic.com/v1/messages",
                    headers={"Content-Type": "application/json"},
                    json={"model": "claude-sonnet-4-20250514", "max_tokens": 1200,
                          "messages": [{"role": "user", "content": f"""Eres un estratega macro senior. Con estos datos de hoy:

{macro_str}

Genera 3 escenarios a 3 meses:
1. 🟢 BULL CASE (probabilidad %)
2. 🟡 BASE CASE (probabilidad %)
3. 🔴 BEAR CASE (probabilidad %)

Para cada escenario:
- Qué lo activaría (catalizadores)
- Impacto en renta variable, renta fija, materias primas
- Posicionamiento táctico recomendado

Concluye con tu escenario preferido y por qué. Responde en español, tono profesional de research institucional."""}]},
                    timeout=30)
                if resp.status_code == 200:
                    txt = "".join([b["text"] for b in resp.json().get("content", []) if b.get("type") == "text"])
                    st.markdown(txt)
                    st.caption("📡 Datos: [FRED](https://fred.stlouisfed.org) + [Finnhub](https://finnhub.io) | Análisis: [Claude API — Anthropic](https://anthropic.com)")
                else:
                    st.warning("No se pudo generar el análisis con IA. Datos disponibles:")
                    st.text(macro_str)
            except Exception as e:
                st.warning(f"Error: {e}")
                st.text(macro_str)


# ╔═══════════════════════════════════════════════════════════════╗
# ║  SCREENER                                                     ║
# ╚═══════════════════════════════════════════════════════════════╝
elif pagina == "🔍 Screener":
    with st.sidebar:
        indice = st.selectbox("Índice", list(INDICES.keys()), format_func=lambda x: INDICES[x][0])
        modo = st.selectbox("Modo", ["VALUE", "MOMENTUM", "QUALITY", "DIVIDENDOS", "TODO"])
        limite = st.slider("Tickers", 10, 100, 30, step=10)
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
                if r:
                    res.append(r)
                if i % 3 == 2:
                    time.sleep(1)
            pb.empty()
            if not res:
                st.error("Sin resultados. Yahoo Finance puede estar limitando. Intenta con menos tickers.")
            else:
                df_raw = pd.DataFrame(res)
                df_f = filtrar(df_raw, modo).sort_values("Score", ascending=False).reset_index(drop=True)
                st.subheader(f"{len(df_f)} de {len(df_raw)} activos")
                if df_f.empty:
                    st.warning("Ningún activo cumple los filtros.")
                else:
                    cols = ["Ticker", "Precio", "Score", "Label", "Mom 3M %", "Vol/Avg 20d",
                            "PER", "ROE %", "Margen Net %", "Potencial %", "MktCap (B$)"]
                    cols = [c for c in cols if c in df_f.columns]
                    st.dataframe(df_f[cols], use_container_width=True, height=500)
                    st.caption("📡 Fuentes: Precios vía [Finnhub](https://finnhub.io) | Fundamentales vía [Yahoo Finance](https://finance.yahoo.com) | Tickers vía [Wikipedia](https://en.wikipedia.org/wiki/List_of_S%26P_500_companies)")

                    # ── RANKING CON PERCENTILES ──
                    st.subheader("📊 Posicionamiento por Percentiles")
                    st.caption("Dónde se sitúa cada activo respecto al universo analizado (percentil 100 = mejor)")
                    metric_cols = ["Score", "Mom 3M %", "Vol/Avg 20d", "PER", "ROE %", "Margen Net %", "Potencial %"]
                    # PER: menor es mejor → invertimos
                    invert = {"PER"}
                    avail_cols = [c for c in metric_cols if c in df_f.columns]
                    if len(avail_cols) > 1 and len(df_f) > 3:
                        from scipy.stats import percentileofscore as pctof
                        pct_data = []
                        for _, row in df_f.head(15).iterrows():
                            pct_row = {"Ticker": row["Ticker"]}
                            for col in avail_cols:
                                vals = df_f[col].dropna()
                                if len(vals) > 1 and pd.notna(row.get(col)):
                                    pct = pctof(vals, row[col])
                                    if col in invert:
                                        pct = 100 - pct
                                    pct_row[col] = round(pct, 0)
                                else:
                                    pct_row[col] = None
                            pct_data.append(pct_row)
                        df_pct = pd.DataFrame(pct_data)
                        st.dataframe(df_pct.style.background_gradient(
                            subset=[c for c in avail_cols if c in df_pct.columns],
                            cmap="RdYlGn", vmin=0, vmax=100),
                            use_container_width=True, height=400)
                    dv = df_f.dropna(subset=["Mom 3M %"]).head(25)
                    if not dv.empty:
                        fig = px.scatter(dv, x="Mom 3M %", y="Score", color="Label",
                                         color_discrete_map=CL, hover_data=["Ticker", "PER"],
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
        tab = st.radio("Vista", ["🔧 Técnico", "📋 Fundamental", "🔧+📋 Completo"])
        st.divider()
        go_btn = st.button("🚀 Analizar", type="primary", use_container_width=True)

    if go_btn and ticker_in:
        ticker_in = ticker_in.upper().strip()
        with st.spinner(f"Analizando {ticker_in}..."):
            hist, info, fuente_precios = descargar(ticker_in, "2y")
            fin, bs, cf = descargar_financials(ticker_in)

        if hist.empty:
            st.error(f"Sin datos para {ticker_in}. Espera 1-2 minutos y reintenta.")
            st.stop()
        if len(hist) < 50:
            st.warning(f"Solo {len(hist)} sesiones disponibles. Algunos indicadores pueden ser parciales.")

        nombre = info.get("longName") or info.get("shortName", ticker_in)
        precio = hist["Close"].iloc[-1]
        moneda = info.get("currency", "")
        st.header(f"🏢 {nombre} ({ticker_in})")
        h1, h2, h3 = st.columns(3)
        h1.metric("Precio", f"{precio:.2f} {moneda}")
        h2.metric("Sector", info.get("sector", "N/A"))
        h3.metric("MktCap", f"${info.get('marketCap', 0)/1e9:.2f}B")

        # Fuentes
        fuente_perfil = info.get("_fuente_perfil", "Yahoo Finance")
        fuente_metricas = info.get("_fuente_metricas", "Yahoo Finance")
        fuente_target = info.get("_fuente_target", "Yahoo Finance")
        st.caption(f"📡 Precios: [{fuente_precios}]({'https://finnhub.io' if fuente_precios == 'Finnhub' else f'https://finance.yahoo.com/quote/{ticker_in}'}) | "
                   f"Perfil: [{fuente_perfil}]({'https://finnhub.io' if fuente_perfil == 'Finnhub' else f'https://finance.yahoo.com/quote/{ticker_in}'}) | "
                   f"Métricas: [{fuente_metricas}]({'https://finnhub.io' if fuente_metricas == 'Finnhub' else f'https://finance.yahoo.com/quote/{ticker_in}'})")

        # Variables por defecto (se sobreescriben si se ejecuta el análisis)
        sc = "N/A"; verd = "N/A"; det = {}; nv = {}; fs = 0
        dcf_price = None; margen_seguridad = 0; vf_txt = "N/A"; pf = 0; mx_v = 0; pts_v = 0

        # ── TÉCNICO ──
        if tab in ["🔧 Técnico", "🔧+📋 Completo"]:
            st.divider()
            st.subheader("🔧 Análisis Técnico — 7 Indicadores")
            hist["RSI"] = calc_rsi(hist["Close"])
            hist["SMA_50"] = hist["Close"].rolling(50).mean()
            hist["SMA_200"] = hist["Close"].rolling(200).mean()
            bu, bm, bl, bp = calc_bb(hist["Close"])
            hist["BB_Up"] = bu; hist["BB_Mid"] = bm; hist["BB_Low"] = bl; hist["BB_PctB"] = bp
            mc, sg, mh = calc_macd(hist["Close"])
            hist["MACD"] = mc; hist["Signal"] = sg; hist["MACD_Hist"] = mh
            hist["MACD_Hist_prev"] = mh.shift(1)
            ax, dp, dm = calc_adx(hist)
            hist["ADX"] = ax.values; hist["DI_Plus"] = dp.values; hist["DI_Minus"] = dm.values
            ov, osm, ot, od = calc_obv(hist)
            hist["OBV"] = ov; hist["OBV_SMA"] = osm
            sk, sd = calc_stoch(hist)
            hist["Stoch_K"] = sk; hist["Stoch_D"] = sd
            at_v, at_p = calc_atr(hist)
            hist["ATR_PCT"] = at_p

            last = hist.iloc[-1]
            sc, det = score_tecnico(last, ot.iloc[-1], od.iloc[-1])
            verd, colv = interpretar(sc)

            s1, s2, s3 = st.columns([1, 2, 1])
            s1.metric("SCORE", f"{sc}/10")
            s2.markdown(f"### {verd}")
            s3.caption(f"SMA200: {'✅' if precio > last['SMA_200'] else '🔴'} | ATR%: {last['ATR_PCT']:.2f}%")

            for ind, d in det.items():
                pct = d["pts"] / d["max"] if d["max"] > 0 else 0
                ic = "✅" if pct >= 0.7 else ("🟡" if pct >= 0.3 else "🔴")
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
            n_pts = min(len(hist), 252)
            hg = hist.iloc[-n_pts:]
            fig = make_subplots(rows=5, cols=1, shared_xaxes=True, vertical_spacing=0.02,
                                row_heights=[0.35, 0.15, 0.15, 0.15, 0.15])
            fig.add_trace(go.Scatter(x=hg.index, y=hg["Close"], name="Precio",
                                      line=dict(color="#f8f8f2", width=1.8)), row=1, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["SMA_50"], name="SMA50",
                                      line=dict(color="#ffb86c", width=1, dash="dash")), row=1, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["SMA_200"], name="SMA200",
                                      line=dict(color="#ff5555", width=1, dash="dash")), row=1, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["BB_Up"], showlegend=False,
                                      line=dict(color="#8be9fd", width=0.5)), row=1, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["BB_Low"], name="BB",
                                      line=dict(color="#8be9fd", width=0.5), fill="tonexty",
                                      fillcolor="rgba(139,233,253,0.08)"), row=1, col=1)
            fig.add_hline(y=nv["stop_loss"], line_dash="solid", line_color="#ff5555", opacity=0.7,
                          row=1, col=1, annotation_text=f"SL {nv['stop_loss']:.2f}",
                          annotation_font_color="#ff5555", annotation_font_size=9)
            fig.add_hline(y=nv["tp1"], line_dash="dot", line_color="#f1fa8c", opacity=0.6,
                          row=1, col=1, annotation_text=f"TP1 {nv['tp1']:.2f}",
                          annotation_font_color="#f1fa8c", annotation_font_size=9)
            ch = ["#50fa7b" if v >= 0 else "#ff5555" for v in hg["MACD_Hist"]]
            fig.add_trace(go.Bar(x=hg.index, y=hg["MACD_Hist"], marker_color=ch, showlegend=False), row=2, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["MACD"], name="MACD",
                                      line=dict(color="#50fa7b", width=1)), row=2, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["Signal"], name="Signal",
                                      line=dict(color="#ff79c6", width=1)), row=2, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["RSI"], name="RSI",
                                      line=dict(color="#bd93f9", width=1)), row=3, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["Stoch_K"], name="%K",
                                      line=dict(color="#f1fa8c", width=1, dash="dot")), row=3, col=1)
            fig.add_hline(y=70, line_dash="dash", line_color="#ff5555", opacity=0.4, row=3, col=1)
            fig.add_hline(y=30, line_dash="dash", line_color="#50fa7b", opacity=0.4, row=3, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["ADX"], name="ADX",
                                      line=dict(color="#ffb86c", width=1.4)), row=4, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["DI_Plus"], name="DI+",
                                      line=dict(color="#50fa7b", width=0.8)), row=4, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["DI_Minus"], name="DI−",
                                      line=dict(color="#ff5555", width=0.8)), row=4, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["OBV"], name="OBV",
                                      line=dict(color="#8be9fd", width=1)), row=5, col=1)
            fig.add_trace(go.Scatter(x=hg.index, y=hg["OBV_SMA"], name="OBV SMA",
                                      line=dict(color="#ff79c6", width=1, dash="dash")), row=5, col=1)
            fig.update_layout(
                title=f"{ticker_in} | Score: {sc}/10 | {verd}",
                template="plotly_dark", paper_bgcolor="#12121f", plot_bgcolor="#1e1e2e",
                height=1000, legend=dict(orientation="h", y=-0.02, font=dict(size=9)),
                hovermode="x unified")
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"📡 Datos OHLCV: [{fuente_precios}]({'https://finnhub.io' if fuente_precios == 'Finnhub' else f'https://finance.yahoo.com/quote/{ticker_in}'}) | Indicadores: cálculo propio sobre datos de cierre")

        # ── FUNDAMENTAL ──
        if tab in ["📋 Fundamental", "🔧+📋 Completo"]:
            st.divider()
            st.subheader("📋 Análisis Fundamental")
            per = info.get("trailingPE")
            pfw = info.get("forwardPE")
            peg = info.get("pegRatio")
            pb = info.get("priceToBook")
            ps = info.get("priceToSalesTrailing12Months")
            eve = info.get("enterpriseToEbitda")
            gn = calc_graham(info)
            fy = calc_fcf_yield(info, cf)

            st.markdown("### 💰 Valoración")
            st.caption(f"📡 Múltiplos: [{fuente_metricas}]({'https://finnhub.io' if fuente_metricas == 'Finnhub' else f'https://finance.yahoo.com/quote/{ticker_in}'}) | Target: [{fuente_target}]({'https://finnhub.io' if fuente_target == 'Finnhub' else f'https://finance.yahoo.com/quote/{ticker_in}'})")
            v1, v2, v3 = st.columns(3)
            with v1:
                st.markdown(mf("PER", per, ".1f", lambda x: x < 15, lambda x: x > 30))
                st.markdown(mf("PER Fwd", pfw, ".1f", lambda x: x < 12, lambda x: x > 25))
            with v2:
                st.markdown(mf("PEG", peg, ".2f", lambda x: x < 1, lambda x: x > 2))
                st.markdown(mf("P/Book", pb, ".2f", lambda x: x < 1.5, lambda x: x > 5))
            with v3:
                st.markdown(mf("P/Ventas", ps, ".2f", lambda x: x < 2, lambda x: x > 10))
                st.markdown(mf("EV/EBITDA", eve, ".1f", lambda x: x < 10, lambda x: x > 20))
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
            roe = info.get("returnOnEquity")
            roa = info.get("returnOnAssets")
            pm = info.get("profitMargins")
            gm = info.get("grossMargins")
            dupont = calc_dupont(fin, bs)
            cagr_r, cagr_n = calc_cagr(fin)
            r1, r2, r3 = st.columns(3)
            with r1:
                st.markdown(mf("ROE", roe * 100 if roe else None, ".1f%", lambda x: x > 15, lambda x: x < 5))
                st.markdown(mf("ROA", roa * 100 if roa else None, ".1f%", lambda x: x > 8, lambda x: x < 2))
            with r2:
                st.markdown(mf("M.Bruto", gm * 100 if gm else None, ".1f%", lambda x: x > 40, lambda x: x < 20))
                st.markdown(mf("M.Neto", pm * 100 if pm else None, ".1f%", lambda x: x > 15, lambda x: x < 3))
            with r3:
                if cagr_r is not None:
                    st.markdown(f"{'✅' if cagr_r > 7 else '🟡'} **CAGR Rev:** {cagr_r:+.1f}%")
                if cagr_n is not None:
                    st.markdown(f"{'✅' if cagr_n > 7 else '🟡'} **CAGR BN:** {cagr_n:+.1f}%")
            if dupont:
                d1, d2, d3, d4 = st.columns(4)
                d1.metric("ROE DuPont", f"{dupont['ROE']:.2f}%")
                d2.metric("Margen", f"{dupont['Margen_Neto']:.2f}%")
                d3.metric("Rot.Act", f"{dupont['Rot_Activos']:.3f}x")
                d4.metric("Apalanc", f"{dupont['Apalanc']:.2f}x")

            st.markdown("### 🏥 Salud Financiera")
            st.caption(f"📡 Estados financieros: [Yahoo Finance](https://finance.yahoo.com/quote/{ticker_in}/financials) (fuente original: [SEC EDGAR](https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&company={ticker_in}&type=10-K))")
            cr = info.get("currentRatio")
            de = info.get("debtToEquity")
            s1, s2, s3 = st.columns(3)
            with s1:
                st.markdown(mf("R.Corriente", cr, ".2f", lambda x: x > 1.5, lambda x: x < 1))
            with s2:
                st.markdown(mf("D/E", de, ".1f", lambda x: x < 80, lambda x: x > 200))
            with s3:
                if not bs.empty and not fin.empty:
                    z, zz = calc_altman(info, fin, bs)
                    if z:
                        st.markdown(f"**Altman Z:** {z} → {zz}")

            fs = 0  # default si no hay datos para Piotroski

            if not fin.empty and not bs.empty and not cf.empty:
                st.markdown("### 🔢 Piotroski F-Score")
                fs, fd = calc_piotroski(fin, bs, cf)
                ifs = "🟢" if fs >= 7 else ("🟡" if fs >= 4 else "🔴")
                st.markdown(f"### {ifs} F-Score: {fs}/9")
                for c, v in fd.items():
                    if c.startswith("_"):
                        continue
                    st.markdown(f"{'✅' if v['ok'] else '❌'} {c} — `{v['val']}`")

            # Dividendos
            dy = info.get("dividendYield")
            if dy and dy > 0:
                st.markdown("### 💵 Dividendos")
                st.markdown(f"{'✅' if dy > 0.03 else '🟡'} **Yield:** {dy*100:.2f}%")
                dr = info.get("dividendRate")
                if dr:
                    st.markdown(f"**Pago anual/acción:** {dr:.2f} {moneda}")
                pay = info.get("payoutRatio")
                if pay:
                    st.markdown(f"**Payout:** {pay*100:.1f}% — {'✅ Sostenible' if pay < 0.6 else '⚠️ Elevado'}")

            # ── VALORACIÓN DCF ──
            st.markdown("### 💎 Valoración DCF (Discounted Cash Flow)")
            dcf_price, dcf_det = calc_dcf(info, cf, fin)
            if dcf_price and dcf_det:
                margen_seguridad = (dcf_price / precio - 1) * 100
                if margen_seguridad > 20:
                    dcf_ic = "🟢 INFRAVALORADO"
                elif margen_seguridad > -10:
                    dcf_ic = "🟡 PRECIO JUSTO"
                else:
                    dcf_ic = "🔴 SOBREVALORADO"

                dc1, dc2, dc3 = st.columns(3)
                dc1.metric("Precio Justo DCF", f"{dcf_price:.2f} {moneda}")
                dc2.metric("Precio Actual", f"{precio:.2f} {moneda}")
                dc3.metric("Margen de Seguridad", f"{margen_seguridad:+.1f}%")
                st.markdown(f"**{dcf_ic}** según modelo DCF")

                with st.expander("📐 Detalle del modelo DCF"):
                    st.markdown(f"""
**Inputs del modelo:**
- FCF actual: ${dcf_det['fcf_actual']/1e6:,.0f}M
- Crecimiento proyectado (5 años): {dcf_det['growth_rate']}% anual
- WACC (coste de capital): {dcf_det['wacc']}% (Beta: {dcf_det['beta']})
- Crecimiento terminal: {dcf_det['g_terminal']}%

**Valoración:**
- VP flujos proyectados: ${dcf_det['pv_fcf']}B
- VP valor terminal: ${dcf_det['pv_terminal']}B
- Enterprise Value: ${dcf_det['enterprise_value']}B
- Deuda neta: ${dcf_det['deuda']}B − Cash ${dcf_det['cash']}B
- Equity Value: ${dcf_det['equity_value']}B
- Acciones: {dcf_det['shares_m']}M → **Precio justo: {dcf_price:.2f} {moneda}**
""")
                    st.caption("📡 FCF y estados financieros: [Yahoo Finance](https://finance.yahoo.com/quote/{}/financials) (SEC EDGAR)".format(ticker_in))
                    st.caption("⚠️ Modelo simplificado con fines académicos. No constituye asesoramiento financiero.")
            else:
                st.info("No hay datos suficientes de FCF para calcular DCF.")

            # ── VEREDICTO FUNDAMENTAL ──
            st.markdown("### 🏆 Veredicto Fundamental")
            pts_v = 0
            mx_v = 0
            if per:
                mx_v += 2
                pts_v += (2 if per < 15 else 1 if per < 25 else 0)
            if roe:
                mx_v += 2
                pts_v += (2 if roe > 0.20 else 1 if roe > 0.10 else 0)
            if pm:
                mx_v += 2
                pts_v += (2 if pm > 0.15 else 1 if pm > 0.05 else 0)
            if de is not None:
                mx_v += 2
                pts_v += (2 if de < 80 else 1 if de < 150 else 0)
            if not fin.empty and not bs.empty and not cf.empty:
                mx_v += 2
                pts_v += (2 if fs >= 7 else 1 if fs >= 4 else 0)

            if mx_v > 0:
                pf = pts_v / mx_v
                if pf >= 0.75:
                    vf_txt = "🟢 FUNDAMENTALMENTE SÓLIDA"
                elif pf >= 0.45:
                    vf_txt = "🟡 FUNDAMENTALMENTE ACEPTABLE"
                else:
                    vf_txt = "🔴 FUNDAMENTALMENTE DÉBIL"
                st.markdown(f"**{vf_txt}** — Puntuación: {pts_v}/{mx_v} ({pf*100:.0f}%)")
                st.progress(pf)

                # Detalle del veredicto
                st.caption("Criterios evaluados:")
                if per:
                    ic = "✅" if per < 15 else ("🟡" if per < 25 else "🔴")
                    st.caption(f"  {ic} PER: {per:.1f}")
                if roe:
                    ic = "✅" if roe > 0.20 else ("🟡" if roe > 0.10 else "🔴")
                    st.caption(f"  {ic} ROE: {roe*100:.1f}%")
                if pm:
                    ic = "✅" if pm > 0.15 else ("🟡" if pm > 0.05 else "🔴")
                    st.caption(f"  {ic} Margen Neto: {pm*100:.1f}%")
                if de is not None:
                    ic = "✅" if de < 80 else ("🟡" if de < 150 else "🔴")
                    st.caption(f"  {ic} D/E: {de:.1f}")
                if not fin.empty and not bs.empty and not cf.empty:
                    ic = "✅" if fs >= 7 else ("🟡" if fs >= 4 else "🔴")
                    st.caption(f"  {ic} Piotroski: {fs}/9")
            else:
                st.warning("Datos insuficientes para el veredicto.")

        # ── INFORME EXPORTABLE ──
        st.divider()
        st.subheader("📄 Exportar Informe")
        report_date = datetime.now().strftime("%d/%m/%Y %H:%M")

        # Construir contenido del informe
        report_lines = []
        report_lines.append(f"{'='*60}")
        report_lines.append(f"INFORME DE ANÁLISIS: {ticker_in}")
        report_lines.append(f"Fecha: {report_date}")
        report_lines.append(f"{'='*60}")
        report_lines.append(f"")
        report_lines.append(f"EMPRESA: {nombre}")
        report_lines.append(f"Precio: {precio:.2f} {moneda}")
        report_lines.append(f"Sector: {info.get('sector', 'N/A')}")
        report_lines.append(f"MktCap: ${info.get('marketCap', 0)/1e9:.2f}B")
        report_lines.append(f"")

        if tab in ["🔧 Técnico", "🔧+📋 Completo"]:
            report_lines.append(f"{'─'*60}")
            report_lines.append(f"ANÁLISIS TÉCNICO")
            report_lines.append(f"{'─'*60}")
            report_lines.append(f"Score Técnico: {sc}/10 — {verd}")
            if "SMA_200" in hist.columns:
                last_r = hist.iloc[-1]
                report_lines.append(f"Tendencia LP (SMA200): {'Alcista' if precio > last_r.get('SMA_200', 0) else 'Bajista'}")
                report_lines.append(f"Tendencia MP (SMA50): {'Alcista' if precio > last_r.get('SMA_50', 0) else 'Bajista'}")
                report_lines.append(f"Volatilidad (ATR%): {last_r.get('ATR_PCT', 0):.2f}%")
            report_lines.append(f"")
            for ind, d in det.items():
                report_lines.append(f"  {ind:<16} {d['val']:>10}  {d['pts']:.1f}/{d['max']:.1f}  {d['msg']}")
            report_lines.append(f"")
            if nv:
                report_lines.append(f"NIVELES OPERATIVOS:")
                report_lines.append(f"  Entrada Agresiva:  {nv['entrada_agresiva']:.2f}")
                report_lines.append(f"  Entrada Óptima:    {nv['entrada_optima']:.2f}")
                report_lines.append(f"  Stop Loss:         {nv['stop_loss']:.2f} (-{nv['riesgo_pct']:.1f}%)")
                report_lines.append(f"  TP1 (R/R 2:1):     {nv['tp1']:.2f} (+{((nv['tp1']/precio-1)*100):.1f}%)")
                report_lines.append(f"  TP2 (R/R 3:1):     {nv['tp2']:.2f} (+{((nv['tp2']/precio-1)*100):.1f}%)")
                if nv.get('tp3'):
                    report_lines.append(f"  TP3 (Consenso):    {nv['tp3']:.2f} (+{((nv['tp3']/precio-1)*100):.1f}%)")
            report_lines.append(f"")

        if tab in ["📋 Fundamental", "🔧+📋 Completo"]:
            report_lines.append(f"{'─'*60}")
            report_lines.append(f"ANÁLISIS FUNDAMENTAL")
            report_lines.append(f"{'─'*60}")
            if per: report_lines.append(f"  PER: {per:.1f}")
            if pfw: report_lines.append(f"  PER Forward: {pfw:.1f}")
            if peg: report_lines.append(f"  PEG: {peg:.2f}")
            if pb: report_lines.append(f"  P/Book: {pb:.2f}")
            if eve: report_lines.append(f"  EV/EBITDA: {eve:.1f}")
            gn_val = calc_graham(info)
            if gn_val: report_lines.append(f"  Graham Number: {gn_val:.2f}")
            fy_val = calc_fcf_yield(info, cf)
            if fy_val: report_lines.append(f"  FCF Yield: {fy_val:.2f}%")
            if roe: report_lines.append(f"  ROE: {roe*100:.1f}%")
            if roa: report_lines.append(f"  ROA: {roa*100:.1f}%")
            if pm: report_lines.append(f"  Margen Neto: {pm*100:.1f}%")
            if cr: report_lines.append(f"  Ratio Corriente: {cr:.2f}")
            if de is not None: report_lines.append(f"  D/E: {de:.1f}")
            if dcf_price and dcf_price is not None:
                report_lines.append(f"  DCF Precio Justo: {dcf_price:.2f} {moneda} ({margen_seguridad:+.1f}% vs actual)")
            report_lines.append(f"")
            if not fin.empty and not bs.empty and not cf.empty:
                report_lines.append(f"  Piotroski F-Score: {fs}/9")
            report_lines.append(f"")
            if mx_v > 0:
                report_lines.append(f"  VEREDICTO: {vf_txt} ({pts_v}/{mx_v} — {pf*100:.0f}%)")
            report_lines.append(f"")

        report_lines.append(f"{'─'*60}")
        report_lines.append(f"FUENTES")
        report_lines.append(f"{'─'*60}")
        report_lines.append(f"  Precios: {fuente_precios}")
        report_lines.append(f"  Fundamentales: Yahoo Finance / Finnhub")
        report_lines.append(f"  Estados financieros: SEC EDGAR vía Yahoo Finance")
        report_lines.append(f"")
        report_lines.append(f"  Generado por: TFM Investment Intelligence App")
        report_lines.append(f"  Master IA Sector Financiero — VIU 2025/26")
        report_lines.append(f"{'='*60}")

        report_text = "\n".join(report_lines)

        st.download_button(
            "📄 Descargar Informe (.txt)",
            report_text.encode("utf-8"),
            f"informe_{ticker_in}_{datetime.now().strftime('%Y%m%d')}.txt",
            "text/plain",
            use_container_width=True
        )
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
    st.markdown("Edita la tabla con tus posiciones:")

    if "cart_df" not in st.session_state:
        st.session_state["cart_df"] = pd.DataFrame({
            "Ticker": ["AAPL", "MSFT", "GOOGL", "GLD", "TLT"],
            "Cantidad": [10, 5, 3, 20, 15],
            "Precio Compra": [150.0, 240.0, 130.0, 180.0, 95.0],
            "Divisa": ["USD"] * 5,
        })

    edited = st.data_editor(
        st.session_state["cart_df"], num_rows="dynamic", use_container_width=True,
        column_config={
            "Ticker": st.column_config.TextColumn("Ticker", required=True),
            "Cantidad": st.column_config.NumberColumn("Cantidad", min_value=0, required=True),
            "Precio Compra": st.column_config.NumberColumn("Precio Compra", min_value=0.0, format="%.2f"),
            "Divisa": st.column_config.SelectboxColumn("Divisa", options=["USD", "EUR", "CHF", "GBP", "JPY"]),
        },
        key="cart_ed")
    st.session_state["cart_df"] = edited

    if st.button("🚀 Analizar Cartera", type="primary", use_container_width=True):
        dc = edited.dropna(subset=["Ticker"]).copy()
        dc = dc[dc["Cantidad"] > 0]
        if len(dc) < 2:
            st.error("Mínimo 2 posiciones.")
            st.stop()

        precios = {}
        valores = {}
        errs = []
        with st.spinner("Descargando datos..."):
            for _, row in dc.iterrows():
                t = row["Ticker"].upper().strip()
                ht, _, src = descargar(t, periodo_c)
                if not ht.empty and len(ht) > 20:
                    precios[t] = ht["Close"]
                    valores[t] = row["Cantidad"] * ht["Close"].iloc[-1]
                else:
                    errs.append(t)
                time.sleep(0.5)
        if errs:
            st.warning(f"Sin datos: {', '.join(errs)}")
        if len(precios) < 2:
            st.error("Datos insuficientes.")
            st.stop()

        dfp = pd.DataFrame(precios).dropna()
        dfr = dfp.pct_change().dropna()
        vt = sum(valores.values())
        pesos = {t: v / vt for t, v in valores.items()}
        tv = [t for t in pesos if t in dfr.columns]
        w = np.array([pesos[t] for t in tv])
        w = w / w.sum()
        rc = dfr[tv].dot(w)

        ret_a = rc.mean() * TD * 100
        vol_a = rc.std() * np.sqrt(TD) * 100
        rfd = rf / TD
        sharpe = (rc.mean() - rfd) * TD / (rc.std() * np.sqrt(TD))
        ds = rc[rc < 0]
        vol_d = np.std(ds) * np.sqrt(TD) if len(ds) > 0 else np.nan
        sortino = ((rc.mean() - rfd) * TD) / vol_d if vol_d and vol_d > 0 else np.nan
        cum = (1 + rc).cumprod()
        pk = cum.cummax()
        dd = (cum - pk) / pk
        mdd = dd.min()
        var95 = np.percentile(rc.dropna(), 5)
        cvar95 = rc[rc <= var95].mean()

        try:
            sph, _, _ = descargar("^GSPC", periodo_c)
            spr = sph["Close"].pct_change().dropna()
            al = pd.concat([rc, spr], axis=1).dropna()
            al.columns = ["c", "s"]
            beta = np.cov(al["c"], al["s"])[0, 1] / np.var(al["s"])
        except Exception:
            beta = np.nan
            al = None

        hhi = round(np.sum(w ** 2), 4)
        cm = dfr[tv].corr()
        cv = cm.values.copy()
        np.fill_diagonal(cv, np.nan)
        corr_avg = round(np.nanmean(cv), 3)

        sigma = dfr[tv].cov().values * TD
        pv = np.sqrt(w @ sigma @ w)
        mrc_v = sigma @ w
        cr_pct = (w * mrc_v) / pv * 100
        cr_dict = dict(zip(tv, np.round(cr_pct, 2)))

        st.subheader("📊 Resumen")
        st.metric("Valor Total", f"${vt:,.2f}")
        st.caption("📡 Precios: [Finnhub](https://finnhub.io) / [Yahoo Finance](https://finance.yahoo.com) | Benchmark: S&P 500 vía [^GSPC](https://finance.yahoo.com/quote/%5EGSPC)")
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
            fig = go.Figure(data=go.Heatmap(z=cm.values, x=tv, y=tv, colorscale="RdYlGn_r",
                                             zmin=-1, zmax=1, text=np.round(cm.values, 2),
                                             texttemplate="%{text}"))
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
            fig3.add_trace(go.Scatter(x=dd.index, y=dd * 100, fill="tozeroy",
                                       fillcolor="rgba(255,85,85,0.2)",
                                       line=dict(color="#ff5555", width=1.5)))
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
                    sa = (sc2 / sc2.iloc[0] - 1) * 100
                    fig4.add_trace(go.Scatter(x=sa.index, y=sa, name="S&P 500",
                                               line=dict(color="#ffb86c", dash="dash")))
                except Exception:
                    pass
            fig4.update_layout(title="Retorno vs S&P 500", template="plotly_dark",
                               paper_bgcolor="#12121f", plot_bgcolor="#1e1e2e", height=400)
            st.plotly_chart(fig4, use_container_width=True)

        issues = []
        if hhi > 0.15:
            issues.append(f"⚠️ Concentración alta (HHI={hhi:.3f})")
        if corr_avg > 0.50:
            issues.append(f"⚠️ Correlación elevada ({corr_avg:.2f})")
        if sharpe < 0:
            issues.append(f"🔴 Sharpe negativo ({sharpe:.2f})")
        if abs(mdd) > 0.25:
            issues.append(f"🔴 Max DD severo ({mdd*100:.1f}%)")
        if issues:
            st.subheader("⚠️ Alertas")
            for i in issues:
                st.markdown(i)
        else:
            st.success("✅ Indicadores de riesgo aceptables.")


# ╔═══════════════════════════════════════════════════════════════╗
# ║  MACRO DASHBOARD (FRED)                                       ║
# ╚═══════════════════════════════════════════════════════════════╝
elif pagina == "📊 Macro":
    st.header("📊 Macro Dashboard — Datos Económicos")

    if not fred_client:
        st.error("Necesitas configurar la API key de FRED en Settings → Secrets.")
        st.stop()

    series_config = {
        "Fed Funds Rate": {"id": "FEDFUNDS", "desc": "Tipo de interés de referencia de la Fed", "unit": "%"},
        "US CPI (YoY)": {"id": "CPIAUCSL", "desc": "Índice de precios al consumo", "unit": "Index"},
        "US 10Y Treasury": {"id": "DGS10", "desc": "Rendimiento bono US a 10 años", "unit": "%"},
        "US 2Y Treasury": {"id": "DGS2", "desc": "Rendimiento bono US a 2 años", "unit": "%"},
        "Desempleo US": {"id": "UNRATE", "desc": "Tasa de desempleo Estados Unidos", "unit": "%"},
        "VIX": {"id": "VIXCLS", "desc": "Índice de volatilidad del mercado (miedo)", "unit": "Pts"},
        "DXY (Dollar Index)": {"id": "DTWEXBGS", "desc": "Fortaleza del dólar vs cesta de divisas", "unit": "Index"},
        "US GDP": {"id": "GDP", "desc": "Producto Interior Bruto de Estados Unidos", "unit": "$B"},
    }

    with st.sidebar:
        start_date = st.date_input("Desde", value=datetime(2020, 1, 1))
        selected = st.multiselect("Series", list(series_config.keys()), default=["Fed Funds Rate", "US 10Y Treasury", "VIX", "Desempleo US"])

    if not selected:
        st.info("Selecciona al menos una serie en la barra lateral.")
    else:
        for name in selected:
            cfg = series_config[name]
            try:
                data = fred_client.get_series(cfg["id"], observation_start=start_date.strftime("%Y-%m-%d"))
                if not data.empty:
                    data = data.dropna()
                    last_val = data.iloc[-1]
                    prev_val = data.iloc[-2] if len(data) > 1 else last_val

                    st.subheader(f"{name}")
                    st.caption(f"{cfg['desc']} — 📡 [{cfg['id']}](https://fred.stlouisfed.org/series/{cfg['id']})")

                    m1, m2, m3 = st.columns([1, 1, 2])
                    with m1:
                        st.metric("Último", f"{last_val:.2f} {cfg['unit']}", f"{last_val - prev_val:+.2f}")
                    with m2:
                        st.metric("Mín período", f"{data.min():.2f}")
                        st.metric("Máx período", f"{data.max():.2f}")

                    fig = go.Figure()
                    fig.add_trace(go.Scatter(x=data.index, y=data, name=name,
                                              line=dict(color="#8be9fd", width=2),
                                              fill="tozeroy", fillcolor="rgba(139,233,253,0.05)"))
                    fig.update_layout(template="plotly_dark", paper_bgcolor="#12121f",
                                      plot_bgcolor="#1e1e2e", height=300,
                                      yaxis_title=cfg["unit"], margin=dict(t=10, b=30))
                    st.plotly_chart(fig, use_container_width=True)
                    st.divider()
            except Exception as e:
                st.warning(f"Error cargando {name}: {e}")


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
            # Recopilar contexto de datos reales
            contexto_parts = []

            # Detectar tickers mencionados (palabras en mayúsculas de 1-5 letras)
            import re
            posibles_tickers = re.findall(r'\b[A-Z]{1,5}\b', pregunta.upper())
            tickers_validos = [t for t in posibles_tickers if t not in ["QUE", "LOS", "LAS", "DEL", "POR", "CON", "UNA", "COMO", "PARA", "MAS", "SER", "HAY", "SON", "EST", "THE", "AND", "FOR"]]

            for t in tickers_validos[:3]:
                h, info_r, _ = descargar(t, "6mo")
                if not h.empty:
                    precio_r = h["Close"].iloc[-1]
                    chg_1m = ((precio_r / h["Close"].iloc[-22] - 1) * 100) if len(h) > 22 else 0
                    contexto_parts.append(
                        f"**{t}**: Precio {precio_r:.2f}, cambio 1M: {chg_1m:+.1f}%, "
                        f"PE: {info_r.get('trailingPE', 'N/A')}, "
                        f"Sector: {info_r.get('sector', 'N/A')}, "
                        f"Target analistas: {info_r.get('targetMeanPrice', 'N/A')}, "
                        f"Recomendación: {info_r.get('recommendationKey', 'N/A')}"
                    )

            # Noticias relevantes de Finnhub
            noticias_ctx = []
            if fh_client and tickers_validos:
                for t in tickers_validos[:2]:
                    try:
                        today = datetime.now().strftime("%Y-%m-%d")
                        week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
                        news = fh_client.company_news(t, _from=week_ago, to=today)
                        if news:
                            for n in news[:3]:
                                noticias_ctx.append(f"- [{n.get('source', '')}] {n.get('headline', '')}")
                    except Exception:
                        pass

            # Datos macro de FRED
            macro_ctx = []
            if fred_client:
                for name, sid in [("Fed Funds Rate", "FEDFUNDS"), ("US 10Y", "DGS10"), ("VIX", "VIXCLS")]:
                    try:
                        s = fred_client.get_series(sid, observation_start="2024-01-01").dropna()
                        if not s.empty:
                            macro_ctx.append(f"{name}: {s.iloc[-1]:.2f}")
                    except Exception:
                        pass

            # Construir contexto
            contexto = "DATOS DE MERCADO REALES (hoy):\n"
            if contexto_parts:
                contexto += "\n".join(contexto_parts) + "\n"
            if macro_ctx:
                contexto += "\nMACRO: " + " | ".join(macro_ctx) + "\n"
            if noticias_ctx:
                contexto += "\nNOTICIAS RECIENTES:\n" + "\n".join(noticias_ctx[:6]) + "\n"

            # Llamar a Claude API
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
                    },
                    timeout=30
                )
                if response.status_code == 200:
                    data = response.json()
                    respuesta = ""
                    for block in data.get("content", []):
                        if block.get("type") == "text":
                            respuesta += block["text"]

                    st.markdown("---")
                    st.subheader("📋 Informe de Research")
                    st.markdown(respuesta)

                    # Fuentes utilizadas
                    st.markdown("---")
                    st.caption("📡 **Fuentes del análisis:**")
                    st.caption("• Precios y métricas: [Finnhub](https://finnhub.io) / [Yahoo Finance](https://finance.yahoo.com)")
                    if noticias_ctx:
                        st.caption("• Noticias: [Finnhub News API](https://finnhub.io)")
                    if macro_ctx:
                        st.caption("• Datos macro: [FRED — Federal Reserve Economic Data](https://fred.stlouisfed.org)")
                    st.caption("• Análisis generado por: [Claude API — Anthropic](https://anthropic.com)")

                    # Mostrar datos usados
                    with st.expander("📊 Datos brutos utilizados"):
                        st.text(contexto)
                else:
                    st.error(f"Error API: {response.status_code}")
                    st.info("El Research Assistant necesita acceso a la API de Claude. Los datos de mercado sí se han recopilado:")
                    st.markdown(contexto)
                    st.caption("📡 Datos: [Finnhub](https://finnhub.io) | [Yahoo Finance](https://finance.yahoo.com) | [FRED](https://fred.stlouisfed.org)")
            except Exception as e:
                st.warning(f"No se pudo conectar con Claude API: {e}")
                st.info("Datos recopilados para tu investigación:")
                st.markdown(contexto)
                st.caption("📡 Datos: [Finnhub](https://finnhub.io) | [Yahoo Finance](https://finance.yahoo.com) | [FRED](https://fred.stlouisfed.org)")

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
        """)
