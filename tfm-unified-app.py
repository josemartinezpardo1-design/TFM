"""
TFM — PLATAFORMA DE INVERSIÓN INTELIGENTE
Master IA para el Sector Financiero — VIU 2025/26

Módulos:
  1. Screener: filtrado multi-índice con señales compuestas y score 0–10
  2. Análisis Individual: técnico (7 indicadores) + fundamental (6 bloques)

Ejecutar: streamlit run app.py
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
import warnings
warnings.filterwarnings("ignore")

st.set_page_config(
    page_title="TFM — Investment Intelligence",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ── Caché global para yfinance ────────────────────────────────
@st.cache_data(ttl=1800, show_spinner=False)
def descargar_ticker(ticker, period="2y"):
    """Descarga datos de yfinance con caché de 30 min. No cachea fallos."""
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period=period)
        info = stock.info or {}
        # Si no hay datos, lanzar excepción para que NO se cachee
        if hist.empty or len(info) < 5:
            raise ValueError(f"Sin datos para {ticker}")
        try: fin = stock.financials
        except: fin = pd.DataFrame()
        try: bs = stock.balance_sheet
        except: bs = pd.DataFrame()
        try: cf = stock.cashflow
        except: cf = pd.DataFrame()
        return hist, info, fin, bs, cf
    except Exception as e:
        # st.cache_data NO cachea si hay excepción → reintentará la próxima vez
        raise e


def descargar_ticker_safe(ticker, period="2y"):
    """Wrapper que captura errores y devuelve vacíos sin cachear el fallo."""
    try:
        return descargar_ticker(ticker, period)
    except:
        return pd.DataFrame(), {}, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


# ╔═══════════════════════════════════════════════════════════════╗
# ║  FUNCIONES COMPARTIDAS                                       ║
# ╚═══════════════════════════════════════════════════════════════╝

# ── Indicadores técnicos ──────────────────────────────────────
def calc_rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calc_macd(series, fast=12, slow=26, signal=9):
    ema_f = series.ewm(span=fast, adjust=False).mean()
    ema_s = series.ewm(span=slow, adjust=False).mean()
    macd = ema_f - ema_s
    sig = macd.ewm(span=signal, adjust=False).mean()
    return macd, sig, macd - sig

def calc_adx(df, period=14):
    high, low, close = df["High"], df["Low"], df["Close"]
    tr = pd.concat([high-low, (high-close.shift(1)).abs(), (low-close.shift(1)).abs()], axis=1).max(axis=1)
    up = high - high.shift(1)
    dn = low.shift(1) - low
    plus_dm = pd.Series(np.where((up > dn) & (up > 0), up, 0), index=df.index)
    minus_dm = pd.Series(np.where((dn > up) & (dn > 0), dn, 0), index=df.index)
    atr = tr.rolling(period).mean()
    plus_di = 100 * (plus_dm.rolling(period).mean() / atr)
    minus_di = 100 * (minus_dm.rolling(period).mean() / atr)
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    return dx.rolling(period).mean(), plus_di, minus_di

def calc_obv(df):
    # Vectorizado — sin for loop
    direction = np.sign(df["Close"].diff())
    obv = (direction * df["Volume"]).fillna(0).cumsum()
    obv_sma = obv.rolling(20).mean()
    obv_trend = pd.Series(np.where(obv > obv_sma, 1, -1), index=df.index)
    p20 = df["Close"].pct_change(20); o20 = obv.pct_change(20)
    div = pd.Series(np.where((p20<0)&(o20>0), "Alcista", np.where((p20>0)&(o20<0), "Bajista", "Neutral")), index=df.index)
    return obv, obv_sma, obv_trend, div

def calc_stochastic(df, k_period=14, d_period=3):
    low_min = df["Low"].rolling(k_period).min()
    high_max = df["High"].rolling(k_period).max()
    k = 100 * (df["Close"] - low_min) / (high_max - low_min).replace(0, np.nan)
    return k, k.rolling(d_period).mean()

def calc_bollinger(series, period=20, std=2):
    mid = series.rolling(period).mean()
    sigma = series.rolling(period).std()
    upper = mid + std * sigma
    lower = mid - std * sigma
    pct_b = (series - lower) / (upper - lower).replace(0, np.nan)
    return upper, mid, lower, pct_b

def calc_atr(df, period=14):
    tr = pd.concat([df["High"]-df["Low"], (df["High"]-df["Close"].shift(1)).abs(), (df["Low"]-df["Close"].shift(1)).abs()], axis=1).max(axis=1)
    atr = tr.rolling(period).mean()
    return atr, atr / df["Close"] * 100

# ── Fundamental helpers ───────────────────────────────────────
def _safe(df, key, col=0, default=0):
    try:
        val = df.loc[key].iloc[col] if key in df.index else default
        return val if not pd.isna(val) else default
    except: return default

def calcular_piotroski(fin, bs, cf):
    score = 0; detalle = {}
    try:
        ta0=_safe(bs,"Total Assets",0,1); ta1=_safe(bs,"Total Assets",1,1)
        ni0=_safe(fin,"Net Income",0); ni1=_safe(fin,"Net Income",1)
        cfo=_safe(cf,"Operating Cash Flow",0)
        ca0=_safe(bs,"Current Assets",0); cl0=_safe(bs,"Current Liabilities",0) or 1
        ca1=_safe(bs,"Current Assets",1); cl1=_safe(bs,"Current Liabilities",1) or 1
        ltd0=_safe(bs,"Long Term Debt",0); ltd1=_safe(bs,"Long Term Debt",1)
        rev0=_safe(fin,"Total Revenue",0) or 1; rev1=_safe(fin,"Total Revenue",1) or 1
        gp0=_safe(fin,"Gross Profit",0); gp1=_safe(fin,"Gross Profit",1)
        roa0=ni0/ta0; roa1=ni1/ta1; cr0=ca0/cl0; cr1=ca1/cl1
        lev0=ltd0/ta0; lev1=ltd1/ta1; gm0=gp0/rev0; gm1=gp1/rev1; at0=rev0/ta0; at1=rev1/ta1
        tests = [
            ("F1 ROA positivo", roa0>0, f"{roa0*100:.2f}%"),
            ("F2 CFO positivo", cfo>0, f"${cfo/1e6:.0f}M"),
            ("F3 ROA mejora vs año anterior", roa0>roa1, f"{roa0*100:.2f}% vs {roa1*100:.2f}%"),
            ("F4 Calidad (CFO > Net Income)", cfo>ni0, f"CFO {cfo/1e6:.0f}M > NI {ni0/1e6:.0f}M"),
            ("F5 Menor apalancamiento", lev0<lev1, f"{lev0:.3f} vs {lev1:.3f}"),
            ("F6 Mejor liquidez corriente", cr0>cr1, f"{cr0:.2f} vs {cr1:.2f}"),
            ("F7 Sin dilución accionistas", True, "(manual)"),
            ("F8 Margen bruto creciente", gm0>gm1, f"{gm0*100:.1f}% vs {gm1*100:.1f}%"),
            ("F9 Rotación activos creciente", at0>at1, f"{at0:.3f} vs {at1:.3f}"),
        ]
        for n,c,v in tests:
            p = 1 if c else 0; score += p
            detalle[n] = {"ok": bool(c), "val": v}
    except Exception as e:
        detalle["_error"] = {"ok": False, "val": str(e)}
    return score, detalle

def calcular_altman_z(info, fin, bs):
    try:
        ta=_safe(bs,"Total Assets",0,1); ca=_safe(bs,"Current Assets",0); cl=_safe(bs,"Current Liabilities",0)
        re=_safe(bs,"Retained Earnings",0); ebit=_safe(fin,"EBIT",0) or _safe(fin,"Operating Income",0)
        tl=_safe(bs,"Total Liabilities Net Minority Interest",0) or _safe(bs,"Total Debt",0) or 1
        rev=_safe(fin,"Total Revenue",0); mc=info.get("marketCap",0)
        z=1.2*((ca-cl)/ta)+1.4*(re/ta)+3.3*(ebit/ta)+0.6*(mc/tl)+1.0*(rev/ta)
        zona = "🟢 ZONA SEGURA" if z>2.99 else ("🟡 ZONA GRIS" if z>1.81 else "🔴 ZONA PELIGRO")
        return round(z,2), zona
    except: return None, "Sin datos"

def calcular_roic(fin, bs):
    try:
        ebit=_safe(fin,"EBIT",0) or _safe(fin,"Operating Income",0)
        ic=_safe(bs,"Total Assets",0,1)-_safe(bs,"Current Liabilities",0)
        if ic<=0: return None
        return round(ebit*(1-0.21)/ic*100, 2)
    except: return None

def calcular_graham_number(info):
    try:
        eps=info.get("trailingEps") or info.get("forwardEps"); bvps=info.get("bookValue")
        if eps and bvps and eps>0 and bvps>0: return round((22.5*eps*bvps)**0.5, 2)
    except: pass
    return None

def calcular_fcf_yield(info, cf):
    try:
        fcf=info.get("freeCashflow")
        if not fcf: fcf=_safe(cf,"Operating Cash Flow",0)-abs(_safe(cf,"Capital Expenditure",0))
        mc=info.get("marketCap")
        if fcf and mc and mc>0: return round(fcf/mc*100, 2)
    except: pass
    return None

def calcular_dupont(fin, bs):
    try:
        ni=_safe(fin,"Net Income",0); rev=_safe(fin,"Total Revenue",0) or 1
        ta=_safe(bs,"Total Assets",0) or 1; eq=_safe(bs,"Stockholders Equity",0) or 1
        nm=ni/rev; at=rev/ta; lv=ta/eq
        return {"ROE":round(nm*at*lv*100,2),"Margen_Neto":round(nm*100,2),"Rot_Activos":round(at,3),"Apalanc":round(lv,2)}
    except: return None

def calcular_cagr_historico(fin):
    try:
        if fin.empty or len(fin.columns)<2: return None, None
        def cagr(s):
            v=s.dropna();
            if len(v)<2: return None
            vi=v.iloc[-1]; vf=v.iloc[0]; n=len(v)-1
            if vi<=0 or vf<=0: return None
            return round(((vf/vi)**(1/n)-1)*100, 1)
        rv=fin.loc["Total Revenue"] if "Total Revenue" in fin.index else None
        ni=fin.loc["Net Income"] if "Net Income" in fin.index else None
        return (cagr(rv) if rv is not None else None, cagr(ni) if ni is not None else None)
    except: return None, None

def metric_fund(nombre, val, fmt, bueno, malo):
    if val is None: return f"**{nombre}:** N/A"
    ic = "✅" if bueno(val) else ("🔴" if malo(val) else "🟡")
    # Separar sufijo (%) del formato
    if fmt.endswith("%"):
        fmt_clean = fmt[:-1]
        return f"{ic} **{nombre}:** {val:{fmt_clean}}%"
    return f"{ic} **{nombre}:** {val:{fmt}}"


# ╔═══════════════════════════════════════════════════════════════╗
# ║  SCREENER — FUNCIONES ESPECÍFICAS                            ║
# ╚═══════════════════════════════════════════════════════════════╝

@st.cache_data(ttl=86400)
def obtener_sp500_tickers():
    try:
        t = pd.read_html(requests.get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", headers={"User-Agent":"Mozilla/5.0"}).text)
        return [x.replace(".","-") for x in t[0]["Symbol"].tolist()]
    except: return []

@st.cache_data(ttl=86400)
def obtener_sp600_tickers():
    try:
        t = pd.read_html(requests.get("https://en.wikipedia.org/wiki/List_of_S%26P_600_companies", headers={"User-Agent":"Mozilla/5.0"}).text)
        df = t[0] if "Symbol" in t[0].columns else t[1]
        col = "Symbol" if "Symbol" in df.columns else "Ticker symbol"
        return [x.replace(".","-") for x in df[col].tolist()]
    except: return []

@st.cache_data(ttl=86400)
def obtener_ibex35_tickers():
    return ["SAN.MC","BBVA.MC","ITX.MC","IBE.MC","TEF.MC","FER.MC","AMS.MC","REP.MC","CABK.MC","ACS.MC",
            "GRF.MC","MAP.MC","ENG.MC","RED.MC","IAG.MC","FDR.MC","MEL.MC","COL.MC","CLNX.MC","SAB.MC",
            "BKT.MC","AENA.MC","LOG.MC","CIE.MC","ACX.MC","MRL.MC","PHM.MC","ROVI.MC","VIS.MC","ALM.MC",
            "SGRE.MC","SLR.MC","UNI.MC","EDR.MC","SOL.MC"]

@st.cache_data(ttl=86400)
def obtener_dax40_tickers():
    return ["SAP.DE","SIE.DE","ALV.DE","DTE.DE","AIR.DE","MBG.DE","MUV2.DE","DHL.DE","BAS.DE","BMW.DE",
            "IFX.DE","BEI.DE","BAYN.DE","HEN3.DE","ADS.DE","VOW3.DE","DB1.DE","SY1.DE","RWE.DE","FRE.DE",
            "CON.DE","MTX.DE","ENR.DE","HNR1.DE","PAH3.DE","ZAL.DE","HEI.DE","QIA.DE","MRK.DE","SHL.DE",
            "DTG.DE","DBK.DE","P911.DE","1COV.DE","BNR.DE","LHA.DE","PUM.DE","TKA.DE","LEG.DE","VNA.DE"]

@st.cache_data(ttl=86400)
def obtener_ftse100_tickers():
    return ["AZN.L","SHEL.L","HSBA.L","ULVR.L","BP.L","GSK.L","RIO.L","REL.L","DGE.L","LSEG.L",
            "NG.L","BA.L","GLEN.L","VOD.L","PRU.L","CPG.L","AAL.L","LLOY.L","BARC.L","BATS.L",
            "AHT.L","BKG.L","CRH.L","EXPN.L","IMB.L","NWG.L","RKT.L","SSE.L","TSCO.L","WPP.L"]

INDICES = {
    "SP500":("S&P 500",obtener_sp500_tickers), "SP600":("S&P 600 SmallCap",obtener_sp600_tickers),
    "IBEX35":("IBEX 35",obtener_ibex35_tickers), "DAX40":("DAX 40",obtener_dax40_tickers),
    "FTSE100":("FTSE 100",obtener_ftse100_tickers),
}

def calcular_candle(row):
    try:
        op,hi,lo,cl = row["Open"],row["High"],row["Low"],row["Close"]
        rng=hi-lo
        if rng==0: return "Doji"
        cuerpo=abs(cl-op); si=min(op,cl)-lo; ss=hi-max(op,cl); pc=cuerpo/rng
        if pc<0.15: return "Doji"
        if si>cuerpo*2 and ss<cuerpo*0.5: return "Hammer"
        if ss>cuerpo*2 and si<cuerpo*0.5: return "ShootingStar"
        if cl>op and pc>0.6: return "Marubozu+"
        if cl<op and pc>0.6: return "Marubozu-"
        return "Alcista" if cl>op else "Bajista"
    except: return "N/A"

def calcular_chaikin_ad(hist):
    try:
        rng=(hist["High"]-hist["Low"]).replace(0, np.nan)
        mfm=((hist["Close"]-hist["Low"])-(hist["High"]-hist["Close"]))/rng
        ad=(mfm*hist["Volume"]).cumsum()
        return ad, ad.rolling(20).mean()
    except: return pd.Series(dtype=float), pd.Series(dtype=float)

def _score_compuesto_screener(row):
    s = 0.0
    mom3=row.get("Mom 3M %",np.nan)
    if pd.notna(mom3) and mom3>20: s+=1.0
    elif pd.notna(mom3) and mom3>10: s+=0.7
    elif pd.notna(mom3) and mom3>0: s+=0.4
    if pd.notna(row.get("vs SMA50 %")) and row["vs SMA50 %"]>0: s+=0.5
    if pd.notna(row.get("vs SMA200 %")) and row["vs SMA200 %"]>0: s+=0.5
    vr=row.get("Vol/Avg 20d",np.nan)
    if pd.notna(vr) and vr>2.0: s+=1.0
    elif pd.notna(vr) and vr>1.3: s+=0.5
    vt=row.get("Vol Tend 20/50",np.nan)
    if pd.notna(vt) and vt>1.0: s+=0.5
    per=row.get("PER",np.nan)
    if pd.notna(per) and 0<per<12: s+=1.5
    elif pd.notna(per) and 0<per<20: s+=1.0
    elif pd.notna(per) and 0<per<30: s+=0.3
    roe=row.get("ROE %",np.nan)
    if pd.notna(roe) and roe>25: s+=1.0
    elif pd.notna(roe) and roe>15: s+=0.7
    elif pd.notna(roe) and roe>8: s+=0.3
    mg=row.get("Margen Net %",np.nan)
    if pd.notna(mg) and mg>20: s+=1.0
    elif pd.notna(mg) and mg>10: s+=0.7
    elif pd.notna(mg) and mg>3: s+=0.3
    de=row.get("D/E",np.nan)
    if pd.notna(de) and de<50: s+=0.5
    elif pd.notna(de) and de<100: s+=0.3
    if row.get("Breakout 20d")=="SI": s+=0.75
    if row.get("Near 52W High")=="SI": s+=0.5
    sig=str(row.get("Señales",""))
    if "BREAKOUT" in sig: s+=0.75
    if "VOL_CONFIRM" in sig: s+=0.5
    if "ACUMULACION" in sig: s+=0.5
    if "TRAMPA_ALC" in sig: s-=0.5
    if "CAPITULACION" in sig: s-=0.3
    pot=row.get("Potencial %",np.nan)
    if pd.notna(pot) and pot>20: s+=0.5
    elif pd.notna(pot) and pot>10: s+=0.3
    return round(min(max(s,0.0),10.0),1)

def _label_score(score):
    if score>=7.5: return "COMPRAR"
    elif score>=6.0: return "VIGILAR"
    elif score>=4.0: return "NEUTRO"
    else: return "EVITAR"

def analizar_ticker_screener(ticker):
    try:
        hist, info, _, _, _ = descargar_ticker_safe(ticker, "1y")
        if hist.empty or len(hist)<60 or len(info)<5: return None
        last=hist.iloc[-1]; prev=hist.iloc[-2] if len(hist)>1 else last
        precio=last["Close"]
        if precio<=0: return None
        mktcap_b=round(info.get("marketCap",0)/1e9,2)
        mom_1m=((precio/hist["Close"].iloc[-21]-1)*100) if len(hist)>21 else np.nan
        mom_3m=((precio/hist["Close"].iloc[-63]-1)*100) if len(hist)>63 else np.nan
        sma50=hist["Close"].rolling(50).mean().iloc[-1] if len(hist)>=50 else np.nan
        sma200=hist["Close"].rolling(200).mean().iloc[-1] if len(hist)>=200 else np.nan
        vs50=round((precio/sma50-1)*100,2) if pd.notna(sma50) else np.nan
        vs200=round((precio/sma200-1)*100,2) if pd.notna(sma200) else np.nan
        mx52=hist["High"].max(); mn52=hist["Low"].min()
        dmx=round((precio/mx52-1)*100,2); dmn=round((precio/mn52-1)*100,2)
        mx20=hist["High"].iloc[-21:-1].max() if len(hist)>21 else np.nan
        bk20="SI" if pd.notna(mx20) and precio>mx20 else "NO"
        n52="SI" if abs(dmx)<3 else "NO"
        gap=round((last["Open"]/prev["Close"]-1)*100,2) if prev["Close"]>0 else 0
        rng=round((last["High"]-last["Low"])/precio*100,2)
        cambios=hist["Close"].diff(); dias_c=0
        for v in cambios.iloc[::-1]:
            if pd.isna(v): break
            if v>0 and dias_c>=0: dias_c=dias_c+1 if dias_c>0 else 1
            elif v<0 and dias_c<=0: dias_c=dias_c-1 if dias_c<0 else -1
            else: break
        vela=calcular_candle(last)
        vh=last["Volume"]; va20=hist["Volume"].rolling(20).mean().iloc[-1]; va50=hist["Volume"].rolling(50).mean().iloc[-1]
        vr20=round(vh/va20,2) if va20>0 else np.nan; vr50=round(va20/va50,2) if va50>0 else np.nan
        v5=hist["Volume"].iloc[-5:].mean(); v20=hist["Volume"].iloc[-20:].mean()
        if v20>0:
            r=v5/v20; vt="Creciente" if r>1.15 else ("Decreciente" if r<0.85 else "Estable")
        else: vt="N/A"
        ad,ads=calcular_chaikin_ad(hist)
        al="N/A"
        if not ad.empty and not ads.empty:
            a1=ad.iloc[-1]; a2=ads.iloc[-1]
            if pd.notna(a1) and pd.notna(a2): al="Acumulación" if a1>a2 else "Distribución"
        señ=[]; rd=(precio/prev["Close"]-1) if prev["Close"]>0 else 0
        if rd>0 and pd.notna(vr20) and vr20>1.5: señ.append("VOL_CONFIRM")
        if rd>0 and pd.notna(vr20) and vr20<0.7: señ.append("TRAMPA_ALC")
        if bk20=="SI" and pd.notna(vr20) and vr20>1.5: señ.append("BREAKOUT")
        if n52=="SI": señ.append("NEAR_52W_HI")
        if pd.notna(dmn) and dmn<30 and vt=="Creciente" and al=="Acumulación": señ.append("ACUMULACION")
        if rd<-0.03 and pd.notna(vr20) and vr20>2.5: señ.append("CAPITULACION")
        rsi14=None
        try:
            d=hist["Close"].diff(); g=d.where(d>0,0).rolling(14).mean(); l=(-d.where(d<0,0)).rolling(14).mean()
            rsi14=(100-(100/(1+g/l))).iloc[-1]
        except: pass
        if vela=="Hammer" and rsi14 is not None and rsi14<35: señ.append("HAMMER_GIRO")
        if pd.notna(mom_3m) and mom_3m>15 and vt=="Creciente": señ.append("MOMENTUM")
        señ_s=" | ".join(señ) if señ else "-"
        per=info.get("trailingPE"); pfw=info.get("forwardPE"); peg=info.get("pegRatio")
        roe=info.get("returnOnEquity"); mn=info.get("profitMargins"); mb=info.get("grossMargins")
        de=info.get("debtToEquity"); dy=info.get("dividendYield"); rec=info.get("recommendationKey","N/A")
        tm=info.get("targetMeanPrice"); pot=round((tm/precio-1)*100,1) if tm and precio>0 else np.nan
        fy=np.nan
        try:
            f=info.get("freeCashflow"); mc=info.get("marketCap")
            if f and mc and mc>0: fy=round(f/mc*100,2)
        except: pass
        r={"Ticker":ticker,"Precio":round(precio,2),"MktCap (B$)":mktcap_b,
           "Mom 1M %":round(mom_1m,2) if pd.notna(mom_1m) else np.nan,
           "Mom 3M %":round(mom_3m,2) if pd.notna(mom_3m) else np.nan,
           "vs SMA50 %":vs50,"vs SMA200 %":vs200,"Dist Max52W %":dmx,"Dist Min52W %":dmn,
           "Breakout 20d":bk20,"Near 52W High":n52,"Gap %":gap,"Rango Intra %":rng,
           "Días Consec":dias_c,"Vela":vela,"Vol/Avg 20d":vr20,"Vol Tend 20/50":vr50,
           "Vol Tendencia":vt,"Acum/Distrib":al,"Señales":señ_s,
           "PER":round(per,1) if per else np.nan,"PER Fwd":round(pfw,1) if pfw else np.nan,
           "PEG":round(peg,2) if peg else np.nan,"ROE %":round(roe*100,1) if roe else np.nan,
           "Margen Net %":round(mn*100,1) if mn else np.nan,"Margen Bruto %":round(mb*100,1) if mb else np.nan,
           "D/E":round(de,1) if de else np.nan,"FCF Yield %":fy,
           "Div Yield %":round(dy*100,2) if dy else 0,"Potencial %":pot,"Consenso":rec}
        r["Score"]=_score_compuesto_screener(r); r["Label"]=_label_score(r["Score"])
        return r
    except: return None

def aplicar_filtros(df, modo):
    d=df.copy()
    if modo=="VALUE": mask=d["PER"].between(0,20)&(d["Margen Net %"]>8)&(d["D/E"]<150); lb="VALUE — Baratas con buenos márgenes"
    elif modo=="MOMENTUM": mask=(d["Mom 3M %"]>10)&(d["Vol Tend 20/50"]>1.0)&(d["vs SMA50 %"]>0); lb="MOMENTUM — Tendencia fuerte"
    elif modo=="BREAKOUT": mask=(d["Breakout 20d"]=="SI")&(d["Vol/Avg 20d"]>1.3); lb="BREAKOUT — Rupturas con volumen"
    elif modo=="ACUMULACION": mask=(d["Dist Min52W %"]<30)&d["Acum/Distrib"].str.contains("Acum",na=False)&d["Vol Tendencia"].str.contains("Crec",na=False); lb="ACUMULACIÓN — Zona baja"
    elif modo=="DIVIDENDOS": mask=(d["Div Yield %"]>2.5)&(d["Margen Net %"]>5)&(d["D/E"]<200); lb="DIVIDENDOS — Yield estable"
    elif modo=="QUALITY": mask=(d["ROE %"]>15)&(d["Margen Net %"]>12)&(d["Mom 3M %"]>0); lb="QUALITY — Alta rentabilidad"
    else: mask=pd.Series([True]*len(d),index=d.index); lb="TODOS"
    return d[mask].copy(), lb

COLORES_LABEL={"COMPRAR":"#50fa7b","VIGILAR":"#8be9fd","NEUTRO":"#f1fa8c","EVITAR":"#ff5555"}
COLORES_SEÑAL={"BREAKOUT":"#50fa7b","VOL_CONFIRM":"#8be9fd","ACUMULACION":"#bd93f9","MOMENTUM":"#ffb86c","NEAR_52W_HI":"#f1fa8c","HAMMER_GIRO":"#ff79c6","CAPITULACION":"#ff5555","TRAMPA_ALC":"#6272a4"}


# ╔═══════════════════════════════════════════════════════════════╗
# ║  SCORING TÉCNICO INDIVIDUAL (0–10)                           ║
# ╚═══════════════════════════════════════════════════════════════╝
def calcular_score_tecnico(row, obv_trend_val, obv_div):
    score=0.0; det={}
    rsi=row["RSI"]
    if rsi<30: p=2.5;m=f"Sobrevendido ({rsi:.0f})"
    elif rsi<45: p=2.2;m=f"Zona acumulación ({rsi:.0f})"
    elif rsi<55: p=2.0;m=f"Neutral ({rsi:.0f})"
    elif rsi<65: p=1.5;m=f"Momentum positivo ({rsi:.0f})"
    elif rsi<75: p=0.5;m=f"Zona caliente ({rsi:.0f})"
    else: p=0.0;m=f"Sobrecomprado ({rsi:.0f})"
    score+=p; det["RSI"]={"pts":p,"max":2.5,"val":f"{rsi:.1f}","msg":m}

    mv=row["MACD"];sv=row["Signal"];hv=row["MACD_Hist"];ph=row.get("MACD_Hist_prev",0) or 0
    al=mv>sv; ac=hv>ph
    if al and ac: p=2.0;m="Cruce alcista acelerando"
    elif al: p=1.5;m="Por encima de señal"
    elif not al and ac: p=0.7;m="Bajista perdiendo fuerza"
    else: p=0.0;m="Bajista acelerando"
    score+=p; det["MACD"]={"pts":p,"max":2.0,"val":f"{mv:.4f}","msg":m}

    av=row["ADX"];dp=row["DI_Plus"];dm=row["DI_Minus"]
    if av>30 and dp>dm: p=2.0;m=f"Alcista FUERTE (ADX={av:.0f})"
    elif av>20 and dp>dm: p=1.5;m=f"Alcista moderada (ADX={av:.0f})"
    elif av>20 and dp<dm: p=0.3;m=f"Bajista activa (ADX={av:.0f})"
    elif av<20: p=1.0;m=f"Lateralización (ADX={av:.0f})"
    else: p=0.7;m=f"Tendencia débil (ADX={av:.0f})"
    score+=p; det["ADX"]={"pts":p,"max":2.0,"val":f"{av:.1f}","msg":m}

    if obv_trend_val==1 and obv_div=="Alcista": p=1.5;m="Compradora + div. alcista"
    elif obv_trend_val==1: p=1.2;m="Compradora (OBV>SMA20)"
    elif obv_trend_val==-1 and obv_div=="Bajista": p=0.0;m="Vendedora + div. bajista"
    else: p=0.3;m="Vendedora (OBV<SMA20)"
    score+=p; det["OBV"]={"pts":p,"max":1.5,"val":"Alcista" if obv_trend_val==1 else "Bajista","msg":m}

    kv=row["Stoch_K"];dv=row["Stoch_D"]
    if kv<20 and kv>dv: p=1.0;m=f"Sobrevendido+cruce (%K={kv:.0f})"
    elif kv<25: p=0.8;m=f"Sobrevendida (%K={kv:.0f})"
    elif kv>80: p=0.0;m=f"Sobrecomprada (%K={kv:.0f})"
    elif kv>dv: p=0.6;m=f"Momentum+ (%K={kv:.0f}>%D)"
    else: p=0.2;m=f"Momentum- (%K={kv:.0f}<%D)"
    score+=p; det["Stochastic"]={"pts":p,"max":1.0,"val":f"{kv:.1f}","msg":m}

    bb=row["BB_PctB"]
    if bb<0: p=1.0;m=f"Debajo banda inf (%B={bb:.2f})"
    elif bb<0.35: p=1.0;m=f"Zona inferior (%B={bb:.2f})"
    elif bb<0.65: p=0.5;m=f"Zona media (%B={bb:.2f})"
    elif bb<1.0: p=0.1;m=f"Zona superior (%B={bb:.2f})"
    else: p=0.0;m=f"Encima banda sup (%B={bb:.2f})"
    score+=p; det["Bollinger%B"]={"pts":p,"max":1.0,"val":f"{bb:.2f}","msg":m}

    return round(min(score,10.0),1), det

def interpretar_score(score):
    if score>=8.0: return "🟢 COMPRAR / ACUMULAR","#50fa7b"
    elif score>=6.0: return "🔵 MANTENER / VIGILAR","#8be9fd"
    elif score>=4.0: return "🟡 NEUTRO / ESPERAR","#f1fa8c"
    else: return "🔴 REDUCIR / VENDER","#ff5555"


# ═════════════════════════════════════════════════════════════════
# NIVELES OPERATIVOS — Entrada, Stop Loss, Take Profit
# ═════════════════════════════════════════════════════════════════
def calcular_niveles_operativos(hist, info):
    """
    Calcula niveles operativos basados en ATR, soportes técnicos y target de analistas.
    Retorna dict con todos los niveles y el detalle de cálculo.
    """
    last = hist.iloc[-1]
    precio = last["Close"]
    atr_val = last.get("ATR", None)
    if atr_val is None or pd.isna(atr_val):
        # Calcular ATR si no está en el DataFrame
        tr = pd.concat([
            hist["High"] - hist["Low"],
            (hist["High"] - hist["Close"].shift(1)).abs(),
            (hist["Low"] - hist["Close"].shift(1)).abs()
        ], axis=1).max(axis=1)
        atr_val = tr.rolling(14).mean().iloc[-1]

    # ── ENTRADAS ──
    entrada_agresiva = round(precio, 2)

    # Entrada óptima: el soporte técnico más cercano por debajo del precio
    bb_low = last.get("BB_Low", np.nan)
    sma50 = last.get("SMA_50", np.nan)
    soportes = [v for v in [bb_low, sma50] if pd.notna(v) and v < precio]
    entrada_optima = round(max(soportes), 2) if soportes else round(precio * 0.97, 2)

    # ── STOP LOSS ──
    # Base: precio - 1.5 × ATR
    sl_atr = precio - 1.5 * atr_val

    # Override: soporte reciente (mínimo 20 sesiones)
    soporte_20d = hist["Low"].iloc[-20:].min()

    # Usamos el más protector (el más alto de los dos, pero siempre por debajo del precio)
    if soporte_20d > sl_atr and soporte_20d < precio:
        stop_loss = round(soporte_20d, 2)
        sl_nota = f"Soporte 20d ({soporte_20d:.2f}) > SL ATR ({sl_atr:.2f}) → se usa soporte"
    else:
        stop_loss = round(sl_atr, 2)
        sl_nota = f"ATR×1.5 ({sl_atr:.2f}) > Soporte 20d ({soporte_20d:.2f}) → se usa ATR"

    # ── RIESGO (distancia entrada agresiva → stop loss) ──
    riesgo = precio - stop_loss
    riesgo_pct = round(riesgo / precio * 100, 2) if precio > 0 else 0

    # ── TAKE PROFITS ──
    tp1 = round(precio + 2 * riesgo, 2)  # R/R 2:1
    tp2 = round(precio + 3 * riesgo, 2)  # R/R 3:1

    # TP3: target de consenso de analistas
    target_analistas = info.get("targetMeanPrice")
    tp3 = round(target_analistas, 2) if target_analistas and target_analistas > precio else None

    return {
        "precio": precio,
        "entrada_agresiva": entrada_agresiva,
        "entrada_optima": entrada_optima,
        "stop_loss": stop_loss,
        "sl_nota": sl_nota,
        "riesgo": round(riesgo, 2),
        "riesgo_pct": riesgo_pct,
        "atr": round(atr_val, 2),
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "soporte_20d": round(soporte_20d, 2),
    }


# ╔═══════════════════════════════════════════════════════════════╗
# ║  SIDEBAR GLOBAL + ROUTING                                    ║
# ╚═══════════════════════════════════════════════════════════════╝
with st.sidebar:
    st.title("📊 TFM Investment App")
    st.caption("Master IA Sector Financiero — VIU")
    st.divider()
    pagina = st.radio("Módulo", ["🔍 Screener", "📈 Análisis Individual"])
    st.divider()


# ╔═══════════════════════════════════════════════════════════════╗
# ║  PÁGINA 1: SCREENER                                          ║
# ╚═══════════════════════════════════════════════════════════════╝
if pagina == "🔍 Screener":
    with st.sidebar:
        indice=st.selectbox("Índice",list(INDICES.keys()),format_func=lambda x:INDICES[x][0])
        modo=st.selectbox("Modo",["VALUE","MOMENTUM","BREAKOUT","ACUMULACION","DIVIDENDOS","QUALITY","TODO"])
        limite=st.slider("Tickers",10,200,80,step=10)
        min_mc=st.number_input("MktCap mín ($B)",value=0.5,step=0.5,min_value=0.0)
        st.divider()
        ejecutar=st.button("🚀 Ejecutar Screener",type="primary",use_container_width=True)

    st.header(f"🔍 Screener: {INDICES[indice][0]} — {modo}")
    MODO_DESC={"VALUE":"PER 0–20, margen>8%, D/E<150","MOMENTUM":"Mom3M>10%, vol↑, sobre SMA50",
               "BREAKOUT":"Ruptura 20d + vol>1.3x","ACUMULACION":"Zona baja + Chaikin A/D + vol↑",
               "DIVIDENDOS":"Yield>2.5%, margen>5%","QUALITY":"ROE>15%, margen>12%, mom+","TODO":"Sin filtro"}
    st.caption(f"Filtro: {MODO_DESC.get(modo,'')}")

    if ejecutar:
        with st.spinner(f"Obteniendo tickers de {INDICES[indice][0]}..."):
            tickers=INDICES[indice][1]()
        if not tickers: st.error("No se pudieron obtener tickers.")
        else:
            ta=tickers[:limite]
            st.info(f"{len(tickers)} tickers cargados. Analizando {len(ta)}...")
            resultados=[]; pb=st.progress(0)
            for i,t in enumerate(ta):
                pb.progress((i+1)/len(ta),text=f"Analizando {t} ({i+1}/{len(ta)})")
                r=analizar_ticker_screener(t)
                if r and r.get("MktCap (B$)",0)>=min_mc: resultados.append(r)
                if i % 5 == 4: time.sleep(0.5)  # Pausa cada 5 tickers para no saturar Yahoo
            pb.empty()
            if not resultados: st.error("Sin resultados.")
            else:
                df_raw=pd.DataFrame(resultados)
                df_f,lb=aplicar_filtros(df_raw,modo)
                df_f=df_f.sort_values("Score",ascending=False).reset_index(drop=True)
                st.subheader(f"{len(df_f)} de {len(df_raw)} — {lb}")
                if df_f.empty: st.warning("Ningún activo cumple los filtros.")
                else:
                    k1,k2,k3,k4=st.columns(4)
                    k1.metric("Filtradas",len(df_f)); k2.metric("Score medio",f"{df_f['Score'].mean():.1f}")
                    k3.metric("🟢 COMPRAR",len(df_f[df_f['Label']=='COMPRAR']))
                    k4.metric("🔵 VIGILAR",len(df_f[df_f['Label']=='VIGILAR']))
                    cols=["Ticker","Precio","Score","Label","Señales","Mom 3M %","Vol/Avg 20d","Vol Tendencia",
                          "vs SMA50 %","Dist Max52W %","PER","ROE %","Margen Net %","Potencial %","MktCap (B$)"]
                    cols=[c for c in cols if c in df_f.columns]
                    st.dataframe(df_f[cols],use_container_width=True,height=500)

                    # Scatter
                    df_v=df_f.dropna(subset=["Mom 3M %","Vol/Avg 20d"]).head(25)
                    if not df_v.empty:
                        fig=px.scatter(df_v,x="Mom 3M %",y="Vol/Avg 20d",color="Label",color_discrete_map=COLORES_LABEL,
                            size="MktCap (B$)",hover_data=["Ticker","Score","Señales"],text="Ticker",title="Momentum vs Volumen")
                        fig.update_traces(textposition="top center",textfont_size=9)
                        fig.update_layout(template="plotly_dark",height=500,paper_bgcolor="#12121f",plot_bgcolor="#1e1e2e")
                        st.plotly_chart(fig,use_container_width=True)

                    # Señales
                    todas=" | ".join(df_f["Señales"].fillna("-").tolist())
                    tipos=[s.strip() for s in todas.split("|") if s.strip() not in ("-","")]
                    if tipos:
                        cnt=Counter(tipos); sdf=pd.DataFrame(cnt.most_common(10),columns=["Señal","N"])
                        fig2=go.Figure(go.Bar(x=sdf["Señal"],y=sdf["N"],marker_color=[COLORES_SEÑAL.get(s,"#6272a4") for s in sdf["Señal"]]))
                        fig2.update_layout(title="Señales Detectadas",template="plotly_dark",height=400,paper_bgcolor="#12121f",plot_bgcolor="#1e1e2e")
                        st.plotly_chart(fig2,use_container_width=True)

                    st.session_state["screener_results"]=df_f
                    csv=df_f.to_csv(index=False).encode("utf-8")
                    st.download_button("📥 Descargar CSV",csv,f"screener_{indice}_{modo}.csv","text/csv")

    elif "screener_results" in st.session_state:
        st.info("Mostrando últimos resultados.")
        df_p=st.session_state["screener_results"]
        cols=["Ticker","Precio","Score","Label","Señales","Mom 3M %","PER","ROE %","Potencial %"]
        cols=[c for c in cols if c in df_p.columns]
        st.dataframe(df_p[cols],use_container_width=True,height=400)
    else:
        st.info("👈 Configura y pulsa **Ejecutar Screener**.")


# ╔═══════════════════════════════════════════════════════════════╗
# ║  PÁGINA 2: ANÁLISIS INDIVIDUAL                               ║
# ╚═══════════════════════════════════════════════════════════════╝
elif pagina == "📈 Análisis Individual":
    with st.sidebar:
        scr_tickers=st.session_state.get("screener_results",pd.DataFrame()).get("Ticker",[]).tolist() if "screener_results" in st.session_state else []
        ticker_in=st.text_input("Ticker",value=scr_tickers[0] if scr_tickers else "MSFT")
        if scr_tickers:
            alt=st.selectbox("O del Screener:",["—"]+scr_tickers)
            if alt!="—": ticker_in=alt
        tab_sel=st.radio("Vista",["🔧 Técnico","📋 Fundamental","🔧+📋 Completo"])
        st.divider()
        analizar=st.button("🚀 Analizar",type="primary",use_container_width=True)

    if analizar and ticker_in:
        ticker_in=ticker_in.upper().strip()
        with st.spinner(f"Descargando datos de {ticker_in}..."):
            hist, info, fin, bs, cf = descargar_ticker_safe(ticker_in, "2y")

        if hist.empty:
            st.error(f"No se pudieron descargar datos para {ticker_in}. Yahoo Finance puede estar bloqueando temporalmente. Espera unos minutos y reintenta.")
            st.stop()
        if len(hist) < 50:
            st.warning(f"Solo se obtuvieron {len(hist)} sesiones para {ticker_in}. Resultados pueden ser parciales.")
        if not info:
            st.error(f"No se pudo obtener info fundamental de {ticker_in}.")
            st.stop()

        nombre=info.get("longName") or info.get("shortName",ticker_in)
        precio=hist["Close"].iloc[-1]; moneda=info.get("currency","")
        st.header(f"🏢 {nombre} ({ticker_in})")
        h1,h2,h3,h4=st.columns(4)
        h1.metric("Precio",f"{precio:.2f} {moneda}"); h2.metric("Sector",info.get("sector","N/A"))
        h3.metric("MktCap",f"${info.get('marketCap',0)/1e9:.2f}B"); h4.metric("Industria",info.get("industry","N/A"))

        # ── TÉCNICO ──
        if tab_sel in ["🔧 Técnico","🔧+📋 Completo"]:
            st.divider(); st.subheader("🔧 Análisis Técnico — 7 Indicadores")
            with st.spinner("Calculando..."):
                hist["RSI"]=calc_rsi(hist["Close"])
                hist["SMA_50"]=hist["Close"].rolling(50).mean(); hist["SMA_200"]=hist["Close"].rolling(200).mean()
                bu,bm,bl,bp=calc_bollinger(hist["Close"])
                hist["BB_Up"],hist["BB_Mid"],hist["BB_Low"],hist["BB_PctB"]=bu,bm,bl,bp
                mc,sg,mh=calc_macd(hist["Close"])
                hist["MACD"],hist["Signal"],hist["MACD_Hist"]=mc,sg,mh; hist["MACD_Hist_prev"]=mh.shift(1)
                ax,dp,dm=calc_adx(hist); hist["ADX"],hist["DI_Plus"],hist["DI_Minus"]=ax.values,dp.values,dm.values
                ov,os,ot,od=calc_obv(hist); hist["OBV"],hist["OBV_SMA"]=ov,os
                sk,sd=calc_stochastic(hist); hist["Stoch_K"],hist["Stoch_D"]=sk,sd
                at,ap=calc_atr(hist); hist["ATR_PCT"]=ap

            last=hist.iloc[-1]
            score,det=calcular_score_tecnico(last,ot.iloc[-1],od.iloc[-1])
            verd,colv=interpretar_score(score)

            s1,s2,s3=st.columns([1,2,1])
            s1.metric("SCORE",f"{score}/10"); s2.markdown(f"### {verd}")
            s3.caption(f"SMA200: {'✅' if precio>last['SMA_200'] else '🔴'} | SMA50: {'✅' if precio>last['SMA_50'] else '🔴'} | ATR%: {last['ATR_PCT']:.2f}%")

            for ind,d in det.items():
                pct=d["pts"]/d["max"] if d["max"]>0 else 0
                ic="✅" if pct>=0.7 else ("🟡" if pct>=0.3 else "🔴")
                st.markdown(f"{ic} **{ind}** — {d['val']} — `{d['pts']:.1f}/{d['max']:.1f}` — {d['msg']}")

            tgt=info.get("targetMeanPrice")
            if tgt: st.markdown(f"🎯 **Target:** {tgt:.2f} {moneda} ({((tgt/precio)-1)*100:+.1f}%)")

            # ── NIVELES OPERATIVOS ──
            st.markdown("---")
            st.markdown("### 🎯 Niveles Operativos")
            # Necesitamos ATR en el DataFrame
            atr_raw, _ = calc_atr(hist)
            hist["ATR"] = atr_raw
            niveles = calcular_niveles_operativos(hist, info)

            n1, n2, n3 = st.columns(3)
            with n1:
                st.markdown("**ENTRADAS**")
                st.markdown(f"🟢 **Agresiva:** {niveles['entrada_agresiva']:.2f} {moneda}")
                st.markdown(f"🔵 **Óptima:** {niveles['entrada_optima']:.2f} {moneda}")
            with n2:
                st.markdown("**STOP LOSS**")
                st.markdown(f"🔴 **SL:** {niveles['stop_loss']:.2f} {moneda} (−{niveles['riesgo_pct']:.1f}%)")
                st.caption(f"ATR(14): {niveles['atr']:.2f} | Soporte 20d: {niveles['soporte_20d']:.2f}")
                st.caption(niveles['sl_nota'])
            with n3:
                st.markdown("**TAKE PROFIT**")
                st.markdown(f"🎯 **TP1 (R/R 2:1):** {niveles['tp1']:.2f} {moneda} (+{((niveles['tp1']/precio-1)*100):.1f}%)")
                st.markdown(f"🎯 **TP2 (R/R 3:1):** {niveles['tp2']:.2f} {moneda} (+{((niveles['tp2']/precio-1)*100):.1f}%)")
                if niveles['tp3']:
                    st.markdown(f"🎯 **TP3 (Consenso):** {niveles['tp3']:.2f} {moneda} (+{((niveles['tp3']/precio-1)*100):.1f}%)")
                else:
                    st.markdown("🎯 **TP3 (Consenso):** N/A")

            # Gráfico 5 paneles
            n=min(len(hist),252); h=hist.iloc[-n:]
            fig=make_subplots(rows=5,cols=1,shared_xaxes=True,vertical_spacing=0.02,
                row_heights=[0.35,0.15,0.15,0.15,0.15],
                subplot_titles=[f"{ticker_in} — Precio+BB+SMA","MACD","RSI+Stoch","ADX+DI","OBV"])
            fig.add_trace(go.Scatter(x=h.index,y=h["Close"],name="Precio",line=dict(color="#f8f8f2",width=1.8)),row=1,col=1)
            fig.add_trace(go.Scatter(x=h.index,y=h["SMA_50"],name="SMA50",line=dict(color="#ffb86c",width=1.2,dash="dash")),row=1,col=1)
            fig.add_trace(go.Scatter(x=h.index,y=h["SMA_200"],name="SMA200",line=dict(color="#ff5555",width=1.2,dash="dash")),row=1,col=1)
            fig.add_trace(go.Scatter(x=h.index,y=h["BB_Up"],line=dict(color="#8be9fd",width=0.6),showlegend=False),row=1,col=1)
            fig.add_trace(go.Scatter(x=h.index,y=h["BB_Low"],name="Bollinger",line=dict(color="#8be9fd",width=0.6),fill="tonexty",fillcolor="rgba(139,233,253,0.08)"),row=1,col=1)
            # Niveles operativos en el gráfico
            fig.add_hline(y=niveles["entrada_agresiva"],line_dash="solid",line_color="#50fa7b",opacity=0.7,row=1,col=1,
                annotation_text=f"Entrada Agresiva {niveles['entrada_agresiva']:.2f}",annotation_position="top left",annotation_font_color="#50fa7b",annotation_font_size=9)
            fig.add_hline(y=niveles["entrada_optima"],line_dash="dash",line_color="#8be9fd",opacity=0.7,row=1,col=1,
                annotation_text=f"Entrada Óptima {niveles['entrada_optima']:.2f}",annotation_position="bottom left",annotation_font_color="#8be9fd",annotation_font_size=9)
            fig.add_hline(y=niveles["stop_loss"],line_dash="solid",line_color="#ff5555",opacity=0.8,row=1,col=1,
                annotation_text=f"Stop Loss {niveles['stop_loss']:.2f}",annotation_position="bottom left",annotation_font_color="#ff5555",annotation_font_size=9)
            fig.add_hline(y=niveles["tp1"],line_dash="dot",line_color="#f1fa8c",opacity=0.6,row=1,col=1,
                annotation_text=f"TP1 {niveles['tp1']:.2f}",annotation_position="top left",annotation_font_color="#f1fa8c",annotation_font_size=9)
            fig.add_hline(y=niveles["tp2"],line_dash="dot",line_color="#ffb86c",opacity=0.6,row=1,col=1,
                annotation_text=f"TP2 {niveles['tp2']:.2f}",annotation_position="top left",annotation_font_color="#ffb86c",annotation_font_size=9)
            if niveles["tp3"]:
                fig.add_hline(y=niveles["tp3"],line_dash="dot",line_color="#bd93f9",opacity=0.6,row=1,col=1,
                    annotation_text=f"TP3 Consenso {niveles['tp3']:.2f}",annotation_position="top left",annotation_font_color="#bd93f9",annotation_font_size=9)
            ch=["#50fa7b" if v>=0 else "#ff5555" for v in h["MACD_Hist"]]
            fig.add_trace(go.Bar(x=h.index,y=h["MACD_Hist"],marker_color=ch,showlegend=False),row=2,col=1)
            fig.add_trace(go.Scatter(x=h.index,y=h["MACD"],name="MACD",line=dict(color="#50fa7b",width=1.2)),row=2,col=1)
            fig.add_trace(go.Scatter(x=h.index,y=h["Signal"],name="Signal",line=dict(color="#ff79c6",width=1.2)),row=2,col=1)
            fig.add_trace(go.Scatter(x=h.index,y=h["RSI"],name="RSI",line=dict(color="#bd93f9",width=1.2)),row=3,col=1)
            fig.add_trace(go.Scatter(x=h.index,y=h["Stoch_K"],name="%K",line=dict(color="#f1fa8c",width=1,dash="dot")),row=3,col=1)
            fig.add_hline(y=70,line_dash="dash",line_color="#ff5555",opacity=0.5,row=3,col=1)
            fig.add_hline(y=30,line_dash="dash",line_color="#50fa7b",opacity=0.5,row=3,col=1)
            fig.add_trace(go.Scatter(x=h.index,y=h["ADX"],name="ADX",line=dict(color="#ffb86c",width=1.4)),row=4,col=1)
            fig.add_trace(go.Scatter(x=h.index,y=h["DI_Plus"],name="DI+",line=dict(color="#50fa7b",width=0.9)),row=4,col=1)
            fig.add_trace(go.Scatter(x=h.index,y=h["DI_Minus"],name="DI−",line=dict(color="#ff5555",width=0.9)),row=4,col=1)
            fig.add_hline(y=25,line_dash="dash",line_color="white",opacity=0.3,row=4,col=1)
            fig.add_trace(go.Scatter(x=h.index,y=h["OBV"],name="OBV",line=dict(color="#8be9fd",width=1.2)),row=5,col=1)
            fig.add_trace(go.Scatter(x=h.index,y=h["OBV_SMA"],name="OBV SMA",line=dict(color="#ff79c6",width=1,dash="dash")),row=5,col=1)
            fig.update_layout(title=f"{ticker_in} | Score: {score}/10 | {verd}",template="plotly_dark",
                paper_bgcolor="#12121f",plot_bgcolor="#1e1e2e",height=1000,
                legend=dict(orientation="h",y=-0.02,font=dict(size=9)),hovermode="x unified")
            st.plotly_chart(fig,use_container_width=True)

        # ── FUNDAMENTAL ──
        if tab_sel in ["📋 Fundamental","🔧+📋 Completo"]:
            st.divider(); st.subheader("📋 Análisis Fundamental Completo")

            # Bloque 1: Valoración
            st.markdown("### 💰 Valoración")
            per=info.get("trailingPE"); pfw=info.get("forwardPE"); peg=info.get("pegRatio")
            pb=info.get("priceToBook"); ps=info.get("priceToSalesTrailing12Months"); eve=info.get("enterpriseToEbitda")
            gn=calcular_graham_number(info); fy=calcular_fcf_yield(info,cf)
            v1,v2,v3=st.columns(3)
            with v1:
                st.markdown(metric_fund("PER",per,".1f",lambda x:x<15,lambda x:x>30))
                st.markdown(metric_fund("PER Fwd",pfw,".1f",lambda x:x<12,lambda x:x>25))
            with v2:
                st.markdown(metric_fund("PEG",peg,".2f",lambda x:x<1,lambda x:x>2))
                st.markdown(metric_fund("P/Book",pb,".2f",lambda x:x<1.5,lambda x:x>5))
            with v3:
                st.markdown(metric_fund("P/Ventas",ps,".2f",lambda x:x<2,lambda x:x>10))
                st.markdown(metric_fund("EV/EBITDA",eve,".1f",lambda x:x<10,lambda x:x>20))
            if fy: ic="✅" if fy>5 else ("🔴" if fy<0 else "🟡"); st.markdown(f"{ic} **FCF Yield:** {fy:.2f}%")
            if gn and precio:
                dif=(precio/gn-1)*100; ic="✅ INFRAVALORADO" if precio<gn else "⚠️ SOBREVALORADO"
                st.markdown(f"**Graham:** {gn:.2f} {moneda} → {dif:+.1f}% — {ic}")
            tm=info.get("targetMeanPrice")
            if tm and precio:
                up=(tm/precio-1)*100; ic="✅" if up>10 else ("🔴" if up<-10 else "🟡")
                st.markdown(f"{ic} **Target:** {tm:.2f} {moneda} ({up:+.1f}%)")
                tl=info.get("targetLowPrice"); th=info.get("targetHighPrice")
                if tl and th: st.caption(f"Rango: {tl:.2f}–{th:.2f} | Analistas: {info.get('numberOfAnalystOpinions','N/A')} | {(info.get('recommendationKey') or '').upper()}")

            # Bloque 2: Rentabilidad
            st.markdown("### 📈 Rentabilidad & Calidad")
            roe=info.get("returnOnEquity"); roa=info.get("returnOnAssets"); roic=calcular_roic(fin,bs)
            gm=info.get("grossMargins"); om=info.get("operatingMargins"); pm=info.get("profitMargins")
            dupont=calcular_dupont(fin,bs); cagr_r,cagr_n=calcular_cagr_historico(fin)
            r1,r2,r3=st.columns(3)
            with r1:
                st.markdown(metric_fund("ROE",roe*100 if roe else None,".1f%",lambda x:x>15,lambda x:x<5))
                st.markdown(metric_fund("ROA",roa*100 if roa else None,".1f%",lambda x:x>8,lambda x:x<2))
                st.markdown(metric_fund("ROIC",roic,".2f%",lambda x:x>15,lambda x:x<8))
            with r2:
                st.markdown(metric_fund("M.Bruto",gm*100 if gm else None,".1f%",lambda x:x>40,lambda x:x<20))
                st.markdown(metric_fund("M.Operativo",om*100 if om else None,".1f%",lambda x:x>15,lambda x:x<5))
                st.markdown(metric_fund("M.Neto",pm*100 if pm else None,".1f%",lambda x:x>15,lambda x:x<3))
            with r3:
                if cagr_r is not None: ic="✅" if cagr_r>7 else ("🔴" if cagr_r<0 else "🟡"); st.markdown(f"{ic} **CAGR Rev:** {cagr_r:+.1f}%/año")
                if cagr_n is not None: ic="✅" if cagr_n>7 else ("🔴" if cagr_n<0 else "🟡"); st.markdown(f"{ic} **CAGR BN:** {cagr_n:+.1f}%/año")
            if dupont:
                st.markdown("**DuPont:**")
                d1,d2,d3,d4=st.columns(4)
                d1.metric("ROE",f"{dupont['ROE']:.2f}%"); d2.metric("Margen",f"{dupont['Margen_Neto']:.2f}%")
                d3.metric("Rot.Act",f"{dupont['Rot_Activos']:.3f}x"); d4.metric("Apalanc.",f"{dupont['Apalanc']:.2f}x")

            # Bloque 3: Salud
            st.markdown("### 🏥 Salud Financiera")
            cr=info.get("currentRatio"); qr=info.get("quickRatio"); de=info.get("debtToEquity"); pay=info.get("payoutRatio")
            s1,s2,s3=st.columns(3)
            with s1:
                st.markdown(metric_fund("R.Corriente",cr,".2f",lambda x:x>1.5,lambda x:x<1))
                st.markdown(metric_fund("R.Rápido",qr,".2f",lambda x:x>1,lambda x:x<0.7))
            with s2:
                st.markdown(metric_fund("D/E",de,".1f",lambda x:x<80,lambda x:x>200))
                if pay: st.markdown(metric_fund("Payout",pay*100,".1f%",lambda x:x<60,lambda x:x>90))
            with s3:
                if not bs.empty and not fin.empty:
                    z,zz=calcular_altman_z(info,fin,bs)
                    if z: st.markdown(f"**Altman Z:** {z} → {zz}")

            # Bloque 4: Piotroski
            if not fin.empty and not bs.empty and not cf.empty:
                st.markdown("### 🔢 Piotroski F-Score")
                fs,fd=calcular_piotroski(fin,bs,cf)
                if fs>=7: ifs="🟢"; tfs="SÓLIDA"
                elif fs>=4: ifs="🟡"; tfs="MEDIA"
                else: ifs="🔴"; tfs="DÉBIL"
                st.markdown(f"### {ifs} F-Score: {fs}/9 — {tfs}")
                for c,v in fd.items():
                    if c.startswith("_"): continue
                    st.markdown(f"{'✅' if v['ok'] else '❌'} {c} — `{v['val']}`")

            # Bloque 5: Dividendos
            dy=info.get("dividendYield")
            if dy and dy>0:
                st.markdown("### 💵 Dividendos")
                dd1,dd2,dd3=st.columns(3)
                dd1.markdown(f"{'✅' if dy>0.03 else '🟡'} **Yield:** {dy*100:.2f}%")
                dr=info.get("dividendRate")
                if dr: dd2.markdown(f"**Pago/acción:** {dr:.2f} {moneda}")
                if pay: dd3.markdown(f"**Payout:** {pay*100:.1f}% — {'✅' if pay<0.6 else '⚠️'}")

            # Bloque 6: Veredicto
            st.markdown("### 🏆 Veredicto Fundamental")
            pts=0; mx=0
            if per: mx+=2; pts+=(2 if per<15 else 1 if per<25 else 0)
            if roe: mx+=2; pts+=(2 if roe>0.20 else 1 if roe>0.10 else 0)
            if pm: mx+=2; pts+=(2 if pm>0.15 else 1 if pm>0.05 else 0)
            if de is not None: mx+=2; pts+=(2 if de<80 else 1 if de<150 else 0)
            if not fin.empty and not bs.empty and not cf.empty: mx+=2; pts+=(2 if fs>=7 else 1 if fs>=4 else 0)
            if mx>0:
                pf=pts/mx
                if pf>=0.75: vf="🟢 SÓLIDA"
                elif pf>=0.45: vf="🟡 ACEPTABLE"
                else: vf="🔴 DÉBIL"
                st.markdown(f"**{vf}** — {pts}/{mx} ({pf*100:.0f}%)")
                st.progress(pf)

    else:
        st.info("👈 Introduce un ticker y pulsa **Analizar**.")
