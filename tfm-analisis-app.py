"""
TFM — MÓDULO ANÁLISIS INDIVIDUAL (Streamlit + Plotly)
Análisis técnico profundo (7 indicadores, score 0–10)
+ Análisis fundamental completo (6 bloques narrativos)

Ejecutar:  streamlit run tfm_analisis_app.py
"""

import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import warnings
warnings.filterwarnings("ignore")

st.set_page_config(
    page_title="TFM — Análisis Individual",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ═════════════════════════════════════════════════════════════════
# 1. LIBRERÍA DE INDICADORES TÉCNICOS (baja correlación)
#    RSI · MACD · ADX · OBV · Stochastic · Bollinger %B · ATR
# ═════════════════════════════════════════════════════════════════
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
    hist = macd - sig
    return macd, sig, hist


def calc_adx(df, period=14):
    high, low, close = df["High"], df["Low"], df["Close"]
    tr = pd.concat([
        high - low,
        (high - close.shift(1)).abs(),
        (low - close.shift(1)).abs()
    ], axis=1).max(axis=1)

    up_move = high - high.shift(1)
    down_move = low.shift(1) - low

    plus_dm = pd.Series(np.where((up_move > down_move) & (up_move > 0), up_move, 0), index=df.index)
    minus_dm = pd.Series(np.where((down_move > up_move) & (down_move > 0), down_move, 0), index=df.index)

    atr = tr.rolling(period).mean()
    plus_di = 100 * (plus_dm.rolling(period).mean() / atr)
    minus_di = 100 * (minus_dm.rolling(period).mean() / atr)

    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    adx = dx.rolling(period).mean()
    return adx, plus_di, minus_di


def calc_obv(df):
    obv = pd.Series(0.0, index=df.index)
    for i in range(1, len(df)):
        if df["Close"].iloc[i] > df["Close"].iloc[i - 1]:
            obv.iloc[i] = obv.iloc[i - 1] + df["Volume"].iloc[i]
        elif df["Close"].iloc[i] < df["Close"].iloc[i - 1]:
            obv.iloc[i] = obv.iloc[i - 1] - df["Volume"].iloc[i]
        else:
            obv.iloc[i] = obv.iloc[i - 1]

    obv_sma = obv.rolling(20).mean()
    obv_trend = pd.Series(np.where(obv > obv_sma, 1, -1), index=df.index)

    # Divergencia: precio baja pero OBV sube (alcista) o viceversa
    price_trend_20 = df["Close"].pct_change(20)
    obv_trend_20 = obv.pct_change(20)
    divergencia = pd.Series(
        np.where(
            (price_trend_20 < 0) & (obv_trend_20 > 0), "Alcista",
            np.where((price_trend_20 > 0) & (obv_trend_20 < 0), "Bajista", "Neutral")
        ),
        index=df.index,
    )
    return obv, obv_sma, obv_trend, divergencia


def calc_stochastic(df, k_period=14, d_period=3):
    low_min = df["Low"].rolling(k_period).min()
    high_max = df["High"].rolling(k_period).max()
    k = 100 * (df["Close"] - low_min) / (high_max - low_min).replace(0, np.nan)
    d = k.rolling(d_period).mean()
    return k, d


def calc_bollinger(series, period=20, std=2):
    mid = series.rolling(period).mean()
    sigma = series.rolling(period).std()
    upper = mid + std * sigma
    lower = mid - std * sigma
    pct_b = (series - lower) / (upper - lower).replace(0, np.nan)
    return upper, mid, lower, pct_b


def calc_atr(df, period=14):
    tr = pd.concat([
        df["High"] - df["Low"],
        (df["High"] - df["Close"].shift(1)).abs(),
        (df["Low"] - df["Close"].shift(1)).abs()
    ], axis=1).max(axis=1)
    atr = tr.rolling(period).mean()
    atr_pct = atr / df["Close"] * 100
    return atr, atr_pct


# ═════════════════════════════════════════════════════════════════
# 2. SISTEMA DE PUNTUACIÓN TÉCNICA 0–10
#    Momentum (3.5) + Tendencia (4.0) + Volumen (1.5) + Volatil. (1.0)
# ═════════════════════════════════════════════════════════════════
def calcular_score_tecnico(row, obv_trend_val, obv_div):
    score = 0.0
    detalle = {}

    # ── RSI (0–2.5 pts) ──
    rsi = row["RSI"]
    if   rsi < 30:  pts = 2.5; msg = f"Sobrevendido ({rsi:.0f}) → rebote potencial"
    elif rsi < 45:  pts = 2.2; msg = f"Zona de acumulación ({rsi:.0f})"
    elif rsi < 55:  pts = 2.0; msg = f"Neutral ({rsi:.0f})"
    elif rsi < 65:  pts = 1.5; msg = f"Momentum positivo ({rsi:.0f})"
    elif rsi < 75:  pts = 0.5; msg = f"Zona caliente ({rsi:.0f}) — precaución"
    else:           pts = 0.0; msg = f"Sobrecomprado ({rsi:.0f}) — riesgo caída"
    score += pts
    detalle["RSI"] = {"pts": pts, "max": 2.5, "val": f"{rsi:.1f}", "msg": msg}

    # ── MACD (0–2.0 pts) ──
    macd_v = row["MACD"]
    sig_v = row["Signal"]
    hist_v = row["MACD_Hist"]
    prev_h = row.get("MACD_Hist_prev", 0) or 0
    alcista = macd_v > sig_v
    aceler = hist_v > prev_h
    if   alcista and aceler:     pts = 2.0; msg = "Cruce alcista acelerando"
    elif alcista:                pts = 1.5; msg = "Por encima de señal"
    elif not alcista and aceler: pts = 0.7; msg = "Bajista pero perdiendo fuerza"
    else:                        pts = 0.0; msg = "Bajista y acelerando a la baja"
    score += pts
    detalle["MACD"] = {"pts": pts, "max": 2.0, "val": f"{macd_v:.4f}", "msg": msg}

    # ── ADX (0–2.0 pts) ──
    adx_v = row["ADX"]
    di_p = row["DI_Plus"]
    di_m = row["DI_Minus"]
    if   adx_v > 30 and di_p > di_m: pts = 2.0; msg = f"Tendencia alcista FUERTE (ADX={adx_v:.0f})"
    elif adx_v > 20 and di_p > di_m: pts = 1.5; msg = f"Tendencia alcista moderada (ADX={adx_v:.0f})"
    elif adx_v > 20 and di_p < di_m: pts = 0.3; msg = f"Tendencia bajista activa (ADX={adx_v:.0f})"
    elif adx_v < 20:                  pts = 1.0; msg = f"Sin tendencia — lateralización (ADX={adx_v:.0f})"
    else:                              pts = 0.7; msg = f"Tendencia débil (ADX={adx_v:.0f})"
    score += pts
    detalle["ADX"] = {"pts": pts, "max": 2.0, "val": f"{adx_v:.1f}", "msg": msg}

    # ── OBV (0–1.5 pts) ──
    if   obv_trend_val == 1 and obv_div == "Alcista":  pts = 1.5; msg = "Presión compradora + divergencia alcista"
    elif obv_trend_val == 1:                            pts = 1.2; msg = "Presión compradora (OBV > SMA20)"
    elif obv_trend_val == -1 and obv_div == "Bajista":  pts = 0.0; msg = "Presión vendedora + divergencia bajista"
    else:                                               pts = 0.3; msg = "Presión vendedora (OBV < SMA20)"
    score += pts
    obv_label = "Alcista" if obv_trend_val == 1 else "Bajista"
    detalle["OBV"] = {"pts": pts, "max": 1.5, "val": obv_label, "msg": msg}

    # ── Stochastic (0–1.0 pts) ──
    k_v = row["Stoch_K"]
    d_v = row["Stoch_D"]
    if   k_v < 20 and k_v > d_v: pts = 1.0; msg = f"Sobrevendido + cruce alcista (%K={k_v:.0f})"
    elif k_v < 25:                pts = 0.8; msg = f"Zona sobrevendida (%K={k_v:.0f})"
    elif k_v > 80:                pts = 0.0; msg = f"Zona sobrecomprada (%K={k_v:.0f})"
    elif k_v > d_v:               pts = 0.6; msg = f"Momentum positivo (%K={k_v:.0f} > %D)"
    else:                         pts = 0.2; msg = f"Momentum negativo (%K={k_v:.0f} < %D)"
    score += pts
    detalle["Stochastic"] = {"pts": pts, "max": 1.0, "val": f"{k_v:.1f}", "msg": msg}

    # ── Bollinger %B (0–1.0 pts) ──
    pct_b = row["BB_PctB"]
    if   pct_b < 0:    pts = 1.0; msg = f"Debajo de banda inferior (%B={pct_b:.2f}) — rebote inminente"
    elif pct_b < 0.35: pts = 1.0; msg = f"Zona inferior-media (%B={pct_b:.2f}) — potencial alcista"
    elif pct_b < 0.65: pts = 0.5; msg = f"Zona media (%B={pct_b:.2f})"
    elif pct_b < 1.0:  pts = 0.1; msg = f"Zona superior (%B={pct_b:.2f}) — cuidado"
    else:              pts = 0.0; msg = f"Encima de banda superior (%B={pct_b:.2f}) — sobrecomprado"
    score += pts
    detalle["Bollinger%B"] = {"pts": pts, "max": 1.0, "val": f"{pct_b:.2f}", "msg": msg}

    return round(min(score, 10.0), 1), detalle


def interpretar_score(score):
    if   score >= 8.0: return "🟢 COMPRAR / ACUMULAR", "#50fa7b"
    elif score >= 6.0: return "🔵 MANTENER / VIGILAR", "#8be9fd"
    elif score >= 4.0: return "🟡 NEUTRO / ESPERAR", "#f1fa8c"
    else:              return "🔴 REDUCIR / VENDER", "#ff5555"


# ═════════════════════════════════════════════════════════════════
# 3. MÉTRICAS FUNDAMENTALES AVANZADAS
# ═════════════════════════════════════════════════════════════════
def _safe(df, key, col=0, default=0):
    try:
        val = df.loc[key].iloc[col] if key in df.index else default
        return val if not pd.isna(val) else default
    except:
        return default


def calcular_piotroski(fin, bs, cf):
    score = 0
    detalle = {}
    try:
        ta0 = _safe(bs, "Total Assets", 0, 1)
        ta1 = _safe(bs, "Total Assets", 1, 1)
        ni0 = _safe(fin, "Net Income", 0)
        ni1 = _safe(fin, "Net Income", 1)
        cfo = _safe(cf, "Operating Cash Flow", 0)
        ca0 = _safe(bs, "Current Assets", 0)
        cl0 = _safe(bs, "Current Liabilities", 0) or 1
        ca1 = _safe(bs, "Current Assets", 1)
        cl1 = _safe(bs, "Current Liabilities", 1) or 1
        ltd0 = _safe(bs, "Long Term Debt", 0)
        ltd1 = _safe(bs, "Long Term Debt", 1)
        rev0 = _safe(fin, "Total Revenue", 0) or 1
        rev1 = _safe(fin, "Total Revenue", 1) or 1
        gp0 = _safe(fin, "Gross Profit", 0)
        gp1 = _safe(fin, "Gross Profit", 1)

        roa0 = ni0 / ta0;  roa1 = ni1 / ta1
        cr0 = ca0 / cl0;   cr1 = ca1 / cl1
        lev0 = ltd0 / ta0; lev1 = ltd1 / ta1
        gm0 = gp0 / rev0;  gm1 = gp1 / rev1
        at0 = rev0 / ta0;  at1 = rev1 / ta1

        tests = [
            ("F1 ROA positivo",              roa0 > 0,     f"{roa0*100:.2f}%"),
            ("F2 CFO positivo",              cfo > 0,      f"${cfo/1e6:.0f}M"),
            ("F3 ROA mejora vs año anterior", roa0 > roa1,  f"{roa0*100:.2f}% vs {roa1*100:.2f}%"),
            ("F4 Calidad (CFO > Net Income)", cfo > ni0,    f"CFO {cfo/1e6:.0f}M > NI {ni0/1e6:.0f}M"),
            ("F5 Menor apalancamiento",       lev0 < lev1,  f"{lev0:.3f} vs {lev1:.3f}"),
            ("F6 Mejor liquidez corriente",   cr0 > cr1,    f"{cr0:.2f} vs {cr1:.2f}"),
            ("F7 Sin dilución accionistas",   True,          "(ver shares outstanding manual)"),
            ("F8 Margen bruto creciente",     gm0 > gm1,    f"{gm0*100:.1f}% vs {gm1*100:.1f}%"),
            ("F9 Rotación activos creciente", at0 > at1,    f"{at0:.3f} vs {at1:.3f}"),
        ]
        for nombre, cond, valor in tests:
            pts = 1 if cond else 0
            score += pts
            detalle[nombre] = {"ok": bool(cond), "val": valor}
    except Exception as e:
        detalle["_error"] = {"ok": False, "val": str(e)}
    return score, detalle


def calcular_altman_z(info, fin, bs):
    try:
        ta = _safe(bs, "Total Assets", 0, 1)
        ca = _safe(bs, "Current Assets", 0)
        cl = _safe(bs, "Current Liabilities", 0)
        re = _safe(bs, "Retained Earnings", 0)
        ebit = _safe(fin, "EBIT", 0) or _safe(fin, "Operating Income", 0)
        tl = _safe(bs, "Total Liabilities Net Minority Interest", 0) or _safe(bs, "Total Debt", 0) or 1
        rev = _safe(fin, "Total Revenue", 0)
        mktcap = info.get("marketCap", 0)
        z = 1.2*((ca-cl)/ta) + 1.4*(re/ta) + 3.3*(ebit/ta) + 0.6*(mktcap/tl) + 1.0*(rev/ta)
        if   z > 2.99: zona = "🟢 ZONA SEGURA"
        elif z > 1.81: zona = "🟡 ZONA GRIS"
        else:          zona = "🔴 ZONA PELIGRO"
        return round(z, 2), zona
    except:
        return None, "Sin datos"


def calcular_roic(fin, bs):
    try:
        ebit = _safe(fin, "EBIT", 0) or _safe(fin, "Operating Income", 0)
        nopat = ebit * (1 - 0.21)
        ta = _safe(bs, "Total Assets", 0, 1)
        cl = _safe(bs, "Current Liabilities", 0)
        inv_capital = ta - cl
        if inv_capital <= 0: return None
        return round(nopat / inv_capital * 100, 2)
    except:
        return None


def calcular_graham_number(info):
    try:
        eps = info.get("trailingEps") or info.get("forwardEps")
        bvps = info.get("bookValue")
        if eps and bvps and eps > 0 and bvps > 0:
            return round((22.5 * eps * bvps) ** 0.5, 2)
        return None
    except:
        return None


def calcular_fcf_yield(info, cf):
    try:
        fcf = info.get("freeCashflow")
        if not fcf:
            cfo = _safe(cf, "Operating Cash Flow", 0)
            capex = abs(_safe(cf, "Capital Expenditure", 0))
            fcf = cfo - capex
        mktcap = info.get("marketCap")
        if fcf and mktcap and mktcap > 0:
            return round(fcf / mktcap * 100, 2)
        return None
    except:
        return None


def calcular_dupont(fin, bs):
    try:
        ni = _safe(fin, "Net Income", 0)
        rev = _safe(fin, "Total Revenue", 0) or 1
        ta = _safe(bs, "Total Assets", 0) or 1
        eq = _safe(bs, "Stockholders Equity", 0) or 1
        net_margin = ni / rev
        asset_turn = rev / ta
        leverage = ta / eq
        roe = net_margin * asset_turn * leverage
        return {
            "ROE": round(roe * 100, 2),
            "Margen_Neto": round(net_margin * 100, 2),
            "Rot_Activos": round(asset_turn, 3),
            "Apalanc": round(leverage, 2),
        }
    except:
        return None


def calcular_cagr_historico(fin):
    try:
        if fin.empty or len(fin.columns) < 2:
            return None, None
        def cagr(series):
            valores = series.dropna()
            if len(valores) < 2: return None
            v_ini = valores.iloc[-1]
            v_fin = valores.iloc[0]
            n = len(valores) - 1
            if v_ini <= 0 or v_fin <= 0: return None
            return round(((v_fin / v_ini) ** (1/n) - 1) * 100, 1)
        rev = fin.loc["Total Revenue"] if "Total Revenue" in fin.index else None
        ni = fin.loc["Net Income"] if "Net Income" in fin.index else None
        return (cagr(rev) if rev is not None else None,
                cagr(ni)  if ni  is not None else None)
    except:
        return None, None


# ═════════════════════════════════════════════════════════════════
# 4. GRÁFICO TÉCNICO MULTI-PANEL (Plotly — 5 filas)
# ═════════════════════════════════════════════════════════════════
def grafico_tecnico_plotly(hist, ticker, score, veredicto, color_v):
    n = min(len(hist), 252)
    h = hist.iloc[-n:]

    fig = make_subplots(
        rows=5, cols=1, shared_xaxes=True,
        vertical_spacing=0.02,
        row_heights=[0.35, 0.15, 0.15, 0.15, 0.15],
        subplot_titles=[
            f"{ticker} — Precio + Bollinger + SMA",
            "MACD + Histograma",
            "RSI + Stochastic %K",
            "ADX + DI+/DI−",
            "OBV + SMA20",
        ],
    )

    # Panel 1: Precio + SMA + Bollinger
    fig.add_trace(go.Scatter(x=h.index, y=h["Close"], name="Precio",
        line=dict(color="#f8f8f2", width=1.8)), row=1, col=1)
    fig.add_trace(go.Scatter(x=h.index, y=h["SMA_50"], name="SMA 50",
        line=dict(color="#ffb86c", width=1.2, dash="dash")), row=1, col=1)
    fig.add_trace(go.Scatter(x=h.index, y=h["SMA_200"], name="SMA 200",
        line=dict(color="#ff5555", width=1.2, dash="dash")), row=1, col=1)
    fig.add_trace(go.Scatter(x=h.index, y=h["BB_Up"], name="BB Upper",
        line=dict(color="#8be9fd", width=0.6), opacity=0.5, showlegend=False), row=1, col=1)
    fig.add_trace(go.Scatter(x=h.index, y=h["BB_Low"], name="Bollinger",
        line=dict(color="#8be9fd", width=0.6), fill="tonexty",
        fillcolor="rgba(139,233,253,0.08)", opacity=0.5), row=1, col=1)

    # Panel 2: MACD
    colors_hist = ["#50fa7b" if v >= 0 else "#ff5555" for v in h["MACD_Hist"]]
    fig.add_trace(go.Bar(x=h.index, y=h["MACD_Hist"], name="Histograma",
        marker_color=colors_hist, opacity=0.6, showlegend=False), row=2, col=1)
    fig.add_trace(go.Scatter(x=h.index, y=h["MACD"], name="MACD",
        line=dict(color="#50fa7b", width=1.2)), row=2, col=1)
    fig.add_trace(go.Scatter(x=h.index, y=h["Signal"], name="Signal",
        line=dict(color="#ff79c6", width=1.2)), row=2, col=1)

    # Panel 3: RSI + Stochastic
    fig.add_trace(go.Scatter(x=h.index, y=h["RSI"], name="RSI(14)",
        line=dict(color="#bd93f9", width=1.2)), row=3, col=1)
    fig.add_trace(go.Scatter(x=h.index, y=h["Stoch_K"], name="Stoch%K",
        line=dict(color="#f1fa8c", width=1.0, dash="dot")), row=3, col=1)
    fig.add_hline(y=70, line_dash="dash", line_color="#ff5555", opacity=0.5, row=3, col=1)
    fig.add_hline(y=30, line_dash="dash", line_color="#50fa7b", opacity=0.5, row=3, col=1)

    # Panel 4: ADX + DI
    fig.add_trace(go.Scatter(x=h.index, y=h["ADX"], name="ADX",
        line=dict(color="#ffb86c", width=1.4)), row=4, col=1)
    fig.add_trace(go.Scatter(x=h.index, y=h["DI_Plus"], name="DI+",
        line=dict(color="#50fa7b", width=0.9), opacity=0.8), row=4, col=1)
    fig.add_trace(go.Scatter(x=h.index, y=h["DI_Minus"], name="DI−",
        line=dict(color="#ff5555", width=0.9), opacity=0.8), row=4, col=1)
    fig.add_hline(y=25, line_dash="dash", line_color="white", opacity=0.3, row=4, col=1)

    # Panel 5: OBV
    fig.add_trace(go.Scatter(x=h.index, y=h["OBV"], name="OBV",
        line=dict(color="#8be9fd", width=1.2)), row=5, col=1)
    fig.add_trace(go.Scatter(x=h.index, y=h["OBV_SMA"], name="SMA20 OBV",
        line=dict(color="#ff79c6", width=1.0, dash="dash")), row=5, col=1)

    fig.update_layout(
        title=dict(
            text=f"{ticker} — Análisis Técnico | Score: {score}/10 | {veredicto}",
            font=dict(color=color_v, size=16),
        ),
        template="plotly_dark",
        paper_bgcolor="#12121f",
        plot_bgcolor="#1e1e2e",
        height=1000,
        showlegend=True,
        legend=dict(orientation="h", y=-0.02, font=dict(size=9)),
        hovermode="x unified",
    )

    for i in range(1, 6):
        fig.update_yaxes(gridcolor="rgba(255,255,255,0.05)", row=i, col=1)
        fig.update_xaxes(gridcolor="rgba(255,255,255,0.05)", row=i, col=1)

    return fig


# ═════════════════════════════════════════════════════════════════
# 5. INTERFAZ STREAMLIT
# ═════════════════════════════════════════════════════════════════

# ── Sidebar ──
with st.sidebar:
    st.title("📈 Análisis Individual")
    st.caption("Master IA Sector Financiero — VIU")
    st.divider()

    ticker = st.text_input("Ticker a analizar", value="MSFT",
        help="Cualquier ticker de yfinance: AAPL, MAIRE.MI, SAN.MC, GCT...")

    tab_select = st.radio("Vista", ["🔧 Técnico", "📋 Fundamental", "🔧 + 📋 Completo"])

    st.divider()
    analizar = st.button("🚀 Analizar", type="primary", use_container_width=True)


# ── Contenido principal ──
if analizar and ticker:
    ticker = ticker.upper().strip()

    with st.spinner(f"Descargando 2 años de datos para {ticker}..."):
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="2y")
            info = stock.info

            try:
                fin = stock.financials
                bs = stock.balance_sheet
                cf = stock.cashflow
            except:
                fin = bs = cf = pd.DataFrame()

        except Exception as e:
            st.error(f"Error descargando datos: {e}")
            st.stop()

    if hist.empty or len(hist) < 100:
        st.error(f"Datos insuficientes para {ticker} ({len(hist)} sesiones).")
        st.stop()

    # ── Info cabecera ──
    nombre = info.get("longName") or info.get("shortName", ticker)
    precio = hist["Close"].iloc[-1]
    moneda = info.get("currency", "")

    st.header(f"🏢 {nombre} ({ticker})")
    hc1, hc2, hc3, hc4 = st.columns(4)
    hc1.metric("Precio", f"{precio:.2f} {moneda}")
    hc2.metric("Sector", info.get("sector", "N/A"))
    hc3.metric("Market Cap", f"${info.get('marketCap', 0)/1e9:.2f}B")
    hc4.metric("Industria", info.get("industry", "N/A"))

    # ══════════════════════════════════════════════
    # ANÁLISIS TÉCNICO
    # ══════════════════════════════════════════════
    if tab_select in ["🔧 Técnico", "🔧 + 📋 Completo"]:
        st.divider()
        st.subheader("🔧 Análisis Técnico — 7 Indicadores de Baja Correlación")

        with st.spinner("Calculando indicadores..."):
            hist["RSI"] = calc_rsi(hist["Close"])
            hist["SMA_50"] = hist["Close"].rolling(50).mean()
            hist["SMA_200"] = hist["Close"].rolling(200).mean()

            bb_up, bb_mid, bb_low, pct_b = calc_bollinger(hist["Close"])
            hist["BB_Up"], hist["BB_Mid"], hist["BB_Low"], hist["BB_PctB"] = bb_up, bb_mid, bb_low, pct_b

            macd, sig, hist_macd = calc_macd(hist["Close"])
            hist["MACD"], hist["Signal"], hist["MACD_Hist"] = macd, sig, hist_macd
            hist["MACD_Hist_prev"] = hist["MACD_Hist"].shift(1)

            adx, di_plus, di_minus = calc_adx(hist)
            hist["ADX"] = adx.values
            hist["DI_Plus"] = di_plus.values
            hist["DI_Minus"] = di_minus.values

            obv, obv_sma, obv_trend, obv_div = calc_obv(hist)
            hist["OBV"], hist["OBV_SMA"] = obv, obv_sma

            k, d = calc_stochastic(hist)
            hist["Stoch_K"], hist["Stoch_D"] = k, d

            atr, atr_pct = calc_atr(hist)
            hist["ATR_PCT"] = atr_pct

        # Score
        last = hist.iloc[-1]
        score, detalle = calcular_score_tecnico(last, obv_trend.iloc[-1], obv_div.iloc[-1])
        veredicto, color_v = interpretar_score(score)

        # ── Score visual ──
        sc1, sc2, sc3 = st.columns([1, 2, 1])
        with sc1:
            st.metric("SCORE TÉCNICO", f"{score}/10")
        with sc2:
            st.markdown(f"### {veredicto}")
        with sc3:
            sma200_ok = "✅ Alcista" if precio > last["SMA_200"] else "🔴 Bajista"
            sma50_ok = "✅ Alcista" if precio > last["SMA_50"] else "🔴 Bajista"
            st.caption(f"Tendencia LP (SMA200): {sma200_ok}")
            st.caption(f"Tendencia MP (SMA50): {sma50_ok}")
            st.caption(f"Volatilidad (ATR%): {last['ATR_PCT']:.2f}%")

        # ── Tabla desglose por indicador ──
        st.markdown("**Desglose por indicador:**")
        for ind, datos in detalle.items():
            pct = datos["pts"] / datos["max"] if datos["max"] > 0 else 0
            icono = "✅" if pct >= 0.7 else ("🟡" if pct >= 0.3 else "🔴")
            st.markdown(
                f"{icono} **{ind}** — {datos['val']} — "
                f"`{datos['pts']:.1f}/{datos['max']:.1f}` — {datos['msg']}"
            )

        target = info.get("targetMeanPrice")
        if target:
            pot = ((target / precio) - 1) * 100
            st.markdown(f"🎯 **Target analistas:** {target:.2f} {moneda} ({pot:+.1f}% potencial)")

        # ── Gráfico 5 paneles ──
        fig_tec = grafico_tecnico_plotly(hist, ticker, score, veredicto, color_v)
        st.plotly_chart(fig_tec, use_container_width=True)

    # ══════════════════════════════════════════════
    # ANÁLISIS FUNDAMENTAL
    # ══════════════════════════════════════════════
    if tab_select in ["📋 Fundamental", "🔧 + 📋 Completo"]:
        st.divider()
        st.subheader("📋 Análisis Fundamental Completo")

        # ── BLOQUE 1: VALORACIÓN ──
        st.markdown("### 💰 Valoración — Múltiplos de Precio")
        per = info.get("trailingPE")
        per_fw = info.get("forwardPE")
        peg = info.get("pegRatio")
        pb = info.get("priceToBook")
        ps = info.get("priceToSalesTrailing12Months")
        ev_ebitda = info.get("enterpriseToEbitda")
        graham_num = calcular_graham_number(info)
        fcf_yield = calcular_fcf_yield(info, cf)

        def metric_fund(nombre, val, fmt, bueno, malo):
            if val is None: return f"**{nombre}:** N/A"
            ic = "✅" if bueno(val) else ("🔴" if malo(val) else "🟡")
            return f"{ic} **{nombre}:** {val:{fmt}} "

        v1, v2, v3 = st.columns(3)
        with v1:
            st.markdown(metric_fund("PER Trailing", per, ".1f", lambda x: x<15, lambda x: x>30))
            st.markdown(metric_fund("PER Forward", per_fw, ".1f", lambda x: x<12, lambda x: x>25))
        with v2:
            st.markdown(metric_fund("PEG", peg, ".2f", lambda x: x<1, lambda x: x>2))
            st.markdown(metric_fund("P/Book", pb, ".2f", lambda x: x<1.5, lambda x: x>5))
        with v3:
            st.markdown(metric_fund("P/Ventas", ps, ".2f", lambda x: x<2, lambda x: x>10))
            st.markdown(metric_fund("EV/EBITDA", ev_ebitda, ".1f", lambda x: x<10, lambda x: x>20))

        if fcf_yield:
            ic = "✅" if fcf_yield > 5 else ("🔴" if fcf_yield < 0 else "🟡")
            st.markdown(f"{ic} **FCF Yield:** {fcf_yield:.2f}% (> 5% = atractivo)")
        if graham_num and precio:
            dif = (precio / graham_num - 1) * 100
            ic = "✅ INFRAVALORADO" if precio < graham_num else "⚠️ SOBREVALORADO"
            st.markdown(f"**Número de Graham:** {graham_num:.2f} {moneda} → precio {dif:+.1f}% vs Graham — {ic}")

        target_m = info.get("targetMeanPrice")
        target_low = info.get("targetLowPrice")
        target_hi = info.get("targetHighPrice")
        if target_m and precio:
            upside = (target_m / precio - 1) * 100
            ic = "✅" if upside > 10 else ("🔴" if upside < -10 else "🟡")
            st.markdown(f"{ic} **Target Analistas:** {target_m:.2f} {moneda} ({upside:+.1f}% potencial)")
            if target_low and target_hi:
                st.caption(f"Rango: {target_low:.2f} – {target_hi:.2f} {moneda} | "
                           f"Analistas: {info.get('numberOfAnalystOpinions', 'N/A')} | "
                           f"Consenso: {(info.get('recommendationKey') or '').upper()}")

        # ── BLOQUE 2: RENTABILIDAD & CALIDAD ──
        st.markdown("### 📈 Rentabilidad & Calidad")
        roe = info.get("returnOnEquity")
        roa = info.get("returnOnAssets")
        roic_v = calcular_roic(fin, bs)
        gm = info.get("grossMargins")
        om = info.get("operatingMargins")
        pm = info.get("profitMargins")
        dupont = calcular_dupont(fin, bs)
        cagr_r, cagr_n = calcular_cagr_historico(fin)

        r1, r2, r3 = st.columns(3)
        with r1:
            st.markdown(metric_fund("ROE", roe*100 if roe else None, ".1f%", lambda x: x>15, lambda x: x<5))
            st.markdown(metric_fund("ROA", roa*100 if roa else None, ".1f%", lambda x: x>8, lambda x: x<2))
            st.markdown(metric_fund("ROIC", roic_v, ".2f%", lambda x: x>15, lambda x: x<8))
        with r2:
            st.markdown(metric_fund("Margen Bruto", gm*100 if gm else None, ".1f%", lambda x: x>40, lambda x: x<20))
            st.markdown(metric_fund("Margen Operativo", om*100 if om else None, ".1f%", lambda x: x>15, lambda x: x<5))
            st.markdown(metric_fund("Margen Neto", pm*100 if pm else None, ".1f%", lambda x: x>15, lambda x: x<3))
        with r3:
            if cagr_r is not None:
                ic = "✅" if cagr_r > 7 else ("🔴" if cagr_r < 0 else "🟡")
                st.markdown(f"{ic} **CAGR Ingresos:** {cagr_r:+.1f}%/año")
            if cagr_n is not None:
                ic = "✅" if cagr_n > 7 else ("🔴" if cagr_n < 0 else "🟡")
                st.markdown(f"{ic} **CAGR Benef. Neto:** {cagr_n:+.1f}%/año")

        # DuPont
        if dupont:
            st.markdown("**Descomposición DuPont (ROE):**")
            dc1, dc2, dc3, dc4 = st.columns(4)
            dc1.metric("ROE (DuPont)", f"{dupont['ROE']:.2f}%")
            dc2.metric("Margen Neto", f"{dupont['Margen_Neto']:.2f}%")
            dc3.metric("Rot. Activos", f"{dupont['Rot_Activos']:.3f}x")
            dc4.metric("Apalancamiento", f"{dupont['Apalanc']:.2f}x")

        # ── BLOQUE 3: SALUD FINANCIERA ──
        st.markdown("### 🏥 Salud Financiera & Riesgo")
        cr = info.get("currentRatio")
        qr = info.get("quickRatio")
        de = info.get("debtToEquity")
        pay = info.get("payoutRatio")

        s1, s2, s3 = st.columns(3)
        with s1:
            st.markdown(metric_fund("Ratio Corriente", cr, ".2f", lambda x: x>1.5, lambda x: x<1))
            st.markdown(metric_fund("Ratio Rápido", qr, ".2f", lambda x: x>1, lambda x: x<0.7))
        with s2:
            st.markdown(metric_fund("D/E", de, ".1f", lambda x: x<80, lambda x: x>200))
            if pay:
                st.markdown(metric_fund("Payout Ratio", pay*100, ".1f%", lambda x: x<60, lambda x: x>90))
        with s3:
            if not bs.empty and not fin.empty:
                z, z_zona = calcular_altman_z(info, fin, bs)
                if z is not None:
                    st.markdown(f"**Altman Z-Score:** {z} → {z_zona}")

        # ── BLOQUE 4: PIOTROSKI F-SCORE ──
        if not fin.empty and not bs.empty and not cf.empty:
            st.markdown("### 🔢 Piotroski F-Score (0–9)")
            f_score, f_det = calcular_piotroski(fin, bs, cf)

            if   f_score >= 7: ic_f = "🟢"; txt_f = "EMPRESA SÓLIDA — señal VALUE positiva"
            elif f_score >= 4: ic_f = "🟡"; txt_f = "EMPRESA MEDIA — neutral"
            else:              ic_f = "🔴"; txt_f = "EMPRESA DÉBIL — precaución"

            st.markdown(f"### {ic_f} F-SCORE: {f_score}/9 → {txt_f}")
            for crit, vals in f_det.items():
                if crit.startswith("_"): continue
                ic = "✅" if vals["ok"] else "❌"
                st.markdown(f"{ic} {crit} — `{vals['val']}`")

        # ── BLOQUE 5: DIVIDENDOS ──
        dy = info.get("dividendYield")
        if dy and dy > 0:
            st.markdown("### 💵 Dividendos")
            d1, d2, d3 = st.columns(3)
            ic_dy = "✅" if dy > 0.03 else "🟡"
            d1.markdown(f"{ic_dy} **Dividend Yield:** {dy*100:.2f}%")
            dr = info.get("dividendRate")
            if dr: d2.markdown(f"**Pago anual/acción:** {dr:.2f} {moneda}")
            if pay:
                ic_p = "✅ Sostenible" if pay < 0.6 else "⚠️ Elevado"
                d3.markdown(f"**Payout:** {pay*100:.1f}% — {ic_p}")

        # ── BLOQUE 6: VEREDICTO FUNDAMENTAL ──
        st.markdown("### 🏆 Veredicto Fundamental")
        pts_f = 0; mx_f = 0
        if per:     mx_f += 2; pts_f += (2 if per < 15  else 1 if per < 25 else 0)
        if roe:     mx_f += 2; pts_f += (2 if roe > 0.20 else 1 if roe > 0.10 else 0)
        if pm:      mx_f += 2; pts_f += (2 if pm > 0.15  else 1 if pm > 0.05  else 0)
        if de is not None: mx_f += 2; pts_f += (2 if de < 80 else 1 if de < 150 else 0)
        if not fin.empty and not bs.empty and not cf.empty:
            mx_f += 2
            pts_f += (2 if f_score >= 7 else 1 if f_score >= 4 else 0)

        if mx_f > 0:
            pct_fund = pts_f / mx_f
            if   pct_fund >= 0.75: vf = "🟢 FUNDAMENTALMENTE SÓLIDA"; c_vf = "green"
            elif pct_fund >= 0.45: vf = "🟡 FUNDAMENTALMENTE ACEPTABLE"; c_vf = "orange"
            else:                  vf = "🔴 FUNDAMENTALMENTE DÉBIL"; c_vf = "red"
            st.markdown(f"**{vf}** — Puntuación: {pts_f}/{mx_f} ({pct_fund*100:.0f}%)")

            # Barra visual
            st.progress(pct_fund, text=f"{pts_f}/{mx_f}")
        else:
            st.warning("Datos insuficientes para emitir veredicto fundamental.")

elif not analizar:
    st.info("👈 Introduce un ticker en la barra lateral y pulsa **Analizar**.")
    st.markdown(
        """
        **Este módulo incluye:**

        **Análisis Técnico** — 7 indicadores de baja correlación (RSI, MACD, ADX, OBV,
        Stochastic, Bollinger %B, ATR) con scoring ponderado 0–10 y gráfico de 5 paneles.

        **Análisis Fundamental** — 6 bloques: Valoración (PER, PEG, EV/EBITDA, Graham, FCF Yield),
        Rentabilidad (ROE, ROIC, DuPont, CAGR), Salud Financiera (Altman Z-Score, ratios de liquidez),
        Piotroski F-Score (0–9), Dividendos, y Veredicto Final.
        """
    )
