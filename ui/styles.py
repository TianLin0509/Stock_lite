"""全局 CSS 样式 — 浅色可爱风 + 移动端响应式"""

import streamlit as st


def inject_css():
    """注入全局 CSS 样式"""
    # 异步加载 Google Fonts（不阻塞首屏渲染）
    st.markdown(
        '<link rel="preconnect" href="https://fonts.googleapis.com">'
        '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>'
        '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?'
        'family=Nunito:wght@600;700;800&family=Noto+Sans+SC:wght@400;500;700'
        '&display=swap" media="print" onload="this.media=\'all\'">',
        unsafe_allow_html=True,
    )
    st.markdown("""
<style>
/* Google Fonts：异步加载，不阻塞首屏渲染 */

:root {
  --bg:        #f6f8ff;
  --bg-card:   #ffffff;
  --bg-soft:   #eef2ff;
  --border:    #dde3ff;
  --up:        #22c55e;
  --down:      #ef4444;
  --blue:      #6366f1;
  --blue-lt:   #eef2ff;
  --pink:      #ec4899;
  --pink-lt:   #fdf2f8;
  --teal:      #06b6d4;
  --orange:    #f97316;
  --orange-lt: #fff7ed;
  --purple:    #a855f7;
  --purple-lt: #faf5ff;
  --text:      #1e1b4b;
  --text-mid:  #6b7280;
  --text-lo:   #9ca3af;
  --shadow:    0 2px 16px rgba(99,102,241,0.08);
  --shadow-md: 0 4px 24px rgba(99,102,241,0.12);
  --radius:    16px;
  --radius-sm: 10px;
}

/* 禁用 Streamlit rerun 时旧内容的半透明残影效果 */
[data-stale="true"] { opacity: 1 !important; }

/* Spinner 文字：不换行，省略号截断 */
.stSpinner > div {
  white-space: nowrap !important;
  overflow: hidden !important;
  text-overflow: ellipsis !important;
  font-size: 0.82rem !important;
}

/* 动态加载点动画 */
.loading-dots::after {
  content: '';
  animation: dots 1.5s steps(4, end) infinite;
}
@keyframes dots {
  0%   { content: ''; }
  25%  { content: '.'; }
  50%  { content: '..'; }
  75%  { content: '...'; }
  100% { content: ''; }
}

/* ═══════════════════════════════════════════════════════════════
   搜索行：输入框 + 一键分析 + 重置（同一行，统一风格）
   用 :has(.stTextInput, .stSelectbox) 精确锁定，不影响其他按钮行
   ═══════════════════════════════════════════════════════════════ */
[data-testid="stHorizontalBlock"]:has(.stTextInput, .stSelectbox) {
  background: transparent !important;
  border: none !important;
  box-shadow: none !important;
  padding: 0 !important;
  margin-left: 0 !important;
  width: 100% !important;
  border-radius: 0 !important;
  align-items: flex-end !important;
  display: flex !important;
  flex-wrap: nowrap !important;
  gap: 8px !important;
}
/* 桌面端搜索行 flex 比例 */
[data-testid="stHorizontalBlock"]:has(.stTextInput, .stSelectbox) > div:first-child {
  flex: 4 1 0 !important;
  min-width: 0 !important;
}
[data-testid="stHorizontalBlock"]:has(.stTextInput, .stSelectbox) > div:nth-child(2) {
  flex: 1.5 1 0 !important;
  min-width: 0 !important;
}
[data-testid="stHorizontalBlock"]:has(.stTextInput, .stSelectbox) > div:nth-child(3) {
  flex: 1 1 0 !important;
  min-width: 0 !important;
}
[data-testid="stHorizontalBlock"]:has(.stTextInput, .stSelectbox)::before {
  display: none !important;
}
/* 搜索行按钮：与输入框统一的 border 风格 */
[data-testid="stHorizontalBlock"]:has(.stTextInput, .stSelectbox) .stButton button {
  border-radius: 50px !important;
  border: 2px solid var(--border) !important;
  background: var(--bg-card) !important;
  color: var(--text) !important;
  font-weight: 600 !important;
  font-size: 0.88rem !important;
  padding: 0.48rem 1.2rem !important;
  min-height: unset !important;
  white-space: nowrap !important;
  box-shadow: none !important;
  transition: all 0.2s ease !important;
}
/* 桌面悬浮 */
[data-testid="stHorizontalBlock"]:has(.stTextInput, .stSelectbox) .stButton button:hover {
  border-color: var(--blue) !important;
  color: var(--blue) !important;
  box-shadow: 0 0 0 3px rgba(99,102,241,0.10) !important;
  transform: translateY(-1px) !important;
}
/* 触摸按下（桌面+手机通用） */
[data-testid="stHorizontalBlock"]:has(.stTextInput, .stSelectbox) .stButton button:active {
  transform: scale(0.97) !important;
  border-color: var(--blue) !important;
  color: var(--blue) !important;
  box-shadow: 0 0 0 3px rgba(99,102,241,0.12) !important;
}

/* ═══════════════════════════════════════════════════════════════
   全局基础
   ═══════════════════════════════════════════════════════════════ */
html, body, [data-testid="stAppViewContainer"] {
  background: var(--bg) !important;
  font-family: 'Noto Sans SC', 'PingFang SC', -apple-system, sans-serif;
  color: var(--text);
}
/* 避免 Streamlit 顶部固定 header 遮挡页面内容 */
[data-testid="stHeader"] { background: transparent !important; }
.block-container { padding-top: 3rem !important; }
[data-testid="stSidebar"] {
  background: #ffffff !important;
  border-right: 1px solid var(--border) !important;
}
[data-testid="stSidebar"] * { color: var(--text-mid) !important; }
[data-testid="stSidebar"] h3,
[data-testid="stSidebar"] strong { color: var(--blue) !important; }

/* App header */
.app-header {
  background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 50%, #ec4899 100%);
  border-radius: var(--radius);
  padding: 1.6rem 2rem;
  margin-bottom: 1.2rem;
  position: relative; overflow: hidden;
  box-shadow: 0 8px 32px rgba(99,102,241,0.25);
}
.app-header::before {
  content: '📊 📈 💹 📉 🏦';
  position: absolute; top: 12px; right: 8px;
  font-size: 1.4rem; opacity: 0.13;
  white-space: nowrap; letter-spacing: 0.6em;
}
.app-header h1 {
  font-family: 'Nunito', sans-serif;
  font-size: 1.9rem; font-weight: 800;
  color: #fff; margin: 0;
  text-shadow: 0 2px 8px rgba(0,0,0,0.15);
}
.app-header p {
  color: rgba(255,255,255,0.82);
  font-size: 0.86rem; margin: 0.35rem 0 0;
  font-weight: 500;
}

/* Model status badge */
.model-badge {
  display: inline-flex; align-items: center; gap: 6px;
  background: var(--bg-soft);
  border: 1.5px solid var(--border);
  border-radius: 50px;
  padding: 4px 14px;
  font-size: 0.82rem; font-weight: 600;
  color: var(--blue);
  margin-bottom: 1rem;
}
.model-badge.ok   { background: #f0fdf4; border-color: #86efac; color: #16a34a; }
.model-badge.warn { background: #fff7ed; border-color: #fed7aa; color: #c2410c; }
.model-badge.err  { background: #fef2f2; border-color: #fca5a5; color: #dc2626; }

/* Status banner */
.status-banner {
  border-radius: var(--radius-sm);
  padding: 0.75rem 1.1rem;
  margin: 0.5rem 0;
  font-size: 0.86rem;
  line-height: 1.6;
  display: flex; align-items: flex-start; gap: 10px;
}
.status-banner.info    { background: var(--blue-lt);   border: 1px solid #c7d2fe; color: #3730a3; }
.status-banner.warn    { background: #fff7ed;           border: 1px solid #fed7aa; color: #92400e; }
.status-banner.error   { background: #fef2f2;           border: 1px solid #fca5a5; color: #991b1b; }
.status-banner.success { background: #f0fdf4;           border: 1px solid #86efac; color: #14532d; }

/* Cards */
.card {
  background: var(--bg-card);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 1.4rem 1.6rem;
  margin: 0.7rem 0;
  box-shadow: var(--shadow);
}

/* ═══════════════════════════════════════════════════════════════
   Tabs — 带右侧渐变遮罩提示可滚动
   ═══════════════════════════════════════════════════════════════ */
.stTabs {
  position: relative;
}
.stTabs [data-baseweb="tab-list"] {
  background: var(--bg-card) !important;
  border-radius: 50px !important;
  padding: 4px !important;
  border: 1px solid var(--border) !important;
  gap: 2px !important;
  box-shadow: var(--shadow) !important;
  width: fit-content !important;
}
.stTabs [data-baseweb="tab"] {
  border-radius: 50px !important;
  font-family: 'Nunito', sans-serif !important;
  font-weight: 600 !important;
  font-size: 0.85rem !important;
  color: var(--text-mid) !important;
  padding: 6px 20px !important;
}
.stTabs [aria-selected="true"] {
  background: linear-gradient(135deg, var(--blue), var(--purple)) !important;
  color: #fff !important;
  box-shadow: 0 2px 10px rgba(99,102,241,0.3) !important;
}

/* Analysis wrap */
.analysis-wrap {
  background: var(--bg-card); border: 1px solid var(--border);
  border-radius: var(--radius); padding: 1.6rem 1.8rem;
  box-shadow: var(--shadow); line-height: 1.8; font-size: 0.92rem;
}

/* ═══════════════════════════════════════════════════════════════
   Buttons — 通用
   ═══════════════════════════════════════════════════════════════ */
.stButton button {
  border-radius: 50px !important;
  font-family: 'Nunito', sans-serif !important;
  font-weight: 700 !important; font-size: 0.88rem !important;
  padding: 0.5rem 1.4rem !important;
}
.stButton button[kind="primary"] {
  background: linear-gradient(135deg, #3b82f6 0%, #06b6d4 100%) !important;
  border: none !important; color: #fff !important;
  box-shadow: 0 3px 12px rgba(59,130,246,0.3) !important;
}
.stButton button[kind="primary"]:hover {
  transform: translateY(-1px) !important;
  box-shadow: 0 5px 18px rgba(59,130,246,0.4) !important;
}

/* ═══════════════════════════════════════════════════════════════
   操作按钮行（预期差/趋势/基本面）
   用 :not(:has(.stTextInput, .stSelectbox)) 排除搜索行，避免选择器冲突
   ═══════════════════════════════════════════════════════════════ */
[data-testid="stHorizontalBlock"]:has(.stButton):not(:has(.stTextInput, .stSelectbox)) {
  background: var(--bg-card) !important;
  border: 1px solid var(--border) !important;
  border-radius: 50px !important;
  padding: 3px !important;
  gap: 1px !important;
  box-shadow: var(--shadow) !important;
  width: fit-content !important;
  margin-left: 8px !important;
  position: relative;
}
/* 左侧小竖线 */
[data-testid="stHorizontalBlock"]:has(.stButton):not(:has(.stTextInput, .stSelectbox))::before {
  content: "";
  position: absolute;
  left: -6px; top: 25%; height: 50%; width: 2px;
  border-radius: 2px;
  background: linear-gradient(180deg, var(--blue), var(--purple));
  opacity: 0.45;
}
[data-testid="stHorizontalBlock"]:has(.stButton):not(:has(.stTextInput, .stSelectbox)) .stButton button {
  border-radius: 50px !important;
  font-family: 'Nunito', sans-serif !important;
  font-weight: 600 !important;
  font-size: 0.78rem !important;
  padding: 5px 12px !important;
  min-height: unset !important;
  white-space: nowrap !important;
  transition: all 0.15s ease !important;
}
/* 悬浮效果 */
[data-testid="stHorizontalBlock"]:has(.stButton):not(:has(.stTextInput, .stSelectbox)) .stButton button:hover {
  transform: scale(1.05) !important;
  z-index: 2 !important;
  position: relative !important;
}
[data-testid="stHorizontalBlock"]:has(.stButton):not(:has(.stTextInput, .stSelectbox)) .stButton button:not([kind="primary"]) {
  background: transparent !important;
  border: none !important;
  color: var(--text-mid) !important;
  box-shadow: none !important;
}
[data-testid="stHorizontalBlock"]:has(.stButton):not(:has(.stTextInput, .stSelectbox)) .stButton button:not([kind="primary"]):hover {
  background: var(--bg-soft) !important;
  color: var(--blue) !important;
  box-shadow: 0 2px 8px rgba(99,102,241,0.15) !important;
}
[data-testid="stHorizontalBlock"]:has(.stButton):not(:has(.stTextInput, .stSelectbox)) .stButton button[kind="primary"] {
  box-shadow: 0 2px 8px rgba(99,102,241,0.25) !important;
}
[data-testid="stHorizontalBlock"]:has(.stButton):not(:has(.stTextInput, .stSelectbox)) .stButton button[kind="primary"]:hover {
  box-shadow: 0 4px 14px rgba(99,102,241,0.4) !important;
}

/* ═══════════════════════════════════════════════════════════════
   Input / Select / Metrics
   ═══════════════════════════════════════════════════════════════ */
.stTextInput input {
  border-radius: 50px !important;
  border: 2px solid var(--border) !important;
  background: var(--bg-card) !important;
  padding: 0.55rem 1.2rem !important;
  font-size: 0.95rem !important;
}
.stTextInput input:focus {
  border-color: var(--blue) !important;
  box-shadow: 0 0 0 3px rgba(99,102,241,0.12) !important;
}
.stSelectbox [data-baseweb="select"] > div {
  border-radius: var(--radius-sm) !important;
  border-color: var(--border) !important;
  background: var(--bg-card) !important;
}
[data-testid="metric-container"] {
  background: var(--bg-card) !important;
  border: 1px solid var(--border) !important;
  border-radius: var(--radius-sm) !important;
  padding: 0.9rem 1rem !important;
  box-shadow: var(--shadow) !important;
}
[data-testid="stMetricLabel"] {
  font-size: 0.72rem !important; color: var(--text-lo) !important;
  font-weight: 600 !important; letter-spacing: 0.05em !important;
  text-transform: uppercase !important;
}
[data-testid="stMetricValue"] {
  font-family: 'Nunito', sans-serif !important;
  font-size: 1.15rem !important; font-weight: 800 !important;
  color: var(--text) !important;
}

/* Disclaimer */
.disclaimer {
  background: #fff7ed; border: 1px solid #fed7aa;
  border-radius: var(--radius-sm); padding: 0.7rem 1rem;
  font-size: 0.78rem; color: #c2410c; margin-top: 0.8rem; line-height: 1.6;
}
hr { border-color: var(--border) !important; margin: 1rem 0 !important; }

/* Token badge */
.token-badge {
  position: fixed; top: 12px; right: 20px; z-index: 9999;
  background: rgba(99, 102, 241, 0.9); color: white;
  padding: 4px 14px; border-radius: 20px;
  font-size: 0.78em; font-weight: 600;
  backdrop-filter: blur(8px); box-shadow: 0 2px 8px rgba(0,0,0,0.15);
}

/* ═══════════════════════════════════════════════════════════════
   📱 MOBILE RESPONSIVE — 768px 以下生效
   ═══════════════════════════════════════════════════════════════ */

@media (max-width: 768px) {

  /* ── 全局：更紧凑的间距（保留顶部空间避免遮挡） ── */
  .block-container {
    padding: 3rem 0.8rem 0.5rem !important;
  }

  /* ── 全局文本：防止长串溢出 ── */
  .stMarkdown, .stMarkdown p, .stMarkdown li,
  .role-content, .analysis-wrap,
  [data-testid="stMarkdownContainer"] {
    overflow-wrap: break-word !important;
    word-break: break-word !important;
    hyphens: auto;
  }

  /* ── st.container(border=True) 内边距缩小 ── */
  [data-testid="stVerticalBlock"] > [data-testid="stVerticalBlockBorderWrapper"] {
    padding: 0.6rem 0.7rem !important;
  }

  /* ── Header：缩小字号和内边距 ── */
  .app-header {
    padding: 1rem 1.2rem;
    border-radius: 12px;
    margin-bottom: 0.8rem;
  }
  .app-header h1 { font-size: 1.3rem; }
  .app-header p { font-size: 0.76rem; }
  .app-header::before { display: none; }

  /* ── 分析内容区：手机端缩小内边距 ── */
  .analysis-wrap {
    padding: 1rem 1rem !important;
    font-size: 0.86rem !important;
  }

  /* ══════════════════════════════════════════════════════════
     搜索行：手机端保持三元素同行，不堆叠
     ══════════════════════════════════════════════════════════ */
  [data-testid="stHorizontalBlock"]:has(.stTextInput, .stSelectbox) {
    display: flex !important;
    flex-direction: row !important;
    flex-wrap: nowrap !important;
    align-items: flex-end !important;
    gap: 6px !important;
  }
  /* 输入框与按钮比例：给按钮更多空间，文字不拥挤 */
  [data-testid="stHorizontalBlock"]:has(.stTextInput, .stSelectbox) > div:first-child {
    flex: 2.5 1 0 !important;
    min-width: 0 !important;
    width: 0 !important;
  }
  [data-testid="stHorizontalBlock"]:has(.stTextInput, .stSelectbox) > div:nth-child(2) {
    flex: 1.8 1 0 !important;
    min-width: 0 !important;
    width: 0 !important;
  }
  [data-testid="stHorizontalBlock"]:has(.stTextInput, .stSelectbox) > div:nth-child(3) {
    flex: 1 1 0 !important;
    min-width: 0 !important;
    width: 0 !important;
  }
  .stTextInput input {
    font-size: 0.9rem !important;
    padding: 0.5rem 0.8rem !important;
    border-radius: 12px !important;
  }
  /* 搜索行按钮手机适配 */
  [data-testid="stHorizontalBlock"]:has(.stTextInput, .stSelectbox) .stButton button {
    border-radius: 12px !important;
    min-height: 42px !important;
    font-size: 0.78rem !important;
    padding: 0.4rem 0.5rem !important;
    border-width: 1.5px !important;
    -webkit-tap-highlight-color: transparent !important;
  }
  /* 手机触摸：模拟桌面 hover 的蓝色边框特效 */
  [data-testid="stHorizontalBlock"]:has(.stTextInput, .stSelectbox) .stButton button:active {
    border-color: var(--blue) !important;
    color: var(--blue) !important;
    box-shadow: 0 0 0 3px rgba(99,102,241,0.15) !important;
    background: var(--blue-lt) !important;
    transform: scale(0.97) !important;
  }

  /* ── 通用按钮手机适配 ── */
  .stButton button {
    font-size: 0.82rem !important;
    padding: 0.55rem 0.8rem !important;
    border-radius: 12px !important;
    min-height: 44px !important;
  }

  /* ── 指标卡片 ── */
  [data-testid="metric-container"] {
    padding: 0.55rem 0.65rem !important;
    border-radius: 8px !important;
  }
  [data-testid="stMetricLabel"] {
    font-size: 0.62rem !important;
    letter-spacing: 0 !important;
  }
  [data-testid="stMetricValue"] {
    font-size: 0.92rem !important;
  }

  /* ══════════════════════════════════════════════════════════
     Tabs：可滚动 + 右侧渐变遮罩暗示更多内容
     ══════════════════════════════════════════════════════════ */
  .stTabs {
    position: relative;
  }
  .stTabs::after {
    content: "";
    position: absolute;
    top: 0; right: 0;
    width: 40px; height: 40px;
    background: linear-gradient(to left, var(--bg) 30%, transparent);
    pointer-events: none;
    z-index: 2;
    border-radius: 0 12px 12px 0;
  }
  .stTabs [data-baseweb="tab-list"] {
    border-radius: 12px !important;
    overflow-x: auto !important;
    -webkit-overflow-scrolling: touch;
    scrollbar-width: none;
    width: 100% !important;
    flex-wrap: nowrap !important;
  }
  .stTabs [data-baseweb="tab-list"]::-webkit-scrollbar {
    display: none;
  }
  .stTabs [data-baseweb="tab"] {
    font-size: 0.75rem !important;
    padding: 5px 12px !important;
    white-space: nowrap !important;
    flex-shrink: 0 !important;
  }

  /* ── 状态横幅 ── */
  .status-banner {
    font-size: 0.78rem;
    padding: 0.6rem 0.85rem;
    border-radius: 8px;
    flex-direction: column;
    gap: 4px;
  }

  /* ── Model badge ── */
  .model-badge { font-size: 0.74rem; padding: 3px 10px; }

  /* ── 分析内容中的表格：横向可滚动 ── */
  .stMarkdown table,
  [data-testid="stContainer"] table,
  [data-testid="stMarkdownContainer"] table {
    display: block;
    overflow-x: auto;
    -webkit-overflow-scrolling: touch;
    white-space: nowrap;
    font-size: 0.76rem;
    max-width: 100%;
  }
  .stMarkdown table th,
  .stMarkdown table td,
  [data-testid="stMarkdownContainer"] th,
  [data-testid="stMarkdownContainer"] td {
    padding: 4px 8px !important;
    font-size: 0.76rem !important;
  }

  /* ── 免责声明 ── */
  .disclaimer { font-size: 0.72rem; padding: 0.55rem 0.8rem; }

  /* ── Plotly 图表：降低高度 ── */
  [data-testid="stPlotlyChart"] > div { max-height: 360px !important; }
  .js-plotly-plot .plotly .main-svg { max-height: 360px !important; }

  /* ── 进度条文字 ── */
  [data-testid="stProgressBarLabel"] { font-size: 0.76rem !important; }

  /* ── 分析标题缩小 ── */
  h4 { font-size: 1rem !important; }

  /* ══════════════════════════════════════════════════════════
     列布局自适应：窄屏自动堆叠
     但排除搜索行（保持横排）和指标行、图表行
     ══════════════════════════════════════════════════════════ */
  [data-testid="stHorizontalBlock"] {
    flex-wrap: wrap !important;
    gap: 0.3rem !important;
  }
  [data-testid="stHorizontalBlock"] > div[data-testid="column"] {
    flex: 1 1 100% !important;
    min-width: 100% !important;
    width: 100% !important;
  }

  /* ── 操作按钮行（预期差/趋势/基本面）：手机横排 ── */
  [data-testid="stHorizontalBlock"]:has(.stButton):not(:has(.stTextInput, .stSelectbox)) {
    display: flex !important;
    flex-direction: row !important;
    flex-wrap: nowrap !important;
    scrollbar-width: none;
    border-radius: 30px !important;
    padding: 2px !important;
    gap: 0 !important;
    width: 100% !important;
    margin-left: 0 !important;
    border: 1px solid var(--border) !important;
  }
  [data-testid="stHorizontalBlock"]:has(.stButton):not(:has(.stTextInput, .stSelectbox))::before {
    display: none !important;
  }
  [data-testid="stHorizontalBlock"]:has(.stButton):not(:has(.stTextInput, .stSelectbox))::-webkit-scrollbar {
    display: none;
  }
  [data-testid="stHorizontalBlock"]:has(.stButton):not(:has(.stTextInput, .stSelectbox)) > div[data-testid="column"],
  [data-testid="stHorizontalBlock"]:has(.stButton):not(:has(.stTextInput, .stSelectbox)) > div {
    flex: 1 1 0 !important;
    min-width: 0 !important;
    width: 0 !important;
  }
  [data-testid="stHorizontalBlock"]:has(.stButton):not(:has(.stTextInput, .stSelectbox)) .stButton button {
    font-size: 0.72rem !important;
    padding: 6px 4px !important;
    border-radius: 50px !important;
    white-space: nowrap !important;
    overflow: hidden !important;
    text-overflow: ellipsis !important;
    min-height: 36px !important;
  }
  /* 手机端触摸反馈：缩放而非放大（不溢出） */
  [data-testid="stHorizontalBlock"]:has(.stButton):not(:has(.stTextInput, .stSelectbox)) .stButton button:active {
    transform: scale(0.95) !important;
    background: var(--bg-soft) !important;
  }

  /* ── st.status 折叠态 ── */
  [data-testid="stStatusWidget"] { max-width: 100% !important; }
  [data-testid="stStatusWidget"] label {
    white-space: nowrap !important;
    overflow: hidden !important;
    text-overflow: ellipsis !important;
    font-size: 0.78rem !important;
  }

  /* ── 雷达图+评分条：手机端竖排 ── */
  [data-testid="stHorizontalBlock"]:has([data-testid="stPlotlyChart"]) {
    flex-direction: column !important;
  }
  [data-testid="stHorizontalBlock"]:has([data-testid="stPlotlyChart"]) > div[data-testid="column"] {
    flex: 1 1 100% !important;
    min-width: 100% !important;
  }

  /* ── 图表宽度 ── */
  .js-plotly-plot .plotly .main-svg { max-width: 100% !important; }

  /* ── caption 状态栏 ── */
  [data-testid="stCaptionContainer"] {
    font-size: 0.72rem !important;
    line-height: 1.4 !important;
  }

  /* ── 指标卡片：每行2个 ── */
  [data-testid="stHorizontalBlock"]:has([data-testid="metric-container"]) > div[data-testid="column"] {
    flex: 1 1 46% !important;
    min-width: 46% !important;
  }

  /* ── 侧边栏宽度限制 ── */
  [data-testid="stSidebar"] > div:first-child {
    width: 85vw !important;
    max-width: 300px !important;
  }

  /* ── Token badge：手机右下角，更清晰 ── */
  .token-badge {
    top: auto; bottom: 12px; right: 12px;
    font-size: 0.72em; padding: 4px 12px;
    opacity: 0.92;
    background: rgba(99, 102, 241, 0.85);
  }
}

/* ═══════════════════════════════════════════════════════════════
   📱 极窄屏幕（≤480px，小屏手机竖屏）
   ═══════════════════════════════════════════════════════════════ */
@media (max-width: 480px) {
  .app-header h1 { font-size: 1.1rem; }
  .app-header p { font-size: 0.68rem; }
  .app-header { padding: 0.75rem 0.9rem; }
  .block-container { padding: 3rem 0.5rem 0.3rem !important; }

  .stTabs [data-baseweb="tab"] {
    font-size: 0.68rem !important;
    padding: 4px 8px !important;
  }
  [data-testid="stMetricValue"] { font-size: 0.8rem !important; }
  [data-testid="stMetricLabel"] { font-size: 0.56rem !important; }
  /* 表格字号进一步缩小 */
  .stMarkdown table th, .stMarkdown table td,
  [data-testid="stMarkdownContainer"] th,
  [data-testid="stMarkdownContainer"] td {
    font-size: 0.7rem !important;
    padding: 3px 6px !important;
  }
  /* Plotly 图表更矮 */
  [data-testid="stPlotlyChart"] > div { max-height: 300px !important; }
  h4 { font-size: 0.92rem !important; }

  /* 极窄屏：指标完全单列 */
  [data-testid="stHorizontalBlock"]:has([data-testid="metric-container"]) > div[data-testid="column"] {
    flex: 1 1 100% !important;
    min-width: 100% !important;
  }
  /* 操作按钮行更紧凑 */
  [data-testid="stHorizontalBlock"]:has(.stButton):not(:has(.stTextInput, .stSelectbox)) .stButton button {
    font-size: 0.65rem !important;
    padding: 4px 2px !important;
  }
  /* 搜索行按钮更紧凑 */
  [data-testid="stHorizontalBlock"]:has(.stTextInput, .stSelectbox) .stButton button {
    font-size: 0.72rem !important;
    padding: 0.35rem 0.4rem !important;
    min-height: 38px !important;
  }
}
</style>
""", unsafe_allow_html=True)
