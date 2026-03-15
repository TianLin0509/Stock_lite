"""Tab: 🔮 玄学炒股 — 今日运势占卜"""

import time
import streamlit as st


def render_mystic_tab(client, cfg, selected_model):
    """渲染玄学炒股 Tab"""
    from datetime import datetime
    from ai.client import call_ai, get_ai_client

    # 重新获取 client 以确保最新状态
    client_m, cfg_m, _ = get_ai_client(selected_model)

    st.markdown("---")
    st.markdown("#### 🔮 玄学炒股 · 今日运势")

    today_str = datetime.now().strftime("%Y年%m月%d日")
    weekday = ["星期一", "星期二", "星期三", "星期四", "星期五", "星期六", "星期日"][datetime.now().weekday()]

    cached = st.session_state.get("_mystic_result", {})
    if cached.get("date") == today_str:
        st.markdown(cached["content"])
        return

    if not client_m:
        st.warning("请先在左侧配置 AI 模型")
        return

    with st.status("🔮 正卦象推演中...", expanded=True) as status:
        st.write("📅 获取今日日期与天干地支...")
        time.sleep(0.3)
        st.write("🌙 查询黄历宜忌...")
        time.sleep(0.25)
        st.write("🎴 抽取今日塔罗牌...")
        time.sleep(0.25)
        st.write("🐉 推算生肖与五行运势...")
        time.sleep(0.2)
        st.write("🔮 综合推演炒股运势，请虔诚等待...")

        stock_name = st.session_state.get("stock_name", "")
        stock_extra = f"\n\n用户当前关注的股票：{stock_name}，请也对这只股票给出玄学点评。" if stock_name else ""

        prompt = f"""今天是 {today_str} {weekday}。

请你扮演一位精通易经八卦、紫微斗数、塔罗牌、黄历、星座的玄学大师，为今日的A股炒股运势做一次趣味占卜。

请联网搜索今天的真实黄历信息（天干地支、宜忌、冲煞等），然后结合以下维度给出有趣的分析：

## 要求输出格式（用 emoji 让内容生动有趣）：

### 📅 今日黄历
- 农历日期、天干地支、值神
- 宜：xxx  忌：xxx

### 🎯 今日炒股运势评级
给出一个明确的等级：大吉 / 吉 / 小吉 / 中平 / 小凶 / 凶 / 大凶
并配上一句有趣的点评（模仿古人口吻）

### 🐉 五行与板块
根据今日五行旺衰，推荐适合的板块（如：火旺利军工光伏、水旺利航运水利等）
也指出今日五行克制、应回避的板块

### 🎴 塔罗牌指引
随机抽一张塔罗牌，解读其对今日炒股的启示

### ⏰ 吉时与凶时
给出今日适合买入/卖出的吉时（用十二时辰+现代时间对照）
给出应该避开操作的凶时

### 🎲 今日幸运数字 & 尾号
给出今日幸运数字，以及适合关注的股票代码尾号

### ⚠️ 玄学大师忠告
用一段文言文风格的话总结今日建议，最后加一句现代吐槽（制造反差萌）
{stock_extra}

**注意：这是趣味内容，请在最后用小字提醒用户"以上内容纯属娱乐，不构成投资建议，请理性投资"。**"""

        system = (
            "你是一位学贯中西的玄学大师，精通易经、紫微斗数、塔罗牌、西方星座，"
            "同时对A股市场有深入了解。你的风格：专业中带着幽默，神秘中带着接地气，"
            "古典与现代混搭。请联网搜索今天的真实黄历数据来增强可信度。"
        )

        result, err = call_ai(client_m, cfg_m, prompt, system=system, max_tokens=4000,
                              username=st.session_state.get("current_user", ""))

        if err:
            status.update(label="❌ 卦象推演失败", state="error")
            st.error(f"玄学大师暂时失联：{err}")
            return

        st.write("✨ 卦象已成！")
        time.sleep(0.3)
        status.update(label="🔮 今日运势已揭晓！", state="complete")

    st.session_state["_mystic_result"] = {"date": today_str, "content": result}
    st.markdown(result)
