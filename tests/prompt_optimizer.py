"""Prompt 迭代优化器 — 批量调用 API 分析报告质量"""

import sys
import json
import time
import os
from pathlib import Path
from datetime import datetime

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

# 模拟 streamlit secrets
os.environ["STREAMLIT_SECRETS_PATH"] = str(
    Path(__file__).parent.parent / ".streamlit" / "secrets.toml"
)

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).parent / "prompt_iterations"
OUTPUT_DIR.mkdir(exist_ok=True)


def get_doubao_config():
    """读取豆包 Seed 2.0 Pro 配置"""
    try:
        import toml
        secrets = toml.load(Path(__file__).parent.parent / ".streamlit" / "secrets.toml")
    except ImportError:
        # 手动解析
        secrets_path = Path(__file__).parent.parent / ".streamlit" / "secrets.toml"
        secrets = {}
        for line in secrets_path.read_text(encoding="utf-8").split("\n"):
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                secrets[k.strip()] = v.strip().strip('"')

    return {
        "api_key": secrets.get("DOUBAO_API_KEY", ""),
        "base_url": "https://ark.cn-beijing.volces.com/api/v3",
        "model": "doubao-seed-2-0-pro-260215",
        "supports_search": True,
        "provider": "doubao",
    }


# 选股池：覆盖不同行业/市值/风格
STOCK_POOL_ROUND1 = [
    "000001",  # 平安银行 - 大盘银行
    "600519",  # 贵州茅台 - 白酒龙头
    "300750",  # 宁德时代 - 新能源龙头
    "002415",  # 海康威视 - 安防AI
    "601127",  # 赛力斯 - 新能源车
    "000858",  # 五粮液 - 白酒
    "603986",  # 兆易创新 - 半导体
    "002594",  # 比亚迪 - 新能源整车
    "600036",  # 招商银行 - 银行
    "300059",  # 东方财富 - 券商互联网
    "601012",  # 隆基绿能 - 光伏
    "000333",  # 美的集团 - 家电
    "002475",  # 立讯精密 - 消费电子
    "300124",  # 汇川技术 - 工控
    "600900",  # 长江电力 - 电力
    "002371",  # 北方华创 - 半导体设备
    "300850",  # 新强联 - 风电轴承
    "601899",  # 紫金矿业 - 有色
    "000568",  # 泸州老窖 - 白酒
    "688981",  # 中芯国际 - 芯片代工
]

STOCK_POOL_NEW = [
    "601318",  # 中国平安 - 保险
    "002230",  # 科大讯飞 - AI语音
    "300760",  # 迈瑞医疗 - 医疗器械
    "601888",  # 中国中免 - 免税
    "002049",  # 紫光国微 - 芯片设计
    "600809",  # 山西汾酒 - 白酒
    "300274",  # 阳光电源 - 光伏逆变器
    "002129",  # TCL中环 - 硅片
    "600438",  # 通威股份 - 光伏+农业
    "300661",  # 圣邦股份 - 模拟芯片
]


def call_doubao_direct(cfg, prompt, system, max_tokens=10000):
    """直接调用豆包API（非流式）"""
    import requests

    url = cfg["base_url"].rstrip("/") + "/responses"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {cfg['api_key']}",
    }
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    body = {
        "model": cfg["model"],
        "input": messages,
        "tools": [{"type": "web_search", "max_keyword": 3}],
        "stream": False,
        "max_output_tokens": max_tokens,
    }

    try:
        resp = requests.post(url, headers=headers, json=body, timeout=300)
        resp.encoding = "utf-8"
        if resp.status_code != 200:
            return None, f"API错误 {resp.status_code}: {resp.text[:200]}"
        data = resp.json()
        # 提取文本
        parts = []
        for item in data.get("output", []):
            if item.get("type") == "message":
                for c in item.get("content", []):
                    if c.get("type") == "output_text":
                        parts.append(c.get("text", ""))
            if "text" in item:
                parts.append(item["text"])
        text = "\n".join(parts) if parts else data.get("output_text", "")
        return text, None
    except Exception as e:
        return None, str(e)


def build_context_for_stock(code6):
    """为单只股票构建数据上下文（简化版，不启动streamlit）"""
    # 延迟导入避免streamlit初始化问题
    try:
        from data.tushare_client import (
            to_ts_code, get_basic_info, get_price_df, get_financial,
            get_capital_flow, get_dragon_tiger, get_northbound_flow,
            get_margin_trading, get_sector_peers, get_holders_info,
            get_pledge_info, get_fund_holdings, price_summary,
        )
        from data.indicators import compute_indicators, format_indicators_section

        ts_code = to_ts_code(code6)

        # 基础数据
        info, _ = get_basic_info(ts_code)
        df, _ = get_price_df(ts_code)
        fin, _ = get_financial(ts_code)

        # 技术指标
        ps = price_summary(df) if not df.empty else "暂无"
        indicators = compute_indicators(df)
        ind_section = format_indicators_section(indicators)

        # 资金数据
        cap, _ = get_capital_flow(ts_code)
        dragon, _ = get_dragon_tiger(ts_code)
        nb, _ = get_northbound_flow(ts_code)
        margin, _ = get_margin_trading(ts_code)

        # 其他
        sector, _ = get_sector_peers(ts_code)
        holders, _ = get_holders_info(ts_code)
        pledge, _ = get_pledge_info(ts_code)
        fund, _ = get_fund_holdings(ts_code)

        name = info.get("名称", "") or info.get("name", code6)

        # 尝试获取深度数据
        context = {
            "basic_info": str(info),
            "capital": cap or "暂无",
            "dragon": dragon or "暂无",
            "northbound": nb or "暂无",
            "margin": margin or "暂无",
            "sector": sector or "暂无",
            "holders": holders or "暂无",
            "pledge": pledge or "暂无",
            "fund": fund or "暂无",
        }

        # 尝试新增接口
        try:
            from data.report_data import build_report_context
            full_ctx, _ = build_report_context(ts_code, name)
            context.update(full_ctx)
        except Exception as e:
            logger.warning("build_report_context 失败: %s", e)

        return name, ts_code, context, ps, ind_section

    except Exception as e:
        logger.error("构建 %s 上下文失败: %s", code6, e)
        return code6, to_ts_code(code6) if 'to_ts_code' in dir() else code6, {}, "", ""


def analyze_report_quality(text, stock_name):
    """分析单份报告的质量指标"""
    import re

    quality = {
        "stock": stock_name,
        "total_chars": len(text),
        "has_scores_block": bool(re.search(r"<<<SCORES>>>.*<<<END_SCORES>>>", text, re.DOTALL)),
        "has_rating": bool(re.search(r"操作评级[：:]", text)),
        "has_three_scenarios": bool(re.search(r"乐观.*中性.*悲观|三情景", text, re.DOTALL)),
        "has_risk_checklist": bool(re.search(r"排雷|🔴.*🟢|9项", text)),
        "has_contradiction": bool(re.search(r"矛盾", text)),
        "has_stop_loss": bool(re.search(r"止损", text)),
        "has_target_price": bool(re.search(r"目标价", text)),
        "bold_unclosed": 0,
        "vague_words": 0,
        "data_missing_marks": len(re.findall(r"⚠️数据缺失", text)),
    }

    # 检查未闭合加粗
    for line in text.split("\n"):
        if line.count("**") % 2 == 1:
            quality["bold_unclosed"] += 1

    # 模糊词统计
    vague = re.findall(r"可能|或许|大概率|基本面良好|投资有风险", text)
    quality["vague_words"] = len(vague)

    # 评分提取
    scores_match = re.search(r"<<<SCORES>>>(.*?)<<<END_SCORES>>>", text, re.DOTALL)
    if scores_match:
        block = scores_match.group(1)
        scores = {}
        for line in block.strip().split("\n"):
            m = re.match(r"(.+?)[：:]\s*(\d+(?:\.\d+)?)\s*/\s*10", line.strip())
            if m:
                scores[m.group(1).strip()] = float(m.group(2))
        quality["scores"] = scores
        # 检查评分区分度
        vals = list(scores.values())
        if vals and all(6 <= v <= 8 for v in vals[:4]):
            quality["scores_clustered"] = True
        else:
            quality["scores_clustered"] = False
    else:
        quality["scores"] = {}
        quality["scores_clustered"] = None

    # 证伪条件检查
    quality["has_falsification"] = bool(re.search(r"证伪|若.*则失效|若.*则.*无效", text))

    return quality


def run_iteration(iteration_num, stock_codes, cfg):
    """运行一轮测试"""
    # 读取当前 prompt
    from ai.prompts_report import build_report_prompt, REPORT_SYSTEM

    results = []
    iter_dir = OUTPUT_DIR / f"iter_{iteration_num:02d}"
    iter_dir.mkdir(exist_ok=True)

    for i, code6 in enumerate(stock_codes):
        logger.info(f"[迭代{iteration_num}] [{i+1}/{len(stock_codes)}] 分析 {code6}...")

        try:
            name, ts_code, context, ps, ind_section = build_context_for_stock(code6)
            user_prompt, system_prompt = build_report_prompt(
                name, ts_code, context, ps, ind_section,
            )

            start_t = time.time()
            text, err = call_doubao_direct(cfg, user_prompt, system_prompt, max_tokens=10000)
            elapsed = time.time() - start_t

            if err:
                logger.error(f"  {code6} 调用失败: {err}")
                results.append({"stock": code6, "error": err})
                continue

            if not text:
                logger.error(f"  {code6} 返回空内容")
                results.append({"stock": code6, "error": "空响应"})
                continue

            # 保存原始报告
            (iter_dir / f"{code6}_{name}.md").write_text(text, encoding="utf-8")

            # 质量分析
            quality = analyze_report_quality(text, f"{name}({code6})")
            quality["elapsed_seconds"] = round(elapsed, 1)
            quality["input_chars"] = len(user_prompt)
            results.append(quality)

            logger.info(f"  ✅ {name}({code6}) 完成 | {len(text)}字 | {elapsed:.1f}s | "
                        f"评分块={'有' if quality['has_scores_block'] else '❌无'}")

            # 每5只股票休息一下
            if (i + 1) % 5 == 0:
                time.sleep(2)

        except Exception as e:
            logger.error(f"  {code6} 异常: {e}")
            results.append({"stock": code6, "error": str(e)})

    # 保存质量汇总
    summary_path = iter_dir / "quality_summary.json"
    summary_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")

    return results


def generate_quality_report(results, iteration_num):
    """生成质量分析报告"""
    total = len(results)
    errors = sum(1 for r in results if "error" in r)
    valid = [r for r in results if "error" not in r]

    lines = [
        f"# 迭代 {iteration_num} 质量分析报告",
        f"",
        f"## 基本统计",
        f"- 总调用: {total} | 成功: {len(valid)} | 失败: {errors}",
    ]

    if valid:
        avg_chars = sum(r["total_chars"] for r in valid) / len(valid)
        avg_time = sum(r.get("elapsed_seconds", 0) for r in valid) / len(valid)
        lines.append(f"- 平均报告长度: {avg_chars:.0f} 字")
        lines.append(f"- 平均耗时: {avg_time:.1f}s")

        # 关键指标统计
        metrics = {
            "has_scores_block": "评分块完整",
            "has_rating": "有操作评级",
            "has_three_scenarios": "有三情景推演",
            "has_risk_checklist": "有排雷清单",
            "has_contradiction": "有矛盾检测",
            "has_stop_loss": "有止损位",
            "has_target_price": "有目标价",
            "has_falsification": "有证伪条件",
        }

        lines.append(f"\n## 关键指标完成率")
        lines.append(f"| 指标 | 完成率 |")
        lines.append(f"|------|--------|")
        for key, label in metrics.items():
            count = sum(1 for r in valid if r.get(key))
            pct = count / len(valid) * 100
            lines.append(f"| {label} | {count}/{len(valid)} ({pct:.0f}%) |")

        # 评分区分度
        clustered = sum(1 for r in valid if r.get("scores_clustered") is True)
        lines.append(f"\n## 评分质量")
        lines.append(f"- 评分扎堆(6-8)的报告: {clustered}/{len(valid)}")

        # 格式问题
        bold_issues = sum(r.get("bold_unclosed", 0) for r in valid)
        vague_total = sum(r.get("vague_words", 0) for r in valid)
        lines.append(f"- 未闭合加粗总数: {bold_issues}")
        lines.append(f"- 模糊词汇总数: {vague_total}")

        # 各股票评分一览
        lines.append(f"\n## 各股票评分")
        lines.append(f"| 股票 | 基本面 | 预期差 | 技术面 | 资金面 | 综合 |")
        lines.append(f"|------|--------|--------|--------|--------|------|")
        for r in valid:
            s = r.get("scores", {})
            lines.append(
                f"| {r['stock']} | "
                f"{s.get('基本面', '-')} | {s.get('预期差', '-')} | "
                f"{s.get('技术面', '-')} | {s.get('资金面', '-')} | "
                f"{s.get('综合加权', '-')} |"
            )

    return "\n".join(lines)


if __name__ == "__main__":
    # Windows GBK 兼容
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--iteration", type=int, default=1, help="迭代编号")
    parser.add_argument("--stocks", nargs="+", default=None, help="指定股票代码")
    parser.add_argument("--count", type=int, default=20, help="股票数量")
    args = parser.parse_args()

    cfg = get_doubao_config()
    if not cfg["api_key"]:
        print("❌ DOUBAO_API_KEY 未配置")
        sys.exit(1)

    if args.stocks:
        codes = args.stocks
    elif args.iteration == 1:
        codes = STOCK_POOL_ROUND1[:args.count]
    else:
        # 后续轮次：前10只复用，后10只新增
        codes = STOCK_POOL_ROUND1[:10] + STOCK_POOL_NEW[:10]

    print(f"🚀 开始迭代 {args.iteration}，分析 {len(codes)} 只股票...")
    results = run_iteration(args.iteration, codes, cfg)

    report = generate_quality_report(results, args.iteration)
    report_path = OUTPUT_DIR / f"iter_{args.iteration:02d}" / "quality_report.md"
    report_path.write_text(report, encoding="utf-8")

    print(f"\n📊 质量报告已生成: {report_path}")
    print(report)
