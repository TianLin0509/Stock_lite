"""运行全部10轮 Prompt 迭代优化 — 自动分析+总结"""

import sys
import io
import json
import time
import shutil
import re
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

from tests.prompt_optimizer import (
    get_doubao_config, call_doubao_direct,
    build_context_for_stock, analyze_report_quality,
    generate_quality_report,
    STOCK_POOL_ROUND1, STOCK_POOL_NEW,
    OUTPUT_DIR,
)

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

CFG = get_doubao_config()
PROMPTS_FILE = Path(__file__).parent.parent / "ai" / "prompts_report.py"


def run_single_stock(code6, cfg, iteration):
    """分析单只股票，返回 (quality_dict, report_text)"""
    from ai.prompts_report import build_report_prompt, REPORT_SYSTEM

    try:
        name, ts_code, context, ps, ind_section = build_context_for_stock(code6)
        user_prompt, system_prompt = build_report_prompt(
            name, ts_code, context, ps, ind_section,
        )

        start_t = time.time()
        text, err = call_doubao_direct(cfg, user_prompt, system_prompt, max_tokens=10000)
        elapsed = time.time() - start_t

        if err or not text:
            return {"stock": f"{name}({code6})", "error": err or "空响应"}, ""

        quality = analyze_report_quality(text, f"{name}({code6})")
        quality["elapsed_seconds"] = round(elapsed, 1)
        return quality, text

    except Exception as e:
        return {"stock": code6, "error": str(e)}, ""


def run_iteration(iteration_num, stock_codes):
    """运行一轮"""
    # 重新加载 prompt 模块（可能已被修改）
    import importlib
    import ai.prompts_report
    importlib.reload(ai.prompts_report)

    iter_dir = OUTPUT_DIR / f"iter_{iteration_num:02d}"
    iter_dir.mkdir(exist_ok=True)

    results = []
    for i, code6 in enumerate(stock_codes):
        logger.info(f"[迭代{iteration_num}] [{i+1}/{len(stock_codes)}] {code6}...")
        quality, text = run_single_stock(code6, CFG, iteration_num)
        results.append(quality)

        if text:
            name_part = quality.get("stock", code6).split("(")[0]
            (iter_dir / f"{code6}_{name_part}.md").write_text(text, encoding="utf-8")

        # 每5只休息
        if (i + 1) % 5 == 0:
            time.sleep(3)

    # 保存质量数据
    (iter_dir / "quality_summary.json").write_text(
        json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")

    report = generate_quality_report(results, iteration_num)
    (iter_dir / "quality_report.md").write_text(report, encoding="utf-8")

    return results, report


def analyze_and_improve(iteration_num, results, all_history):
    """用 AI 分析质量问题并生成改进建议"""
    valid = [r for r in results if "error" not in r]
    if not valid:
        return "无有效结果，跳过分析"

    # 读取几份报告样本
    iter_dir = OUTPUT_DIR / f"iter_{iteration_num:02d}"
    samples = []
    for f in sorted(iter_dir.glob("*.md"))[:3]:
        if f.name != "quality_report.md":
            text = f.read_text(encoding="utf-8")
            samples.append(f"### {f.stem}\n{text[:3000]}...\n")

    # 统计汇总
    metrics_summary = generate_quality_report(results, iteration_num)

    # 构建分析 prompt
    analysis_prompt = f"""你是一位 Prompt 工程专家。以下是投研报告 Prompt 第{iteration_num}轮测试的质量统计和报告样本。

## 质量统计
{metrics_summary}

## 报告样本（前3000字）
{"".join(samples[:2])}

## 历史改进记录
{chr(10).join(all_history[-3:]) if all_history else '这是第一轮，无历史记录'}

---

请分析当前 Prompt 的核心问题（最多5条），并给出具体的改进建议。格式：

### 问题 N：[问题名称]
- **现象**：...
- **根因**：...
- **改进方案**：具体说明 Prompt 哪部分怎么改
- **预期效果**：...

最后用一段话总结本轮最优先的3个改进点。"""

    text, err = call_doubao_direct(CFG, analysis_prompt,
                                    "你是 Prompt 工程专家，专注于提升 AI 输出质量。",
                                    max_tokens=3000)
    if err:
        return f"分析失败: {err}"
    return text or "无分析结果"


def apply_improvements(iteration_num, improvement_text):
    """基于改进建议，用 AI 生成新的 Prompt 并写入文件"""
    current_prompt = PROMPTS_FILE.read_text(encoding="utf-8")

    apply_prompt = f"""你是一位 Prompt 工程师。以下是当前的投研报告 Prompt 代码和改进建议。

## 当前 Prompt 代码
```python
{current_prompt}
```

## 改进建议
{improvement_text}

---

请输出修改后的完整 Python 代码（整个 ai/prompts_report.py 文件）。
注意：
1. 只修改 Prompt 文本内容（REPORT_SYSTEM、build_report_prompt 中的 user_prompt、build_summary_prompt）
2. 不要改变函数签名、import、STYLE_WEIGHTS 等代码结构
3. 保持 <<<SCORES>>>...<<<END_SCORES>>> 格式不变（程序解析依赖）
4. 输出纯 Python 代码，不要有任何说明文字
5. 确保代码语法正确

```python
"""

    text, err = call_doubao_direct(CFG, apply_prompt,
                                    "你是 Python 代码生成专家。只输出代码，不要任何解释。",
                                    max_tokens=8000)
    if err or not text:
        logger.error("生成新 Prompt 失败: %s", err)
        return False

    # 提取 python 代码块
    code_match = re.search(r'```python\s*\n(.*?)```', text, re.DOTALL)
    if code_match:
        new_code = code_match.group(1)
    else:
        # 尝试直接取全文
        new_code = text.strip()
        if not new_code.startswith(('"""', "'''", "from ", "import ")):
            logger.error("无法提取有效代码")
            return False

    # 验证语法
    try:
        compile(new_code, "prompts_report.py", "exec")
    except SyntaxError as e:
        logger.error("新 Prompt 语法错误: %s", e)
        return False

    # 备份并写入
    backup = PROMPTS_FILE.parent / f"prompts_report_iter{iteration_num:02d}.py.bak"
    shutil.copy2(PROMPTS_FILE, backup)
    PROMPTS_FILE.write_text(new_code, encoding="utf-8")
    logger.info("✅ Prompt 已更新（迭代 %d → %d）", iteration_num, iteration_num + 1)
    return True


def generate_final_report(all_history, all_quality):
    """生成最终汇总报告（Markdown）"""
    lines = [
        "# A股投研助手 Prompt 迭代优化报告",
        f"",
        f"**生成时间**：{datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"**模型**：豆包 Seed 2.0 Pro",
        f"**迭代轮次**：{len(all_history)}",
        f"**总分析股票数**：{sum(len(q) for q in all_quality)}",
        f"",
        "---",
        "",
    ]

    for i, (history, quality) in enumerate(zip(all_history, all_quality), 1):
        valid = [r for r in quality if "error" not in r]
        lines.append(f"## 第 {i} 轮迭代")
        lines.append(f"")
        lines.append(f"**分析股票数**：{len(quality)} | 成功：{len(valid)} | 失败：{len(quality)-len(valid)}")

        if valid:
            avg_chars = sum(r["total_chars"] for r in valid) / len(valid)
            scores_ok = sum(1 for r in valid if r.get("has_scores_block"))
            rating_ok = sum(1 for r in valid if r.get("has_rating"))
            clustered = sum(1 for r in valid if r.get("scores_clustered") is True)
            lines.append(f"**平均字数**：{avg_chars:.0f} | 评分完整：{scores_ok}/{len(valid)} | "
                        f"评级完整：{rating_ok}/{len(valid)} | 评分扎堆：{clustered}/{len(valid)}")

        lines.append(f"")
        lines.append(f"### 改进分析")
        lines.append(history)
        lines.append(f"")
        lines.append("---")
        lines.append("")

    return "\n".join(lines)


def send_email_report(report_text):
    """发送报告到邮箱"""
    try:
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart

        # 读取邮件配置
        try:
            import toml
            secrets = toml.load(Path(__file__).parent.parent / ".streamlit" / "secrets.toml")
        except ImportError:
            secrets_path = Path(__file__).parent.parent / ".streamlit" / "secrets.toml"
            secrets = {}
            for line in secrets_path.read_text(encoding="utf-8").split("\n"):
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    k, v = line.split("=", 1)
                    secrets[k.strip()] = v.strip().strip('"')

        smtp_user = secrets.get("EMAIL_SENDER", "")
        smtp_pass = secrets.get("EMAIL_PASSWORD", "")
        smtp_host = secrets.get("EMAIL_SMTP_HOST", "smtp.qq.com")
        smtp_port = int(secrets.get("EMAIL_SMTP_PORT", "465"))

        if not smtp_user or not smtp_pass:
            logger.warning("邮件配置缺失，保存到本地文件")
            return False

        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"A股投研助手 Prompt 迭代优化报告 ({datetime.now().strftime('%Y-%m-%d')})"
        msg["From"] = smtp_user
        msg["To"] = "290045045@qq.com"

        # Markdown 作为纯文本附加
        msg.attach(MIMEText(report_text, "plain", "utf-8"))

        with smtplib.SMTP_SSL(smtp_host, smtp_port) as server:
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)

        logger.info("✅ 报告已发送到 290045045@qq.com")
        return True

    except Exception as e:
        logger.error("邮件发送失败: %s", e)
        return False


def main():
    all_history = []
    all_quality = []

    for iteration in range(1, 11):
        print(f"\n{'='*60}")
        print(f"  迭代 {iteration}/10 开始")
        print(f"{'='*60}\n")

        # 选股
        if iteration == 1:
            codes = STOCK_POOL_ROUND1[:20]
        else:
            # 前10只复用第1轮，后10只新增
            codes = STOCK_POOL_ROUND1[:10] + STOCK_POOL_NEW[:10]

        # 运行测试
        results, report = run_iteration(iteration, codes)
        all_quality.append(results)

        # 分析并生成改进建议
        print(f"\n📊 分析质量并生成改进建议...")
        improvement = analyze_and_improve(iteration, results, all_history)
        all_history.append(f"**迭代 {iteration} 改进分析**\n\n{improvement}")

        # 保存改进建议
        iter_dir = OUTPUT_DIR / f"iter_{iteration:02d}"
        (iter_dir / "improvement.md").write_text(improvement, encoding="utf-8")

        print(f"\n改进建议:\n{improvement[:500]}...\n")

        # 应用改进（最后一轮不应用）
        if iteration < 10:
            print(f"🔧 应用改进到 Prompt...")
            success = apply_improvements(iteration, improvement)
            if not success:
                print(f"⚠️ 改进应用失败，使用当前 Prompt 继续")
            time.sleep(5)  # 等待文件写入

    # 生成最终汇总报告
    print(f"\n📝 生成最终汇总报告...")
    final_report = generate_final_report(all_history, all_quality)

    final_path = OUTPUT_DIR / "final_report.md"
    final_path.write_text(final_report, encoding="utf-8")
    print(f"📄 报告已保存: {final_path}")

    # 发送邮件
    if send_email_report(final_report):
        print("✅ 报告已发送到 290045045@qq.com")
    else:
        print(f"⚠️ 邮件发送失败，请手动查看: {final_path}")

    print(f"\n🎉 全部10轮迭代完成！")


if __name__ == "__main__":
    main()
