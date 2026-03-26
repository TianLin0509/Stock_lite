from main import is_top100_review_generate_command, is_top100_review_query, parse_kline_predict_command


def test_parse_kline_predict_command_accepts_supported_prefixes():
    assert parse_kline_predict_command("k线预测 贵州茅台") == "贵州茅台"
    assert parse_kline_predict_command("形态预测 600519") == "600519"
    assert parse_kline_predict_command("走势预测: 平安银行") == "平安银行"
    assert parse_kline_predict_command("比亚迪 k线") == "比亚迪"
    assert parse_kline_predict_command("预测 比亚迪") == "比亚迪"
    assert parse_kline_predict_command("比亚迪 预测") == "比亚迪"


def test_parse_kline_predict_command_returns_none_for_regular_text():
    assert parse_kline_predict_command("贵州茅台") is None


def test_is_top100_review_query_accepts_supported_commands():
    assert is_top100_review_query("Top100复盘")
    assert is_top100_review_query("复盘Top100")
    assert is_top100_review_query("top100 review")
    assert not is_top100_review_query("Top100")


def test_is_top100_review_generate_command_accepts_supported_commands():
    assert is_top100_review_generate_command("Top100复盘生成")
    assert is_top100_review_generate_command("生成Top100复盘")
    assert not is_top100_review_generate_command("Top100复盘")
