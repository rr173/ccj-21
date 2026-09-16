"""领域常量与介质规则.

介质规则决定三类裁定行为:
  confirm     : "single" 单侧证词即可确认链路; "both" 需要两端证词, 否则 unconfirmed
  down_policy : 两端证词一 up 一 down 时的介质级规则
                "trust"     按 (可信级别, 新鲜度) 裁定
                "down_wins" 任一端报 down 即判 down (如以太网 PHY 链路down权威)
  base_cost   : 路径代价基准, 实际代价 = base_cost * (2 - quality)
  min_quality : 证词断言 "up" 的最低质量; 低于该值视为断言 down/不可用
"""

MEDIA = {
    "fiber":    {"base_cost": 1.0,  "min_quality": 0.20, "confirm": "both",   "down_policy": "trust"},
    "ethernet": {"base_cost": 2.0,  "min_quality": 0.20, "confirm": "both",   "down_policy": "down_wins"},
    "wifi":     {"base_cost": 5.0,  "min_quality": 0.25, "confirm": "single", "down_policy": "trust"},
    "radio":    {"base_cost": 10.0, "min_quality": 0.30, "confirm": "single", "down_policy": "trust"},
}

# 两端都报 up 但质量差超过该容忍度时记为质量冲突
QUALITY_CONFLICT_TOLERANCE = 0.30

# 抖动门限: 连续 DOWN_AFTER 个坏样本边才置不可用; 连续 UP_AFTER 个好样本才恢复.
# 短暂抖动只计入证据 (damp_note), 不翻转拓扑.
DAMP_DOWN_AFTER = 3
DAMP_UP_AFTER = 2

# 代价以微单位整数计算, 保证等价路径判定是精确的, 决胜规则确定
COST_SCALE = 1_000_000

# 证词状态
ST_ACCEPTED = "accepted"
ST_SIDECAR = "sidecar"

# 边裁定状态
EDGE_UP = "up"
EDGE_DOWN = "down"
EDGE_UNCONFIRMED = "unconfirmed"


def edge_cost_micro(medium: str, quality: float) -> int:
    """单跳代价(微单位): 质量越高代价越低; 同路径不同质量时稳定选代价更低者."""
    rule = MEDIA[medium]
    return round(rule["base_cost"] * (2.0 - quality) * COST_SCALE)


def cost_str(cost_micro: int) -> str:
    return f"{cost_micro / COST_SCALE:.3f}"
