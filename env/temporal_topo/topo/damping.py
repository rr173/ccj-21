"""抖动阻尼: 批量质量样本越过门限后才改变边的可用性.

对每条边按时间顺序重放裁定后的质量样本:
  * 连续 DAMP_DOWN_AFTER 个坏样本 (质量低于介质门限) 才把边置为不可用;
  * 连续 DAMP_UP_AFTER 个好样本才恢复;
  * 不足门限的短暂抖动只计入证据 (damp_note), 拓扑不来回翻转.

阻尼器从全部历史 (含已封存区间) 确定性重放, 因此迟到证词插入历史后
重放结果仍然确定; 封存区间的结果已冻结, 只有未封存区间会随新修订改变。
"""

from . import models


class Damper:
    def __init__(self):
        self.up = True
        self.bad = 0
        self.good = 0

    def sample(self, quality, min_quality):
        """喂入一个样本, 返回 (damper_up, note)."""
        if quality >= min_quality:
            self.good += 1
            self.bad = 0
        else:
            self.bad += 1
            self.good = 0
        note = ""
        if self.up and self.bad >= models.DAMP_DOWN_AFTER:
            self.up = False
            note = (f"damper up->down: {self.bad} consecutive bad samples "
                    f"crossed threshold {models.DAMP_DOWN_AFTER}")
        elif not self.up and self.good >= models.DAMP_UP_AFTER:
            self.up = True
            note = (f"damper down->up: {self.good} consecutive good samples "
                    f"crossed threshold {models.DAMP_UP_AFTER}")
        elif self.up and self.bad > 0:
            note = (f"flap suppressed as evidence only: bad {self.bad}/"
                    f"{models.DAMP_DOWN_AFTER}")
        elif not self.up and self.good > 0:
            note = (f"recovery pending: good {self.good}/{models.DAMP_UP_AFTER}")
        return self.up, note


def stage_damp(store, job_id):
    """推导作业阶段2: 对暂存区裁定结果做阻尼, 更新 usable / damp_note."""
    rows = store.q(
        "SELECT * FROM staging_edges WHERE job_id=? "
        "ORDER BY ep_a, ep_b, medium, epoch_start", (job_id,))
    dampers = {}
    updates = []
    for r in rows:
        key = (r["ep_a"], r["ep_b"], r["medium"])
        damper = dampers.setdefault(key, Damper())
        rule = models.MEDIA[r["medium"]]
        sample_q = r["quality"] if r["state"] == models.EDGE_UP else 0.0
        damper_up, note = damper.sample(sample_q, rule["min_quality"])
        usable = 1 if (r["state"] == models.EDGE_UP and damper_up) else 0
        updates.append((usable, note, job_id, r["epoch_start"],
                        r["ep_a"], r["ep_b"], r["medium"]))
    with store.tx() as c:
        c.executemany(
            "UPDATE staging_edges SET usable=?, damp_note=? "
            "WHERE job_id=? AND epoch_start=? AND ep_a=? AND ep_b=? AND medium=?",
            updates)
