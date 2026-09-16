"""temporal-topo: 可追溯的时态网络拓扑系统.

现场节点通过邻接观测维护网络拓扑；证词按观察者序号去重、按采样时刻归入
历史区间；冲突按可信级别/新鲜度/介质规则裁定；拓扑以不可改写的修订版本
演进，封存界限之后的迟到证词进入旁路档案。所有摄取/推导/故障域计算都是
可崩溃续跑的分阶段作业。
"""

from . import ingest as ingest  # noqa: F401  (注册 ingest-file 作业类型)
from . import derive as derive  # noqa: F401  (注册 derive 作业类型)
from . import failure as failure  # noqa: F401 (注册 failure-domain 作业类型)
