# 持续到来的异常作业比例的序贯证据监测

这是现场机器人应用分析的合成资料项目。`data`保存业务记录，`analysis_data.io`负责严格JSON读取和原始字节指纹，`analysis_data.domain`提供已有的基础汇总。字段与数学口径见[资料约定](docs/data-contract.md)。所有厂区、设备、收益与结果均为合成，不代表任何实际项目的效果。

Python 3.11及以上版本可直接运行，当前无第三方Python依赖。项目根目录中的常用命令：

```sh
python -m unittest discover -s tests -v
python -m compileall -q analysis_data tests
python -m analysis_data.cli inspect data/normal.json
```

当前命令展示原始资料摘要与基础统计。结果写入其他目录时应保留输入来源指纹，原始资料不覆盖。更多边界资料与normal采用同样的JSON结构，invalid前缀文件保存不合法输入。

## 序贯e值监测（eseq）

在基础统计之上，`eseq`提供只读序贯监测：数据到来时更新证据，随时查看仍能说明假设与误报预算；它只读取已确认的0/1结果，不控制现场设备，不发送真实消息。假设、联合误报控制（非负超鞅+并合界）与口径见[序贯监测说明](docs/sequential-monitoring.md)。

```sh
python -m eseq.cli run data/normal.json --root /tmp/mon
python -m eseq.cli audit /tmp/mon <plan_id>
python -m eseq.cli enumcheck data/normal.json --n 10
python -m eseq.cli demo
```

容器可执行 `docker build -t field-analysis .` 后用 `docker run --rm field-analysis` 查看资料，或 `docker run --rm field-analysis python -m unittest discover -s tests -v` 运行现有检查。运行期间无外网或账号要求。
