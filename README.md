# 零售品类经营数仓｜PySpark · Parquet

场景假设：一家自行车零售商在月度经营复盘时，需要知道销售变化由哪些品类驱动。订单行、商品版本和品类字典来自不同系统。项目把这些数据合成可对账的销售事实，并产出月度品类经营表，方便定位下钻方向。

**经营结论**：2013 年 12 月样本销售额环比增长 5.25%，Bikes 贡献约 96.6% 的净增额。[复盘报告与质量校验](ANALYSIS.md)

## 从源头到指标

```text
CRM 销售明细 + CRM 商品历史 + ERP 品类字典
  → Bronze：保留源文本字段
  → Silver：解析日期与数量、计算有效金额、分离拒绝记录
  → Gold：当前商品维度 + 按销售月份分区的销售事实
  → Mart：按月、按品类汇总金额、销量、订单数和环比
```

| 层 | 输出与校验 |
|---|---|
| Bronze | 三份 CSV 分别落盘，保留原始字段 |
| Silver | 原始行数 = 有效行数 + 拒绝行数，拒绝记录保留原因 |
| Gold | 一行对应一条有效源销售明细，商品键和品类字典键先查唯一性，关联前后行数和金额必须相同 |
| Mart | 一行对应一个自然月和一个当前品类，月度销售金额和行数合计对齐事实表 |

销售额优先用数量 × 单价绝对值计算，价格缺失时采用有效原值。日期、数量或最终金额不符合规则的记录进入拒绝表。品类使用最新商品版本映射，缺失归入 Unknown。品类订单数按订单号去重，同一订单可包含多个品类，跨品类汇总订单数需回到事实表去重。环比只取上一自然月同品类金额。

## 复现

先下载上游项目的样例数据。仓库通过脚本读取 `01_datasets` 下的三份文件。[DataWithBaraa 数据来源](https://github.com/DataWithBaraa/sql-data-warehouse-project)

使用 Python 3.12、JDK 17 和 PowerShell。在仓库根目录运行：

```powershell
python -m venv .venv
& ./.venv/Scripts/python.exe -m pip install -r requirements.txt
$env:JAVA_HOME = 'C:/path/to/jdk-17'
$env:SPARK_LOCAL_HOSTNAME = 'localhost'
$env:PYSPARK_PYTHON = (Resolve-Path .venv/Scripts/python.exe).Path
& ./.venv/Scripts/python.exe scripts/build_warehouse.py --source 'C:/path/to/01_datasets' --output warehouse
```

`warehouse/current.json` 指向最后一次通过对账的运行。该版本目录中包含 Parquet 表、`audit.json`、`report/category_monthly.csv` 和日期过滤的 Spark 执行计划。

## 技术选择与适用范围

PySpark 执行清洗、窗口和关联聚合，Parquet 按月组织事实以便观察分区裁剪。当前 Windows 本地版本由 PyArrow 将 Spark 结果写入 Parquet，再由 Spark 读回对账。写入阶段汇集到 driver，适用于这套公开样本；扩展到集群时应使用分布式 Parquet 写入。每次运行构建独立结果目录，检查通过后替换发布指针。
