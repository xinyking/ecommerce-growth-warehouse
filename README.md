# 电商数据仓库实验｜PySpark · Spark SQL

基于公开教程数据，复现销售明细关联商品历史版本时的行数膨胀，并比较 Spark 对同一 Join 的两种物理计划。另有一组可直接运行的电商会话漏斗合成测试。

## 已验证

| 销售场景 | 明细行数 | 源金额合计 |
|---|---:|---:|
| 源销售明细 | 60,398 | 29,355,502.00 |
| 关联全部商品历史版本 | 89,818 | 43,520,650.00 |
| 关联唯一当前商品版本 | 60,398 | 29,355,502.00 |
| 广播当前商品版本 | 60,398 | 29,355,502.00 |

在 Windows 本地 Spark 4.0.2 上运行。排序合并对照方案的销售侧有 `Exchange hashpartitioning`，显式广播方案省去该处 Shuffle。商品版本筛选仍需一次维表侧的重分区。实验包含物理计划对照和结果对账，集群性能待测。

## 复现

准备 Python 3.12、JDK 17 和 PowerShell，从仓库根目录运行。

```powershell
python -m venv .venv
& ./.venv/Scripts/python.exe -m pip install -r requirements.txt
& ./.venv/Scripts/python.exe scripts/check_fixture.py
& ./scripts/run_spark_smoke.ps1 -JavaHome 'C:/path/to/jdk-17'
```

两次验证都读取仓库中的合成事件。Spark 结果预期为 3 个会话进入浏览、1 个达到加购、1 个完成有序购买。单独计算的事件层共有 3 条去重购买事件，其中有效金额合计 50 USD。

销售案例的数据来自 DataWithBaraa 教程。下载其源码后，将 [Spark 第一课](SPARK-01.md) 的 `--source` 指向包含 `sales_details.csv` 和 `prd_info.csv` 的 CRM 目录。案例复用了商品键转换思路，汇总使用源销售金额，完整 SQL Server Gold 对账留作后续验证。[教程与数据来源](https://github.com/DataWithBaraa/sql-data-warehouse-project)

## 文档与后续阶段

- [第一课与实测结果](SPARK-01.md)
- [执行手册](GUIDE.md)
- [产品需求](PRD.md) 与 [实施规格](SPEC.md)
- `ingest/` 保存待在 BigQuery 验证的 GA4 剖析与提取 SQL。

GA4 离线分区流水线、Kafka / Flink 实时链路、用户画像及实验评估目前属于设计范围。
