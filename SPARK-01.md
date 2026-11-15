# Spark 第一课｜销售明细关联商品

使用已经做过的教程里的 `sales_details.csv` 和 `prd_info.csv`。沿用 Silver 的商品键截取规则，用最新开始日期取当前商品版本并检查唯一性。本实验仅复用这段逻辑，金额按源 CSV 汇总，暂未接入 SQL Server 的完整清洗及客户、品类关联。

## 运行

项目目录下使用 PowerShell。已按下列方式在 Windows 本地运行。

```powershell
$env:JAVA_HOME = (Get-ChildItem .tools -Directory -Filter 'jdk-17*' | Select-Object -First 1).FullName
$env:SPARK_LOCAL_HOSTNAME = 'localhost'
$env:PYSPARK_PYTHON = (Resolve-Path ./.venv/Scripts/python.exe).Path
& ./.venv/Scripts/python.exe scripts/spark_sales_lesson.py --source 'C:/path/to/sql-data-warehouse-project/01_datasets/source_crm'
```

## 按顺序读结果

| 场景 | 行数 | 原始销售金额合计 |
|---|---:|---:|
| 销售源表 | 60,398 | 29,355,502.00 |
| 关联全部商品版本 | 89,818 | 43,520,650.00 |
| 关联唯一当前版本 | 60,398 | 29,355,502.00 |
| 使用广播关联当前版本 | 60,398 | 29,355,502.00 |

1. 先看粒度。商品历史中同一商品可能有多条记录，销售行匹配几条就输出几条。确认当前商品键唯一再关联，未匹配销售明细为 0。
2. 看 SortMergeJoin。定位销售侧的 `Exchange hashpartitioning`，它按商品键重新分配记录；两边同键落到对应分区，排序后关联。
3. 看 BroadcastHashJoin。右侧当前商品表通过 `BroadcastExchange` 发给执行侧，销售侧省去这一处按商品键的 Shuffle。商品维表内部仍使用窗口进行版本筛选，因此计划里仍有维表侧 `Exchange`。
4. 本实验关闭 AQE 和自动广播以控制对照，广播方案显式提示。结果可用于对比执行机制。耗时收益与分布式运行待单独测试。

## STAR 复盘

- S：销售事实关联商品历史版本后出现行数与金额膨胀。
- T：保持销售明细粒度，同时理解 Spark 对同一条 Join 可采用的执行方式。
- A：取最新商品并检查键唯一，用源表行数和金额对账，比较排序合并与广播哈希的执行计划。
- R：行数由错误关联的 89,818 恢复到源表的 60,398，金额与源表一致。广播方案保留相同结果，在计划中省去销售侧该次 Shuffle。

## 下一课

沿用同一份销售 CSV，将日期解析为业务日期，写分区 Parquet。先验证写读，再用 `EXPLAIN FORMATTED` 查 `PartitionFilters`。第一课的 Shuffle 分区用于执行，下一课的日期分区用于文件组织和查询裁剪。
