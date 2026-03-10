这是一份为你量身定制的专业级中英文双语 `README.md` 文档。这份文档结构清晰、用词专业，非常适合展示在你的 GitHub 主页上，作为申请 FinTech 和 AI+金融方向研究生的核心项目展示。

你可以直接复制以下 Markdown 内容，并在项目根目录下新建或覆盖原有的 `README.md` 文件：

---

# 📊 MacroFin Analytics Platform

> **A Big Data Analytics Platform for Global Finance and Macroeconomics**

[English](https://www.google.com/search?q=%23english-version) | [中文版](https://www.google.com/search?q=%23chinese-version)

---

### 📖 Introduction

**MacroFin Analytics** is a distributed big data visualization platform designed to analyze the complex relationships between global macroeconomic indicators and financial market performance. Built for FinTech research, this project leverages the Hadoop ecosystem to process multi-dimensional economic datasets and provides an interactive, real-time data visualization dashboard.

### 🛠️ Architecture & Tech Stack

* **Data Storage**: Apache Hadoop (HDFS)
* **Data Processing & Querying**: Apache Hive (MapReduce)
* **Backend API Framework**: Java Spring Boot
* **Frontend Visualization**: HTML5, CSS3, JavaScript, Apache ECharts

### 🗄️ Dataset

The project utilizes a comprehensive **Finance & Economics Dataset** comprising historical data across 24 dimensions, including:

* Market Indices (S&P 500, Dow Jones, NASDAQ)
* Macro Indicators (GDP Growth, Unemployment Rate, Inflation Rate, Interest Rate)
* Commodities & Forex (Crude Oil, Gold, USD/EUR, USD/JPY)
* Consumer Behavior (Consumer Confidence Index, Consumer Spending)

### 💡 Key Analytical Features

1. **Global Market Activity**: Analyzes the distribution of total trading volume across major stock indices.
2. **Macroeconomic Trends**: Explores the inverse relationship between Annual GDP Growth and Unemployment Rates (derived from the Phillips Curve).
3. **Consumer Behavior Impact**: Evaluates how the Consumer Confidence Index drives variations in Consumer Spending.
4. **Commodity Market Dynamics**: Tracks the historical pricing trends of safe-haven assets (Gold) versus energy commodities (Crude Oil).

### 🚀 Quick Start

**1. Hadoop & HDFS Setup**

```bash
hadoop fs -mkdir -p /MacroFin_Analytics
hadoop fs -put finance_economics_dataset.csv /MacroFin_Analytics/

```

**2. Apache Hive Initialization**
Execute the DDL script in your Hive CLI to map the CSV data to an external table:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS finance_economics (...) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
LOCATION '/MacroFin_Analytics/';

```

**3. Spring Boot Backend**

* Ensure your HiveServer2 is running.
* Run the Spring Boot application via Maven:

```bash
mvn clean spring-boot:run

```

**4. Access the Dashboard**
Open your web browser and navigate to: `http://localhost:8080/index.html`

### 🔮 Future Work (AI + Finance Integration)

* Implement Machine Learning models (e.g., LSTM, XGBoost) to predict stock index movements based on macroeconomic indicators.
* Deploy real-time streaming analytics using Apache Kafka and Spark Streaming.

---

### 📖 项目简介

**MacroFin Analytics (宏观金融大数据分析平台)** 是一个分布式的金融大数据可视化系统，旨在探索全球宏观经济指标与金融市场表现之间的复杂联动关系。本项目专为金融科技（FinTech）研究设计，依托 Hadoop 生态系统处理多维经济数据集，并提供具备实时交互能力的数据可视化看板。

### 🛠️ 技术架构与栈

* **底层数据存储**: Apache Hadoop (HDFS)
* **大数据处理与查询**: Apache Hive (MapReduce)
* **后端 API 服务**: Java Spring Boot
* **前端交互与可视化**: HTML5, CSS3, JavaScript, Apache ECharts

### 🗄️ 数据集概览

本项目采用深度 **金融与宏观经济数据集 (Finance & Economics Dataset)**，涵盖 24 个维度的历史核心数据，主要包括：

* **市场指数**：标普 500 (S&P 500)、道琼斯 (Dow Jones)、纳斯达克 (NASDAQ)
* **宏观指标**：GDP 增长率、失业率、通货膨胀率、基准利率
* **外汇与大宗商品**：原油价格、黄金价格、美元/欧元汇率、美元/日元汇率
* **消费者行为**：消费者信心指数、总消费支出

### 💡 核心分析维度

1. **全球市场活跃度分析**：基于各大股票指数的总交易量分布，评估市场流动性与资金偏好。
2. **宏观经济趋势洞察**：对比历年平均 GDP 增长率与失业率走势，验证经典宏观经济学规律（如菲利普斯曲线的衍生表现）。
3. **消费者行为与经济影响**：分析消费者信心指数区间的变化如何直接驱动消费支出的增减。
4. **大宗商品避险情绪追踪**：对比历史周期中避险资产（黄金）与能源资产（原油）的价格波动趋势。

### 🚀 快速启动指南

**1. 配置 Hadoop 与 HDFS**

```bash
# 创建项目专属数据目录并上传原始数据集
hadoop fs -mkdir -p /MacroFin_Analytics
hadoop fs -put finance_economics_dataset.csv /MacroFin_Analytics/

```

**2. 初始化 Apache Hive 数据仓库**
在 Hive 终端中执行建表语句，将底层 CSV 文件映射为外部表：

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS finance_economics (...) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
LOCATION '/MacroFin_Analytics/';

```

**3. 启动 Spring Boot 后端服务**

* 确保虚拟机的 HiveServer2 服务已成功启动。
* 通过 Maven 编译并运行 Spring Boot 项目：

```bash
mvn clean spring-boot:run

```

**4. 访问可视化看板**
打开浏览器，访问前端控制台：`http://localhost:8080/index.html`

### 🔮 未来展望 (AI+金融方向升级)

* 引入机器学习算法（如 LSTM 时间序列预测、XGBoost），基于历史宏观指标预测未来的股票指数波动。
* 接入 Apache Kafka 与 Spark Streaming，实现金融市场数据的实时流计算与监控。
