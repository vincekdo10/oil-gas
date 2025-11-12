# 🚀 Oil & Gas Services dbt Demo Project

## ⚡ dbt Fusion Ready

This project is **100% compatible with dbt Fusion**, the next-generation dbt runtime that delivers:

- **30x faster** compilation and execution
- **30% warehouse cost savings** through state-aware orchestration  
- Enhanced developer experience with VS Code integration
- Native SQL understanding across dialects
- Future-proof Rust-based architecture

## 📊 Overview

A comprehensive demo showcasing oil & gas service operations analytics, featuring:

- **Rig operations** and utilization tracking
- **Well production** metrics and performance
- **Service revenue** analytics across basins
- **Safety incident** monitoring and reporting
- **Equipment utilization** and maintenance
- **Financial reporting** by service line and customer

## 🎯 dbt Cloud Features Demonstrated

### ✅ Core Features
- [x] Staging, intermediate, and mart layered architecture
- [x] Incremental models for large fact tables
- [x] Snapshots for slowly changing dimensions
- [x] Custom data tests and assertions
- [x] Seeds for reference data
- [x] Macros for reusable logic
- [x] Analyses for ad-hoc queries

### ✅ Advanced Features
- [x] **dbt Semantic Layer** with semantic models and metrics
- [x] Exposures for dashboard tracking
- [x] Documentation with descriptions and column-level metadata
- [x] Data quality tests using dbt_expectations
- [x] Modular configuration with meta dictionaries
- [x] Cross-project references (ready for multi-project setups)

### ✅ Fusion Compatibility
- [x] SQL-only models (no Python)
- [x] Modern dbt syntax (no deprecated patterns)
- [x] Fusion-compatible package versions
- [x] Clean YAML structure with proper typing
- [x] Meta-based configurations (no arbitrary configs)
- [x] Latest release track compatible

## 🏗️ Project Structure

```
oilgas_demo/
├── models/
│   ├── staging/          # Source data cleaning and type casting
│   ├── intermediate/     # Business logic transformations
│   └── marts/            # Analytics-ready tables
│       ├── core/         # Dimensions and core facts
│       ├── finance/      # Revenue and financial metrics
│       └── operations/   # Operational dashboards
├── seeds/                # 7 CSV files with 2,215+ rows of demo data
├── snapshots/            # SCD Type 2 tracking
├── tests/                # Custom data quality assertions
├── macros/               # Reusable SQL functions
├── analyses/             # Ad-hoc analytical queries
├── semantic_models/      # dbt Semantic Layer definitions
├── metrics/              # Business metric definitions
└── exposures/            # Dashboard and BI tool tracking
```

## 🚀 Quick Start (dbt Cloud)

### Prerequisites
- dbt Cloud account
- Snowflake connection configured
- Database: `OILGAS_DEMO`
- Default schema: `ANALYTICS`

### Setup Instructions

1. **Create a new dbt Cloud project** targeting Snowflake
   - Use the "Latest" release track for Fusion readiness

2. **Clone this repository** in dbt Cloud IDE

3. **Install packages:**
   ```bash
   dbt deps
   ```

4. **Load seed data:**
   ```bash
   dbt seed
   ```

5. **Run initial build:**
   ```bash
   dbt build
   ```

6. **Generate documentation:**
   ```bash
   dbt docs generate
   dbt docs serve
   ```

### Incremental Development

Run specific model groups:
```bash
# Staging only
dbt run --select staging

# Finance mart
dbt run --select marts.finance

# Incremental refresh
dbt run --select config.materialized:incremental

# Full refresh incrementals
dbt run --select config.materialized:incremental --full-refresh
```

## 📈 dbt Semantic Layer Setup

This project includes a complete semantic layer configuration for AI chatbot integration.

### Semantic Models Included:
1. **Revenue** - Service revenue by basin, customer, and service line
2. **Operations** - Rig utilization and well production metrics
3. **Safety** - Incident tracking and severity analysis
4. **Equipment** - Asset utilization and maintenance metrics

### Key Metrics:
- `total_revenue`: Aggregate service revenue
- `avg_daily_rate`: Average rig day rates
- `rig_utilization_pct`: Percentage of active rig days
- `safety_incident_rate`: Incidents per 1000 operating days
- `equipment_uptime_pct`: Equipment availability percentage

### Enable Semantic Layer in dbt Cloud:
1. Go to **Account Settings** > **Projects**
2. Select your project
3. Enable **dbt Semantic Layer**
4. Configure **metadata connection**
5. Set up **service tokens** for API access

### Query with AI Chatbot:
The semantic layer enables natural language queries like:
- "What was our total revenue last quarter by basin?"
- "Show me rig utilization trends over the past 6 months"
- "Which customers generated the most revenue this year?"

## 🛠️ Technology Stack

- **dbt Core**: Transformation framework
- **dbt Cloud**: Development and orchestration platform
- **dbt Fusion**: Next-gen Rust-based runtime (30x faster)
- **Snowflake**: Cloud data warehouse
- **dbt Semantic Layer**: Metrics and AI integration

## 📦 Packages Used (Fusion-Compatible)

- `dbt_utils >= 1.3.0` - Cross-database macros
- `dbt_expectations >= 0.10.0` - Advanced data quality tests
- `codegen >= 0.12.1` - Code generation utilities
- `audit_helper >= 0.9.0` - Model comparison tools

## 🧪 Data Quality

The project includes comprehensive testing:

- **Schema tests**: Uniqueness, not_null, relationships, accepted_values
- **dbt_expectations tests**: Advanced validations (e.g., row count checks)
- **Custom tests**: Business logic assertions
  - Positive revenue validation
  - Valid date ranges
  - Safety incident resolution tracking

Run tests:
```bash
dbt test
```

## 📊 Sample Data

Seeds contain realistic demo data:
- **200 rigs** across major US basins
- **500 wells** with production metrics
- **1,000 service orders** spanning 2 years
- **150 equipment** assets
- **300 safety incidents** with severity tracking
- **50 customers** including major operators
- **15 basins** covering key US oil & gas regions

## 🎓 Learning Resources

- [dbt Fusion Documentation](https://docs.getdbt.com/docs/dbt-cloud/fusion)
- [dbt Semantic Layer Guide](https://docs.getdbt.com/docs/build/semantic-layer)
- [dbt Best Practices](https://docs.getdbt.com/guides/best-practices)

## 🤝 Contributing

This is a demo project designed to showcase dbt Cloud and Fusion capabilities. Feel free to:
- Extend the models with additional metrics
- Add more sophisticated tests
- Enhance the semantic layer with custom measures
- Integrate with BI tools (Tableau, PowerBI, Looker)

## 📝 Notes

- All models use Snowflake-compatible SQL
- No Python models (Fusion doesn't support them yet)
- Configurations use `meta` dictionaries (Fusion best practice)
- Ready for production deployment on "Latest" release track

## 🔒 Security

- Never commit `profiles.yml` with credentials
- Use dbt Cloud environment variables for sensitive configs
- Leverage Snowflake role-based access control (RBAC)

## 📞 Support

For questions about:
- **dbt Cloud**: [Support Portal](https://docs.getdbt.com/docs/dbt-support)
- **dbt Fusion**: [Fusion Documentation](https://docs.getdbt.com/docs/dbt-cloud/fusion)
- **This Demo**: File an issue in the repository

---

**Built with ❤️ for the dbt Community**

*Showcasing the power of dbt Fusion and the dbt Semantic Layer*
