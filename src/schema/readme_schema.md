This directory implements translation and parsing of data schemas into an easily specified target in order to be used in diverse data ingestion/translation pipelines
- diverse & customizable data types parsing
- contextual validation for each parsed object, implemented as functions
- contextual default values

Now, it contains schemas for: 
- news articles
- source sites

Easy adaptation for any schema and source

schema.* contains schemas and general types parsing and translation logic

---

### Schema Usage

The schema system provides a unified API for data normalization:

```python
from schema import Parser, SOURCE_SCHEMA, SOURCE_STATS_SCHEMA

# Initialize parser with schemas
parser = Parser({
    "Source": SOURCE_SCHEMA,
    "SourceStats": SOURCE_STATS_SCHEMA
})

# Normalize a record
normalized = parser.normalize_record(raw_record, "Source")
```

The system automatically:
- Maps flat data to nested object structure
- Converts types using dedicated parsers
- Applies defaults for missing values
- Validates required fields and data integrity
