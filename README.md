# Wikipedia Movie Plots ETL Pipeline

A lightweight data pipeline that ingests, cleans, validates, and stores movie plot data for analytics.

## 📋 Project Overview

This pipeline processes the Wikipedia Movie Plots dataset through the following stages:
1. **Ingest** - Load 200-500 rows from the dataset
2. **Clean & Transform** - Standardize, derive features, and filter data
3. **Validate** - Run data quality checks
4. **Store** - Save as partitioned Parquet files by decade
5. **Query** - Search movies by keyword and analyze results

## 🏗️ Project Structure

```
movie-plots-pipeline/
├── src/
│   ├── pipeline.py          # Main ETL pipeline
│   └── query.py             # Query and search functionality
│   └── spark_pipeline.py    # Complete PySpark ETL + Search pipeline
├── output/
│   ├── parquet/             # Partitioned data by decade
│   │   ├── decade=1900/     # 20 movies
│   │   ├── decade=1910/     # 244 movies
│   │   └── decade=1920/     # 177 movies
│   ├── validation_results.json
│   └── query_*.json         # Query results
├── requirements.txt
├── run.bat                  # Windows automation script
├── Makefile                 # Unix/Mac automation
└── README.md
```

## 📊 Pipeline Results

### Dataset Statistics
- **Input**: 500 rows loaded from CSV
- **Output**: 441 rows after cleaning (59 short plots filtered)
- **Time Period**: Early cinema (1900s-1920s)
- **Partitions**: 3 decades of data

### Data Quality
- ✅ No null titles
- ✅ No null plots  
- ✅ All plot lengths positive
- ⚠️ Non-unique titles (expected - remakes and same-titled movies)
- ✅ Meets minimum threshold (150+ rows)

### Query Results
- **"space"**: 1 movie found
- **"love"**: 205 movies found (46% of dataset)
- **"war"**: 149 movies found (34% of dataset)

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- pip

### Installation

```bash
# Clone the repository
git clone <your-repo-url>
cd movie-plots-pipeline

# Install dependencies
pip install -r requirements.txt
```

### Required Dependencies

```txt
pandas>=1.5.0
pyarrow>=10.0.0
numpy>=1.23.0
```

## 📊 Running the Pipeline

### Windows (Recommended)

```bash
# Run the entire pipeline
run.bat all

# Or run individual stages
run.bat setup     # Install dependencies
run.bat pipeline  # Run ETL pipeline
run.bat query     # Run queries
run.bat clean     # Remove output directory
```

### Unix/Mac/Linux

```bash
# Run the entire pipeline
make all

# Or run individual stages
make setup     # Install dependencies
make pipeline  # Run ETL pipeline
make query     # Run queries
make clean     # Remove output directory
```

### Manual Execution (All Platforms)

```bash
# 1. Run the ETL pipeline
python src/pipeline.py

# 2. Run queries
python src/query.py
```

### pyspark Execution (All Platforms)

```bash
# 1. Run the ETL pipeline
python src/spark_pipeline.py

```


## 📊 Pipeline Results (Example Run on Full Dataset)

- **Input**: ~34,883 movies (full deduped dataset)
- **After cleaning**: ~30,000+ high-quality entries (plots ≥50 words)
- **Partitions**: By decade (1890–2020)
- **Search Examples**:
  - `"space"`: ~87 matches (incl. 2001: A Space Odyssey)
  - `"love"`: Thousands of matches
  - `"robot"`: 198 matches
  - `"magic"`: 412 matches

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- Java 8 or 11 (required for Spark)
- Apache Spark 3.3+ with Hadoop Winutils (for Windows)

> **Windows Users**: Download pre-built Spark from https://spark.apache.org/downloads.html  
> Extract and add `bin` to PATH. Also download `winutils.exe` for your Hadoop version.

### Installation

```bash
# Install PySpark
pip install pyspark

# Or via requirements.txt
pip install -r requirements.txt

## 🔍 Query Functionality

The query script searches for movies by keyword and returns the top 5 with the longest plots.

### Usage Example

```python
from src.query import MovieQueryEngine

# Initialize the query engine
engine = MovieQueryEngine(parquet_dir="output/parquet")

# Search for a keyword
results = engine.search_by_keyword("space", top_n=5)

# Print results
engine.print_results(results)

# Save to JSON
engine.save_query_results(results, "output/query_space.json")
```

### Query Output Format

```json
{
  "keyword": "love",
  "total_matches": 205,
  "results": [
    {
      "title": "Birth of a Nation",
      "plot_length": 933,
      "decade": 1910,
      "year": 1915,
      "genre": "epic"
    },
    {
      "title": "Dr. Jekyll and Mr. Hyde",
      "plot_length": 832,
      "decade": 1920,
      "year": 1920,
      "genre": "horror"
    }
  ]
}
```

## ✅ Data Validation

The pipeline performs the following validation checks:

1. **No null titles** - All movies must have a title ✅
2. **No null plots** - All movies must have a plot description ✅
3. **Positive plot length** - All plot_length values must be > 0 ✅
4. **Unique titles** - No duplicate movie titles ⚠️ (Expected failure: remakes exist)
5. **Minimum row threshold** - At least 150 rows after filtering ✅

Validation results are saved to `output/validation_results.json`

**Note**: The `unique_titles` check fails as expected because the dataset contains movies with the same title from different years or remakes. This is normal and doesn't affect the pipeline's functionality.

## 🗂️ Data Transformations

The pipeline applies these transformations:

- **Standardize column names**: Convert to lowercase with underscores
  - `Release Year` → `release_year`
  - `Title` → `title`
  - `Plot` → `plot`
- **Remove empty plots**: Filter out rows with missing/empty plot text (0 rows removed in this run)
- **Compute plot_length**: Count number of words in each plot (range: 50-933 words)
- **Create title_clean**: Lowercase, underscore-separated version of title
- **Filter short plots**: Remove plots with fewer than 50 words (59 rows filtered)
- **Extract decade**: Derive decade from release_year (e.g., 1915 → 1910)

## 📁 Output Files

After running the pipeline, you'll find:

```
output/
├── parquet/
│   ├── decade=1900/
│   │   └── data.parquet         # 20 movies from 1900-1909
│   ├── decade=1910/
│   │   └── data.parquet         # 244 movies from 1910-1919
│   └── decade=1920/
│       └── data.parquet         # 177 movies from 1920-1929
├── validation_results.json      # Data quality check results
├── query_space.json             # 1 movie found
├── query_love.json              # 205 movies found
└── query_war.json               # 149 movies found
```

## 🎯 Assumptions & Design Decisions

1. **Input file location**: The CSV file is assumed to be in the root directory as `wiki_movie_plots_deduped.csv`
2. **Release year column**: Assumes a `Release Year` column exists in the dataset
3. **Plot length threshold**: Movies with plots < 50 words are filtered out (removed 59/500 rows)
4. **Decade partitioning**: Data is partitioned by decade (calculated from release year)
5. **Encoding**: CSV is read with UTF-8 encoding
6. **Non-unique titles**: Duplicate titles are allowed as movies can share titles across different years
7. **Sample size**: Limited to 500 rows for development/testing purposes

## 🔧 Automation Commands

### Windows (run.bat)
```bash
run.bat all       # Run complete pipeline + queries
run.bat setup     # Install dependencies
run.bat pipeline  # Run ETL pipeline only
run.bat query     # Run query script only
run.bat clean     # Remove output directory
```

### Unix/Mac/Linux (Makefile)
```bash
make all       # Run complete pipeline + queries
make setup     # Install dependencies
make pipeline  # Run ETL pipeline only
make query     # Run query script only
make clean     # Remove output directory
make test      # Run validation checks only
```

## 📦 Bonus Features

### Implemented
- ✅ Clean, modular code structure
- ✅ Type hints for better code documentation
- ✅ Comprehensive error handling
- ✅ Production-ready validation
- ✅ Cross-platform support (Windows batch + Unix Makefile)

### Future Enhancements
- [ ] **PySpark Implementation**: For handling larger datasets (millions of rows)
- [ ] **AWS LocalStack**: Emulate S3 for cloud storage testing
- [ ] **Docker Support**: Containerize the pipeline for reproducibility
- [ ] **Automated Tests**: Unit tests with pytest for each pipeline stage
- [ ] **CI/CD Pipeline**: GitHub Actions for automated testing
- [ ] **Logging Framework**: Structured logging with different levels (INFO, DEBUG, ERROR)
- [ ] **Configuration File**: YAML/JSON config for pipeline parameters
- [ ] **Data Lineage**: Track data transformations and changes

## 🐛 Troubleshooting

### Common Issues

**Issue**: `FileNotFoundError: wiki_movie_plots_deduped.csv`

**Solution**: Ensure the CSV file is in the project root directory
```bash
# Check if file exists
dir wiki_movie_plots_deduped.csv  # Windows
ls wiki_movie_plots_deduped.csv   # Unix/Mac
```

---

**Issue**: `make: command not found` (Windows)

**Solution**: Use `run.bat` instead of `make`
```bash
run.bat all  # Instead of make all
```

---

**Issue**: Parquet files not loading in query script

**Solution**: Run `pipeline.py` first to generate the Parquet files
```bash
python src/pipeline.py
python src/query.py
```

---

**Issue**: `TypeError: Object of type bool_ is not JSON serializable`

**Solution**: Update the validation function to convert numpy booleans:
```python
checks = {name: bool(result) for name, result in checks.items()}
```

---

**Issue**: Validation check `unique_titles` fails

**Solution**: This is expected behavior. The dataset contains movies with duplicate titles (remakes, same-named films from different years). This doesn't affect pipeline functionality.

## 📝 Code Quality

This project follows:
- **PEP 8** style guidelines for Python code
- **Modular design** with single-responsibility functions
- **Type hints** for function parameters and return values
- **Docstrings** for all classes and methods
- **Error handling** for file operations and data processing
- **Clear naming** conventions for variables and functions
- **DRY principle** (Don't Repeat Yourself) throughout the codebase

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

