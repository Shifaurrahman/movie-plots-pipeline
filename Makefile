.PHONY: all setup clean pipeline query test

# Default target - runs everything
all: setup pipeline query

# Install dependencies
setup:
	@echo "📦 Installing dependencies..."
	pip install -r requirements.txt

# Run the ETL pipeline
pipeline:
	@echo "🚀 Running ETL pipeline..."
	python src/pipeline.py

# Run queries
query:
	@echo "🔍 Running queries..."
	python src/query.py

# Run validation checks only
test:
	@echo "✅ Running validation checks..."
	python -c "from src.pipeline import MoviePlotsPipeline; p = MoviePlotsPipeline('wiki_movie_plots_deduped.csv'); df = p.ingest(); df = p.clean_and_transform(df); p.validate(df)"

# Clean output directory
clean:
	@echo "🧹 Cleaning output directory..."
	rm -rf output/
	@echo "✅ Clean complete"

# Help
help:
	@echo "Available targets:"
	@echo "  make all       - Run complete pipeline (setup + pipeline + query)"
	@echo "  make setup     - Install dependencies"
	@echo "  make pipeline  - Run ETL pipeline only"
	@echo "  make query     - Run query script only"
	@echo "  make test      - Run validation checks"
	@echo "  make clean     - Remove output directory"