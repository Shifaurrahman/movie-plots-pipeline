"""
ETL Pipeline for Wikipedia Movie Plots Dataset
Main pipeline orchestration script
"""
import pandas as pd
import json
from pathlib import Path
from datetime import datetime
import re


class MoviePlotsPipeline:
    def __init__(self, input_file: str, output_dir: str = "output"):
        self.input_file = input_file
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.validation_results = {}
        
    def ingest(self, row_limit: int = 500) -> pd.DataFrame:
        """Load and sample the dataset"""
        print(f"📥 Ingesting data from {self.input_file}...")
        df = pd.read_csv(self.input_file, encoding='utf-8', nrows=row_limit)
        print(f"✅ Loaded {len(df)} rows")
        return df
    
    def clean_and_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all cleaning and transformation logic"""
        print("\n🧹 Cleaning and transforming data...")
        
        # Standardize column names
        df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_')
        print(f"  - Standardized column names: {list(df.columns)}")
        
        # Remove rows with missing or empty plots
        initial_count = len(df)
        df = df.dropna(subset=['plot'])
        df = df[df['plot'].str.strip() != '']
        print(f"  - Removed {initial_count - len(df)} rows with missing/empty plots")
        
        # Compute derived columns
        df['plot_length'] = df['plot'].str.split().str.len()
        df['title_clean'] = df['title'].str.lower().str.replace(' ', '_').str.replace(r'[^\w_]', '', regex=True)
        print(f"  - Added derived columns: plot_length, title_clean")
        
        # Filter out plots with fewer than 50 words
        before_filter = len(df)
        df = df[df['plot_length'] >= 50]
        print(f"  - Filtered out {before_filter - len(df)} plots with < 50 words")
        
        # Extract decade from release year
        df['decade'] = (df['release_year'] // 10) * 10
        print(f"  - Added decade column")
        
        print(f"✅ Final dataset: {len(df)} rows")
        return df
    
    def validate(self, df: pd.DataFrame) -> dict:
        """Run data quality checks"""
        print("\n✓ Running validation checks...")
        
        checks = {
            'no_null_title': df['title'].notna().all(),
            'no_null_plot': df['plot'].notna().all(),
            'plot_length_positive': (df['plot_length'] > 0).all(),
            'unique_titles': df['title'].is_unique,
            'min_row_threshold': len(df) >= 150
        }
        
        # Convert numpy booleans to Python booleans for JSON serialization
        checks = {name: bool(result) for name, result in checks.items()}
        
        self.validation_results = {
            'timestamp': datetime.now().isoformat(),
            'total_rows': int(len(df)),
            'checks': {
                name: {
                    'passed': result,
                    'description': self._get_check_description(name)
                }
                for name, result in checks.items()
            },
            'all_passed': all(checks.values())
        }
        
        # Print results
        for check, result in checks.items():
            status = "✓" if result else "✗"
            print(f"  {status} {check}: {result}")
        
        # Save validation results
        validation_file = self.output_dir / 'validation_results.json'
        with open(validation_file, 'w') as f:
            json.dump(self.validation_results, f, indent=2)
        print(f"\n✅ Validation results saved to {validation_file}")
        
        return self.validation_results
    
    def _get_check_description(self, check_name: str) -> str:
        """Get human-readable description for validation check"""
        descriptions = {
            'no_null_title': 'All titles are non-null',
            'no_null_plot': 'All plots are non-null',
            'plot_length_positive': 'All plot lengths are greater than 0',
            'unique_titles': 'All titles are unique',
            'min_row_threshold': 'At least 150 rows remain after filtering'
        }
        return descriptions.get(check_name, check_name)
    
    def store(self, df: pd.DataFrame):
        """Store data as partitioned Parquet files"""
        print("\n💾 Storing data as partitioned Parquet...")
        
        parquet_dir = self.output_dir / 'parquet'
        parquet_dir.mkdir(exist_ok=True)
        
        # Partition by decade
        for decade in df['decade'].unique():
            decade_df = df[df['decade'] == decade]
            decade_file = parquet_dir / f'decade={decade}' / 'data.parquet'
            decade_file.parent.mkdir(exist_ok=True)
            decade_df.to_parquet(decade_file, index=False, engine='pyarrow')
            print(f"  - Saved {len(decade_df)} rows to decade={decade}")
        
        print(f"✅ Data stored in {parquet_dir}")
        
    def run(self):
        """Execute the full pipeline"""
        print("🚀 Starting ETL Pipeline\n" + "="*50)
        
        # 1. Ingest
        df = self.ingest(row_limit=500)
        
        # 2. Clean & Transform
        df = self.clean_and_transform(df)
        
        # 3. Validate
        validation = self.validate(df)
        
        if not validation['all_passed']:
            print("\n⚠️  Warning: Some validation checks failed!")
        
        # 4. Store
        self.store(df)
        
        print("\n" + "="*50)
        print("✅ Pipeline completed successfully!")
        
        return df


if __name__ == "__main__":
    # Run the pipeline
    pipeline = MoviePlotsPipeline(
        input_file="wiki_movie_plots_deduped.csv",
        output_dir="output"
    )
    
    processed_df = pipeline.run()