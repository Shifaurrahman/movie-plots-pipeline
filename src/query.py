"""
Query Script for Movie Plots
Search for movies by keyword and return top results by plot length
"""
import pandas as pd
import json
from pathlib import Path
from typing import List, Dict


class MovieQueryEngine:
    def __init__(self, parquet_dir: str = "output/parquet"):
        self.parquet_dir = Path(parquet_dir)
        self._load_data()
    
    def _load_data(self):
        """Load all partitioned Parquet files"""
        print(f"📂 Loading data from {self.parquet_dir}...")
        
        dfs = []
        for partition_dir in self.parquet_dir.glob('decade=*'):
            parquet_file = partition_dir / 'data.parquet'
            if parquet_file.exists():
                df = pd.read_parquet(parquet_file)
                dfs.append(df)
        
        if not dfs:
            raise ValueError(f"No Parquet files found in {self.parquet_dir}")
        
        self.df = pd.concat(dfs, ignore_index=True)
        print(f"✅ Loaded {len(self.df)} total rows from {len(dfs)} partitions")
    
    def search_by_keyword(self, keyword: str, top_n: int = 5) -> Dict:
        """
        Search for movies containing keyword and return top N by plot length
        
        Args:
            keyword: Search term to find in plots
            top_n: Number of top results to return
            
        Returns:
            Dictionary with keyword and results list
        """
        print(f"\n🔍 Searching for keyword: '{keyword}'")
        
        # Case-insensitive search
        mask = self.df['plot'].str.contains(keyword, case=False, na=False)
        matching = self.df[mask].copy()
        
        print(f"  - Found {len(matching)} movies matching '{keyword}'")
        
        # Sort by plot length descending and take top N
        top_results = matching.nlargest(top_n, 'plot_length')
        
        # Format results
        results = []
        for _, row in top_results.iterrows():
            results.append({
                'title': row['title'],
                'plot_length': int(row['plot_length']),
                'decade': int(row['decade']),
                'year': int(row['release_year']) if pd.notna(row['release_year']) else None,
                'genre': row.get('genre', 'unknown')
            })
        
        output = {
            'keyword': keyword,
            'total_matches': len(matching),
            'results': results
        }
        
        return output
    
    def save_query_results(self, results: Dict, output_file: str = "query_results.json"):
        """Save query results to JSON file"""
        output_path = Path(output_file)
        with open(output_path, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"\n💾 Results saved to {output_path}")
    
    def print_results(self, results: Dict):
        """Pretty print query results"""
        print(f"\n{'='*60}")
        print(f"Keyword: '{results['keyword']}'")
        print(f"Total matches: {results['total_matches']}")
        print(f"Top {len(results['results'])} movies by plot length:")
        print(f"{'='*60}\n")
        
        for i, movie in enumerate(results['results'], 1):
            print(f"{i}. {movie['title']}")
            print(f"   Year: {movie['year']} | Decade: {movie['decade']}s")
            print(f"   Genre: {movie['genre']}")
            print(f"   Plot Length: {movie['plot_length']} words")
            print()


def main():
    """Main execution function"""
    # Initialize query engine
    engine = MovieQueryEngine(parquet_dir="output/parquet")
    
    # Example queries
    keywords = ["space", "love", "war"]
    
    for keyword in keywords:
        # Search
        results = engine.search_by_keyword(keyword, top_n=5)
        
        # Print results
        engine.print_results(results)
        
        # Save to file
        output_file = f"output/query_{keyword}.json"
        engine.save_query_results(results, output_file)


if __name__ == "__main__":
    main()