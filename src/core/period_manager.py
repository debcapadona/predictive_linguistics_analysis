"""
period_manager.py - Core module for managing date ranges and periods

Provides consistent date range handling across all analyzers
"""

from datetime import datetime, timedelta
from typing import List, Tuple, Dict

class PeriodManager:
    """Manages date ranges and period definitions"""
    
    def __init__(self):
        """Initialize period manager"""
        pass
    
    @staticmethod
    def parse_date(date_str: str) -> datetime:
        """
        Parse date string to datetime object
        
        Args:
            date_str: Date string in YYYY-MM-DD format
            
        Returns:
            datetime object
        """
        return datetime.strptime(date_str, '%Y-%m-%d')
    
    @staticmethod
    def create_periods_from_config(config: Dict) -> List[Tuple[str, datetime, datetime]]:
        """
        Create period definitions from configuration
        
        Args:
            config: Configuration dictionary with date_ranges section
            
        Returns:
            List of (label, start_date, end_date) tuples
        """
        dr = config['date_ranges']
        
        periods = [
            (
                "Baseline",
                PeriodManager.parse_date(dr['baseline_start']),
                PeriodManager.parse_date(dr['baseline_end'])
            ),
            (
                "Monitoring",
                PeriodManager.parse_date(dr['monitoring_start']),
                PeriodManager.parse_date(dr['monitoring_end'])
            ),
            (
                "Event",
                PeriodManager.parse_date(dr['event_start']),
                PeriodManager.parse_date(dr['event_end'])
            )
        ]
        
        return periods
    
    @staticmethod
    def filter_by_date_range(items: List[Dict], start_date: datetime, end_date: datetime, 
                            date_field: str = 'date') -> List[Dict]:
        """
        Filter items within a date range
        
        Args:
            items: List of dictionaries with date information
            start_date: Start of range (inclusive)
            end_date: End of range (exclusive)
            date_field: Name of date field in items
            
        Returns:
            Filtered list of items
        """
        filtered = [
            item for item in items 
            if start_date <= item[date_field] < end_date
        ]
        return filtered
    
    @staticmethod
    def create_period_objects(items: List[Dict], period_definitions: List[Tuple],
                            date_field: str = 'date') -> List[Dict]:
        """
        Create period objects with filtered items
        
        Args:
            items: List of all items
            period_definitions: List of (label, start_date, end_date) tuples
            date_field: Name of date field in items
            
        Returns:
            List of period dictionaries
        """
        periods = []
        
        for label, start_date, end_date in period_definitions:
            period_items = PeriodManager.filter_by_date_range(
                items, start_date, end_date, date_field
            )
            
            periods.append({
                'label': label,
                'start_date': start_date,
                'end_date': end_date,
                'items': period_items,
                'item_count': len(period_items)
            })
        
        return periods
    
    @staticmethod
    def calculate_days_in_period(start_date: datetime, end_date: datetime) -> int:
        """Calculate number of days in a period"""
        return (end_date - start_date).days
    
    @staticmethod
    def generate_date_range(start_date: datetime, end_date: datetime) -> List[datetime]:
        """
        Generate list of dates in range
        
        Args:
            start_date: Start date
            end_date: End date
            
        Returns:
            List of datetime objects for each day
        """
        dates = []
        current = start_date
        while current < end_date:
            dates.append(current)
            current += timedelta(days=1)
        return dates