"""
stats_calculator.py - Core module for statistical calculations

Provides consistent statistical analysis across all analyzers
Includes z-scores, velocity, acceleration, and significance testing
"""

import statistics
from typing import List, Dict, Any

class StatsCalculator:
    """Calculate statistical metrics for predictive analysis"""
    
    @staticmethod
    def calculate_z_score(value: float, baseline_mean: float, baseline_stdev: float) -> float:
        """
        Calculate z-score for a value
        
        Args:
            value: Current value
            baseline_mean: Baseline mean
            baseline_stdev: Baseline standard deviation
            
        Returns:
            Z-score
        """
        if baseline_stdev > 0:
            return (value - baseline_mean) / baseline_stdev
        elif value > baseline_mean:
            return 10.0  # Arbitrarily high for zero stdev
        else:
            return 0.0
    
    @staticmethod
    def calculate_baseline_stats(counts: List[float]) -> Dict[str, float]:
        """
        Calculate baseline statistics
        
        Args:
            counts: List of baseline period counts
            
        Returns:
            Dictionary with mean and stdev
        """
        if not counts or all(c == 0 for c in counts):
            return {'mean': 0, 'stdev': 0}
        
        mean = statistics.mean(counts)
        
        if len(counts) > 1:
            stdev = statistics.stdev(counts)
        else:
            # Estimate stdev for single period
            stdev = mean * 0.3 if mean > 0 else 1.0
        
        return {'mean': mean, 'stdev': stdev}
    
    @staticmethod
    def calculate_all_z_scores(counts: List[float], baseline_periods: int = 1) -> List[float]:
        """
        Calculate z-scores for all periods after baseline
        
        Args:
            counts: List of counts across all periods
            baseline_periods: Number of baseline periods
            
        Returns:
            List of z-scores for post-baseline periods
        """
        baseline = counts[:baseline_periods]
        baseline_stats = StatsCalculator.calculate_baseline_stats(baseline)
        
        z_scores = []
        for i in range(baseline_periods, len(counts)):
            z = StatsCalculator.calculate_z_score(
                counts[i],
                baseline_stats['mean'],
                baseline_stats['stdev']
            )
            z_scores.append(z)
        
        return z_scores
    
    @staticmethod
    def calculate_velocity(counts: List[float]) -> List[float]:
        """
        Calculate velocity (rate of change) between periods
        
        Args:
            counts: List of counts
            
        Returns:
            List of velocities
        """
        velocities = []
        for i in range(1, len(counts)):
            velocity = counts[i] - counts[i-1]
            velocities.append(velocity)
        return velocities
    
    @staticmethod
    def calculate_acceleration(velocities: List[float]) -> float:
        """
        Calculate acceleration (change in velocity)
        
        Args:
            velocities: List of velocities
            
        Returns:
            Acceleration value
        """
        if len(velocities) >= 2:
            return velocities[-1] - velocities[-2]
        return 0
    
    @staticmethod
    def calculate_full_stats(counts: List[float], baseline_periods: int = 1) -> Dict[str, Any]:
        """
        Calculate all statistics for a series of counts
        
        Args:
            counts: List of counts across periods
            baseline_periods: Number of baseline periods
            
        Returns:
            Dictionary with all statistics
        """
        baseline = counts[:baseline_periods]
        baseline_stats = StatsCalculator.calculate_baseline_stats(baseline)
        
        z_scores = StatsCalculator.calculate_all_z_scores(counts, baseline_periods)
        velocities = StatsCalculator.calculate_velocity(counts)
        acceleration = StatsCalculator.calculate_acceleration(velocities)
        
        return {
            'counts': counts,
            'baseline_mean': baseline_stats['mean'],
            'baseline_stdev': baseline_stats['stdev'],
            'z_scores': z_scores,
            'max_z_score': max(z_scores) if z_scores else 0,
            'velocities': velocities,
            'velocity': velocities[-1] if velocities else 0,
            'acceleration': acceleration
        }
    
    @staticmethod
    def get_signal_strength(z_score: float) -> str:
        """
        Get signal strength label from z-score
        
        Args:
            z_score: Z-score value
            
        Returns:
            Signal strength string
        """
        if z_score > 4.0:
            return "VERY STRONG"
        elif z_score > 3.0:
            return "STRONG"
        elif z_score > 2.5:
            return "HIGH"
        elif z_score > 2.0:
            return "Medium"
        else:
            return "Weak"
    
    @staticmethod
    def get_signal_emoji(z_score: float) -> str:
        """
        Get signal strength emoji from z-score
        
        Args:
            z_score: Z-score value
            
        Returns:
            Emoji string
        """
        if z_score > 4.0:
            return "🔥"
        elif z_score > 3.0:
            return "⚡"
        elif z_score > 2.5:
            return "✓"
        elif z_score > 2.0:
            return "•"
        else:
            return "-"