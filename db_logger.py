import logging
import time
from functools import wraps
from collections import defaultdict

# Create custom logger
db_logger = logging.getLogger('database_operations')
db_logger.setLevel(logging.INFO)

# Create handlers
file_handler = logging.FileHandler('database.log')
file_handler.setLevel(logging.INFO)

# Create formatters and add it to handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# Add handlers to the logger
db_logger.addHandler(file_handler)

# Performance metrics storage
query_metrics = defaultdict(list)

def log_db_operation(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            
            # Store metrics
            operation_name = func.__name__
            query_metrics[operation_name].append(duration)
            
            # Log detailed performance info
            avg_duration = sum(query_metrics[operation_name]) / len(query_metrics[operation_name])
            db_logger.info(
                f"DB Operation: {operation_name}\n"
                f"  Duration: {duration:.3f}s\n"
                f"  Avg Duration: {avg_duration:.3f}s\n"
                f"  Call Count: {len(query_metrics[operation_name])}"
            )
            return result
        except Exception as e:
            db_logger.error(f"DB Operation Failed: {func.__name__} - Error: {str(e)}")
            raise
    return wrapper

def print_performance_summary():
    db_logger.info("\n=== Database Performance Summary ===")
    for operation, durations in query_metrics.items():
        total_time = sum(durations)
        avg_time = total_time / len(durations)
        max_time = max(durations)
        call_count = len(durations)
        
        db_logger.info(
            f"\nOperation: {operation}\n"
            f"  Total Time: {total_time:.3f}s\n"
            f"  Average Time: {avg_time:.3f}s\n"
            f"  Max Time: {max_time:.3f}s\n"
            f"  Call Count: {call_count}"
        )
    db_logger.info("================================")
