import logging
import traceback
from functools import wraps

class TaskErrorHandler:
    def __init__(self, logger_name='airflow.task'):
        self.logger = logging.getLogger(logger_name)
    
    def log_and_raise(self, exc):
        self.logger.error(f"Error: {str(exc)}")
        self.logger.error(traceback.format_exc())
        raise exc

def handle_task_errors(task_func):
    @wraps(task_func)
    def wrapper(*args, **kwargs):
        error_handler = TaskErrorHandler()
        try:
            result = task_func(*args, **kwargs)
            if not result:
                raise ValueError("Task did not return a successful result.")
            return result
        except Exception as exc:
            error_handler.log_and_raise(exc)
    return wrapper