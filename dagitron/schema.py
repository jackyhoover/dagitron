"""JSON schema definitions for YAML validation."""

from typing import Dict, Any, List


# Supported Airflow operators
SUPPORTED_OPERATORS: List[str] = [
    "BashOperator",
    "PythonOperator",
    "EmailOperator", 
    "DummyOperator",
    "BranchPythonOperator",
    "ShortCircuitOperator",
    "FileSensor",
    "HttpSensor",
    "S3KeySensor",
    "SqlSensor",
    "ExternalTaskSensor",
]


# JSON Schema for DAG YAML validation
DAG_SCHEMA: Dict[str, Any] = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["dag", "tasks"],
    "properties": {
        "dag": {
            "type": "object",
            "required": ["dag_id"],
            "properties": {
                "dag_id": {
                    "type": "string",
                    "pattern": "^[a-zA-Z0-9_-]+$",
                    "description": "Unique identifier for the DAG"
                },
                "description": {
                    "type": "string",
                    "description": "Human readable description of the DAG"
                },
                "schedule_interval": {
                    "oneOf": [
                        {"type": "string"},
                        {"type": "null"}
                    ],
                    "description": "Cron expression or predefined schedule"
                },
                "start_date": {
                    "type": "string",
                    "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
                    "description": "Start date in YYYY-MM-DD format"
                },
                "end_date": {
                    "type": "string",
                    "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
                    "description": "End date in YYYY-MM-DD format"
                },
                "catchup": {
                    "type": "boolean",
                    "description": "Whether to catch up on missed runs"
                },
                "max_active_runs": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Maximum number of active DAG runs"
                },
                "max_active_tasks": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Maximum number of active tasks per DAG run"
                },
                "default_args": {
                    "type": "object",
                    "properties": {
                        "owner": {
                            "type": "string",
                            "description": "Owner of the DAG"
                        },
                        "depends_on_past": {
                            "type": "boolean",
                            "description": "Whether tasks depend on past success"
                        },
                        "start_date": {
                            "type": "string",
                            "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
                            "description": "Default start date for tasks"
                        },
                        "email_on_failure": {
                            "type": "boolean",
                            "description": "Send email on task failure"
                        },
                        "email_on_retry": {
                            "type": "boolean",
                            "description": "Send email on task retry"
                        },
                        "email": {
                            "oneOf": [
                                {"type": "string"},
                                {
                                    "type": "array",
                                    "items": {"type": "string"}
                                }
                            ],
                            "description": "Email addresses for notifications"
                        },
                        "retries": {
                            "type": "integer",
                            "minimum": 0,
                            "description": "Number of retries on task failure"
                        },
                        "retry_delay_minutes": {
                            "type": "number",
                            "minimum": 0,
                            "description": "Delay between retries in minutes"
                        }
                    },
                    "additionalProperties": True
                },
                "tags": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Tags for organizing DAGs"
                }
            },
            "additionalProperties": False
        },
        "tasks": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "required": ["name", "operator"],
                "properties": {
                    "name": {
                        "type": "string",
                        "pattern": "^[a-zA-Z0-9_-]+$",
                        "description": "Unique task identifier"
                    },
                    "operator": {
                        "type": "string",
                        "enum": SUPPORTED_OPERATORS,
                        "description": "Airflow operator type"
                    },
                    "depends_on": {
                        "oneOf": [
                            {"type": "string"},
                            {
                                "type": "array",
                                "items": {"type": "string"}
                            }
                        ],
                        "description": "Task dependencies"
                    },
                    # Common operator parameters
                    "bash_command": {
                        "type": "string",
                        "description": "Bash command for BashOperator"
                    },
                    "python_callable": {
                        "type": "string",
                        "description": "Python function name for PythonOperator"
                    },
                    "op_args": {
                        "type": "array",
                        "description": "Positional arguments for python_callable"
                    },
                    "op_kwargs": {
                        "type": "object",
                        "description": "Keyword arguments for python_callable"
                    },
                    "to": {
                        "oneOf": [
                            {"type": "string"},
                            {
                                "type": "array",
                                "items": {"type": "string"}
                            }
                        ],
                        "description": "Email recipients for EmailOperator"
                    },
                    "subject": {
                        "type": "string",
                        "description": "Email subject for EmailOperator"
                    },
                    "html_content": {
                        "type": "string",
                        "description": "Email HTML content for EmailOperator"
                    },
                    "filepath": {
                        "type": "string",
                        "description": "File path for FileSensor"
                    },
                    "endpoint": {
                        "type": "string",
                        "description": "HTTP endpoint for HttpSensor"
                    },
                    "bucket_name": {
                        "type": "string",
                        "description": "S3 bucket name for S3KeySensor"
                    },
                    "bucket_key": {
                        "type": "string",
                        "description": "S3 key for S3KeySensor"
                    },
                    "sql": {
                        "type": "string",
                        "description": "SQL query for SqlSensor"
                    },
                    "conn_id": {
                        "type": "string",
                        "description": "Connection ID for operators that need connections"
                    },
                    "external_dag_id": {
                        "type": "string",
                        "description": "External DAG ID for ExternalTaskSensor"
                    },
                    "external_task_id": {
                        "type": "string",
                        "description": "External task ID for ExternalTaskSensor"
                    },
                    # Common task parameters
                    "retries": {
                        "type": "integer",
                        "minimum": 0,
                        "description": "Number of retries for this specific task"
                    },
                    "retry_delay_minutes": {
                        "type": "number",
                        "minimum": 0,
                        "description": "Retry delay in minutes for this specific task"
                    },
                    "pool": {
                        "type": "string",
                        "description": "Pool to use for this task"
                    },
                    "priority_weight": {
                        "type": "integer",
                        "description": "Priority weight for task scheduling"
                    },
                    "queue": {
                        "type": "string",
                        "description": "Queue to use for this task"
                    },
                    "trigger_rule": {
                        "type": "string",
                        "enum": [
                            "all_success",
                            "all_failed",
                            "all_done",
                            "one_success",
                            "one_failed",
                            "none_failed",
                            "none_skipped",
                            "dummy"
                        ],
                        "description": "Trigger rule for task execution"
                    }
                },
                "additionalProperties": True
            }
        }
    },
    "additionalProperties": False
}


def get_dag_schema() -> Dict[str, Any]:
    """Get the DAG validation schema.
    
    Returns:
        The JSON schema for DAG YAML validation.
    """
    return DAG_SCHEMA


def get_supported_operators() -> List[str]:
    """Get the list of supported Airflow operators.
    
    Returns:
        List of supported operator names.
    """
    return SUPPORTED_OPERATORS.copy()