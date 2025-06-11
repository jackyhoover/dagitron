# Dagitron: Airflow YAML DAG Generator

[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://python.org)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Apache Airflow](https://img.shields.io/badge/airflow-2.0%2B-red.svg)](https://airflow.apache.org)

Dagitron is a Python library that generates Apache Airflow DAGs from YAML specification files. It provides a clean, declarative way to define complex workflows without writing Python code directly.

## 🚀 Features

- **Declarative YAML syntax** for defining Airflow DAGs
- **Support for common Airflow operators** (Bash, Python, Email, Sensors, etc.)
- **Comprehensive dependency management** with cycle detection
- **Schema validation** with helpful error messages
- **CLI tool** for generating and validating DAGs
- **Extensible architecture** with support for custom operators
- **Production-ready** generated DAGs following Airflow best practices

## 📦 Installation

### From PyPI (recommended)
```bash
pip install dagitron
```

### From source
```bash
git clone https://github.com/jackyhoover/dagitron.git
cd dagitron
pip install -e .
```

### Development installation
```bash
git clone https://github.com/jackyhoover/dagitron.git
cd dagitron
pip install -e ".[dev]"
```

## 🎯 Quick Start

### 1. Create a YAML specification

Create a file called `my_pipeline.yaml`:

```yaml
dag:
  dag_id: "my_data_pipeline"
  description: "A simple data processing pipeline"
  schedule_interval: "@daily"
  start_date: "2024-01-01"
  catchup: false
  default_args:
    owner: "data_team"
    retries: 1
    retry_delay_minutes: 5

tasks:
  - name: "extract_data"
    operator: "BashOperator"
    bash_command: "echo 'Extracting data...'"
    
  - name: "process_data"
    operator: "PythonOperator"
    python_callable: "process_function"
    depends_on: ["extract_data"]
    
  - name: "load_data"
    operator: "BashOperator"
    bash_command: "echo 'Loading data...'"
    depends_on: ["process_data"]
```

### 2. Generate the DAG

Using the CLI:
```bash
dagitron generate my_pipeline.yaml --output /path/to/airflow/dags
```

Using Python:
```python
from dagitron import generate_dag_from_yaml

dag = generate_dag_from_yaml("my_pipeline.yaml")
```

### 3. Use in Airflow

Copy the generated DAG file to your Airflow DAGs folder, and it will appear in the Airflow UI.

## 📖 Documentation

### Supported Operators

Dagitron currently supports these Airflow operators:

- **BashOperator** - Execute bash commands
- **PythonOperator** - Execute Python functions
- **BranchPythonOperator** - Conditional branching
- **ShortCircuitOperator** - Skip downstream tasks conditionally
- **EmailOperator** - Send emails
- **DummyOperator** - Placeholder tasks
- **FileSensor** - Wait for files
- **HttpSensor** - Wait for HTTP endpoints
- **S3KeySensor** - Wait for S3 objects
- **SqlSensor** - Wait for SQL conditions
- **ExternalTaskSensor** - Wait for external DAG tasks

### Custom Operators

In addition to built-in operators, Dagitron supports **custom Airflow operators** by specifying their fully-qualified import paths.

#### Using Custom Operators

To use a custom operator, specify the full import path in the `operator` field:

```yaml
tasks:
  - name: "my_custom_task"
    operator: "mycompany.operators.CustomDataOperator"
    # All additional fields are passed as arguments to the operator
    table_name: "customer_data"
    processing_mode: "incremental"
    custom_config:
      batch_size: 1000
      timeout: 300
```

#### Custom Operator Rules

- **Import Path**: Use fully-qualified import paths (e.g., `"package.module.ClassName"`)
- **Arguments**: All YAML fields except system fields (`name`, `operator`, `depends_on`, etc.) are passed as keyword arguments to the operator constructor
- **Installation**: Ensure the custom operator package is installed in your Airflow environment
- **Compatibility**: Custom operators must be compatible with your Airflow version

#### Example Custom Operators

```yaml
tasks:
  # Data quality validation operator
  - name: "validate_data"
    operator: "data_quality.operators.DataQualityOperator"
    dataset: "customer_transactions"
    checks: ["completeness", "uniqueness", "validity"]
    
  # ML model training operator  
  - name: "train_model"
    operator: "ml_platform.operators.ModelTrainingOperator"
    algorithm: "random_forest"
    features: ["age", "income", "credit_score"]
    target: "risk_score"
    
  # Custom notification operator
  - name: "send_slack_alert"
    operator: "notifications.operators.SlackOperator"
    channel: "#data-team" 
    message: "Pipeline completed successfully"
```

See `examples/custom_operators.yaml` for a complete example with various custom operators.

### YAML Schema

#### DAG Configuration

```yaml
dag:
  dag_id: "unique_dag_identifier"           # Required
  description: "Human readable description"  # Optional
  schedule_interval: "@daily"               # Optional, cron or preset
  start_date: "2024-01-01"                 # Optional, YYYY-MM-DD format
  end_date: "2024-12-31"                   # Optional, YYYY-MM-DD format
  catchup: false                           # Optional, default: false
  max_active_runs: 1                       # Optional, default: 1
  max_active_tasks: 16                     # Optional, default: 16
  tags: ["etl", "production"]              # Optional, list of tags
  default_args:                            # Optional
    owner: "team_name"
    depends_on_past: false
    email_on_failure: true
    email_on_retry: false
    email: ["alerts@company.com"]
    retries: 1
    retry_delay_minutes: 5
```

#### Task Configuration

```yaml
tasks:
  - name: "unique_task_name"               # Required
    operator: "BashOperator"               # Required, see supported operators
    depends_on: ["upstream_task"]          # Optional, string or list
    
    # Operator-specific parameters
    bash_command: "echo 'Hello'"           # For BashOperator
    python_callable: "my_function"         # For PythonOperator
    op_args: [arg1, arg2]                  # For PythonOperator
    op_kwargs: {key: value}                # For PythonOperator
    
    # Common task parameters
    retries: 2                             # Optional, override default
    retry_delay_minutes: 10                # Optional, override default
    pool: "my_pool"                        # Optional, resource pool
    priority_weight: 1                     # Optional, scheduling priority
    queue: "my_queue"                      # Optional, executor queue
    trigger_rule: "all_success"            # Optional, trigger condition
```

### Dependency Management

Dependencies can be specified in multiple ways:

```yaml
tasks:
  # Single dependency
  - name: "task_b"
    operator: "DummyOperator"
    depends_on: "task_a"
  
  # Multiple dependencies
  - name: "task_c"
    operator: "DummyOperator"
    depends_on: ["task_a", "task_b"]
  
  # No dependencies (runs first)
  - name: "task_a"
    operator: "DummyOperator"
```

## 🛠 CLI Usage

### Generate DAGs

```bash
# Generate from YAML file
dagitron generate workflow.yaml

# Specify output directory
dagitron generate workflow.yaml --output /path/to/dags

# Dry run (validate without generating)
dagitron generate workflow.yaml --dry-run

# Verbose output
dagitron generate workflow.yaml --verbose
```

### Validate YAML

```bash
# Validate YAML specification
dagitron validate workflow.yaml

# Verbose validation
dagitron validate workflow.yaml --verbose
```

### List Supported Operators

```bash
dagitron operators
```

## 🔧 Python API

### Basic Usage

```python
from dagitron import generate_dag_from_yaml, generate_dag_from_string

# Generate from file
dag = generate_dag_from_yaml("my_workflow.yaml")

# Generate from string
yaml_content = """
dag:
  dag_id: "simple_dag"
  schedule_interval: "@daily"
  start_date: "2024-01-01"
tasks:
  - name: "hello_world"
    operator: "BashOperator"
    bash_command: "echo 'Hello, World!'"
"""
dag = generate_dag_from_string(yaml_content)
```

### Advanced Usage

```python
from dagitron import YamlDagParser, DagGenerator, TaskFactory, DependencyResolver

# Custom components
parser = YamlDagParser()
factory = TaskFactory()
resolver = DependencyResolver()
generator = DagGenerator(parser, factory, resolver)

# Parse and generate
dag_spec = parser.parse_file("workflow.yaml")
dag = generator.generate_dag_from_spec(dag_spec)

# Get DAG summary
summary = generator.get_dag_summary(dag_spec)
print(f"DAG has {summary['task_count']} tasks")
```

### Validation

```python
from dagitron import validate_yaml_spec, get_dag_summary

# Validate specification
is_valid = validate_yaml_spec("workflow.yaml")

# Get detailed summary
summary = get_dag_summary("workflow.yaml")
print(f"Max depth: {summary['max_depth']}")
print(f"Operators used: {summary['operators']}")
```

## 📁 Examples

Check the [`examples/`](examples/) directory for complete YAML specifications:

- [`simple_pipeline.yaml`](examples/simple_pipeline.yaml) - Basic data pipeline
- [`data_processing.yaml`](examples/data_processing.yaml) - Complex ETL workflow with sensors
- [`complex_workflow.yaml`](examples/complex_workflow.yaml) - Advanced patterns with branching

## 🧪 Testing

Run the test suite:

```bash
# Run all tests
python -m pytest

# Run with coverage
python -m pytest --cov=dagitron

# Run specific test file
python -m pytest tests/test_parser.py

# Verbose output
python -m pytest -v
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Add tests for new functionality
5. Run the test suite (`pytest`)
6. Commit your changes (`git commit -m 'Add amazing feature'`)
7. Push to the branch (`git push origin feature/amazing-feature`)
8. Open a Pull Request

### Development Setup

```bash
git clone https://github.com/jackyhoover/dagitron.git
cd dagitron

# Install in development mode with dev dependencies
pip install -e ".[dev]"

# Run tests
python -m pytest

# Run linters
black dagitron tests
flake8 dagitron tests
mypy dagitron
```

## 📋 Requirements

- Python 3.8+
- Apache Airflow 2.0+
- PyYAML 6.0+
- Click 8.0+
- jsonschema 4.0+

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙋 Support

- 📖 [Documentation](https://github.com/jackyhoover/dagitron/docs)
- 🐛 [Issue Tracker](https://github.com/jackyhoover/dagitron/issues)
- 💬 [Discussions](https://github.com/jackyhoover/dagitron/discussions)

## 🗺 Roadmap

- [ ] Support for more Airflow operators
- [x] Custom operator plugins
- [ ] Environment variable templating
- [ ] DAG testing utilities
- [ ] Integration with popular CI/CD systems
- [ ] Web UI for visual DAG building
- [ ] Import/export from existing Python DAGs

## 🏆 Acknowledgments

- Apache Airflow community for the amazing workflow platform
- Contributors and users who help improve Dagitron
- Inspired by declarative infrastructure tools like Kubernetes and Terraform