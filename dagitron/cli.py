"""Command-line interface for Dagitron."""

import os
import sys
from typing import Optional
import click
from .generator import DagGenerator
from .parser import YamlDagParser
from .exceptions import DagitronError


@click.group()
@click.version_option(version="0.1.0")
def cli() -> None:
    """Dagitron: Apache Airflow DAG generator from YAML specifications."""
    pass


@cli.command()
@click.argument("yaml_file", type=click.Path(exists=True, dir_okay=False))
@click.option(
    "--output", "-o",
    type=click.Path(dir_okay=True, file_okay=False),
    help="Output directory for generated DAG file"
)
@click.option(
    "--dry-run", "-n",
    is_flag=True,
    help="Validate and show summary without generating DAG file"
)
@click.option(
    "--verbose", "-v",
    is_flag=True,
    help="Enable verbose output"
)
def generate(yaml_file: str, output: Optional[str], dry_run: bool, verbose: bool) -> None:
    """Generate Airflow DAG from YAML specification file."""
    try:
        # Initialize generator
        generator = DagGenerator()
        
        if verbose:
            click.echo(f"Parsing YAML file: {yaml_file}")
        
        # Parse and validate the YAML file
        parser = YamlDagParser()
        dag_spec = parser.parse_file(yaml_file)
        
        # Get DAG summary
        summary = generator.get_dag_summary(dag_spec)
        
        if verbose or dry_run:
            click.echo("\\nDAG Summary:")
            click.echo(f"  DAG ID: {summary['dag_id']}")
            click.echo(f"  Description: {summary['description']}")
            click.echo(f"  Schedule: {summary['schedule_interval']}")
            click.echo(f"  Start Date: {summary['start_date']}")
            click.echo(f"  Task Count: {summary['task_count']}")
            click.echo(f"  Operators: {', '.join(summary['operators'])}")
            click.echo(f"  Max Depth: {summary['max_depth']}")
            click.echo(f"  Parallel Groups: {summary['parallel_groups']}")
            click.echo(f"  Has Dependencies: {summary['has_dependencies']}")
        
        if dry_run:
            click.echo("\\n✓ YAML specification is valid")
            return
        
        # Generate the DAG
        if verbose:
            click.echo("\\nGenerating DAG...")
        
        dag = generator.generate_dag_from_spec(dag_spec)
        
        # Generate Python DAG file content
        dag_code = _generate_dag_file_content(dag_spec, yaml_file)
        
        # Determine output file path
        if output:
            output_dir = output
        else:
            output_dir = os.getcwd()
        
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, f"{summary['dag_id']}.py")
        
        # Write the DAG file
        with open(output_file, 'w') as f:
            f.write(dag_code)
        
        click.echo(f"\\n✓ DAG generated successfully: {output_file}")
        
        if verbose:
            click.echo(f"\\nTo use this DAG:")
            click.echo(f"1. Copy {output_file} to your Airflow DAGs folder")
            click.echo(f"2. Restart Airflow scheduler if needed")
            click.echo(f"3. The DAG '{summary['dag_id']}' should appear in Airflow UI")
    
    except DagitronError as e:
        click.echo(f"Error: {e}", err=True)
        if verbose and e.details:
            click.echo(f"Details: {e.details}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Unexpected error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument("yaml_file", type=click.Path(exists=True, dir_okay=False))
@click.option(
    "--verbose", "-v",
    is_flag=True,
    help="Enable verbose output"
)
def validate(yaml_file: str, verbose: bool) -> None:
    """Validate YAML specification file."""
    try:
        if verbose:
            click.echo(f"Validating YAML file: {yaml_file}")
        
        # Initialize parser and generator
        parser = YamlDagParser()
        generator = DagGenerator()
        
        # Parse the YAML file
        dag_spec = parser.parse_file(yaml_file)
        
        # Validate the specification
        is_valid = generator.validate_dag_spec(dag_spec)
        
        if is_valid:
            summary = generator.get_dag_summary(dag_spec)
            click.echo("✓ YAML specification is valid")
            
            if verbose:
                click.echo("\\nValidation Summary:")
                click.echo(f"  DAG ID: {summary['dag_id']}")
                click.echo(f"  Task Count: {summary['task_count']}")
                click.echo(f"  Operators: {', '.join(summary['operators'])}")
                click.echo(f"  Max Depth: {summary['max_depth']}")
                click.echo(f"  Dependencies: {'Yes' if summary['has_dependencies'] else 'No'}")
        else:
            click.echo("✗ YAML specification is invalid", err=True)
            sys.exit(1)
    
    except DagitronError as e:
        click.echo(f"Validation Error: {e}", err=True)
        if verbose and e.details:
            click.echo(f"Details: {e.details}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Unexpected error: {e}", err=True)
        sys.exit(1)


@cli.command()
def operators() -> None:
    """List supported Airflow operators."""
    from .factory import TaskFactory
    
    factory = TaskFactory()
    supported_ops = factory.get_supported_operators()
    
    click.echo("Supported Airflow Operators:")
    click.echo("=" * 30)
    
    for op in sorted(supported_ops):
        click.echo(f"  • {op}")
    
    click.echo(f"\\nTotal: {len(supported_ops)} operators supported")


def _generate_dag_file_content(dag_spec: dict, yaml_file: str) -> str:
    """Generate Python DAG file content from specification.
    
    Args:
        dag_spec: Parsed DAG specification.
        yaml_file: Path to source YAML file.
        
    Returns:
        Python code string for the DAG file.
    """
    dag_config = dag_spec["dag"]
    dag_id = dag_config["dag_id"]
    
    template = f'''"""
DAG generated by Dagitron from {os.path.basename(yaml_file)}

This file was automatically generated. Do not edit manually.
To make changes, modify the source YAML file and regenerate.
"""

from dagitron import generate_dag_from_yaml
import os

# Path to the YAML specification file
YAML_FILE = "{os.path.abspath(yaml_file)}"

# Generate the DAG from YAML
if os.path.exists(YAML_FILE):
    dag = generate_dag_from_yaml(YAML_FILE)
    
    # Make the DAG available to Airflow
    globals()["{dag_id}"] = dag
else:
    # Fallback if YAML file is not found
    from airflow import DAG
    from airflow.operators.dummy import DummyOperator
    from datetime import datetime
    
    dag = DAG(
        "{dag_id}",
        description="Generated DAG (YAML file not found)",
        schedule_interval=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
    )
    
    dummy_task = DummyOperator(
        task_id="yaml_file_not_found",
        dag=dag,
    )
    
    globals()["{dag_id}"] = dag
'''
    
    return template


def main() -> None:
    """Main entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()