"""Tests for dependency resolution functionality."""

import pytest
from dagitron.dependencies import DependencyResolver
from dagitron.exceptions import CircularDependencyError, MissingTaskError


class TestDependencyResolver:
    """Test cases for DependencyResolver."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.resolver = DependencyResolver()
    
    def test_resolve_linear_dependencies(self):
        """Test resolving linear dependencies (A -> B -> C)."""
        tasks_config = [
            {"name": "task_a", "operator": "DummyOperator"},
            {"name": "task_b", "operator": "DummyOperator", "depends_on": ["task_a"]},
            {"name": "task_c", "operator": "DummyOperator", "depends_on": ["task_b"]},
        ]
        
        dependencies = self.resolver.resolve_dependencies(tasks_config)
        
        assert dependencies["task_a"] == []
        assert dependencies["task_b"] == ["task_a"]
        assert dependencies["task_c"] == ["task_b"]
    
    def test_resolve_parallel_dependencies(self):
        """Test resolving parallel dependencies."""
        tasks_config = [
            {"name": "start", "operator": "DummyOperator"},
            {"name": "parallel_1", "operator": "DummyOperator", "depends_on": ["start"]},
            {"name": "parallel_2", "operator": "DummyOperator", "depends_on": ["start"]},
            {"name": "parallel_3", "operator": "DummyOperator", "depends_on": ["start"]},
            {"name": "end", "operator": "DummyOperator", "depends_on": ["parallel_1", "parallel_2", "parallel_3"]},
        ]
        
        dependencies = self.resolver.resolve_dependencies(tasks_config)
        
        assert dependencies["start"] == []
        assert dependencies["parallel_1"] == ["start"]
        assert dependencies["parallel_2"] == ["start"]
        assert dependencies["parallel_3"] == ["start"]
        assert set(dependencies["end"]) == {"parallel_1", "parallel_2", "parallel_3"}
    
    def test_resolve_no_dependencies(self):
        """Test resolving tasks with no dependencies."""
        tasks_config = [
            {"name": "independent_1", "operator": "DummyOperator"},
            {"name": "independent_2", "operator": "DummyOperator"},
            {"name": "independent_3", "operator": "DummyOperator"},
        ]
        
        dependencies = self.resolver.resolve_dependencies(tasks_config)
        
        assert dependencies["independent_1"] == []
        assert dependencies["independent_2"] == []
        assert dependencies["independent_3"] == []
    
    def test_resolve_string_dependency(self):
        """Test resolving dependencies specified as strings."""
        tasks_config = [
            {"name": "task_a", "operator": "DummyOperator"},
            {"name": "task_b", "operator": "DummyOperator", "depends_on": "task_a"},  # String instead of list
        ]
        
        dependencies = self.resolver.resolve_dependencies(tasks_config)
        
        assert dependencies["task_a"] == []
        assert dependencies["task_b"] == ["task_a"]
    
    def test_detect_circular_dependency_simple(self):
        """Test detection of simple circular dependencies (A -> B -> A)."""
        tasks_config = [
            {"name": "task_a", "operator": "DummyOperator", "depends_on": ["task_b"]},
            {"name": "task_b", "operator": "DummyOperator", "depends_on": ["task_a"]},
        ]
        
        with pytest.raises(CircularDependencyError) as exc_info:
            self.resolver.resolve_dependencies(tasks_config)
        
        assert "Circular dependency detected" in str(exc_info.value)
    
    def test_detect_circular_dependency_complex(self):
        """Test detection of complex circular dependencies (A -> B -> C -> A)."""
        tasks_config = [
            {"name": "task_a", "operator": "DummyOperator", "depends_on": ["task_c"]},
            {"name": "task_b", "operator": "DummyOperator", "depends_on": ["task_a"]},
            {"name": "task_c", "operator": "DummyOperator", "depends_on": ["task_b"]},
        ]
        
        with pytest.raises(CircularDependencyError):
            self.resolver.resolve_dependencies(tasks_config)
    
    def test_detect_self_dependency(self):
        """Test detection of self-dependencies."""
        tasks_config = [
            {"name": "task_a", "operator": "DummyOperator", "depends_on": ["task_a"]},
        ]
        
        with pytest.raises(CircularDependencyError):
            self.resolver.resolve_dependencies(tasks_config)
    
    def test_missing_task_dependency(self):
        """Test detection of missing task dependencies."""
        tasks_config = [
            {"name": "task_a", "operator": "DummyOperator", "depends_on": ["nonexistent_task"]},
        ]
        
        with pytest.raises(MissingTaskError) as exc_info:
            self.resolver.resolve_dependencies(tasks_config)
        
        assert "depends on non-existent task" in str(exc_info.value)
    
    def test_get_execution_order(self):
        """Test getting topological execution order."""
        tasks_config = [
            {"name": "task_c", "operator": "DummyOperator", "depends_on": ["task_b"]},
            {"name": "task_a", "operator": "DummyOperator"},
            {"name": "task_b", "operator": "DummyOperator", "depends_on": ["task_a"]},
        ]
        
        dependencies = self.resolver.resolve_dependencies(tasks_config)
        execution_order = self.resolver.get_execution_order(dependencies)
        
        # task_a should come first, then task_b, then task_c
        assert execution_order.index("task_a") < execution_order.index("task_b")
        assert execution_order.index("task_b") < execution_order.index("task_c")
    
    def test_get_task_levels(self):
        """Test getting task execution levels."""
        tasks_config = [
            {"name": "level_0_task", "operator": "DummyOperator"},
            {"name": "level_1_task", "operator": "DummyOperator", "depends_on": ["level_0_task"]},
            {"name": "level_2_task", "operator": "DummyOperator", "depends_on": ["level_1_task"]},
            {"name": "another_level_1", "operator": "DummyOperator", "depends_on": ["level_0_task"]},
        ]
        
        dependencies = self.resolver.resolve_dependencies(tasks_config)
        levels = self.resolver.get_task_levels(dependencies)
        
        assert levels["level_0_task"] == 0
        assert levels["level_1_task"] == 1
        assert levels["level_2_task"] == 2
        assert levels["another_level_1"] == 1
    
    def test_get_parallel_groups(self):
        """Test getting parallel execution groups."""
        tasks_config = [
            {"name": "start", "operator": "DummyOperator"},
            {"name": "parallel_1", "operator": "DummyOperator", "depends_on": ["start"]},
            {"name": "parallel_2", "operator": "DummyOperator", "depends_on": ["start"]},
            {"name": "end", "operator": "DummyOperator", "depends_on": ["parallel_1", "parallel_2"]},
        ]
        
        dependencies = self.resolver.resolve_dependencies(tasks_config)
        parallel_groups = self.resolver.get_parallel_groups(dependencies)
        
        assert len(parallel_groups) == 3  # 3 levels
        assert "start" in parallel_groups[0]
        assert set(parallel_groups[1]) == {"parallel_1", "parallel_2"}
        assert "end" in parallel_groups[2]
    
    def test_validate_acyclic_valid(self):
        """Test acyclic validation with valid dependencies."""
        tasks_config = [
            {"name": "task_a", "operator": "DummyOperator"},
            {"name": "task_b", "operator": "DummyOperator", "depends_on": ["task_a"]},
        ]
        
        dependencies = self.resolver.resolve_dependencies(tasks_config)
        assert self.resolver.validate_acyclic(dependencies) is True
    
    def test_validate_acyclic_invalid(self):
        """Test acyclic validation with circular dependencies."""
        dependencies = {
            "task_a": ["task_b"],
            "task_b": ["task_a"],
        }
        
        assert self.resolver.validate_acyclic(dependencies) is False
    
    def test_complex_dependency_graph(self):
        """Test resolving a complex dependency graph."""
        tasks_config = [
            {"name": "extract_1", "operator": "DummyOperator"},
            {"name": "extract_2", "operator": "DummyOperator"},
            {"name": "validate_1", "operator": "DummyOperator", "depends_on": ["extract_1"]},
            {"name": "validate_2", "operator": "DummyOperator", "depends_on": ["extract_2"]},
            {"name": "transform", "operator": "DummyOperator", "depends_on": ["validate_1", "validate_2"]},
            {"name": "load", "operator": "DummyOperator", "depends_on": ["transform"]},
            {"name": "audit", "operator": "DummyOperator", "depends_on": ["load"]},
            {"name": "cleanup", "operator": "DummyOperator", "depends_on": ["audit"]},
        ]
        
        dependencies = self.resolver.resolve_dependencies(tasks_config)
        levels = self.resolver.get_task_levels(dependencies)
        execution_order = self.resolver.get_execution_order(dependencies)
        
        # Verify levels
        assert levels["extract_1"] == 0
        assert levels["extract_2"] == 0
        assert levels["validate_1"] == 1
        assert levels["validate_2"] == 1
        assert levels["transform"] == 2
        assert levels["load"] == 3
        assert levels["audit"] == 4
        assert levels["cleanup"] == 5
        
        # Verify execution order respects dependencies
        assert execution_order.index("extract_1") < execution_order.index("validate_1")
        assert execution_order.index("extract_2") < execution_order.index("validate_2")
        assert execution_order.index("validate_1") < execution_order.index("transform")
        assert execution_order.index("validate_2") < execution_order.index("transform")
        assert execution_order.index("transform") < execution_order.index("load")