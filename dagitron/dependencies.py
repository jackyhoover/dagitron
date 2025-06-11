"""Dependency resolution for DAG tasks."""

from typing import Dict, List, Set, Any
from .exceptions import DependencyError, CircularDependencyError, MissingTaskError


class DependencyResolver:
    """Resolves and validates task dependencies."""
    
    def __init__(self) -> None:
        """Initialize the dependency resolver."""
        pass
    
    def resolve_dependencies(self, tasks_config: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """Resolve task dependencies from task configurations.
        
        Args:
            tasks_config: List of task configuration dictionaries.
            
        Returns:
            Dictionary mapping task names to their dependencies.
            
        Raises:
            CircularDependencyError: If circular dependencies are detected.
            MissingTaskError: If dependencies reference non-existent tasks.
        """
        # Build task name set
        task_names = {task["name"] for task in tasks_config}
        
        # Build dependency map
        dependencies = {}
        for task in tasks_config:
            task_name = task["name"]
            depends_on = task.get("depends_on", [])
            
            # Normalize depends_on to list
            if isinstance(depends_on, str):
                depends_on = [depends_on]
            elif depends_on is None:
                depends_on = []
            
            # Validate dependencies exist
            for dep in depends_on:
                if dep not in task_names:
                    raise MissingTaskError(
                        f"Task '{task_name}' depends on non-existent task '{dep}'"
                    )
            
            dependencies[task_name] = depends_on
        
        # Check for circular dependencies
        self._check_circular_dependencies(dependencies)
        
        return dependencies
    
    def _check_circular_dependencies(self, dependencies: Dict[str, List[str]]) -> None:
        """Check for circular dependencies using DFS.
        
        Args:
            dependencies: Dictionary mapping task names to their dependencies.
            
        Raises:
            CircularDependencyError: If circular dependencies are detected.
        """
        # Track visited nodes during DFS
        WHITE = 0  # Unvisited
        GRAY = 1   # Visiting
        BLACK = 2  # Visited
        
        colors = {task: WHITE for task in dependencies}
        path = []
        
        def dfs(task: str) -> None:
            if colors[task] == GRAY:
                # Found a back edge - circular dependency
                cycle_start = path.index(task)
                cycle = path[cycle_start:] + [task]
                raise CircularDependencyError(
                    f"Circular dependency detected: {' -> '.join(cycle)}"
                )
            
            if colors[task] == BLACK:
                return
            
            colors[task] = GRAY
            path.append(task)
            
            for dependency in dependencies[task]:
                dfs(dependency)
            
            path.pop()
            colors[task] = BLACK
        
        # Run DFS from each unvisited node
        for task in dependencies:
            if colors[task] == WHITE:
                dfs(task)
    
    def get_execution_order(self, dependencies: Dict[str, List[str]]) -> List[str]:
        """Get topological ordering of tasks for execution.
        
        Args:
            dependencies: Dictionary mapping task names to their dependencies.
            
        Returns:
            List of task names in topological order.
        """
        # Use Kahn's algorithm for topological sorting
        in_degree = {task: 0 for task in dependencies}
        
        # Calculate in-degree for each task
        for task, deps in dependencies.items():
            for dep in deps:
                in_degree[task] += 1
        
        # Queue of tasks with no dependencies
        queue = [task for task, degree in in_degree.items() if degree == 0]
        result = []
        
        while queue:
            task = queue.pop(0)
            result.append(task)
            
            # Remove this task from dependency lists and update in-degrees
            for other_task, deps in dependencies.items():
                if task in deps:
                    in_degree[other_task] -= 1
                    if in_degree[other_task] == 0:
                        queue.append(other_task)
        
        return result
    
    def get_task_levels(self, dependencies: Dict[str, List[str]]) -> Dict[str, int]:
        """Get the execution level for each task.
        
        Tasks at level 0 have no dependencies.
        Tasks at level N depend only on tasks at levels < N.
        
        Args:
            dependencies: Dictionary mapping task names to their dependencies.
            
        Returns:
            Dictionary mapping task names to their execution levels.
        """
        levels = {}
        
        def get_level(task: str) -> int:
            if task in levels:
                return levels[task]
            
            deps = dependencies.get(task, [])
            if not deps:
                levels[task] = 0
                return 0
            
            max_dep_level = max(get_level(dep) for dep in deps)
            levels[task] = max_dep_level + 1
            return levels[task]
        
        for task in dependencies:
            get_level(task)
        
        return levels
    
    def get_parallel_groups(self, dependencies: Dict[str, List[str]]) -> List[List[str]]:
        """Group tasks that can run in parallel.
        
        Args:
            dependencies: Dictionary mapping task names to their dependencies.
            
        Returns:
            List of groups, where each group contains tasks that can run in parallel.
        """
        levels = self.get_task_levels(dependencies)
        groups = {}
        
        for task, level in levels.items():
            if level not in groups:
                groups[level] = []
            groups[level].append(task)
        
        # Return groups in order of execution levels
        return [groups[level] for level in sorted(groups.keys())]
    
    def validate_acyclic(self, dependencies: Dict[str, List[str]]) -> bool:
        """Validate that the dependency graph is acyclic.
        
        Args:
            dependencies: Dictionary mapping task names to their dependencies.
            
        Returns:
            True if the graph is acyclic, False otherwise.
        """
        try:
            self._check_circular_dependencies(dependencies)
            return True
        except CircularDependencyError:
            return False