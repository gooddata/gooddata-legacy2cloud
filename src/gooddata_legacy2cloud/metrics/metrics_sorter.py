# (C) 2026 GoodData Corporation
import logging
import re
from collections import defaultdict, deque

logger = logging.getLogger("migration")


class MetricsSorter:
    """
    Class sorts metrics based on their dependencies.
    """

    def __init__(self, metrics):
        self.metrics = metrics
        self.sorted_metrics = []
        self._sort_metrics()

    def get_sorted(self):
        """
        Returns the sorted metrics list.
        """
        return self.sorted_metrics

    def _sort_metrics(self):
        """
        Sorts metrics by peer dependency.
        """
        dependency_metric_ids = self._get_dependency_array()

        # Step 1: Build the graph and in-degree dictionary
        graph, in_degree = self._build_graph(dependency_metric_ids)

        # Step 2: Identify independent metrics (no dependencies)
        queue = self._identify_independent_metrics(in_degree)

        # Step 3: Process the graph using Kahn's algorithm
        sorted_metrics = self._process_graph(queue, graph, in_degree)

        # Step 4: Handle missing metrics
        sorted_metrics = self._handle_missing_metrics(sorted_metrics)

        # Step 5: Build the sorted list of metric objects
        self.sorted_metrics = self._build_sorted_metrics_list(sorted_metrics)

    def _build_graph(self, dependency_metric_ids):
        """
        Builds the graph and in-degree dictionary from dependency_metric_ids.
        """
        graph = defaultdict(list)  # Adjacency list
        in_degree = {
            metric: 0 for metric in dependency_metric_ids
        }  # Track incoming edges (dependencies)

        for metric, dependencies in dependency_metric_ids.items():
            for dep in dependencies:
                graph[dep].append(metric)  # dep -> metric (directed edge)
                in_degree[metric] += 1  # Increase dependency count

        return graph, in_degree

    def _identify_independent_metrics(self, in_degree):
        """
        Identifies metrics with no dependencies.
        """
        return deque([metric for metric in in_degree if in_degree[metric] == 0])

    def _process_graph(self, queue, graph, in_degree):
        """
        Processes the graph using Kahn's algorithm to sort metrics.
        """
        sorted_metrics = []
        while queue:
            metric = queue.popleft()
            sorted_metrics.append(metric)

            for dependent in graph[metric]:  # Remove the dependency from the graph
                in_degree[dependent] -= 1
                if (
                    in_degree[dependent] == 0
                ):  # If no remaining dependencies, add to queue
                    queue.append(dependent)

        return sorted_metrics

    def _handle_missing_metrics(self, sorted_metrics):
        """
        Handles missing metrics that were not successfully processed.
        """
        missing_metrics = [
            metric["data"]["id"]
            for metric in self.metrics
            if metric["data"]["id"] not in sorted_metrics
        ]

        # If there are still unresolved dependencies
        if len(sorted_metrics) != len(self.metrics):
            logger.warning("Need to extend sorted metrics with missing metrics")
            logger.warning("Missing metrics: %s", missing_metrics)
            sorted_metrics.extend(missing_metrics)

        return sorted_metrics

    def _build_sorted_metrics_list(self, sorted_metrics):
        """
        Builds the sorted list of metric objects from sorted metric IDs.
        """
        metric_dict = {metric["data"]["id"]: metric for metric in self.metrics}

        # Build the sorted list of metric objects
        sorted_metrics_full = [
            metric_dict[metric_id]
            for metric_id in sorted_metrics
            if metric_id in metric_dict
        ]

        return sorted_metrics_full

    def _get_metric_ids_from_maql(self, maql):
        """
        Extracts all strings with the template {metric/XYZ} from the given MAQL.
        """
        pattern = r"\{metric/([^\}]+)\}"
        return re.findall(pattern, maql)

    def _get_dependency_array(self):
        """
        Returns an array of dependencies for each metric.
        Only includes dependencies that exist within the current set of metrics being migrated.
        e.g. {"metric0": ["metric1","metric2"] }
        """
        # First, collect all metric IDs in the current set
        all_metric_ids = set(metric["data"]["id"] for metric in self.metrics)
        dependency_array = {}
        for metric in self.metrics:
            id = metric["data"]["id"]
            maql_content = metric["data"]["attributes"]["content"]["maql"]
            peer_metrics = self._get_metric_ids_from_maql(maql_content)

            # Filter dependencies to only include those in the current set
            internal_dependencies = [
                dep for dep in peer_metrics if dep in all_metric_ids
            ]

            dependency_array[id] = internal_dependencies

        return dependency_array
