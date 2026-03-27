# (C) 2026 GoodData Corporation
import logging

from gooddata_platform2cloud.metrics.contants import GDC_TIME_DATE, PLATFORM_NULL_DATE
from gooddata_platform2cloud.metrics.data_classes import MetricContext
from gooddata_platform2cloud.metrics.maql.helpers import (
    get_content_granularity,
    get_datetime_diff_expression,
)

logger = logging.getLogger("migration")

ALL_OTHER = "ALL OTHER"
DATETIME_DIFF_TYPES = [
    "attributeDisplayForm object",
    "attribute object",
    "time macro",
    "string",
]


class MaqlBuilder:
    """
    This class is responsible for converting the expression tree into MAQL.
    """

    def __init__(self, ctx: MetricContext, expression_tree, is_cloud=False):
        self.ctx = ctx
        self.expression_tree = expression_tree
        self.maql = ""
        self.is_cloud = is_cloud
        self.errors = []
        self._process_tree()

    def _process_tree(self):
        """
        Process the expression tree.
        """
        try:
            self.maql = self._construct_query(self.expression_tree, None, False)
        except Exception as e:
            logger.error("ERROR [construct_query]: %s", e)
            raise e

    def _construct_query(self, json_obj, parent=None, isNested=True):
        type = json_obj["type"]
        value = json_obj.get("value", "")

        if type == "metric":
            content = ""
            for content_item in json_obj["content"]:
                content += self._construct_query(content_item, type)

            if isNested:
                if parent == "top":  # in case of TOP context
                    return f" IN (SELECT {content})"
                else:
                    return f"(SELECT {content})"
            else:
                return f"SELECT {content}"

        elif type == "expression":
            return self._construct_query(json_obj["content"][0], type)

        elif type == "top":
            if len(json_obj["content"]) == 1:
                return f" TOP({self._construct_query(json_obj['content'][0], type)})"
            else:
                return f" TOP({self._construct_query(json_obj['content'][0], type)}) {self._construct_query(json_obj['content'][1], type)}"

        elif type in ["function"]:  # can be SUM, AVG, etc.
            content = []
            for item in json_obj["content"]:
                content.append(self._construct_query(item, type, False))
            return f"{value}({', '.join(content)})"

        elif type == "rank function":
            content = []
            for item in json_obj["content"]:
                content.append(self._construct_query(item, type, False))
            return f"{' '.join(content)}"

        elif type in ["*", "/", ">", "<", "=", "+", "-", "<>", "<=", ">="]:
            content = []
            for item in json_obj["content"]:
                content.append(
                    {"value": self._construct_query(item, type), "type": item["type"]}
                )

            if type == "-" and len(content) == 2:
                granularity = get_content_granularity(json_obj["content"])
                if (
                    granularity
                    and content[0]["type"] in DATETIME_DIFF_TYPES
                    and content[1]["type"] in DATETIME_DIFF_TYPES
                ):
                    return get_datetime_diff_expression(content, granularity)

            # fix handling of None value
            if content[1]["value"] is None and len(content) == 2:
                expression = f"{content[0]['value']} {type} NULL"
                return expression

            expression_values = [item["value"] for item in content]
            expression = f" {type} ".join(expression_values)

            # need to add parentheses if it is a nested expression in between
            result = f"({expression})" if parent == "between" else expression
            return result

        elif type == "where":
            return " WHERE " + self._construct_query(json_obj["content"][0], type)

        elif type in [
            "fact object",
            "attribute object",
            "metric object",
            "attributeDisplayForm object",
            "prompt object",
        ]:
            if self.is_cloud:
                return value

            return f"[{value}]"

        elif type in ["attributeElement object"]:
            if self.is_cloud:
                if (
                    "type" in json_obj["extra"]
                    and json_obj["extra"]["type"] == GDC_TIME_DATE
                    and value == PLATFORM_NULL_DATE
                ):
                    return None
                else:
                    return f'"{value}"'

            return f"[{value}]"

        elif type == "by":
            content = []
            for content_item in json_obj["content"]:
                content.append(self._construct_query(content_item, type))

            keyword = "BY"

            if parent in ["rank function", "running function"]:
                keyword = "WITHIN"  # it is changing in for rank function

            if len(content) > 1 and content[-1] == ALL_OTHER:
                return f" {keyword} {', '.join(content[:-1])}, {ALL_OTHER}"
            else:
                return f" {keyword} {', '.join(content)}"

        elif type in ["not in", "in"]:
            keyword = type
            return f" {self._construct_query(json_obj['content'][0], type)} {keyword} {self._construct_query(json_obj['content'][1], type)}"

        elif type == "list":
            content = []
            for item in json_obj["content"]:
                content.append(self._construct_query(item, type))
            return f"({', '.join(content)})"

        elif type in ["and", "or"]:
            keyword = type.upper()
            content = []
            for item in json_obj["content"]:
                content.append(self._construct_query(item, type))
            return f" {keyword} ".join(content)

        elif type == "all other":
            content = []
            for item in json_obj["content"]:
                content.append(self._construct_query(item, type))
            subquery = " EXCEPT " + ", ".join(content) if len(content) > 0 else ""
            return ALL_OTHER + subquery

        elif type == "()":
            return f"({self._construct_query(json_obj['content'][0], type)})"

        elif type == "between":
            between_content = []
            for index in [1, 2]:
                between_content.append(
                    self._construct_query(json_obj["content"][index], type)
                )

            return f"{self._construct_query(json_obj['content'][0], type)} BETWEEN {' AND '.join(between_content)} "

        elif type == "with pf":
            if "content" in json_obj:
                return f" WITH PARENT FILTER EXCEPT {self._construct_query(json_obj['content'][0], type)}"
            else:
                return " WITH PARENT FILTER"

        elif type == "without pf":
            return " WITHOUT PARENT FILTER"

        elif type == "stop":  # related to pf
            content = []
            for item in json_obj["content"]:
                content.append(self._construct_query(item, type))
            return ", ".join(content)

        elif type in ["ilike", "like", "not like", "not ilike"]:
            keyword = type.upper()
            return f"{self._construct_query(json_obj['content'][0], type)} {keyword} {self._construct_query(json_obj['content'][1], type)}"

        elif type == "for":
            return f" FOR {self._construct_query(json_obj['content'][0], type)}"

        elif type == "of":
            content = []
            for item in json_obj["content"]:
                content.append(self._construct_query(item, type))

            return f" OF ({', '.join(content)})"

        elif type == "case":
            content = []
            for item in json_obj["content"]:
                content.append(self._construct_query(item, type))

            return "CASE" + " ".join(content) + " END"

        elif type == "when":
            whencond = self._construct_query(json_obj["content"][0], type)
            thencond = self._construct_query(json_obj["content"][1], type)

            return " WHEN " + whencond + " THEN " + thencond

        elif type == "else":
            content = []
            for item in json_obj["content"]:
                content.append(self._construct_query(item, type))

            return "ELSE " + " ".join(content)

        elif type in ["number", "count", "direction", "time macro"]:
            return value

        elif type == "all":
            return "ALL " + self._construct_query(json_obj["content"][0], type)

        elif type == "neg":
            return "- " + self._construct_query(json_obj["content"][0], type)

        elif type == "string":
            return f'"{value}"'

        elif type == "not":
            return f" NOT {self._construct_query(json_obj['content'][0], type)}"

        elif type == "using":
            return f" USING {self._construct_query(json_obj['content'][0], type)}"

        elif type == "current":
            return "CURRENT ROW"

        elif type in ["preceding", "following"]:
            keyword = type.upper()
            return f"{value} {keyword}"

        elif type == "window":
            content = []
            for item in json_obj["content"]:
                content.append(self._construct_query(item, type))
            return f"ROWS BETWEEN {' AND '.join(content)}"

        elif type == "running function":
            content = []
            for item in json_obj["content"]:
                content.append(self._construct_query(item, type))
            return f"{' '.join(content)}"

        else:
            msg = f"Unknown type: {type} - {value}"
            logger.warning("%s", msg)
            self.errors.append(msg)
            return ""

    def get(self):
        return self.maql

    def get_errors(self):
        return self.errors
