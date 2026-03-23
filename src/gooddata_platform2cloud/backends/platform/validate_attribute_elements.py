# (C) 2026 GoodData Corporation
class ValidateAttributeElements:
    """
    Class handles the problem with missing attribute values
    By running validation on the project, we can get the list of attributes that are missing values.
    """

    def __init__(self, objects, domain, object_resolver=None):
        self.domain = domain
        self.object_resolver = object_resolver
        self.cache = {}
        self.warnings_count = 0
        self.cached_values_count = 0
        self._process_objects_into_cache(objects)

    def _get_attribute_url(self, attribute_pars):
        """Retrieves attribute URL."""
        attr_object = attribute_pars[3]
        attr_url = (
            "object" in attr_object
            and "uri" in attr_object["object"]
            and attr_object["object"]["uri"]
            or ""
        )
        return attr_url

    @staticmethod
    def _is_default_display_form(display_form):
        content = display_form.get("content", {})
        default = content.get("default")
        return default in [1, "1", True]

    def _resolve_element_urls(self, attr_url, element_id):
        """
        Build cache URL keys for a missing element.
        Includes the original attribute-style URL and, when resolvable,
        display-form elements URLs from links.elements (default form first).
        """
        urls = [f"{attr_url}/elements?id={element_id}"]
        if not self.object_resolver:
            return urls

        try:
            obj = self.object_resolver(attr_url)
        except Exception:
            return urls

        if "attributeDisplayForm" in obj:
            elements_link = obj["attributeDisplayForm"].get("links", {}).get("elements")
            if elements_link:
                urls.append(f"{elements_link}?id={element_id}")
            return list(dict.fromkeys(urls))

        if "attribute" in obj:
            display_forms = obj["attribute"].get("content", {}).get("displayForms", [])
            ordered_display_forms = sorted(
                display_forms, key=lambda df: not self._is_default_display_form(df)
            )
            for display_form in ordered_display_forms:
                elements_link = display_form.get("links", {}).get("elements")
                if elements_link:
                    urls.append(f"{elements_link}?id={element_id}")

        return list(dict.fromkeys(urls))

    def _prosess_attribute_elements_into_cache(self, attribute_pars):
        """Processes attribute elements into cache."""
        atrr_url = self._get_attribute_url(attribute_pars)

        # in case of missing attribute elements, return
        if (
            "sli_el" not in attribute_pars[2]
            or "ids" not in attribute_pars[2]["sli_el"]
            or "vals" not in attribute_pars[2]["sli_el"]
        ):
            return

        # element values are on the 3rd position in the attribute pars
        ids_list = attribute_pars[2]["sli_el"]["ids"]
        vals_list = attribute_pars[2]["sli_el"]["vals"]

        for id, val in zip(ids_list, vals_list):
            urls = self._resolve_element_urls(atrr_url, id)
            for url in urls:
                # only cache non-empty values that do not already exist in the cache
                if val is not None and url not in self.cache:
                    self.cache[url] = val
                    self.cached_values_count += 1

    def _process_objects_into_cache(self, objects):
        """Initializes the attributes elements cache."""
        results = objects["projectValidateResult"]["results"]

        for result in results:
            if result.get("from") == "pdm::elem_validation":
                log_items = result["body"]["log"]
                for item in log_items:
                    if (
                        item["level"] == "WARN"
                        and "ecat" in item
                        and item["ecat"] == "ELEMENTS_MISSING"
                    ):
                        self.warnings_count += 1
                        self._prosess_attribute_elements_into_cache(item["pars"])

    def get_objects_for_cache(self):
        """Retrieves attribute elements for Platform cache usage."""
        return self.cache

    def get_statistics(self):
        """Returns validation statistics."""
        return self.warnings_count, self.cached_values_count
