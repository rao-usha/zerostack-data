"""Local JSON Schema validation for model outputs.

Every output is validated locally whatever the vendor enforced (strict schema,
schema subset, json_object or prompt only). Uses ``jsonschema`` when it is
installed; otherwise a small validator that covers the keywords the ontology
output contract uses (type, properties, required, additionalProperties, items,
enum, const, min/maxItems, minLength, anyOf/oneOf/allOf, local ``$ref``).
"""
from __future__ import annotations

from typing import Any, List

_PY_TYPES = {
    "object": dict,
    "array": list,
    "string": str,
    "boolean": bool,
    "null": type(None),
}


def validate(instance: Any, schema: dict) -> List[str]:
    """Return a list of validation error messages (empty when valid)."""
    try:
        import jsonschema  # type: ignore
    except ImportError:
        jsonschema = None
    if jsonschema is not None:  # pragma: no cover - depends on environment
        cls = jsonschema.validators.validator_for(schema)
        return [
            f"{'/'.join(str(p) for p in e.absolute_path) or '$'}: {e.message}"
            for e in cls(schema).iter_errors(instance)
        ]
    errors: List[str] = []
    _check(instance, schema, schema, "$", errors)
    return errors


def _is_type(value: Any, t: str) -> bool:
    if t == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if t == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    py = _PY_TYPES.get(t)
    return True if py is None else isinstance(value, py)


def _resolve(root: dict, ref: str) -> Any:
    if not ref.startswith("#"):
        raise ValueError(f"only local $ref is supported: {ref}")
    node: Any = root
    for part in ref[1:].strip("/").split("/"):
        if part:
            node = node[part.replace("~1", "/").replace("~0", "~")]
    return node


def _check(value: Any, schema: Any, root: dict, path: str, errors: List[str]) -> None:
    if schema is True or schema is None:
        return
    if schema is False:
        errors.append(f"{path}: not allowed")
        return
    if "$ref" in schema:
        _check(value, _resolve(root, schema["$ref"]), root, path, errors)
    t = schema.get("type")
    if t is not None:
        types = t if isinstance(t, list) else [t]
        if not any(_is_type(value, x) for x in types):
            errors.append(f"{path}: expected {t}, got {type(value).__name__}")
            return
    if "enum" in schema and value not in schema["enum"]:
        errors.append(f"{path}: {value!r} not in enum")
    if "const" in schema and value != schema["const"]:
        errors.append(f"{path}: {value!r} != const")
    if isinstance(value, str) and len(value) < schema.get("minLength", 0):
        errors.append(f"{path}: shorter than {schema['minLength']}")
    if isinstance(value, dict):
        props = schema.get("properties", {})
        for k in schema.get("required", []):
            if k not in value:
                errors.append(f"{path}: missing required {k!r}")
        extra = schema.get("additionalProperties", True)
        for k, v in value.items():
            if k in props:
                _check(v, props[k], root, f"{path}/{k}", errors)
            elif extra is False:
                errors.append(f"{path}: unexpected property {k!r}")
            elif isinstance(extra, dict):
                _check(v, extra, root, f"{path}/{k}", errors)
    if isinstance(value, list):
        if len(value) < schema.get("minItems", 0):
            errors.append(f"{path}: fewer than {schema['minItems']} items")
        if "maxItems" in schema and len(value) > schema["maxItems"]:
            errors.append(f"{path}: more than {schema['maxItems']} items")
        items = schema.get("items")
        if isinstance(items, dict):
            for i, v in enumerate(value):
                _check(v, items, root, f"{path}/{i}", errors)
    for sub in schema.get("allOf", []):
        _check(value, sub, root, path, errors)
    for key in ("anyOf", "oneOf"):
        if key in schema:
            matches = 0
            for sub in schema[key]:
                sub_errors: List[str] = []
                _check(value, sub, root, path, sub_errors)
                matches += not sub_errors
            if (key == "anyOf" and matches == 0) or (key == "oneOf" and matches != 1):
                errors.append(f"{path}: does not match {key}")
